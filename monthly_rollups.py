# !/bin/python -u
"""
Contains methods to perform monthly roll up for traffic sum feed.
"""
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.spark_utils import get_sql_context
from util.load_state import LoadState
from util.oxdb import OXDB
from util.parquet_utils import parquet_utils
from datetime import datetime
import yaml
import sys


class MonthlyRollup(object):

    def __init__(self):
        app_name = sys.argv[0].replace(".py", "").split("/")[-1]

        # reading configuration files.
        self.config = yaml.load(open(sys.argv[0].replace(".py", ".yaml")))
        self.env = yaml.load(open(self.config['ENV']))

        self.logger = EtlLogger.get_logger(app_name, log_level = self.config['LOG_LEVEL'])
        self.lock = JobLock(app_name)
        self.sqlContext = get_sql_context(app_name)

    def write_data(self, output_data, is_republish, instance_month_sid):
        try:
            pq_utils = parquet_utils(self.sqlContext, self.env, self.logger)

            self.logger.info("writing data for %d", instance_month_sid)

            hdfs_path = self.env['HIVE_BASE_DIR'] + self.config['FACT_TABLE'] + "/instance_month_sid=" \
                + str(instance_month_sid)
            self.logger.info("hdfs_path: %s", hdfs_path)

            partition_value = str(instance_month_sid)
            self.logger.info("partition_value: %s", partition_value)

            dest_df_table_name = 'mstr_datamart.' + self.config['FACT_TABLE']
            self.logger.info("dest_df_table_name: %s", dest_df_table_name)
            self.logger.info("get fact table as dest_df")
            dest_df = self.sqlContext.table(dest_df_table_name)
            dynamic_partition_column = self.config['DYNAMIC_PARTITION_COLUMN']

            # force output data to match schema of the destination fact table
            self.logger.info("create verify_cols_sql")
            verify_cols_sql = pq_utils.arrange_column_type(output_data, dest_df, self.config['FACT_PARTITION_KEY'],
                                                           dynamic_partition_key=dynamic_partition_column)
            self.logger.info("run verify_cols_sql: %s", verify_cols_sql)
            verified_cols_df = self.sqlContext.sql(verify_cols_sql)

            target_partitioncount_dict = {
                'America/New_York': 950,
                'America/Chicago': 70,
                'America/Los_Angeles': 700,
                'Asia/Tokyo': 55,
                'Europe/London': 95,
                'UTC': 230
            }
            source_df = pq_utils.repartition_for_dynamic_partition_write(verified_cols_df, "platform_timezone",
                                                                         target_partitioncount_dict,
                                                                         default_target_partitioncount=3)

            self.logger.info("about to call pq_utils.write_data")
            shuffle_value = self.config.get('SPARK_SHUFFLE_VALUE', None)
            if shuffle_value:
                query="set spark.sql.shuffle.partitions=" + str(shuffle_value)
                self.sqlContext.sql(query)

            pq_utils.write_data(
                dest_df_table_name,
                source_df,
                self.config['FACT_PARTITION_KEY'],
                partition_value,
                hdfs_path,
                target_file_count=None,
                validate_schema=True,
                is_republish=is_republish,
                dynamic_partition_column=self.config['DYNAMIC_PARTITION_COLUMN'],
                job_name=None
            )
        except Exception as error:
            self.logger.error("Write data failed.ERROR %s", error)
            raise Exception("Write data failed. ERROR: %s", error)

    def run(self):
        """
        Extracts the hours that needed monthly roll up and executes monthly roll up. 
        """
        self.logger.info("Setting hours that need monthly roll ups.")
        db = OXDB(self.env['FMYSQL_META_DSN'])
        self.logger.info("running sql: %s", self.config['UPDATE_RS_START'])
        db.execute(self.config['UPDATE_RS_START'], commit=True)

        self.logger.info("Retrieving hours that needed monthly roll ups.")
        self.logger.info("Running sql : %s", self.config['HOUR_LOOP_SQL'])
        loop_hour = db.get_executed_cursor(self.config['HOUR_LOOP_SQL']).fetchall()
        db.close()

        self.logger.info("Available monthly rollup hours : \n %s", [x[0] for x in loop_hour])

        for value in loop_hour:
            current_hour = value[0]
            self.logger.info("Running monthly rollups for hour : %s", current_hour)
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            platforms_sql = self.config['GET_READY_PLATFORMS'].replace('?', "'" + current_hour + "'")
            self.logger.info("Running sql: %s ", platforms_sql)
            platforms = impala_db.get_executed_cursor(platforms_sql).fetchall()
            impala_db.close()

            if len(platforms) > 0:
                platform_timezone_list = ''
                for platform_tz in platforms:
                    platform_timezone_list += "'{0}', ".format(platform_tz[0])
                    start_date = platform_tz[1]
                    end_date = platform_tz[2]
                self.logger.info("Processing timezones : %s", platform_timezone_list[:-2])
                self.logger.info("Timezone start date : %s :: end date: %s", start_date, end_date)

                aggregate_sql = self.config['ROLL_SQL']
                current_month = datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y-%m-01')
                month_sid = int(datetime.strptime(str(start_date), '%Y%m%d').strftime('%Y%m01'))
                aggregate_sql_parsed = aggregate_sql % (current_month, month_sid, start_date, end_date, platform_timezone_list[:-2])
                self.logger.info("Rollup sql: %s", aggregate_sql_parsed)
                aggregate_df = self.sqlContext.sql(aggregate_sql_parsed)

                is_republish = False

                # get latest load state value
                db = OXDB(self.env['FMYSQL_META_DSN'])
                current_load_state_value = LoadState(db, variable_name=self.config['LOAD_STATE_VAR']).select()[0]
                db.close()

                self.logger.info("current load state: %s", current_load_state_value)

                self.logger.info("Checking for republish")
                if datetime.strptime(current_load_state_value, '%Y-%m-%d %H:%M:%S')\
                        >= datetime.strptime(current_hour, '%Y-%m-%d %H:%M:%S'):
                    self.logger.info("This hour is being republished...")
                    is_republish = True

                self.logger.info("Persisting the data frame data.")
                self.write_data(aggregate_df, is_republish, start_date)
            else:
                self.logger.info("No platforms to perform the roll up for hour %s", current_hour)

            self.logger.info("Updating roll up state table to ensure monthly roll up is complete.")
            db = OXDB(self.env['FMYSQL_META_DSN'])
            db.execute(self.config['UPDATE_RS_END'], current_hour)

            self.logger.info("Updating load state variable %s to hour %s.", self.config['LOAD_STATE_VAR'])
            # update load state that this hour is completed
            LoadState(
                db, variable_name=self.config['LOAD_STATE_VAR']
            ).update_variable_datetime(current_hour, commit=True)
            db.close()


if __name__ == "__main__":
    rollup_obj = MonthlyRollup()
    try:
        start_time = datetime.now()
        if rollup_obj.lock.get_lock():
            rollup_obj.logger.info("==================================================")
            rollup_obj.logger.info("Start running monthly roll up job at %s", datetime.now())
            rollup_obj.run()
            rollup_obj.logger.info("Finished running monthly roll up job at %s", datetime.now())
        else:
            rollup_obj.logger.info("Unable to get lock, exiting...")
    except Exception as err:
        rollup_obj.logger.error("Failed running monthly roll up job. ERROR:%s", err)
        raise Exception("Monthly rollup script failed. ERROR:%s" % err)
    finally:
        rollup_obj.lock.releaseLock()
