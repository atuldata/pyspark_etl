#!/bin/python -u

# ETL Name      : daily_rollups.py
# Purpose       : To aggregate / roll up daily data from the base table supply_demand_geo_hourly_fact

import os
import sys
import datetime
import yaml
import time
import requests
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import expr
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.load_state import LoadState
from util.oxdb import OXDB
from util.hdfs_utils import hdfs_utils
from util.parquet_utils import parquet_utils

class daily_rollups:
    def __init__(self, yaml_file):

        #  reading configuration files
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))
        self.name = yaml_file.replace(".yaml", "")

        # create logger and lock
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name

        # set Spark related variables
        self.conf = SparkConf().setAppName(self.__class__.__name__)
        self.sc = SparkContext(conf=self.conf)
        self.sqlContext = HiveContext(self.sc)
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.hadoop_fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)
        self.hadoop_trash = self.sc._jvm.org.apache.hadoop.fs.Trash
        self.hdfs_utils = hdfs_utils(self.sc, self.logger)

    def runSQL(self, sql):
        # execute a single sql and commit
        try:
            self.logger.debug("running sql: %s", sql)
            db = OXDB(self.env['FMYSQL_META_DSN'])
            db.execute(sql, commit=True)
        except Exception as err:
            raise Exception("Execution of sql: %s failed. Error %s" % (sql, err))
        finally:
            db.close()

    def runSQLWithArgs(self, db, sql, *args):
        # execute a single sql, passing in arguments and commit
        try:
            self.logger.debug("running sql: %s", sql)
            db.execute(sql, args)
        except Exception as err:
            raise Exception("Execution of sql: %s failed. Error %s" % (sql, err))

    def write_data(self, output_data, is_republish):
        try:
            pq_utils = parquet_utils(self.sqlContext, self.env, self.logger)

            self.logger.info("writing data for %d", self.instance_date_sid)

            hdfs_path = self.env['HIVE_BASE_DIR'] + self.config['FACT_TABLE'] + "/instance_date_sid=" + str(self.instance_date_sid)
            self.logger.debug("hdfs_path: os", hdfs_path)

            partition_value = str(self.instance_date_sid)
            self.logger.debug("partition_value: %s", partition_value)

            dest_df_table_name = 'mstr_datamart.' + self.config['FACT_TABLE']
            self.logger.debug("dest_df_table_name: %s", dest_df_table_name)
            self.logger.debug("get fact table as dest_df")
            dest_df = self.sqlContext.table(dest_df_table_name)
            dynamic_partition_column = self.config['DYNAMIC_PARTITION_COLUMN']

            # force output data to match schema of the destination fact table
            self.logger.debug("create verify_cols_sql")
            verify_cols_sql = pq_utils.arrange_column_type(output_data, dest_df, self.config['FACT_PARTITION_KEY'], dynamic_partition_key=dynamic_partition_column)
            self.logger.debug("run verify_cols_sql: %s", verify_cols_sql)
            verified_cols_df = self.sqlContext.sql(verify_cols_sql);

            target_partitioncount_dict = {
                'America/New_York': 45,
                'America/Chicago': 5,
                'America/Los_Angeles': 40,
                'Asia/Tokyo': 5,
                'Europe/London': 5,
                'UTC': 15
            }
            source_df = pq_utils.repartition_for_dynamic_partition_write(verified_cols_df, "platform_timezone",
                            target_partitioncount_dict, default_target_partitioncount=1)
            
            self.logger.debug("about to call pq_utils.write_data")
            pq_utils.write_data(
                dest_df_table_name, 
                source_df,
                self.config['FACT_PARTITION_KEY'], 
                partition_value,
                hdfs_path,
                target_file_count=None,
                validate_schema=True,
                is_republish=is_republish,
                dynamic_partition_column = self.config['DYNAMIC_PARTITION_COLUMN'],
                job_name = None
            )
            
        except:
            exc_type, exc_val = sys.exc_info()[:2]
            self.logger.error("Error: %s, %s", exc_type, exc_val)
            raise

    def loopHourSQL(self):
        try:
            # using get_rows instead of get_executed_cursor, because DB connection was timing out otherwise,
            # due to the long time processing items inside the for loop, so get all the rows and close the connection. 
            # this is just getting a list of timestamps so it should fit in memory fine
            db = OXDB(self.env['FMYSQL_META_DSN'])
            hours = db.get_rows(self.config['HOUR_LOOP_SQL'])
            db.close()
            
            for h in hours:
                if h[0] is None:
                    self.logger.info("No hours to process!")
                    continue 
                roll.logger.info("-----------------------------")
                self.logger.info("Start processing hour: %s", h[0])
                self.logger.info("Extracting publisher platforms")

                # create dataframe with publisher platforms to be processed and register as temp table
                platforms_sql = self.config['GET_READY_PLATFORMS'].replace('?', "'" + h[0] + "'")
                platforms_df = self.sqlContext.sql(platforms_sql)
                platforms_df.registerTempTable("tmp_daily_platform_ready")

                # aggregate the daily fact in instance timezone for these platforms
                self.logger.info("Aggregating daily fact table in instance timezone")

                # get range of instance date sids for these platforms
                instance_date_ranges = self.sqlContext.sql(self.config['GET_INSTANCE_DATE_RANGES']).collect()
                start_instance_date_sid = instance_date_ranges[0][0]
                end_instance_date_sid = instance_date_ranges[0][1]
                start_instance_timestamp = instance_date_ranges[0][2]
                end_instance_timestamp = instance_date_ranges[0][3]
                self.logger.debug("start_instance_date_sid = %s", start_instance_date_sid)
                self.logger.debug("end_instance_date_sid = %s", end_instance_date_sid)
                self.logger.debug("start_instance_timestamp = %s", start_instance_timestamp)
                self.logger.debug("end_instance_timestamp = %s", end_instance_timestamp)


                # do we have a whole instance timezone date to rollup for this hour?
                if start_instance_date_sid == None:
                    # nothing to rollup
                    self.logger.info("No instance data to rollup for hour %s", h[0])
                else:
                    # possible rollup work to do - proceed with running support queries and main aggregation
                    self.instance_date_sid = start_instance_date_sid

                    # get range of utc date sids for these platforms
                    utc_date_sid_range = self.sqlContext.sql(self.config['GET_UTC_DATE_SID_RANGE']).collect()
                    start_utc_date_sid = utc_date_sid_range[0][0]
                    end_utc_date_sid = utc_date_sid_range[0][1]
                    self.logger.debug("start_utc_date_sid = %s", start_utc_date_sid)
                    self.logger.debug("end_utc_date_sid = %s", end_utc_date_sid)

                    # get start and end utc hour sids for these platforms
                    utc_hour_sid_df = self.sqlContext.sql(self.config['GET_UTC_HOUR_SID_RANGE'])
                    utc_hour_sid_range = utc_hour_sid_df.collect()
                    start_utc_hour_sid = utc_hour_sid_range[0][0]
                    end_utc_hour_sid = utc_hour_sid_range[0][1]
                    self.logger.debug("start_utc_hour_sid = %s", start_utc_hour_sid)
                    self.logger.debug("end_utc_hour_sid = %s", end_utc_hour_sid)

                    # get distinct platform timezones for those platforms and build sql clause to put into where clause
                    platform_timezones = self.sqlContext.sql(self.config['GET_PLATFORM_TIMEZONES']).collect()
                    platform_timezone_list = ''
                    for result in platform_timezones:
                        platform_timezone_list += "'{0}', ".format(result[0])
                    self.logger.info("processing timezones: %s", platform_timezone_list[:-2])
                    platform_timezone_clause = "AND sd.platform_timezone in (" + platform_timezone_list[:-2] + ")"
                    self.logger.debug("platform_timezone_clause: %s", platform_timezone_clause)
                    self.platform_timezones = platform_timezones 

                    aggregate_sql = self.config['ROLL_SQL']
                    aggregate_sql_parsed = aggregate_sql %(start_utc_date_sid, start_utc_hour_sid, end_utc_date_sid, end_utc_hour_sid, start_instance_timestamp, end_instance_timestamp, platform_timezone_clause)
                    self.logger.info("aggregate_sql_parsed\n\n%s\n\n", aggregate_sql_parsed)
                    aggregate_df = self.sqlContext.sql(aggregate_sql_parsed)

                    is_republish = False
        
                    # get latest load state value
                    db = OXDB(self.env['FMYSQL_META_DSN'])
                    current_load_state_value = LoadState(
                        db, variable_name=self.config['LOAD_STATE_VAR']
                    ).select()[0]
                    db.close()

                    self.logger.debug("current load state: %s", current_load_state_value)

                    # check if current hour being processed is <= latest load state value
                    # if it is, then we are republishing the daily rollup for this hour
                    if datetime.datetime.strptime(current_load_state_value, '%Y-%m-%d %H:%M:%S') >= datetime.datetime.strptime(h[0], '%Y-%m-%d %H:%M:%S'):
                        self.logger.info("This hour is being republished...")
                        is_republish = True
                    
                    # write the data to disk
                    self.logger.debug("about to call write_data for aggregate_df")
                    self.write_data(aggregate_df, is_republish)

                try:
                    metadb = OXDB(self.env['FMYSQL_META_DSN'])

                    # set current hour ready for monthly rollup
                    self.runSQLWithArgs(metadb, self.config['UPDATE_RS_END_CURRENT'], h[0])

                    # get all the timezones from platform_dim and create a subquery with UNION ALL statements
                    # to pass in as a parameter to mysql
                    timezone_sql = "select CONCAT('SELECT ', decode(unhex(hex(39)), 'US-ASCII'), tz, decode(unhex(hex(39)), 'US-ASCII'), ' tz UNION ALL ') \
                        from (select distinct timezone tz from mstr_datamart.platform_dim) x"
                    timezones = self.sqlContext.sql(timezone_sql).collect()
                    timezone_sql_list = ''
                    for timezone in timezones:
                        timezone_sql_list += timezone[0]
                    # remove trailing "UNION ALL"
                    if timezone_sql_list.endswith(' UNION ALL '):
                        timezone_sql_list = timezone_sql_list[:-11]
                
                    utc_hour_clause = "SELECT CONVERT_TZ(TIMESTAMP(LAST_DAY(CONVERT_TZ({0}, 'UTC', m.tz)), '23:00:00'), m.tz, 'UTC') FROM ( \
                        SELECT distinct tz from ({1}) ) m ".format(h[0], timezone_sql_list)

                    self.logger.debug("utc_hour_clause: %s", utc_hour_clause)

                    # set last hour of month for each timezone containing this UTC hour ready for monthly rollup
                    self.runSQLWithArgs(metadb, self.config['UPDATE_RS_END_EOM'], utc_hour_clause)

                    # commit both UPDATE_RS_END_CURRENT and UPDATE_RS_END_EOM together
                    metadb.commit()

                    # update load state that this hour is completed
                    LoadState(
                        metadb, variable_name=self.config['LOAD_STATE_VAR']
                    ).update_variable_datetime(h[0], commit=True)
    
                except:
                    exc_type, exc_val = sys.exc_info()[:2]
                    self.logger.error("Error: %s, %s", exc_type, exc_val)

                finally:
                    metadb.close()

        except:
            exc_type, exc_val = sys.exc_info()[:2]
            self.logger.error("Error: %s, %s", exc_type, exc_val)

if __name__ == "__main__":

    sys.path.append(os.path.realpath(sys.argv[0]))
    yaml_file = sys.argv[0].replace('.py', '.yaml')

    roll = daily_rollups(yaml_file)

    try:
        start_time = time.time()
        if not roll.lock.getLock():
            roll.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        roll.logger.info("=============================")
        roll.logger.info("Start running daily rollup script %s", datetime.datetime.now())
        roll.runSQL(roll.config['UPDATE_RS_START'])
        roll.loopHourSQL()
        roll.logger.info("Finished running daily rollup script %s", datetime.datetime.now())

        end_time = time.time()
        roll.logger.info("Total elapsed time = %s seconds", (end_time - start_time))

    except SystemExit:
        pass
    except:
        exc_type, exc_val = sys.exc_info()[:2]
        roll.logger.error("Error: %s, %s", exc_type, exc_val)
        raise
    finally:
        roll.lock.releaseLock()
