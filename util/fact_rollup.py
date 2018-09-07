"""This class is responsible for doing the rollup from fact table to rollup table
------------------------------------------------------------------------------------------------------------------
 if 'ROLLUP_TYPE' in self.config:
            rollup_instance = fact_rollup(self.feed_name, self.sqlContext, self.yaml_file, self.intrvl, self.feed_date,
                                          self.is_republish)
            rollup_instance.check_rollup_condition()
-----------------------------------------------------------------------------------------------------------------------
"""
import calendar
from datetime import datetime, timedelta
import yaml
from util.parquet_utils import parquet_utils


'''---------------------------------------------------Class-----------------------------------------------------------------'''


class fact_rollup():
    def __init__(self, sqlContext, yaml_file, feed_date, last_ds_hour, logger):

        self.logger = logger
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))
        self.yaml_file = yaml_file
        self.last_ds_hour = last_ds_hour
        self.sqlContext = sqlContext
        self.feed_date = feed_date
        self.baseDirectory = self.env['HIVE_BASE_DIR']

    # generate sql query for rollup operation based upon filter condition and rollup type
    def generate_rollup_sql(self, source_table, rollup_table, partition_key, rollup_date, start_cond, end_cond,
                            where_filter=None):

        source_df = self.sqlContext.table(source_table)
        source_columns = source_df.columns

        dest_df = self.sqlContext.table(rollup_table)
        dest_columns = dest_df.dtypes
        columns=dest_df.columns
        type_change = {}
        for col in dest_columns:
            type_change[col[0]] = col[1]

        keys = ""
        metrics = ""

        ignore_keys = ['utc_timestamp', 'utc_date_sid', 'utc_rollup_date', 'utc_datehour_sid','utc_month_sid']

        column_list = ""
        for column_name in columns:
            if 'tot_' not in column_name and column_name not in ignore_keys:
                keys = keys + column_name + ","
                column_list = column_list + column_name + ","
            elif 'tot_' in column_name:
                metrics = metrics + " SUM(" + column_name + ") as " + column_name + ","
                column_list = column_list + " CAST( SUM(" + column_name + ")  AS  " + type_change[
                    column_name] + " ) as " + column_name + ","
        metrics = metrics[:-1]
        groupbykey = keys[:-1]
        column_list = column_list[:-1]

        sql = "SELECT cast('" + rollup_date + "' as timestamp) as utc_rollup_date," + column_list + \
              " FROM " + source_table + " WHERE "
        if where_filter:
            sql = sql + where_filter + " AND "
        sql = sql + partition_key + ">='" + start_cond + "' and " + partition_key + "<='" + end_cond + "' group by " + groupbykey
        self.logger.info("Running rollup %s" % (sql))

        return sql


    # This function is responsible for checking the rollup condition and then run the rollup query to load the data
    def check_rollup_condition(self, is_daily_feed=False):
        last_hour = self.feed_date
        num_day = calendar.monthrange(int(last_hour.strftime('%Y')), int(last_hour.strftime('%m')))[1]

        if self.last_ds_hour >= self.feed_date.replace(hour=23):
            last_hour = self.feed_date.replace(hour=23)

        if is_daily_feed:
            if self.last_ds_hour >= last_hour.replace(day=num_day):
                last_hour = last_hour.replace(day=num_day)

        first_day_nextmonth = last_hour.replace(day=1) + timedelta(days=32)
        data_modify = parquet_utils(self.sqlContext, self.env, self.logger)

        if last_hour.replace(hour=0) + timedelta(days=1) == last_hour + timedelta(hours=1) and not is_daily_feed:
            if 'utc_datehour_sid' in self.config['FACT_PARTITION_KEY'].split(','):
                where_clause = 'utc_datehour_sid'
                start_cond = last_hour.strftime('%Y%m%d00')
                end_cond = (last_hour.replace(hour=0) + timedelta(days=1) - timedelta(hours=1)).strftime('%Y%m%d%H')
                where_filter = None
            else:
                where_clause = 'utc_hour_sid'
                start_cond = '0'
                end_cond = (last_hour.replace(hour=0) + timedelta(days=1) - timedelta(hours=1)).strftime('%H')
                where_filter = " utc_date_sid = " + last_hour.strftime('%Y%m%d')

            rollup_date = last_hour.strftime('%Y-%m-%d')
            rollup_key = 'utc_date_sid'
            rollup_value = last_hour.strftime('%Y%m%d')
            sql = self.generate_rollup_sql(self.config['FACT_TABLE'],
                                           self.config['ROLLUP_TYPE']['day']['rollup_table_name'],
                                           where_clause,
                                           rollup_date, start_cond, end_cond, where_filter)

            self.logger.info("Executing daily roll up sql : \n %s ", sql)
            rollup_df = self.sqlContext.sql(sql)

            hdfs_path = self.baseDirectory + \
                        self.config['ROLLUP_TYPE']['day']['rollup_table_name'].split('.')[1] + "/" + \
                        rollup_key + "=%s" % (rollup_value)

            data_modify.write_data(self.config['ROLLUP_TYPE']['day']['rollup_table_name'],
                                   rollup_df,
                                   rollup_key,
                                   rollup_value,
                                   hdfs_path,
                                   target_file_count=self.config.get('DAY_TARGET_FILE_COUNT', None))
            if self.last_ds_hour >= last_hour.replace(day=num_day):
                last_hour = last_hour.replace(day=num_day)

        if last_hour + timedelta(hours=1) == first_day_nextmonth.replace(day=1).replace(hour=0) \
                and not is_daily_feed and self.config['ROLLUP_TYPE'].get('month', None):

            start_cond = last_hour.strftime('%Y%m01')
            end_cond = (first_day_nextmonth.replace(day=1).replace(hour=0) - timedelta(hours=1)).strftime('%Y%m%d')
            rollup_date = last_hour.strftime('%Y-%m-01')
            rollup_key = 'utc_month_sid'
            rollup_value = last_hour.strftime('%Y%m01')

            sql = self.generate_rollup_sql(self.config['ROLLUP_TYPE']['day']['rollup_table_name'],
                                           self.config['ROLLUP_TYPE']['month']['rollup_table_name'],
                                           'utc_date_sid',
                                           rollup_date, start_cond, end_cond)

            self.logger.info("Executing monthly roll up sql : \n %s", sql)
            shuffle_value=self.config.get('SPARK_SHUFFLE_VALUE', None)
            if shuffle_value:
                query="set spark.sql.shuffle.partitions=" + str(shuffle_value)
                self.sqlContext.sql(query)
            rollup_df = self.sqlContext.sql(sql)

            hdfs_path = self.baseDirectory + "/" + \
                        self.config['ROLLUP_TYPE']['month']['rollup_table_name'].split('.')[1] + "/" + \
                        rollup_key + "=%s" % (rollup_value)
            try:
                data_modify.write_data(self.config['ROLLUP_TYPE']['month']['rollup_table_name'], rollup_df, rollup_key,
                                       rollup_value, hdfs_path,target_file_count=self.config.get('MONTH_TARGET_FILE_COUNT', None))
            except Exception, e:
                self.logger.error(e)

        elif is_daily_feed:
            first_day_nextmonth = last_hour.replace(day=1) + timedelta(days=32)

            if last_hour + timedelta(days=1) == first_day_nextmonth.replace(day=1):
                start_cond = last_hour.strftime('%Y%m01')
                end_cond = (first_day_nextmonth.replace(day=1) - timedelta(days=1)).strftime('%Y%m%d')

                rollup_date = last_hour.strftime('%Y-%m-01')
                rollup_key = 'utc_month_sid'
                rollup_value = last_hour.strftime('%Y%m01')

                sql = self.generate_rollup_sql(self.config['FACT_TABLE'],
                                               self.config['ROLLUP_TYPE']['month']['rollup_table_name'],
                                               'utc_date_sid',
                                               rollup_date, start_cond, end_cond)
                self.logger.info("Executing monthly roll up sql: \n %s", sql)
                shuffle_value = self.config.get('SPARK_SHUFFLE_VALUE', None)
                if shuffle_value:
                    query = "set spark.sql.shuffle.partitions=" + str(shuffle_value)
                    self.sqlContext.sql(query)
                rollup_df = self.sqlContext.sql(sql)

                hdfs_path = self.baseDirectory + "/" + \
                            self.config['ROLLUP_TYPE']['month']['rollup_table_name'].split('.')[1] + "/" + \
                            rollup_key + "=%s" % (rollup_value)

                data_modify.write_data(self.config['ROLLUP_TYPE']['month']['rollup_table_name'], rollup_df,
                                       rollup_key,
                                       rollup_value, hdfs_path,target_file_count=self.config.get('MONTH_TARGET_FILE_COUNT', None))
