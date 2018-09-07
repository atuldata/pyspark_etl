#!/bin/python -u

# ETL Name      : daily_adjustment.py
# Purpose       : To calculate the daily revenue adjustments for VARIABLE deal types

import argparse
import sys
import yaml
import sqlparse
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time
from util.load_state import LoadState
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.fact_rollup import fact_rollup
from util.oxdb import OXDB
from util.parquet_utils import parquet_utils
import util.odfi_utils


class daily_adjustment:
    def __init__(self,yaml_file):
        self.logger = EtlLogger.get_logger(self.__class__.__name__)  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name

        #  Reading configuration file ( YAML file )
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))
        self.db = OXDB(self.env['FMYSQL_META_DSN'])
        self.spark_conf = SparkConf().setAppName(self.__class__.__name__)
        self.spark_context = SparkContext(conf=self.spark_conf).getOrCreate()
        self.sql_context = HiveContext(self.spark_context)

    def run_sql(self,sql):
        try:
            for checks in sql:
                sql = checks['SQL']
                if 'SQL_DSN' in checks:
                    dsn = checks['SQL_DSN']
                    db = OXDB(self.env[dsn])
                    if 'SELECT' in sql:
                        rows = db.get_executed_cursor(sql).fetchall()
                    else:
                        rows=db.get_executed_cursor(sql)
                    db.commit()
                    db.close()
        except Exception as exp:
            error = "ETL terminating because of the following exception occurred during the ETL execution: %s" % exp
            self.logger.error(self.logger, error)
            raise exp

        return rows

    def runBulkSQLArgs(self,sql,argument):
        try:
            for checks in sql:
                df=self.sql_context.sql(checks['SQL'],argument)
                self.sql_context.registerDataFrameAsTable(df, checks['TABLE'])
        except Exception as exp:
            error = "ETL terminating because of the following exception occurred during the ETL execution: %s" % exp
            self.logger.error(self.logger, error)
            raise exp

    def loopHourSQL(self):
        self.run_sql(self.config['UPDATE_RS_START'])
        rows=self.run_sql(self.config['HOUR_LOOP_SQL'])

        print rows

        for h in rows :
            h=str(h[0])
            insrt_cpd_data_change = False
            insrt_revshare_data_change = False

            # Do CPD Adjustment (set CPD revenue) first followed by Revenue adjustment
            self.logger.info("Processing CPD Adjustment - extracting advertisers + line_items for: %s", h)
            start_time = time.time()
            self.logger.info(self.config['CPD_RETRIEVE_CUST_SQL'][0]['SQL'].replace('?', "'" + h + "'"))
            df=self.sql_context.sql(self.config['CPD_RETRIEVE_CUST_SQL'][0]['SQL'].replace('?',"'"+h+"'"))

            self.sql_context.registerDataFrameAsTable(df,self.config['CPD_RETRIEVE_CUST_SQL'][0]['TABLE'])
            #For Pricing.Fixed spend check if demand daily has the data till the previous day
            count = self.sql_context.sql(self.config['CHECK_PRICING_FIXED'][0]['SQL']).collect()

            if count[0][0]>0:
                result=self.sql_context.sql(self.config['CHECK_DEMAND_DAILY'][0]['SQL']).collect()
                if result[0][0] <= 1:
                    df = self.sql_context.sql(self.config['CPD_RETRIEVE_TOTAL_FIXED_SPEND'][0]['SQL'])
                    self.sql_context.registerDataFrameAsTable(df, self.config['CPD_RETRIEVE_TOTAL_FIXED_SPEND']['TABLE'])
                else:
                    self.logger.info("Demand daily fact does not have data till the previous day hence aborting the job!")
                    sys.exit(-1)

            utc_date_sid_range = self.sql_context.sql(self.config['CPD_MIN_MAX_UTC_DATE_SIDS'][0]['SQL']).collect()
            if utc_date_sid_range.start_utc_date_sid == None:
                self.logger.info("No CPD data for hour %s", h)
            else:
                insrt_cpd_data_change = True

                self.runBulkSQLArgs(self.config['CPD_ADJUSTMENT_SQL'], (utc_date_sid_range.start_utc_date_sid,
                                                                   utc_date_sid_range.end_utc_date_sid,
                                                                   utc_date_sid_range.start_utc_date_sid,
                                                                   utc_date_sid_range.end_utc_date_sid))
            end_time = time.time()

            parquet_file_obj = parquet_utils(self.sql_context, self.env, self.logger)
            load_query = parquet_file_obj.arrange_column_type(
                self.sql_context.sql(self.config['FINAL_WRITE']['SQL']), self.config['FACT_TABLE'], self.config['FACT_PARTITION_KEY']
            )
            transform_df = self.sql_context.sql(load_query)

            platform_timezone = self.sqlContext.sql(
                "select distinct timezone from mstr_datamart.platform_dim").collect()

            for timezone in platform_timezone:
                timezone = timezone[0]
                hdfs_path = self.baseDirectory + self.config['FACT_TABLE'].split('.')[1] + \
                            "/utc_date_sid=" + self.feed_date.strftime(
                    '%Y%m%d') + "/" + "utc_hour_sid=" + self.feed_date.strftime('%H') + "/platform_timezone=" + timezone

                self.partition_value = self.feed_date.strftime('%Y%m%d') + "," + self.feed_date.strftime(
                    '%H') + "," + timezone

                parquet_file_obj.write_data(self.config['FACT_TABLE'],
                                            transform_df.where("platform_timezone='" + timezone + "'"), self.partition_key,
                                       self.partition_value,
                                       hdfs_path)

            self.logger.info(
                "Hour " + str(h) + " CPD Adjustment elapsed time = " + str(end_time - start_time) + " seconds.")

            self.logger.info("Processing RevShare Daily Adjustment - extracting platforms + ad units for: %s", h)
            start_time = time.time()
            self.runBulkSQLArgs(self.config['REVSHARE_RETRIEVE_CUST_SQL'], (h, h, h, h, h))
            load_query = parquet_file_obj.arrange_column_type(
                self.sql_context.sql(self.config['FINAL_WRITE_CPD']['SQL']), self.config['FACT_TABLE'],
                self.config['FACT_PARTITION_KEY']
            )
            transform_df = self.sql_context.sql(load_query)

            platform_timezone = self.sqlContext.sql(
                "select distinct timezone from mstr_datamart.platform_dim").collect()

            for timezone in platform_timezone:
                timezone = timezone[0]
                hdfs_path = self.baseDirectory + self.config['FACT_TABLE'].split('.')[1] + \
                            "/utc_date_sid=" + self.feed_date.strftime(
                    '%Y%m%d') + "/" + "utc_hour_sid=" + self.feed_date.strftime('%H') + "/platform_timezone=" + timezone

                self.partition_value = self.feed_date.strftime('%Y%m%d') + "," + self.feed_date.strftime(
                    '%H') + "," + timezone

                parquet_file_obj.write_data(self.config['FACT_TABLE'],
                                            transform_df.where("platform_timezone='" + timezone + "'"),
                                            self.partition_key,
                                            self.partition_value,
                                            hdfs_path)

            dates=self.sql_context.sql(self.config['SELECT_DATE_SQL']['SQL']).collect()
            if len(dates) == 0 or dates[0][0] == None:
                self.logger.info("tmp_platform_ready is empty, nothing to do for hour %s", h)
            elif len(dates) != 1:
                self.logger.info(
                    "Multiple dates returned from SELECT_DATE_SQL, something is wrong in tmp_platform_ready for: %s",
                    h)
                sys.exit(-1)
            else:
                self.logger.info("date is %s", dates[0][0])
                insrt_revshare_data_change = True
                utc_date_sid_range = self.sql_context.sql(self.config['REVSHARE_MIN_MAX_UTC_DATE_SIDS']['SQL']).collect()
                self.runBulkSQLArgs(self.config['REVSHARE_ADJUSTMENT_SQL'], utc_date_sid_range)
            end_time = time.time()
            self.logger.info("Hour " + str(h) + " RevShare Adjustment  elapsed time = " + str(
                end_time - start_time) + " seconds.")
            self.run_sql(self.config['UPDATE_RS_END'], h)

            LoadState(
                self.db, variable_name=self.config['LOAD_STATE_VAR']
            ).update_variable_datetime(h, commit=True)

if __name__ == "__main__":

    yaml_file=sys.argv[0].replace(".py", ".yaml")

    s = daily_adjustment(yaml_file)
    try:
        start_time = time.time()
        if not s.lock.getLock():
            s.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        s.logger.info("Start running Daily Adjustment at  %s", start_time)

        s.loopHourSQL()
        end_time = time.time()
        s.logger.info("Finished running Daily Adjustment at  %s", end_time)


        s.logger.info("Total elapsed time = %s seconds", (end_time - start_time))

    except SystemExit:
        pass
    except:
        s.logger.error("Error: %s", sys.exc_info()[0])
        raise
    finally:
        s.lock.releaseLock()
