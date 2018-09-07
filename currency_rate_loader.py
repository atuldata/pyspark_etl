
# The script pulls data from oanda site and 
#  loads it into mstr_datamart.currency_exchange_daily_fact
# Uses two classes currency_rate_client & currency_rate_loader

import os
import re
import sys
import yaml
import time
import csv
import subprocess
import requests
from optparse import OptionParser
from datetime import datetime, timedelta
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.load_state import LoadState
from util.hdfs_utils import hdfs_utils
from util.oxdb import OXDB
from util.parquet_utils import parquet_utils

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import py4j

class currency_rate_client:
    def __init__(self, local_tmp_dir, url, client_id, logger):
        self.url = url
        self.client_id = client_id
        self.api_key = client_id
        self.logger = logger
        self.local_tmp_dir = local_tmp_dir

    def get_currency_rates(self, base_currency='USD', since_date=datetime.now().strftime('%Y-%m-%d'), no_of_days=1):
        # get currency rates for the input base currency, store them in a file, and return the filename.
        dt = datetime.strptime(since_date, "%Y-%m-%d")
        filename = self.local_tmp_dir + '/currency_rate-' + dt.strftime('%Y%m%d') + '-' + base_currency + '.csv'

        with open(filename, 'w') as fp:
            c_url = self.url + base_currency + '.csv?api_key=' + self.api_key + '&date=' + since_date

            r = requests.get(c_url)
            r.raise_for_status()

            formatted_r = r.content
            formatted_r += base_currency + ',' + base_currency + ',' + str(dt.date()) + ',' + str(1) + ',' + str(1)
            fp.write(formatted_r)
            self.logger.info("Successfully downloaded currecny rate to %s ", filename)

        return filename

class currency_rate_loader:

    # Init and bootstrap environment
    def __init__(self, yaml_file):
        self.config = yaml.load(open(yaml_file))
        self.env    = yaml.load(open(self.config['ENV']))

        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name

        self.client = currency_rate_client(self.config['LOCAL_TEMP_FILE_DIR'], self.env['CURRENCY_SERVICE_BASE_URL'], 
                                           self.env['CURRENCY_SERVICE_CLIENT_ID'], self.logger)

        conf = SparkConf().setAppName("currency_rate_loader")
        self.sc = SparkContext(conf=conf)
        self.hiveCtx = HiveContext(self.sc)
        self.hdfsClient = hdfs_utils(self.sc, self.logger)
        self.pqUtil = parquet_utils(self.hiveCtx, self.env, self.logger) 

        self.current_unix_time = int(time.time()) 

    # Generate a list of string in yyyy-mm-dd since the specified since_date   
    def gen_dates(self, start_date, end_date):
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        numdays = (end - start).days + 1
        for x in range(0, numdays):
            yield (start + timedelta(days=x)).strftime('%Y-%m-%d')

    def get_rate_record_count(self, date):
        sql = "SELECT count(1) ct FROM mstr_datamart.currency_exchange_daily_fact WHERE `date` = '" + date + " 00:00:00'"
        qRes = self.hiveCtx.sql(sql)
        return qRes.first().ct

    def get_max_date_from_db(self):
        sql = "SELECT date_add(MAX(`date`), 1) nxtday  FROM mstr_datamart.currency_exchange_daily_fact"
        qRes = self.hiveCtx.sql(sql)
        nxtday = qRes.first().nxtday
        return nxtday.strftime('%Y-%m-%d')

    def get_all_currency_codes(self):
        currency_codes = []
        filename = self.client.get_currency_rates()
        fp = csv.DictReader(open(filename, 'r'), delimiter=',')
        for row in fp:
            currency_codes.append(row['currency'])

        return currency_codes

    def get_all_currency_rates_csv(self, date=datetime.now().strftime('%Y-%m-%d')):
        self.pull_date = date
        filenames = []
        currency_codes = self.get_all_currency_codes()
        filenames.append(self.client.get_currency_rates(base_currency='USD', since_date=date))
        for code in currency_codes:
            time.sleep(0.50)
            try:
                while True:
                    try:
                        self.logger.info("Appending rates for code: %s" % code)
                        filenames.append(
                            self.client.get_currency_rates(base_currency=code, since_date=date))
                        break
                    except:
                        exc_type,exc_val = sys.exc_info()[:2]
                        self.logger.error("#######Error: %s, %s", exc_type,exc_val)
                        time.sleep(1)
                        continue
            except:
                exc_type,exc_val = sys.exc_info()[:2]
                self.logger.error("Error: %s, %s", exc_type,exc_val)
                raise

        return filenames


    def load_currency_table(self, d, filenames):
        """
        This routine will do the following:
          - upload the csv files onto hadfs under HDFS_TMP_DIR
          - create dataframe from those csv files
          - create a new dataframe off the previous df storing the finalized version 
          - track current files if any
          - save new df as parquet files
          - create new partition and point it to the new parquet file
        """
        date_sid_str = datetime.strptime(d, '%Y-%m-%d').strftime('%Y%m%d')
        localfiles = os.path.join(self.config['LOCAL_TEMP_FILE_DIR'], 'currency_rate-' + date_sid_str + '-*.csv') 
        hdfsfiles = os.path.join(self.config['HDFS_TEMP_FILE_DIR'], 'currency_rate-' + date_sid_str + '-*.csv') 
        try: 
            # upload currency csv files to HDFS
            self.hdfsClient.mkdirs(self.config['HDFS_TEMP_FILE_DIR'])
            map(lambda f:self.hdfsClient.copyFromLocalFile(False,f,self.config['HDFS_TEMP_FILE_DIR']), filenames)
            self.logger.info("%d csv files uploaded to HDFS directory  %s", len(filenames),self.config['HDFS_TEMP_FILE_DIR'])
   
            # read in uploaded csv as dataframe
            schema = StructType([StructField("base", StringType(), True), 
                                 StructField("currency", StringType(), True), 
                                 StructField("last_oanda_updated", StringType(), True), 
                                 StructField("exchange_rate", StringType(), True), 
                                 StructField("ask", StringType(), True)])
            dfRates = (self.hiveCtx.read.format("com.databricks.spark.csv")
                                           .option("header", "true")
                                           .option("inferSchema", "false")
                                           .option("delimiter", ",")
                                           .option("escape", "\\")
                                           .schema(schema)
                                           .load(hdfsfiles))
            self.logger.info("Converted csv files into Dataframe with %d records ", dfRates.count())
 
            # Transform to currency_exchange_daily_fact table format, and save it as a dataframe 
            dfRates.registerTempTable("tmp_raw_currency_rate_table")
    
            datestr = "\"" + d + " 00:00:00\""
            transform_sql = "SELECT base, currency, \
                                    CAST(" + datestr + " AS timestamp) `date`, CAST(exchange_rate AS float) exchange_rate, \
                                    CAST(last_oanda_updated AS timestamp) last_oanda_updated \
                                             FROM tmp_raw_currency_rate_table"
            self.logger.info("executing transform_sql: %s", transform_sql)
            dfRateConverted = self.hiveCtx.sql(transform_sql)
            self.logger.info("Converted column types for %d records ", dfRateConverted.count())

            # materialize the dataframe as parquet file and add/replace the partition with the new parquet file.    
            hdfs_partition_directory = self.config['HDFS_TABLE_PARTITION_DIR_TEMPLATE'].format(date_sid_str)
            self.pqUtil.write_data(self.config['CURRENCY_TABLE_NAME'], dfRateConverted, "date_sid", date_sid_str, hdfs_partition_directory, 1, validate_schema=False);

        except:
            exc_type,exc_val = sys.exc_info()[:2]
            self.logger.error("Error: %s, %s", exc_type,exc_val)
            raise
        finally:  
            # clean up HDFS csv files
            self.hdfsClient.moveToTrash(self.config['HDFS_TEMP_FILE_DIR'])
            self.logger.info("Removed HDFS_TEMP_FILE_DIR: %s", self.config['HDFS_TEMP_FILE_DIR'])

            # clean up local csv files
            subprocess.call('rm -f ' + localfiles, shell=True)
            self.logger.info("Removed lcoal csv files...  %s/* ", localfiles)


if __name__ == "__main__":
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-s", "--start_date", action="store", type="string", dest="start_date",
                      help="To pull currency exchange since the given start date(must specify --end_date option) in YYYY-MM-DD format , e.g. '2013-09-13'. Default value is current system date")
    parser.add_option("-e", "--end_date", action="store", type="string", dest="end_date",
                      help="To pull currency exchange up to  the given date (must specify --end_date option) in YYYY-MM-DD format , e.g. '2013-09-13'. Default value is current system date")
    (options, args) = parser.parse_args()

    if len(args) > 0:
        parser.print_usage()
        parser.print_help()
        sys.exit(1)

    loader = currency_rate_loader(yaml_file)

    try:
        start_time = time.time()
        if not loader.lock.getLock():
            loader.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        loader.logger.info("Start running daily currency script %s", datetime.now())

        dont_check_need_to_pull = options.start_date is not None and options.end_date is not None

        if options.start_date is None:
            options.start_date = '%s' % loader.get_max_date_from_db()
            loader.logger.info('Start date unspecified.  Setting to latest date from database: %s', options.start_date)

        if options.end_date is None:
            options.end_date = '%s' % datetime.now().strftime('%Y-%m-%d')
            loader.logger.info('End date unspecified.  Setting to current date: %s',  options.end_date)

        loads_done, loads_skipped = 0,0
        for d in loader.gen_dates(options.start_date, options.end_date):
            # get rate record counts for the day, d
            loader.logger.info('Processing date %s',d)
            rate_record_count = loader.get_rate_record_count(d)
            if dont_check_need_to_pull or rate_record_count != 40401 :
                filenames = loader.get_all_currency_rates_csv(d)
                loader.load_currency_table(d, filenames)
                loads_done += 1
            else:
                loader.logger.info("Data pull was already done. Skipping.")
                loads_skipped += 1

        if (loads_done+loads_skipped) == 0:
            loader.logger.info('All the data appears to be loaded.  No loads were attempted.')
        else:
            loader.logger.info('Finished: %d loads performed, %d loads skipped', loads_done, loads_skipped)

        #update load_state
        loader.logger.info('Update load_state %s', loader.config['LOAD_STATE_VAR'])
        LoadState(
            OXDB(loader.env['FMYSQL_META_DSN']),
            variable_name=loader.config['LOAD_STATE_VAR']
        ).upsert(datetime.now(), commit=True)
            
        loader.logger.info("Finished running daily currency script  %s", datetime.now())
        end_time = time.time()
        loader.logger.info("Total elapsed time = %s seconds", (end_time - start_time))

    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        loader.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        loader.lock.releaseLock()
