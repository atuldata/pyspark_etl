# ETL Name      : dim_update_from_salesforce.py
# Purpose       : Loads salesforce objects from Salesforce API

import os
import re
import sys
import time
import yaml
import subprocess
from requests.exceptions import ConnectionError, SSLError
from datetime import datetime, timedelta
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.load_state import LoadState
from util.oxdb import OXDB
from util.hdfs_utils import hdfs_utils
from util.parquet_utils import parquet_utils

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import py4j


# TODO: This is a very fragile hack that will be fixed when we move to virtualenv
sys.path.append('/usr/lib/python2.6/site-packages')
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed

class dim_update_from_salesforce:
    def __init__(self, yaml_file):
        #  Reading configuration file ( YAML file )
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.config = yaml.load(open(yaml_file))
        self.env    = yaml.load(open(self.config['ENV']))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name

        conf = SparkConf().setAppName("dim_update_from_salesforce")
        self.sc = SparkContext(conf=conf)
        self.hiveCtx = HiveContext(self.sc)
        self.hdfsClient = hdfs_utils(self.sc, self.logger)
        self.pqUtil = parquet_utils(self.hiveCtx, self.env, self.logger)

        self.object_list = self.env['SF_OBJECT_NAMES']

        self.current_unixtime = int(time.time())
        try:
            self.sf_client = Salesforce(username=self.env["SF_USERNAME"],
                                        password=self.env["SF_PASSWORD"],
                                        security_token=self.env["SF_SECURITY_TOKEN"],
                                        sandbox=self.env["SF_IS_SANDBOX"],
                                        proxies=self.config["PROXIES"],
                                        version=self.env["SF_API_VERSION"])
        except (SalesforceAuthenticationFailed, SSLError) as error:
            self.logger.error("Setting up the client: %s", error)
            sys.exit(1)
        # local sf object specific variables
        self.field_dict = {}
        self.col_list = []
        self.col_str = ''
        self.select_str = ''
        self.tmp_csv_file = None


    def init_client_for_object(self, sf_obj_name):
        self.field_dict = {}
        self.col_list = []
        self.col_str = ''
        self.select_str = ''
        self.create_sql = ''
        self.transform_sql = ''
        self.schema = None
        self.tmp_csv_file = self.config['TEMP_FILE_DIR'] + "_" + sf_obj_name + "_" + str(self.current_unixtime) + ".csv"
        self.hdfs_tmp_csv_file = self.config['HDFS_TEMP_FILE_DIR'] + "_" + sf_obj_name + "_" + str(self.current_unixtime) + ".csv"
        self.table_name = "mstr_datamart.SF_" + sf_obj_name
        self.tmp_table_name = "tmp_" + sf_obj_name
        self.table_location = "/user/hive/warehouse/mstr_datamart.db/SF_%s" % (sf_obj_name)

        sfobj = getattr(self.sf_client, sf_obj_name)
        obj_metadata = sfobj.describe()
        fields = obj_metadata['fields']

        for field in fields:
            self.field_dict[field['name']] = \
                (field['name'], field['type'], field['length'], field['byteLength'], field['precision'], field['scale'])

        for colname in self.field_dict.keys():
            self.col_list.append(colname)

        self.col_str = ",".join(self.col_list)
        self.select_str = "select %s from %s" % (self.col_str, sf_obj_name)

    @staticmethod
    def _check_addr_val(val):
        retval = ''
        if val is not None:
            retval = val.encode('utf-8').replace('"', '\\"')
        return retval

    def _checkval(self, val, field_type):
        retval = ''
        if val is not None:
            if field_type == 'boolean':
                if val:
                    retval = "\"1\""
                else:
                    retval = "\"0\""
            elif field_type == 'address':
                retval = "\"" + " ".join([self._check_addr_val(v) for v in val.values()]) + "\""
            elif field_type in ('double', 'percent', 'currency', 'int'):
                retval = "\"" + str(val) + "\""
            else:
                retval = "\"" + val.encode('utf-8').replace('\\', '\\\\').replace('"', '\\"') + "\""
        return retval

    def sf_query_to_file(self):
        start_time = time.time()
        self.logger.info("Running SOQL query %s", self.select_str)
        self.logger.info("TMP file name %s", self.tmp_csv_file)
        tmp_file = open(self.tmp_csv_file, "wb")

        row_cnt = 0
        attempt = 1
        query_result = None
        while attempt < self.config["MAX_ATTEMPTS"]:
            try:
                query_result = self.sf_client.query(self.select_str)
                break
            except (ConnectionError, SSLError) as error:
                self.logger.error("Attempt #%s: %s", attempt, error)
                attempt += 1
                self.logger.warning(
                    "Sleeping for %s seconds before re-attempting.",
                    self.config["WAIT_BETWEEN_ATTEMPTS"])
                time.sleep(self.config["WAIT_BETWEEN_ATTEMPTS"])
        if not query_result:
            self.logger.error("No query_results!!!")
            raise ValueError

        cont = True
        while cont:
            records = query_result['records']
            # write records into tmp file
            for record in records:
                row_cnt += 1
                # also replace tabs, returns, MS carriage returns(\r\n) with space
                csv_rec = ",".join(
                    [re.sub('\r\n|\n|\t|\r', ' ', self._checkval(record[c], self.field_dict[c][1])).replace('\0', '')
                     for c in self.col_list])
                tmp_file.write(csv_rec + "\n")
                # get next batch of records if needed
            if query_result['done'] is False:
                next_records_url = query_result['nextRecordsUrl']
                query_result = self.sf_client.query_more(next_records_url, True)
            else:
                cont = False

        tmp_file.close()
        end_time = time.time()
        self.logger.info("Wrote %s records into TMP file %s in %s secs", row_cnt, self.tmp_csv_file,
                         (end_time - start_time))


    @staticmethod
    def salesforce_type_to_vsql_type(field_type, field_length, field_precision, field_scale):
        if field_type in ['datetime', 'date']:
            return "TIMESTAMP"
        if field_type in ['boolean', 'int']:
            return field_type.upper()
        elif field_type in ['double', 'percent', 'currency']:
            return "DECIMAL(%s, %s)" % (field_precision, field_scale)
        else:
            return "STRING"

    def analyze_columns(self):
        # Iterate thru columns and generate, column schema, and  CREATE TABLE  and TRANSFORMATION sqls
        pretty_str = "\n"
        position_index = 0
        create_stmt = "CREATE EXTERNAL TABLE IF NOT EXISTS %s (%s" % (self.table_name, pretty_str)
        transform_stmt = "SELECT "
        self.schema = StructType()

        for col in self.col_list:
            field_name = self.field_dict[col][0]
            field_type = self.salesforce_type_to_vsql_type(self.field_dict[col][1],
                                                           self.field_dict[col][3],
                                                           self.field_dict[col][4],
                                                           self.field_dict[col][5])
            if position_index > 0:
                create_stmt += ",%s" % pretty_str
                transform_stmt += ",%s" % pretty_str
            create_stmt += "%s %s" % (field_name, field_type)
            transform_stmt += "CAST(%s AS  %s) %s" % (field_name, field_type, field_name)
            self.schema.add(field_name, StringType(), True)
            position_index += 1

        create_stmt += ")%s STORED AS parquet %s" % (pretty_str, pretty_str)
        transform_stmt += " FROM %s" % self.tmp_table_name

        self.create_sql = create_stmt
        self.transform_sql = transform_stmt

    def populate_salesforce_table(self):
        start_time = time.time()

        # upload csv file to HDFS
        self.hdfsClient.copyFromLocalFile(False, self.tmp_csv_file,  self.hdfs_tmp_csv_file)

        # read in uploaded csv as dataframe
        df_sf_data = (self.hiveCtx.read.format("com.databricks.spark.csv")
                                           .option("header", "false")
                                           .option("inferSchema", "false")
                                           .option("delimiter", ",")
                                           .option("escape", "\\")
                                           .schema(self.schema)
                                           .load(self.hdfs_tmp_csv_file))
        df_sf_data.registerTempTable(self.tmp_table_name)

        # do transformation and write the result as a parquet file
        df_converted_sf_data =  self.hiveCtx.sql(self.transform_sql)
        self.pqUtil.write_data(self.table_name, df_converted_sf_data, None, None, self.table_location, 1, validate_schema=False); 

        end_time = time.time()
        self.logger.info("Finished populating salesforce table %s:  %s seconds", self.table_name, (end_time - start_time))

    def recreate_salesforce_table(self):
        try:
            start_time = time.time()

            drop_sql = "DROP TABLE IF EXISTS %s PURGE" % self.table_name
            self.logger.info("Running :  %s", drop_sql)
            self.hiveCtx.sql(drop_sql).collect()

            self.logger.info("Running :  %s", self.create_sql)
            self.hiveCtx.sql(self.create_sql).collect()

            end_time = time.time()
            self.logger.info("Recreated salesforce table %s: %s seconds ", self.table_name, (end_time - start_time))
        except Exception as error:
            self.logger.error("create_salesforce_table: %s", error)
            raise


if __name__ == "__main__":
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    sf_pull = dim_update_from_salesforce(yaml_file)
    try:
        if not sf_pull.lock.getLock():
            sf_pull.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        sf_pull.logger.info("Start running dim_update_from_salesforce")
        q_start = time.time()

        for objectName in sf_pull.object_list:
            sf_pull.init_client_for_object(objectName)
            sf_pull.sf_query_to_file()
            sf_pull.analyze_columns()
            sf_pull.recreate_salesforce_table()
            sf_pull.populate_salesforce_table()
            # remove tmp file
            os.remove(sf_pull.tmp_csv_file)
            sf_pull.hdfsClient.moveToTrash(sf_pull.hdfs_tmp_csv_file)

            sf_pull.tmp_csv_file = None

        #update load_state
        sf_pull.logger.info('Update load_state %s', sf_pull.config['LOAD_STATE_VAR'])
        LoadState(
            OXDB(sf_pull.env['FMYSQL_META_DSN']),
            variable_name=sf_pull.config['LOAD_STATE_VAR']
        ).upsert(datetime.now(), commit=True)

        q_end = time.time()
        sf_pull.logger.info("Finished running dim_update_from_salesforce, total elapsed time: %s seconds", (q_end - q_start))

    except SystemExit:
        pass
    except Exception as error:
        sf_pull.logger.error("MAIN: %s", error)
        raise
    finally:
        sf_pull.lock.releaseLock()
        if sf_pull.tmp_csv_file:
            try:
                os.remove(sf_pull.tmp_csv_file)
                sf_pull.hdfsClient.moveToTrash(sf_pull.hdfs_tmp_csv_file)
            except Exception as error:
                pass

