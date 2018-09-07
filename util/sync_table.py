import sys
import yaml
import datetime
import optparse
import time
from parquet_utils import parquet_utils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import functions as F
from pyspark.sql import types as Types
import py4j

# this class can be used to to sync a table from an external JDBC source to a local Impala table

# typical invocation:
'''
from util.sync_table import sync_table

# create objects or set values for all below needed parameters, then create sync_table object:
sync = sync_table(env, logger, table_name, source_jdbc_url, source_jdbc_driver, source_schema, source_username, 
    source_password, target_db, target_base_directory)

# call perform_sync to sync the table.  on failure, an exception should be thrown.  otherwise, return value will be True
rv = sync.perform_sync()
'''

# note this class can ONLY be used to sync EXISTING tables as swapping the data location via parquet_utilsi assumes
# there is an existing target table...

class sync_table(object):
    _sc = []

    def __init__(self, env, logger, table_name, source_jdbc_url, source_jdbc_driver, source_schema, source_username,
                 source_password, target_db, target_base_directory, validate_schema=True):

        app_name = "sync_table"

        self.env = env
        self.logger = logger
        self.app_name = app_name
        self.source_jdbc_url = source_jdbc_url
        self.source_jdbc_driver = source_jdbc_driver
        self.source_schema = source_schema
        self.table_name = table_name
        self.source_username = source_username
        self.source_password = source_password
        self.target_db = target_db
        self.target_base_directory = target_base_directory
        self.validate_schema = validate_schema

        try:
            self.conf = SparkConf().setAppName(self.app_name)
            # only every want one SparkContext, so keep an array in the class itself
            if len(self._sc) < 1:
                self._sc.append(SparkContext(conf=self.conf))
            self.sc = self._sc[0]
            self.sqlContext = HiveContext(self.sc)
            self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
            self.hadoop_fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)
            self.hadoop_trash = self.sc._jvm.org.apache.hadoop.fs.Trash
        except Exception, err:
            error = "Following exception occurred while setting up Spark: %s", err
            self.logger.error(error)
            exit(1)

    def create_dataframe(self):
        try:
            self.df = self.sqlContext.read.jdbc(self.source_jdbc_url, table=self.source_schema + '.' + self.table_name,
                                    properties={"driver": self.source_jdbc_driver,
                                                "user": self.source_username,
                                                "password": self.source_password})

        except Exception, err:
            raise err

    def write_target_table(self):
        try:
            cols = []
            for f in self.df.schema.fields:
        
                # convert any date columns to timestamp columns and lower case any column names from source
                if f.dataType == Types.DateType():
                    cols.append(F.col(f.name.lower()).cast('timestamp'))
                else:
                    cols.append(F.col(f.name.lower()))
        
            self.df = self.df.select(*cols)
        
            pq_utils = parquet_utils(self.sqlContext, self.env, self.logger)
            pq_utils.write_data(self.target_db + '.' + self.table_name, self.df, None, None, 
                self.target_base_directory + self.table_name)

        except Exception, err:
            raise err

    def perform_sync(self):
        try:
            self.create_dataframe()
            self.write_target_table()
            return True
        except Exception, err:
            error = "Exception occurred: %s", err
            self.logger.error(error)
            exit(1)

