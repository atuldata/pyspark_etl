"""This class is responsible for performing insert into carrier_dim table based upon new entries coming from ODFI
-----------------------------------------------------------------------------------------------------------------------
Call it from another function using
data_modify = parquet_utils(self.sqlContext, self.env, self.logger)
        sql = data_modify.arrange_column_type(tranform_df, fact_table_df, self.partition_key)
        tranform_df = self.sqlContext.sql(sql)
"""
# !/bin/python -u

import json
import os
import re
import sys
import time
import xml.etree.ElementTree as xmle
from datetime import datetime
from optparse import OptionParser
from xml.etree.ElementTree import XMLParser

import py4j
import requests
import yaml
from hdfs import InsecureClient
from hdfs import util as HdfsUtil
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.parquet_utils import *


class ox_openrtb_sum_hourly:
    def __init__(self, sqlContext, config, feed_name, yaml_file, intrvl, feed_date, republish_force,logger,lock):
        self.logger = logger  # use class name as the log name
        self.lock = lock  # use class name as the lock name
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))

        self.yaml_file = yaml_file

        self.sqlContext = sqlContext

        self.feed_name = feed_name
        self.odfi_feed_host = self.env['ODFI_HOST']  # 'http://api.data.openx.org'
        self.intrvl = intrvl
        self.feed_date = feed_date

        self.baseDirectory = self.env['HIVE_BASE_DIR']
        self.hdfsClient = InsecureClient(self.env['HDFS_NAME_NODE_URL_PORT'], user=self.env['HDFS_USER'])
        self.batchid = str(time.time())

    # find maximum occurence of the item to identify the code value.
    def max_element(self, itemList):
        counter = {}
        maxItemCount = 0
        mostPopularItem = itemList[0][0]
        for item in itemList:
            if item[0][0] in counter.keys():
                counter[item[0][0]] += 1
            else:
                counter[item[0][0]] = 1
            if counter[item[0][0]] > maxItemCount:
                maxItemCount = counter[item[0][0]]
                mostPopularItem = item[0][0]
        return mostPopularItem

    # update the new carrier_data or insert new carrier codes in carrier_dim table
    def update_carrier_data(self):
        sql = self.config['GET_MISSING_CARRIER_CODES']
        code = self.sqlContext.sql(sql).collect()
        result_set = []
        code_part = [x[0].encode('UTF-8') for x in code if x[0] is not None]


        index = self.sqlContext.sql(self.config['GET_MAX_CARRIER_ID']).collect()
        index = int(index[0][0])
        schema = ['carrier_sid', 'carrier_name', 'carrier_codes', 'created_datetime''modified_datetime']

        for codes in code_part:
            if codes.find(',') > 0:
                single_codes = codes.split(',')
                final_code = {}
                for single_code in single_codes:

                    try:
                        sql = self.config['GET_SINGLE_CARRIER'] % (single_code)
                        code_name = self.sqlContext.sql(sql).collect()
                    except Exception, e:
                        self.logger.error('Problem retrieving carrier by single code: %s (exception %s)', single_code, e)
                        code_name = None
                    self.logger.info('%s (Single Code) -> %s (Code Name)',(single_code, code_name))

                    if code_name:
                        final_code[single_code] = code_name
                final_code_name = "Unknown - Unknown"
                if final_code:
                    final_code_name = self.max_element(final_code.values())
                    self.logger.info("Missing code_name inserting into carrier_dim: %s", final_code_name)

                index = index + 1

                result_set.append((index, final_code_name, codes, datetime.utcnow()
                                   , datetime.utcnow()))

            else:
                single_code = codes

                try:
                    sql = self.config['GET_SINGLE_CARRIER'] % (single_code)
                    code_name = self.sqlContext.sql(sql).collect()
                except Exception, e:
                    self.logger.error('Problem retrieving carrier by single code: %s (exception %s)', single_code, e)
                    code_name = None

                self.logger.info('%s (Single Code) -> %s (Code Name)', (single_code, code_name))
                if not code_name or not code_name[0][0]:
                    final_code_name = "Unknown - Unknown"
                    index = index + 1
                    result_set.append((index, final_code_name, codes, datetime.utcnow()
                                       , datetime.utcnow()))
                else:
                    final_code_name = code_name[0][0]
                    index = index + 1
                    result_set.append((index, final_code_name, codes, datetime.utcnow()
                                       , datetime.utcnow()))

        if result_set:
            incremental_table = self.sqlContext.createDataFrame(result_set, schema)
            current_table = self.sqlContext.table(self.config['DIM_TABLE'])
            result_df = current_table.unionAll(incremental_table)

            data_modify = parquet_utils(self.sqlContext, self.env, self.logger)
            new_dir = self.baseDirectory + "/" + \
                      self.config['DIM_TABLE'].split('.')[1] + "/" + \
                      self.batchid
            data_modify.write_data(self.config['DIM_TABLE'],
                                   result_df,
                                   None,
                                   None,
                                   new_dir, 1)
        else:
            self.logger.info("No new carrier_codes found")
