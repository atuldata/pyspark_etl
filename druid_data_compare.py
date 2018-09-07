#!/bin/python -u
import os

import sys

import json
import time
import yaml
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.oxdb import OXDB
import datetime
import requests

DELTA = 0.0001

class druid_data_compartor:
    def __init__(self, yaml_file):
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.env = yaml.load(open(self.config['ENV']))
        self.druid_host = self.env['DRUID_HOST_URL']
        self.db = OXDB(self.env['FMYSQL_META_DSN'])

    def getTimeBoundary(self, data_source):
        query = self.config['GET_TIMEBOUNDARY'].replace('$DATA_SOURCE', data_source)
        result = json.loads(requests.post(self.druid_host, data=query).content)
        interval = result[0]["result"]["minTime"]+"/"+result[0]["result"]["maxTime"]
        return interval

    def getDateSid(self, timestamp):
        timestamp = datetime.datetime.strptime(timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        timestamp = datetime.datetime.strftime(timestamp, "%Y%m%d")
        return timestamp

    def getDateHourSid(self, timestamp):
        timestamp = datetime.datetime.strptime(timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        timestamp = datetime.datetime.strftime(timestamp, "%Y%m%d%H")
        return timestamp

    def run(self):

        for ds in self.config['DATA_SOURCES']:
            name = ds['name']
            schema = ds['vertica_schema']
            table = ds['vertica_table']
            partition_key = ds['partition_key']
            druid_query = ds['druid_query']

            self.logger.info("==================================================================")
            self.logger.info("============================ Start comparing "+ds['name']+" =============================")

            interval = self.getTimeBoundary(name)
            druid_query = druid_query.replace("$INTERVALS", interval)
            druid_result = json.loads(requests.post(self.druid_host, data=druid_query).content)

            if druid_result:
                for result in druid_result:
                    druid_data = result['event']
                    timestamp = self.getDateSid(result['timestamp'])

                    for metric in druid_data:
                        metric_value = druid_data[metric]
                        self.logger.info("Insert result of "+ str(timestamp) + "to data_compare_status table")
                        self.db.execute(self.config['INSERT_COMPARE_FACT'], (schema, table, metric, partition_key, timestamp, metric_value, 'DRUID'))

        self.db.commit()

if __name__=='__main__':
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    dc = druid_data_compartor(yaml_file)
    dc.run()
