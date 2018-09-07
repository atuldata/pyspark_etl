#!/bin/python -u

import yaml
import sys
import os

from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.oxdb import OXDB

import requests
import xml.etree.ElementTree as XMLTree
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from util.odfi_utils import get_datasets_schema

class etl_feed_status:
    def __init__(self, yaml_file):
        self.lock = JobLock(self.__class__.__name__)
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])

        # TODO: check and add feed_host in env.yaml
        self.feed_host = self.env['ODFI_HOST']
        self.credentials_file = open(self.env['ODFI_CRED_PATH'])
        self.user, self.passwd = self.credentials_file.read().strip('\n').split(':')

    def update_etl_status(self,feed_name):
        self.logger.info("Checking state for feed '%s'" % (feed_name))
        sql = self.config['CHECKPOINT_INFO']
        self.logger.info('Running SQL : %s' % sql)
        self.db = OXDB(self.env['FMYSQL_META_DSN'])
        max_serial, date_time = self.db.get_row(sql, feed_name, feed_name)

        available_periods = []
        if max_serial is not None:
        
            max_serial = int(max_serial)
            dataset_interval = date_time.strftime('%Y-%m-%d_%H')
            odfi_next_query_serial, odfi_start_timestamp = max_serial, date_time
            odfi_start_timestamp, odfi_end_timestamp = date_time, date_time
            feed_frequency = 0
            readable_interval = 'N/A'
            delta = 0
            feed_ds=get_datasets_schema(self.feed_host, self.user, self.passwd, feed_name, odfi_next_query_serial, self.logger)
            max_odfi_created_timestamp = date_time
            # get the max serial and the created timestamp from the feed
            interval_keys=sorted(feed_ds.keys())
            max_odfi_start_time = date_time
            # even though the max odfi serial isn't really used, we'll keep it in case it's needed.
          
            for interval in interval_keys:
                # print odfi_start_timestamp, odfi_end_timestamp, odfi_serial,odfi_created_timestamp
                # check if this shoudn't be the next period we can load.
                odfi_next_query_serial = feed_ds[interval]['serial']
                odfi_start_timestamp = feed_ds[interval]['startTimestamp']
                odfi_end_timestamp = feed_ds[interval]['endTimestamp']
                odfi_created_timestamp = feed_ds[interval]['dateCreated']
                odfi_row_count = feed_ds[interval]['recordCount']
                # print odfi_created_timestamp
                readable_interval = feed_ds[interval]['readableInterval']
                feed_frequency = odfi_end_timestamp - odfi_start_timestamp

                diff = datetime.utcnow() - odfi_created_timestamp
                min = (diff.days * 24 * 60) + (diff.seconds / 60)
                if odfi_row_count > 0 and min > 60:
                    if max_odfi_start_time + feed_frequency == odfi_start_timestamp:
                        delta += 1
                        max_odfi_start_time = odfi_start_timestamp
                        max_odfi_created_timestamp = odfi_created_timestamp
            load_state_date_time, = self.db.get_row(self.config['GET_LOAD_STATE_DATE_TIME'], feed_name)

            if load_state_date_time is None:
                load_state_date_time = date_time
            # update feed status by providing new delay value
            sql = self.config['INSERT_FEED_STATUS']
            self.logger.info(sql)
            self.db.execute(sql, feed_name, feed_name, max_serial, dataset_interval,
                            odfi_next_query_serial, readable_interval, delta, delta,
                             max_odfi_created_timestamp, load_state_date_time,max_serial, 
                             dataset_interval, odfi_next_query_serial,
                            readable_interval, delta, max_odfi_created_timestamp,
                            load_state_date_time)
            self.db.commit()
            self.db.close()

    def processor(self):
        self.db = OXDB(self.env['FMYSQL_META_DSN'])
        feed_list=self.db.get_executed_cursor(self.config['FEED_NAME']).fetchall()
        self.db.close()
        for feed_name in feed_list:
            self.update_etl_status(feed_name[0])

if __name__=='__main__':
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    checker = etl_feed_status(yaml_file)
    try:
        if not checker.lock.getLock():
            checker.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        checker.processor()
    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        checker.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        checker.lock.releaseLock()
