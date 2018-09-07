#!/bin/python -u
"""
This class is responsible for performing the update of metadata for table such as partition_status and table_stats
"""

import os
import sys
import time
import yaml
import re
from datetime import datetime

from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.impala_utils import *
from util.oxdb import OXDB
from util.load_state import LoadState

class refresh_partition_status:
    def __init__(self, yaml_file):
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.env = yaml.load(open(self.config['ENV']))

    def get_existing_path(self, partition_key,partition_value):
        """
        get the file path of existing partition key and value
        """
        try:
            mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
            sql=self.config['GET_EXISTING_PATH']
            self.logger.info("Running:  %s for partition_key: %s and partition_value: %s ", sql,partition_key,partition_value)
            file_path=mysql_db.get_executed_cursor(sql,partition_key,partition_value).fetchall()
            mysql_db.close()
            return file_path
        except:
            self.logger.error("Error in get_existing_path()")
            raise

    def process_table_metadata(self):
        """
        get the list of table from yaml files and update the metadata for it.
        """
        table_list = self.config['TABLE_LIST']
        for table_name in table_list:
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            data = impala_db.get_executed_cursor("show table stats " + table_name).fetchall()
            impala_db.close()
            key_list=get_partition_column(table_name)

            mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
            mysql_db.execute("SET tx_isolation = 'READ-COMMITTED'")
            record_insert = []
            record_update = []
            if len(key_list) > 0:
                for row in data:
                    row_count = row[len(key_list)]
                    file_count=row[len(key_list)+1]
                    file_size=row[len(key_list)+2]
                    is_stat_collected=1
                    if row_count==-1:
                        is_stat_collected=0
                    if row[0] != 'Total':
                        key = ""
                        partition_key=""
                        partition_value=""
                        for index in range(len(key_list)):
                            partition_key+=key_list[index]+','
                            if row[index].isdigit():
                                partition_value+=row[index]+","
                            else:
                                partition_value += "'"+row[index]+"'"+","
                        partition_key=partition_key[:-1]
                        partition_value=partition_value[:-1]


                        existing_path=self.get_existing_path(partition_key,partition_value)
                        self.logger.info(existing_path)

                        if len(existing_path) == 0:
                            record_insert.append((table_name, row[-1],1,partition_key, partition_value, file_count,table_name.split('.')[1],is_stat_collected, file_size,
                                             row_count))
                        else:
                            existing_location = existing_path[0][0]
                            record_update.append((
                                           file_count,
                                           is_stat_collected,
                                           file_size,
                                           int(row_count),
                                           table_name,
                                           existing_location,
                                           partition_key,
                                           partition_value))

            self.logger.info("update the table_stats")
            if len(record_insert)>0:
                mysql_db.executemany(self.config['INSERT_PARTITION_STATS'],record_insert)
            if len(record_update) > 0:
                mysql_db.executemany(self.config['UPDATE_PARTITION_STATS'], record_update)
            mysql_db.commit()
            mysql_db.close()
            self.logger.info("done updating")
        db = OXDB(self.env['FMYSQL_META_DSN'])
        LoadState(
            db, variable_name=self.__class__.__name__
        ).upsert(datetime.now(), commit=True)

if __name__ == '__main__':
    yaml_file = sys.argv[0].replace('.py', '.yaml')
    instance=refresh_partition_status(yaml_file)
    try:
        start_time = time.time()
        if not instance.lock.getLock():
            instance.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        instance.logger.info("======================================================")
        instance.logger.info("Starting to process to refresh metadata of mysql table")
        instance.process_table_metadata()
        end_time = time.time()
        instance.logger.info("Finished updating the metadata. Total elapsed time: %d seconds.", (end_time - start_time))

    except SystemExit:
        pass
    except:
        instance.logger.error("Error: %s", sys.exc_info()[0])
        raise
    finally:
        instance.lock.releaseLock()
