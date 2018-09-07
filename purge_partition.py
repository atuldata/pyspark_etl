#!/bin/python -u

# This script is used to prune (drop) older partitions based on the 
# retention policies set in the related .yaml file.

import re
import os
import sys
import logging
import logging.handlers
import yaml
import time
from datetime import datetime, timedelta
from util.oxdb import OXDB
from util.load_state import LoadState
from optparse import OptionParser
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
import argparse
import textwrap
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

from pyspark.sql import functions as F
from pyspark.sql import types as Types
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.window import Window
from util.impala_utils import invalidate_metadata

class purge_partition:
    def __init__(self, yaml_file):
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.env = yaml.load(open(self.config['ENV']))

        self.conf = SparkConf().setAppName(yaml_file.replace(".yaml",""))
        self.sc = SparkContext(conf=self.conf)
        self.sqlContext = HiveContext(self.sc)
        self.yaml_file = yaml_file
        self.pid = os.getpid()
        self.unable_to_delete_file = "/tmp/not_deleted_orphan_parquet_files." + str(self.pid) + ".txt"

    # Given a string describing a retention policy, return the date that
    # defines the end of the window.
    def find_date_cutoff(self,str):
        m = re.match("([0-9]+)\s+months?", str)
        if m:
            return datetime.now() + timedelta(days=(-31 * int(m.group(1))))

        m = re.match("([0-9]+)\s+days?", str)
        if m:
            return datetime.now() + timedelta(days=-int(m.group(1)))
        raise Exception("Cannot parse retention string %s" % (str))


    def find_key_cutoff(self,key_type, date,primary_key):
        if 'date_sid' in ','.join(primary_key):
            return date.strftime("%Y%m%d")
        if 'datehour_sid' in ','.join(primary_key):
            return date.strftime("%Y%m%d00")
        elif 'month_sid' in ','.join(primary_key):
            return date.strftime("%Y%m01")
        else:
            raise Exception("Unknown key type %s" % (key_type))

    def get_existing_path(self,table_name, partition_spec):
        try:
            sql = "SHOW TABLE EXTENDED LIKE '" + table_name + "' partition (" + partition_spec + ")"
            self.logger.info(sql)
            file_list=self.sqlContext.sql(sql).collect()
            file_path = file_list[2][0].replace('location:','')
        except Exception as error:
            file_path = None
            raise
        return file_path

    def get_old_partitions(self, schema_name, table_name, key_cutoff,key):

        self.impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
        sql_impala = 'show partitions ' + schema_name+'.'+table_name
        data = self.impala_db.get_executed_cursor(sql_impala).fetchall()
        key_value=[]

        for value in data:
            if value[0]<key_cutoff:
                value_list=[]
                for x in range(len(key)):
                    value_list.append(value[x])
                key_value.append(value_list)

        return key_value

    def get_partition_key(self, schema_name,table_name):
        impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
        sql_impala = 'SHOW CREATE TABLE ' + table_name
        data = impala_db.get_executed_cursor(sql_impala).fetchall()
        impala_db.close()
        if 'PARTITIONED BY' in data[0][0]:
            data = data[0][0].split('PARTITIONED BY')[1]
            start_index = data.find("(")
            end_index = data.find(")")
            data = data[start_index + 1:end_index]
            data = data.split()
            partition_key = data[0::2]
        else:
            partition_key = []
        return partition_key

    # Clear off old partitions (or just describe the changes, if preview is True)
    def cleanup(self,first_table_run):
        for schema_name in self.config['partitions']:
            for table_name in self.config['partitions'][schema_name]:
                if first_table_run<>None and table_name<>first_table_run:
                    continue
                self.logger.info("Processing table: %s", table_name)
                table_config = self.config['partitions'][schema_name][table_name]
                primary_key_column = self.get_partition_key(schema_name, table_name)
                if 'retention' in table_config:
                    date_cutoff = self.find_date_cutoff(table_config['retention'])
                    self.logger.info("date_cutoff is %s ", date_cutoff)
                    key_cutoff = self.find_key_cutoff(table_config['partition_by'], date_cutoff,primary_key_column)
                    self.logger.info("key_cutoff is %s ", key_cutoff)

                    old_partitions = self.get_old_partitions(schema_name,table_name, key_cutoff,primary_key_column)
                    if old_partitions:
                        self.logger.info("Table %s has %d older partitions: %s", schema_name + '.' + table_name, len(old_partitions), old_partitions)
                    else:
                        self.logger.info("Table %s has no older partitions", schema_name + '.' + table_name)
                    primary_key_value=""
                    for old_partition in old_partitions:
                        sql = "ALTER TABLE %s DROP IF EXISTS PARTITION  ("% (schema_name+'.'+table_name)
                        primary_key_value=""
                        for i in range(len(old_partition)):
                            sql=sql+ primary_key_column[i]+"='"+old_partition[i]+"',"
                            primary_key_value+=old_partition[i]+","

                        primary_key_value=primary_key_value[:-1]
                        sql=sql[:-1]+" ) "
                        self.logger.info(sql)

                        result = self.sqlContext.sql(sql).collect()
                        self.mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
                        sql = "update partition_status set is_used_by_impala=0, modified_datetime=now() where table_name='%s' " \
                              "and is_used_by_impala = 1 " \
                              "and partition_key='%s' " \
                              "and partition_value='%s' " % (
                            schema_name + '.' + table_name, ','.join(primary_key_column),primary_key_value)
                        self.logger.info("running %s", sql)
                        self.mysql_db.execute(sql)
                        self.mysql_db.close()

                if 'archive_after' in table_config:
                    archive_cutoff = 0
                    query=self.config['create_table'] %(schema_name + '.' + table_name+"_archive",schema_name + '.' + table_name, table_name+"_archive")
                    self.logger.info(query)
                    impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
                    impala_db.execute(query)
                    impala_db.close()
                    archive_cutoff = self.find_date_cutoff(table_config['archive_after'])
                    archive_key_cutoff = self.find_key_cutoff(table_config['partition_by'], archive_cutoff,
                                                              primary_key_column)

                    archive_partitions = self.get_old_partitions(schema_name, table_name, archive_key_cutoff, primary_key_column)
                    self.logger.info(archive_cutoff)
                    self.logger.info(archive_key_cutoff)
                    print archive_key_cutoff
                    print archive_partitions
                    for old_partition in archive_partitions:
                        primary_key_value = ""
                        for i in range(len(old_partition)):
                            if old_partition[i].isdigit():
                                primary_key_value = primary_key_value+primary_key_column[i] + "=" + old_partition[i] + ","
                            else:
                                primary_key_value = primary_key_value + primary_key_column[i] + "='" + old_partition[
                                    i] + "',"

                        primary_key_value = primary_key_value[:-1]
                        self.logger.info(primary_key_value)
                        new_path=self.get_existing_path(schema_name + '.' + table_name,primary_key_value)
                        self.logger.info("PATH : "+new_path)

                        move_ddl_drop_partition = "ALTER TABLE " + schema_name + '.' + table_name+"_archive"  + " DROP IF EXISTS PARTITION  (" + primary_key_value + ")"
                        self.logger.info(move_ddl_drop_partition)
                        result = self.sqlContext.sql(move_ddl_drop_partition).collect()
                        move_ddl_add_partition = "ALTER TABLE " + schema_name + '.' + table_name+"_archive"   + " ADD PARTITION (" + primary_key_value + ") LOCATION \"" + new_path + "\""
                        self.logger.info(move_ddl_add_partition)
                        result = self.sqlContext.sql(move_ddl_add_partition).collect()

                        sql = "ALTER TABLE %s DROP IF EXISTS PARTITION  ( %s) " % (
                        schema_name + '.' + table_name, primary_key_value)
                        self.logger.info(sql)
                        result = self.sqlContext.sql(sql).collect()

                    impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
                    invalidate_metadata(impala_db, schema_name + '.' + table_name+"_archive")
                    impala_db.close()

                impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
                invalidate_metadata(impala_db,schema_name + '.' + table_name)
                impala_db.close()

        LoadState(
            OXDB(self.env['FMYSQL_META_DSN']),
            variable_name=self.yaml_file.replace(".yaml","")).upsert(datetime.now(), commit=True)
        self.logger.info("Updated and Committed load_state variable %s", self.yaml_file.replace(".yaml",""))

if __name__ == "__main__":
    yaml_file=sys.argv[0].replace('.py','.yaml')
    dp = purge_partition(yaml_file)
    startTime = time.time()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''drop the location for the partition that beyong retention'''))

    parser.add_argument('--table_name', help='Name of the table')

    options = parser.parse_args()

    try:
        if not dp.lock.getLock():
            dp.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        dp.logger.info(
            "================================================================================================");
        dp.logger.info("Start running compute stats")

        dp.cleanup(options.table_name)

        dp.logger.info("Finished running compute stats")
        dp.logger.info("Elapsed Time: %s" % (time.time() - startTime))
    except SystemExit:
        pass
    except:
        exc_type, exc_val = sys.exc_info()[:2]
        dp.logger.error("Error: %s, %s", exc_type, exc_val)
        raise
    finally:
        dp.lock.releaseLock()
