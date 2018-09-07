#!/bin/python -u

# This script is used to prune (drop) older partitions based on the
# retention policies set in the related .yaml file.

import re
import os
import sys

import yaml
import time
from datetime import datetime, timedelta
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
import argparse
import textwrap

from subprocess import Popen, PIPE, check_output,call

import subprocess

class clean_orphan_file:
    def __init__(self, yaml_file):
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.env = yaml.load(open(self.config['ENV']))

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


    # Clear off old partitions (or just describe the changes, if preview is True)
    def cleanup(self,first_table_run):

        for schema_name in self.config['partitions']:
            table_list = self.config['partitions'][schema_name]
            if first_table_run:
                table_list=[first_table_run]
            for table_name in table_list:
                self.logger.info("Processing table: %s", table_name)
                table_config = self.config['partitions'][schema_name][table_name]
                if 'retention' in table_config:
                    date_cutoff = self.find_date_cutoff(table_config['retention'])
                    self.logger.info("date_cutoff is %s ", date_cutoff)


                    print(table_config['retention'],date_cutoff)

                    command='sh /var/dw-grid-etl/python/delete_file.sh "%s" %s' % (str(date_cutoff), str(table_name))

                    print (command)
                    proc = subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
                    (rows, err) = proc.communicate()
                    print(rows,err)


if __name__ == "__main__":
    yaml_file='purge_partition.yaml'
    dp = clean_orphan_file(yaml_file)
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
