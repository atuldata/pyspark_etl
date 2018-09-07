#!/bin/python -u
from threading import Thread
import argparse
import sys
import yaml
import time
import sqlparse
import textwrap
from datetime import datetime, timedelta
from util.load_state import LoadState
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.oxdb import OXDB
from util.impala_utils import get_partition_column,get_column_type,invalidate_metadata

class set_stats:
    def __init__(self, yaml_file):
        self.config=yaml.load(open(yaml_file))
        self.env = yaml.load(open('/var/dw-grid-etl/python/env.yaml'))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name

    def set_table_stats(self, impala_db,table_name,row_count,partition_spec=None):
        try:
            if partition_spec:
                sql = "alter table %s partition (%s) set tblproperties ('numRows'='%s')" % (table_name, partition_spec,row_count)
                self.logger.info(sql)
            else:
                sql = "alter table %s  set tblproperties ('numRows'='%s')" % (
                table_name, row_count)
            impala_db.execute(sql)
            self.logger.debug("analyzing stats complete")
        except Exception as error:
            if partition_spec:
                err_msg = "%s stats collection failed for partition specification %s" % (table_name, partition_spec)
            else:
                err_msg = "%s stats collection failed." % (table_name)
            self.logger.error(err_msg + " Error %s" % error)
            pass

    def set_column_stats(self, impala_db,table_name,column_name,numDvs,numNulls,avgSize,maxsize):
        sql = "alter table %s SET COLUMN STATS %s ('numDvs'='%s','numNulls'='%s','avgSize'='%s','maxsize'='%s')" %(table_name,column_name,numDvs,numNulls,avgSize,maxsize)
        self.logger.info(sql)
        impala_db.execute(sql)

    def set_stats_value(self):
        table_name=self.config['FACT_TABLE']
        impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
        impala_db.execute('set SYNC_DDL=true')
        try:
            impala_db.execute("INVALIDATE METADATA %s" % table_name)
        except Exception as error:
            pass

        data= impala_db.get_executed_cursor("show table stats "+table_name).fetchall()
        self.logger.info("Table Name %s", table_name)
        partition_key=get_partition_column(table_name)
        self.logger.info("Partition Key %s",partition_key)
        result=[]

        for partition_spec_list in data:
            if partition_spec_list[len(partition_key)]==-1 and partition_spec_list[0]!='Total':
                partition_spec = ""
                for index in range(len(partition_key)):
                    if partition_spec_list[index].isdigit():
                        partition_spec += partition_key[index] + '=' + str(partition_spec_list[index]) + ","
                    else:
                        partition_spec += partition_key[index] + "=CAST('" + str(partition_spec_list[index]) + "' AS VARCHAR(100)),"
                result.append(partition_spec[:-1])

        result.sort(reverse=True)

        sql_query=self.config['VERTICA_QUERY']
        vertica_db = OXDB(self.env['VERTICA_DSN'])
        vertica_data = vertica_db.get_executed_cursor(sql_query).fetchall()
        vertica_db.close()
        vertica_dict = {}

        for value in vertica_data:
            partition_spec=""
            no_keys=len(partition_key)
            for index in range(no_keys):
                if 'platform_timezone' in partition_key[index]:
                    partition_spec += partition_key[index]+"=CAST('"+str(value[index])+"' AS VARCHAR(100)),"
                else:
                    partition_spec += partition_key[index] +"="+ str(value[index]) + ","

            vertica_dict[partition_spec[:-1]] = value[no_keys]

        index=0
        while index<len(result):
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            impala_db.execute('set SYNC_DDL=true')
            # creating multiple threads to make it run faster.
            t1 = Thread(target=self.set_table_stats, args=(impala_db,table_name,vertica_dict[result[index]],result[index]))
            t2 = Thread(target=self.set_table_stats, args=(impala_db,table_name,vertica_dict[result[index+1]],result[index+1]))
            t3 = Thread(target=self.set_table_stats, args=(impala_db,table_name,vertica_dict[result[index+2]],result[index+2]))
            t4 = Thread(target=self.set_table_stats, args=(impala_db,table_name,vertica_dict[result[index+3]],result[index+3]))
            index=index+4

            t1.start()
            t2.start()
            t3.start()
            t4.start()

            t1.join()
            t2.join()
            t3.join()
            t4.join()
            impala_db.close()

        impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
        impala_db.execute('set SYNC_DDL=true')
        vertica_db = OXDB(self.env['VERTICA_DSN'])
        sql_query="select column_name from columns where table_schema || '.' || table_name='%s'" %(self.config['FACT_TABLE'])
        column_data = vertica_db.get_executed_cursor(sql_query).fetchall()

        # get stats for the column
        for (column_name) in column_data:
            sql_query="select count(distinct %s), count(CASE WHEN %s IS NULL THEN 1 END),max(len(%s)),avg(len(%s))" %(column_name,column_name,column_name,column_name)
            stats=vertica_db.get_executed_cursor(sql_query).fetchall()
            for (numDvs,numNulls,avgSize,maxsize) in stats:
                self.set_column_stats(impala_db,table_name,column_name,numDvs,numNulls,avgSize,maxsize)
        vertica_db.close()
        impala_db.close()

        #upload load state variable
        db = OXDB(self.env['FMYSQL_META_DSN'])
        LoadState(
            db, variable_name=self.__class__.__name__
        ).upsert(datetime.now(), commit=True)
        self.logger.info("set stats is complete.")

if __name__ == "__main__":


    startTime = time.time()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''set stats for table which has missing stats'''))

    parser.add_argument('--config_file', help='file name for configuration')

    options = parser.parse_args()
    dp = set_stats(options.config_file)
    try:
        if not dp.lock.getLock():
            dp.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        dp.logger.info(
            "================================================================================================");
        dp.logger.info("Start running set stats")
        dp.set_stats_value()
        dp.logger.info("Finished running set stats")
        dp.logger.info("Elapsed Time: %s" % (time.time() - startTime))
    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        dp.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        dp.lock.releaseLock()
