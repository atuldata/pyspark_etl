#!/opt/bin/python -u
from threading import Thread
# !/opt/bin/python -u
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
from util.impala_utils import get_partition_column,get_column_type,analyze_stats

class compute_stats:
    def __init__(self,):

        self.env = yaml.load(open('/var/dw-grid-etl/python/env.yaml'))
        self.logger = EtlLogger.get_logger(self.__class__.__name__)  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name


    def get_table_list(self):
        """
            Get list of tables
        """
        self.impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
        table_list = self.impala_db.get_executed_cursor("show tables").fetchall()
        self.impala_db.close()

        exclude_table_list = ['temp', 'test', 'old', 'stage', 'backup', '1', '2', '3', '4', '5', '6', '7', '8', '9']

        result_list = []
        for name in table_list:
            flag = [1 for str in exclude_table_list if str in name[0]]
            if len(flag) == 0:
                result_list.append(name[0])
        return result_list

    # This function is responsible getting the table list and check if any table or partition has missing stats
    def get_partition_spec(self,row,partition_key):
        row_count = row[len(partition_key)]
        partition_spec = ""
        if row[0] != 'Total' and row_count == -1 or row[len(partition_key) + 6] == 'false':
            for index in range(len(partition_key)):
                if row[index].isdigit():
                    partition_spec += partition_key[index] + '=' + str(row[index]) + ","
                else:
                    partition_spec += partition_key[index] + "='" + str(row[index]) + "',"
        return partition_spec[:-1]

    def get_missing_stats(self,table_name=None):
        if table_name:
            table_list=['mstr_datamart.'+table_name]
        else:
            table_list=self.get_table_list()
        for table_name in table_list:
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            data= impala_db.get_executed_cursor("show table stats "+table_name).fetchall()
            self.logger.info("Table Name %s", table_name)
            partition_key=get_partition_column(table_name)

            result=[]
            for partition_spec_list in data:
                if partition_spec_list[len(partition_key) + 6] == 'false' and partition_spec_list[0]!='Total':
                    partition_spec = ""
                    for index in range(len(partition_key)):
                        if partition_spec_list[index].isdigit():
                            partition_spec += partition_key[index] + '=' + str(partition_spec_list[index]) + ","
                        else:
                            partition_spec += partition_key[index] + "='" + str(partition_spec_list[index]) + "',"
                    result.append(partition_spec[:-1])

            result.sort(reverse=True)
            if partition_key:
                index=0
                while index<len(result):
                    impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
                    if(len(result)-index)<4:
                        analyze_stats(impala_db, table_name, result[index])
                        index+=1
                    else:
                        self.logger.info(result[index])
                        t1 = Thread(target=analyze_stats, args=(impala_db,table_name, result[index]))
                        index+=1

                        self.logger.info(result[index])
                        t2 = Thread(target=analyze_stats, args=(impala_db,table_name, result[index]))
                        index+=1

                        self.logger.info(result[index])
                        t3 = Thread(target=analyze_stats, args=(impala_db,table_name, result[index]))
                        index+=1

                        self.logger.info(result[index])
                        t4 = Thread(target=analyze_stats, args=(impala_db,table_name, result[index]))
                        index+=1

                        t1.start()
                        t2.start()
                        t3.start()
                        t4.start()

                        t1.join()
                        t2.join()
                        t3.join()
                        t4.join()
                    impala_db.close()
            else:
                impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
                row_count=data[0][0]
                if row_count==-1:
                    # Invalidate metadata in Impala
                    analyze_stats(impala_db,table_name)

                data = impala_db.get_executed_cursor("show column stats " + table_name).fetchall()
                for row in data:
                    distinct_column_count=row[2]
                    if distinct_column_count==-1:
                        # compute stats in Impala
                        analyze_stats(impala_db,table_name)
                impala_db.close()

        db = OXDB(self.env['FMYSQL_META_DSN'])
        LoadState(
            db, variable_name=self.__class__.__name__
        ).upsert(datetime.now(), commit=True)
        self.logger.info("Compute Stats is complete.")

if __name__ == "__main__":

    dp = compute_stats()
    startTime = time.time()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''compute stats for table which has missing stats'''))

    parser.add_argument('--table_name', help='Name of the table')

    options = parser.parse_args()

    try:
        if not dp.lock.getLock():
            dp.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        dp.logger.info(
            "================================================================================================");
        dp.logger.info("Start running compute stats")

        dp.get_missing_stats(options.table_name)

        dp.logger.info("Finished running compute stats")
        dp.logger.info("Elapsed Time: %s" % (time.time() - startTime))
    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        dp.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        dp.lock.releaseLock()