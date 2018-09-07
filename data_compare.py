#!/opt/bin/python -u
# ETL Name      : sqoop_pyhive.py
# Purpose       : to interact with hive to convert the data format and drop the older data
import os
import sys
import yaml
import time
import datetime
import requests
import json
import textwrap

from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.oxdb import OXDB
from util.load_state import LoadState
from multiprocessing import Process
import sys
import os
import re
from multiprocessing import Pool
from datetime import datetime, timedelta
from util.spark_utils import *
from threading import Thread
from ConfigParser import ConfigParser
import argparse

class data_compare:

    def __init__(self, yaml_file, data_source_dsn, data_type):
        self.logger = EtlLogger.get_logger(self.__class__.__name__+"_"+data_source_dsn+"_"+data_type)  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__+"_"+data_source_dsn+"_"+data_type)  # use class name as the lock name
        self.config = yaml.load(open(yaml_file))
        self.yaml_file = yaml_file
        self.data_source_dsn = data_source_dsn
        self.data_type = data_type
        self.table_schema = self.config['SCHEMA']
        self.env = yaml.load(open(self.config['ENV']))
        self.sql_context = get_sql_context(self.__class__.__name__+"_"+data_source_dsn+"_"+data_type)
        self.sql_context.setConf('spark.scheduler.mode', 'FAIR')

    def get_vertica_jdbc_settings(self, data_source_dsn, config_file_path):
        """
        Reads odbc.ini configuration file and creates vertica jdbc settings.
        :param config_file_path: odbc.ini file location.
        :return: vertica jdbc settings object.
        """
        cfg_parser = ConfigParser()
        cfg_parser.read(config_file_path)
        jdbc_url = "jdbc:vertica://" + cfg_parser.get(self.env[data_source_dsn], 'servername') \
                   + ":" + cfg_parser.get(self.env[data_source_dsn], 'port') + "/" \
                   + cfg_parser.get(self.env[data_source_dsn], 'database')

        db_properties = {'driver': 'com.vertica.jdbc.Driver',
                         'user': cfg_parser.get(self.env[data_source_dsn], 'username'),
                         'password': cfg_parser.get(self.env[data_source_dsn], 'password')
                         }

        jdbc_settings = {'jdbc_url': jdbc_url, 'db_properties': db_properties}
        return jdbc_settings

    def get_vertica_df(self, data_source_dsn, sql_query):
        """
        Retrieve data from vertica and creates a data frame.
        :param sql_context: Hive sql context object.
        :param sql_query: query or table name to be executed on vertica.
                       Example1: SELECT * FROM TABLE
                       Example2: TABLE_SCHEMA.TABLE_NAME
        :return: data frame object
        """
        jdbc_settings = self.get_vertica_jdbc_settings(data_source_dsn, '/etc/odbc.ini')

        vertica_df = self.sql_context.read.jdbc(
            jdbc_settings['jdbc_url'],
            sql_query,
            properties=jdbc_settings['db_properties']
        )
        return vertica_df

    def get_partition_column(self, table_name):
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB(self.env[self.data_source_dsn], None, False)
            sql_impala = 'SHOW CREATE TABLE ' + self.table_schema+"."+table_name
            data = impala_db.get_executed_cursor(sql_impala).fetchall()
            impala_db.close()
            if 'PARTITIONED BY' in data[0][0]:
                data = data[0][0].split('PARTITIONED BY')[1]
            else:
                return []
            start_index = data.find("(")
            end_index = data.find(")")
            data = data[start_index + 1:end_index]
            data = data.split()
            partition_key = data[0]
            print partition_key

        else:
            src_db = OXDB(self.env[self.data_source_dsn])
            partition_key = src_db.get_executed_cursor(self.config['GET_PARTITION'],
                                                            (self.table_schema,
                                                             table_name)).fetchall()
            src_db.close()
            if  partition_key:
                partition_key = partition_key[0][0].replace(table_name + '.', '')
        return partition_key

    def get_table_list(self):
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB(self.env[self.data_source_dsn], None, False)
            table_list = impala_db.get_executed_cursor("show tables").fetchall()
            impala_db.close()
        else:
            src_db = OXDB(self.env[self.data_source_dsn], self.table_schema, False)
            set_schema_sql = 'set search_path=' + self.table_schema
            src_db.execute(set_schema_sql)
            table_list = src_db.get_executed_cursor(self.config['GET_TABLE'], self.table_schema).fetchall()
            src_db.close()
        exclude_table_list = self.config['EXCLUDE_TABLE_NAMES']
        result_list = []

        for name in table_list:
            flag = [1 for str in exclude_table_list if str in name[0]]
            if '_dim' in name:
                if len(flag) == 0 and bool(re.search(r'\d', name[0])) == False and name[0] in self.config['DIM_TABLES']:
                    if len(name[0]) == name[0].find(self.data_type) + len(self.data_type):
                        result_list.append(name[0])
            else:
                if len(flag) == 0 and bool(re.search(r'\d', name[0])) == False :
                    if len(name[0]) == name[0].find(self.data_type) + len(self.data_type):
                        result_list.append(name[0])

        return result_list

    def get_column_list(self, table_name):
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB(self.env[self.data_source_dsn], None, False)
            column_list = impala_db.get_executed_cursor(
                "describe " + self.table_schema + "." + table_name).fetchall()
            impala_db.close()
        else:
            src_db = OXDB(self.env[self.data_source_dsn])
            column_list = src_db.get_executed_cursor(self.config['GET_COLUMN'], self.table_schema,
                                                          table_name).fetchall()
            src_db.close()

        result_column_list = []
        for name in column_list:
            result_column_list.append(name[0])
        return result_column_list

    def get_required_fact_columns(self, column_list, table_name):
        columns = []
        for column_name in column_list:
            if 'tot_' in column_name:
                columns.append(column_name)
        columns.append('rowcount')
        return columns

    def get_required_dim_columns(self, column_list):
        exclude_columns = ['external_id',
                           'timezone',
                           'secondary',
                           'parent',
                           '_uol',
                           'name']
        result_column_list = []

        for name in column_list:
            flag = [1 for str in exclude_columns if str in name]
            if len(flag) == 0:
                if len(name) == (name.find("_nk") + len("_nk")) and len(result_column_list) < 3:
                    result_column_list.append(name)
                elif len(name) == (name.find("_id") + len("_id")) and len(result_column_list) < 3:
                    result_column_list.append(name)
                elif len(name) == (name.find("_sid") + len("_sid")) and len(result_column_list) < 3:
                    result_column_list.append(name)
        if len(result_column_list) == 0:
            result_column_list.append(column_list[0])
        result_column_list.append('rowcount')
        return result_column_list

    def repupulate_update_records(self,source,table_name):
        mysql_db = OXDB(self.env['VERTICA_DSN'], 'mstr_datamart')
        mysql_db.execute(self.config['REPOPULATE_TABLE'],(source,table_name))
        mysql_db.commit()
        mysql_db.close()

    def generate_sql_for_fact(self, table_name,  column_list, partition_key):

        matrix_col = ""
        matrix = ""

        mysql_db = OXDB(self.env['VERTICA_DSN'],'mstr_datamart')
        print self.data_source_dsn,table_name, self.table_schema, partition_key
        existing_value = mysql_db.get_executed_cursor(self.config['GET_PARTITION_KEY'], (self.data_source_dsn,table_name, self.table_schema, partition_key)).fetchall()

        existing_value = [value[0] for value in existing_value]
        existing_value = ','.join(existing_value)

        mysql_db.commit()
        mysql_db.close()

        for column_name in column_list:
            if 'tot_' in column_name:
                if 'IMPALA' in self.data_source_dsn:
                    matrix = matrix + "sum(" + column_name + "),"
                else:
                    matrix = matrix + "sum_float(" + column_name + "),"
                matrix_col = matrix_col + column_name + ','

        matrix_col = matrix_col[:-1]
        where_clause="  "
        join_clause=""
        key_index=""
        key_list='platform_id'
        if partition_key=='instance_datehour_sid' and 'hourly_fact' in table_name:
            if 'instance_timestamp' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(instance_timestamp),'yyyyMMddHH') as INT)"
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(instance_timestamp,'YYYYMMDDHH') as INT)"
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(from_utc_timestamp(utc_timestamp, coalesce(pd.timezone, 'UTC'))),'yyyyMMddHH') as INT)"
                    return
                    #where_clause= key_index+" in (select "+key_index+" from "+self.table_schema + '.' + table_name +" where "+key_index+ " not in ("+existing_value+")"+ "group by 1 limit 10")
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(NEW_TIME(utc_timestamp, 'UTC',coalesce(pd.timezone, 'UTC')),'YYYYMMDDHH') as INT)"
                    return
                if 'x_platform_id' in column_list:
                    key_list = 'x_platform_id'
                elif 'platform_id' in column_list:
                    key_list = 'platform_id'
                elif 'p_platform_id' in column_list:
                    key_list = 'p_platform_id'
                elif 'a_platform_id' in column_list:
                    key_list = 'a_platform_id'
                join_clause=' ox inner join ' + self.table_schema + '.'+'platform_dim pd ON ( ox.' + key_list + ' = pd.platform_id )'

        elif partition_key=='advt_datehour_sid' and 'hourly_fact' in table_name:
            if 'advt_timestamp' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(advt_timestamp),'yyyyMMddHH') as INT)"
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(advt_timestamp,'YYYYMMDDHH') as INT)"
            if 'advt_date_sid' in column_list and 'advt_hour_sid' in column_list:
                key_index = "advt_date_sid*100+advt_hour_sid"
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(from_utc_timestamp(utc_timestamp, coalesce(ad.advertiser_account_timezone, 'UTC'))),'yyyyMMddHH') as INT)"
                    return
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(NEW_TIME(utc_timestamp, 'UTC',coalesce(ad.advertiser_account_timezone, 'UTC')),'YYYYMMDDHH') as INT)"
                    return
                if 'a_platform_id' in column_list:
                    key_list = 'a_platform_id'
                elif 'platform_id' in column_list:
                    key_list = 'platform_id'
                elif 'p_platform_id' in column_list:
                    key_list = 'p_platform_id'
                join_clause = ' ox inner join ' + self.table_schema + '.'+'advertiser_dim ad ON ( ox.' + key_list + ' = ad.platform_id )'

        elif partition_key=='utc_datehour_sid' and 'hourly_fact' in table_name:
            if 'utc_hour_sid' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "utc_date_sid*100+utc_hour_sid"
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "utc_date_sid*100+utc_hour_sid"
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(utc_timestamp),'yyyyMMddHH') as INT)"
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(utc_timestamp,'YYYYMMDDHH') as INT)"
        else:
            key_index=partition_key

        if existing_value:
            where_clause=where_clause+" where  " + key_index + " not in (" + existing_value + ")"

        sql = 'SELECT ' + key_index +' as '+partition_key +',' + matrix + ' count(1) rowcount' \
              + ' FROM ' + self.table_schema + '.' + table_name \
              + join_clause \
              + where_clause \
              + ' group by 1'

        return sql

    def generate_sql_for_dim(self, table_name, column_list):
        keys = ""
        if len(column_list[:-1]) > 0:
            keys = ','.join(column_list[:-1])
            sql = "select " + keys + ", count(1) rowcount " + " from " + \
                  self.table_schema + "." + table_name + " group by " + keys
        else:
            sql = "select  count(1) rowcount " + " from " + self.table_schema + "." + table_name
        return sql

    def insert_dim_data(self, instance_id, table_name, columns, sql):
        master_df = self.get_vertica_df('VERTICA_DSN', "( " + sql + ") test")

        if 'VERTICA' in self.data_source_dsn:
            source_df = self.get_vertica_df(self.data_source_dsn, "( " + sql + ") test")
        if 'IMPALA' in self.data_source_dsn or 'SPARK' in self.data_source_dsn:
            source_df = self.sql_context.sql(sql)
        self.logger.info("table_name %s", table_name)
        self.logger.info("Column List %s", str(columns))

        # dups = self.source_db.get_executed_cursor(sql+" having count(1)>1").fetchall()
        # mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
        # mysql_db.execute(self.config['INSERT_DUPS'], (self.data_source_dsn,table_name,len(dups),len(dups)))
        # mysql_db.commit()
        # mysql_db.close()

        diff_df = master_df.subtract(source_df)
        master_data = diff_df.collect()
        master_tuple = []
        for data in master_data:
            partition_key = columns[0]
            partition_value = data[0]
            for index in range(1, len(columns)):
                if partition_value is None:
                    partition_value = '-1'
                master_tuple.append((instance_id, 'mstr_datamart',
                                     table_name,
                                     columns[index],
                                     partition_key,
                                     partition_value,
                                     data[index],
                                     'VERTICA_DSN'))
        if len(master_tuple) > 0 and len(master_tuple) < 1000:
            mysql_db = OXDB(self.env['VERTICA_DSN'],'mstr_datamart')
            mysql_db.executemany(self.config['INSERT_DIM_COMPARE_FACT'], master_tuple)
            mysql_db.commit()
            mysql_db.close()

        souce_data_diff = source_df.subtract(master_df)
        source_data = souce_data_diff.collect()

        source_data_tuple = []
        for data in source_data:
            partition_key = columns[0]
            partition_value = data[0]
            for index in range(len(columns)):
                if partition_value is None:
                    partition_value = '-1'
                source_data_tuple.append((instance_id,
                                          'mstr_datamart',
                                          table_name,
                                          columns[index],
                                          partition_key,
                                          partition_value,
                                          data[index],
                                          self.data_source_dsn))

        if len(source_data_tuple) > 0 and len(source_data_tuple) < 1000:
            mysql_db = OXDB(self.env['VERTICA_DSN'], 'mstr_datamart')
            mysql_db.executemany(self.config['INSERT_DIM_COMPARE_FACT'], source_data_tuple)
            mysql_db.commit()
            mysql_db.close()

    def insert_fact_data(self, instance_id, table_name, partition_key,columns, sql):
        source_db = OXDB(self.env[self.data_source_dsn], None, False)
        print'YES', sql
        source_data = source_db.get_executed_cursor(sql).fetchall()
        source_db.close()
        print columns
        source_data_tuple = []
        for data in source_data:
            partition_value = data[0]
            for index in range(len(columns)):
                source_data_tuple.append((
                                          'mstr_datamart',
                                          table_name,
                                          columns[index],
                                          partition_key,
                                          partition_value,
                                          data[index],
                                          self.data_source_dsn))

        if len(source_data_tuple) > 0:
            db = OXDB(self.env['VERTICA_DSN'], 'mstr_datamart')
            self.logger.info("Inserting the dataset")
            db.executemany(self.config['INSERT_FACT_COMPARE_FACT'], source_data_tuple)
            db.commit()
            db.close()

    def update_instance(self):
        db = OXDB(self.env['VERTICA_DSN'], 'mstr_datamart')
        db.execute(self.config['DELETE_INSTANCE1'], self.data_source_dsn)
        db.execute(self.config['UPDATE_INSTANCE2'], self.data_source_dsn)
        db.execute(self.config['UPDATE_INSTANCE3'], self.data_source_dsn)
        db.execute(self.config['UPDATE_INSTANCE4'], self.data_source_dsn)
        db.commit()
        db.close()

    def start_compare(self, table_name):
        instance_id = 4
        column_list = self.get_column_list(table_name)
        self.logger.info(table_name)
        mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
        daily_partition_key_list = []

        columns = self.get_required_fact_columns(column_list, table_name)
        if 'fact' in table_name:
            if '_hourly_fact' in table_name:
                table_list = self.get_table_list()
                base_name = table_name.replace('_hourly_fact', '')
                print base_name
                print table_list
                daily_table = [t for t in table_list if base_name+'_daily_fact' in t]

                for daily_table_name in daily_table:
                    daily_partition_key = self.get_partition_column(daily_table_name)
                    daily_partition_key_list.append(daily_partition_key)
                print daily_table,daily_partition_key_list
                if 'instance_date_sid' in daily_partition_key_list:
                    partition_key = 'instance_datehour_sid'
                    sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
                    if sql:
                        self.logger.info("SQL QUERY: %s", sql)
                        columns_with_partition_key = [partition_key] + columns
                        self.insert_fact_data(instance_id, table_name, partition_key,columns_with_partition_key, sql)
                if 'advt_date_sid' in daily_partition_key_list:
                    partition_key = 'advt_datehour_sid'
                    sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
                    if sql:
                        self.logger.info("SQL QUERY: %s", sql)
                        columns_with_partition_key = [partition_key] + columns
                        self.insert_fact_data(instance_id, table_name, partition_key,columns_with_partition_key, sql)

            partition_key = self.get_partition_column(table_name)

            if partition_key=='utc_date_sid' and '_hourly_fact' in table_name:
                partition_key='utc_datehour_sid'
            if partition_key == 'advt_date_sid' and '_hourly_fact' in table_name:
                partition_key = 'advt_datehour_sid'
                return
            if partition_key == 'instance_date_sid' and '_hourly_fact' in table_name:
                partition_key = 'instance_datehour_sid'
                return
            if partition_key == '' or partition_key == None or len(partition_key)==0:
                self.logger.info("No partition_key for fact table")
                return

            sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
            self.logger.info("SQL QUERY: %s",sql)
            columns_with_partition_key = [partition_key] + columns
            self.insert_fact_data(instance_id, table_name, partition_key,columns_with_partition_key, sql)

        elif 'dim' in table_name:
            columns = self.get_required_dim_columns(column_list)
            sql = self.generate_sql_for_dim(table_name, columns)
            self.logger.info("SQL QUERY: %s", sql)
            self.insert_dim_data(instance_id, table_name, columns, sql)
        else:
            columns = self.get_required_dim_columns(column_list)
            sql = self.generate_sql_for_dim(table_name, columns)
            self.logger.info("SQL QUERY: %s", sql)
            self.insert_dim_data(instance_id, table_name, columns, sql)

    def start_job(self,table_name):
        if table_name:
            table_list = [table_name]
        else:
            table_list = self.get_table_list()
        print table_list
        table_index = 0
        table_len = len(table_list)

        mysql_db = OXDB(self.env['VERTICA_DSN'],'mstr_datamart')
        populate_table_list = mysql_db.get_executed_cursor(self.config['FIND_HOURLY_TABLE'])
        #print populate_table_list
        for (source_a, table_name_a, source_b, table_name_b) in populate_table_list:
            print source_a, table_name_a, source_b, table_name_b
            self.repupulate_update_records(source_a,table_name_a)
            self.repupulate_update_records(source_a, table_name_b)
        mysql_db.close()

        mysql_db = OXDB(self.env['VERTICA_DSN'], 'mstr_datamart')
        populate_table_list = mysql_db.get_executed_cursor(self.config['FIND_DAILY_TABLE'])
        # print populate_table_list
        for (source_a, table_name_a, source_b, table_name_b) in populate_table_list:
            print source_a, table_name_a, source_b, table_name_b
            self.repupulate_update_records(source_a, table_name_a)
            self.repupulate_update_records(source_a, table_name_b)
        mysql_db.close()

        while table_index < table_len:
            self.start_compare(table_list[table_index])
            table_index = table_index + 1


        """    threads = []
            if table_index < table_len:
                t1 = Thread(target=self.start_compare, args=[table_list[table_index]])
                table_index = table_index + 1
                threads.append(t1)

            if table_index < table_len:
                t2 = Thread(target=self.start_compare, args=[table_list[table_index]])
                table_index = table_index + 1
                threads.append(t2)

            if table_index < table_len:
                t3 = Thread(target=self.start_compare, args=[table_list[table_index]])
                table_index = table_index + 1
                threads.append(t3)

            if table_index < table_len:
                t4 = Thread(target=self.start_compare, args=[table_list[table_index]])
                table_index = table_index + 1
                threads.append(t4)

            for t in threads:
                t.start()
            for t in threads:
                t.join()
            if table_index>=table_len:
                break
            
            """

        self.update_instance()
        LoadState(
            OXDB(self.env['FMYSQL_META_DSN']),
            variable_name=self.yaml_file.replace(".yaml", "")).upsert(datetime.now(), commit=True)


if __name__ == "__main__":

    yaml_file = sys.argv[0].replace(".py", ".yaml")
    startTime = time.time()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''compute stats for table which has missing stats'''))

    parser.add_argument('--source', help='data source')
    parser.add_argument('--table_name', help='Name of the table')
    parser.add_argument('--table_type', help='Type of the table')
    options = parser.parse_args()
    dp = data_compare(yaml_file, options.source, options.table_type)

    try:
        if not dp.lock.getLock():
            dp.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        dp.logger.info(
            "================================================================================================");
        dp.logger.info("Start running CompareFact")

        dp.start_job(options.table_name)

        dp.logger.info("Finished running compare_fact")
        dp.logger.info("Elapsed Time: %s" % (time.time() - startTime))
    except SystemExit:
        pass
    except:
        exc_type, exc_val = sys.exc_info()[:2]
        dp.logger.error("Error: %s, %s", exc_type, exc_val)
        raise
    finally:
        dp.lock.releaseLock()
