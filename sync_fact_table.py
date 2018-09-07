#!/opt/bin/python -u

import os
import sys
import argparse
import yaml
import textwrap
import json

from pyspark.sql.functions import col
from ConfigParser import ConfigParser
from datetime import datetime

from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.oxdb import OXDB
from util.load_state import LoadState

from util.spark_utils import get_sql_context, get_missing_values, convert_to_impala_schema
from util.parquet_utils import parquet_utils
import threading


class sync_fact_table(object):
    """
    Perform sync operation for fact data and also revenue adjusment data
    """

    def __init__(self, yaml_file, ds_interval=None):
        """
        Initialization method.
        :param yaml_file : fact table yaml file
        """
        self.config = yaml.load(open(yaml_file))
        self.env = yaml.load(open(self.config['ENV']))
        self.name = yaml_file.replace(".yaml", "")
        if ds_interval:
            self.lock = JobLock(self.name + ds_interval)
            self.logger = EtlLogger.get_logger(log_name=self.name + ds_interval)
        else:
            self.lock = JobLock(self.name)
            self.logger = EtlLogger.get_logger(log_name=self.name)

    def get_vertica_jdbc_settings(self, config_file_path):
        """
        Reads odbc.ini configuration file and creates vertica jdbc settings.
        :param config_file_path: odbc.ini file location.
        :return: vertica jdbc settings object.
        """
        self.logger.info("Retrieving vertica jdbc settings from odbc.ini")
        cfg_parser = ConfigParser()
        cfg_parser.read(config_file_path)
        jdbc_url = "jdbc:vertica://" + cfg_parser.get(self.env['VERTICA_DSN'], 'servername') \
                   + ":" + cfg_parser.get(self.env['VERTICA_DSN'], 'port') + "/" \
                   + cfg_parser.get(self.env['VERTICA_DSN'], 'database')

        db_properties = {'driver': 'com.vertica.jdbc.Driver',
                         'user': cfg_parser.get(self.env['VERTICA_DSN'], 'username'),
                         'password': cfg_parser.get(self.env['VERTICA_DSN'], 'password')
                         }

        jdbc_settings = {'jdbc_url': jdbc_url, 'db_properties': db_properties}
        return jdbc_settings

    def get_vertica_df(self, sql_context, sql_query):
        """
        Retrieve data from vertica and creates a data frame.
        :param sql_context: Hive sql context object.
        :param sql_query: query or table name to be executed on vertica.
                       Example1: SELECT * FROM TABLE
                       Example2: TABLE_SCHEMA.TABLE_NAME
        :return: data frame object
        """
        jdbc_settings = self.get_vertica_jdbc_settings('/etc/odbc.ini')

        vertica_df = sql_context.read.jdbc(
            jdbc_settings['jdbc_url'],
            sql_query,
            column="rowid%10",
            lowerBound=0,
            upperBound=10,
            numPartitions=10,
            properties=jdbc_settings['db_properties']
        )
        return vertica_df

    def write_dataset(self, fact_df, partition_value, hdfs_path, republishing_data,
                      dynamic_partition_column, additional_partition_columns=[], write_mode='overwrite'):

        """
        repartition dataframe and write the data
        :param fact_df dataframe that will be written to a path
        :param partition_value partition value need to be added in base table
        :param hdfs_path path to hadoop storage
        :param republishing_data if data is republished this is true
        :param dynamic_partition_column dynamic partition columns
        :param additional_partition_columns addition column for partition
        :param write_mode could be overwrite or append
        :return: None
        """
        source_type = fact_df.dtypes
        dest_df = sql_context.table(self.config['FACT_TABLE'])
        dest_type = dest_df.dtypes
        dest_columns = dest_df.columns
        parquet_file_obj = parquet_utils(sql_context, self.env, self.logger)
        column_names = parquet_file_obj.get_matched_type(source_type, dest_type, dest_columns)
        self.logger.info(str(column_names))
        fact_df = fact_df.selectExpr(column_names)

        target_partitioncount_dict = self.config['TARGET_PARTITIONCOUNT_DICT']
        repartitioned_df = parquet_file_obj.repartition_for_dynamic_partition_write(fact_df, "platform_timezone",
                                                                                    target_partitioncount_dict,
                                                                                    default_target_partitioncount=1,
                                                                                    additional_partition_columns=additional_partition_columns)
        parquet_file_obj.write_data(self.config['FACT_TABLE'],
                                    repartitioned_df,
                                    self.config['FACT_PARTITION_KEY'],
                                    partition_value,
                                    hdfs_path,
                                    target_file_count=None,
                                    validate_schema=True,
                                    is_republish=republishing_data,
                                    dynamic_partition_column=dynamic_partition_column,
                                    job_name=self.name,
                                    write_mode=write_mode,
                                    analyze_using='impala',
                                    compute_stats=False,
                                    insert_partition_info=False
                                    )

    def load_data(self, sql_context, feed_date=None):
        """
        Retrieve data from vertica and creates a data frame.
        :param sql_context: Hive sql context object.
        find the data that's need to be loaded from vertica to impala
        :return: None
        """
        condition = ""
        if feed_date:
            condition = " WHERE rs_utc_hour='" + str(feed_date) + "'"

        vertica_db = OXDB(self.env['VERTICA_DSN'], schema='mstr_datamart')
        vertica_data = vertica_db.get_executed_cursor(
            self.config['GET_ROLLUP_STATE'] + condition).fetchall()
        vertica_db.close()
        mysql_db = OXDB(self.env['FMYSQL_META_DSN'])
        mysql_data = mysql_db.get_executed_cursor(
            self.config['GET_ROLLUP_STATE'] + condition).fetchall()
        mysql_db.close()

        vertica_dict = {}
        mysql_dict = {}

        master_data_list = [tuple(data) for data in vertica_data]
        target_data_list = [tuple(data) for data in mysql_data]

        data_changed = [value[0] for value in master_data_list if value not in target_data_list]

        for data in vertica_data:
            vertica_dict[data[0]] = data[1:]
        for data in mysql_data:
            mysql_dict[data[0]] = data[1:]

        data_changed.sort()
        self.logger.info(str(data_changed[0:10]))
        for rs_utc_hour in data_changed[0:10]:
            [vertica_new_input_data, vertica_has_oxts, vertica_has_conv, vertica_needs_rs_run,
             vertica_republishing_data] = vertica_dict[rs_utc_hour]

            is_revenue_sync_needed = False
            where_clause = ""
            job_name = self.name
            write_mode = 'overwrite'
            is_utc_sync_needed = True
            # check condition to whether run revenue adjusment sync of utc hour sync
            if rs_utc_hour in mysql_dict:
                [mysql_new_input_data, mysql_has_oxts, mysql_has_conv, mysql_needs_rs_run, mysql_republishing_data] = \
                    mysql_dict[rs_utc_hour]

                if vertica_republishing_data == 1:
                    write_mode = 'overwrite'
                    is_utc_sync_needed = True
                elif mysql_has_conv == 1 and mysql_has_oxts == 1 and vertica_has_conv == 1 and vertica_has_oxts == 1 \
                        and vertica_new_input_data == 0 and vertica_needs_rs_run == 0:
                    is_revenue_sync_needed = True
                    write_mode = 'overwrite'
                    is_utc_sync_needed = False
                elif mysql_has_conv == 1 and mysql_has_oxts == 0 and vertica_has_oxts == 1 and vertica_new_input_data == 1:
                    where_clause = ""
                    write_mode = 'overwrite'
                    is_utc_sync_needed = True
                elif mysql_has_conv == 0 and mysql_has_oxts == 1 and vertica_has_conv == 1 and vertica_new_input_data == 1:
                    where_clause = ""
                    write_mode = 'overwrite'
                    is_utc_sync_needed = True
            else:
                if vertica_has_oxts == 1 and vertica_has_conv == 1 and vertica_new_input_data == 0 and vertica_needs_rs_run == 0:
                    is_revenue_sync_needed = True

            if is_utc_sync_needed:
                # get data from vertica and create dataframe

                load_query = self.config['FACT_LOAD_UTC_QUERY'].replace('low', rs_utc_hour.strftime('%Y%m%d')).replace(
                    'high', rs_utc_hour.strftime('%H')).replace('where_clause', where_clause)
                load_query = "( " + load_query + ") utc"
                hdfs_path = self.env['HIVE_BASE_DIR'] + self.config['FACT_TABLE'].split('.')[1] + \
                            "/utc_date_sid=" + rs_utc_hour.strftime('%Y%m%d') + "/" + "utc_hour_sid=" + str(
                    int(rs_utc_hour.strftime('%H')))
                partition_value = rs_utc_hour.strftime('%Y%m%d') + "," + str(int(rs_utc_hour.strftime('%H')))
                self.logger.info(load_query)
                fact_df = self.get_vertica_df(sql_context, load_query)

                # perform write data using dynamic partition and update rollupstate
                self.write_dataset(
                    fact_df,
                    partition_value,
                    hdfs_path,
                    vertica_republishing_data,
                    ['platform_timezone'],
                    [],
                    write_mode,
                )
                self.update_rollup_state(
                    rs_utc_hour,
                    1,
                    vertica_has_oxts,
                    vertica_has_conv,
                    vertica_needs_rs_run,
                    0,
                    vertica_republishing_data,
                )
                db = OXDB(self.env['FMYSQL_META_DSN'])
                LoadState(
                    db, variable_name='grid_oxts_last_hour_loaded'
                ).upsert(rs_utc_hour, commit=True)
                db.close()
                self.logger.info("load complete for %s", str(rs_utc_hour))

            if is_revenue_sync_needed:
                # get ready platform
                platforms_sql = self.config['GET_READY_PLATFORMS'].replace('?', "'" + str(rs_utc_hour) + "'")
                platforms_df = sql_context.sql(platforms_sql)
                platforms_df.registerTempTable("tmp_daily_platform_ready")
                # get range of instance date sids for these platforms
                instance_date_sid_range = sql_context.sql(self.config['GET_INSTANCE_DATE_SID_RANGE']).collect()
                start_instance_date_sid = instance_date_sid_range[0][0]
                end_instance_date_sid = instance_date_sid_range[0][1]
                self.logger.debug("start_instance_date_sid = %s", start_instance_date_sid)
                self.logger.debug("end_instance_date_sid = %s", end_instance_date_sid)

                if start_instance_date_sid:
                    # possible rollup work to do - proceed with running support queries and main aggregation
                    self.instance_date_sid = start_instance_date_sid

                    # get range of utc date sids for these platforms
                    utc_date_sid_range = sql_context.sql(self.config['GET_UTC_DATE_SID_RANGE']).collect()
                    start_utc_date_sid = utc_date_sid_range[0][0]
                    end_utc_date_sid = utc_date_sid_range[0][1]
                    self.logger.debug("start_utc_date_sid = %s", start_utc_date_sid)
                    self.logger.debug("end_utc_date_sid = %s", end_utc_date_sid)

                    # get start and end utc hour sids for these platforms
                    utc_hour_sid_df = sql_context.sql(self.config['GET_UTC_HOUR_SID_RANGE'])
                    utc_hour_sid_range = utc_hour_sid_df.collect()
                    start_utc_hour_sid = utc_hour_sid_range[0][0]
                    end_utc_hour_sid = utc_hour_sid_range[0][1]
                    self.logger.debug("start_utc_hour_sid = %s", start_utc_hour_sid)
                    self.logger.debug("end_utc_hour_sid = %s", end_utc_hour_sid)

                    # get distinct platform timezones for those platforms and build sql clause to put into where clause
                    platform_timezones = sql_context.sql(self.config['GET_PLATFORM_TIMEZONES']).collect()
                    platform_timezone_list = ''
                    for result in platform_timezones:
                        platform_timezone_list += "'{0}', ".format(result[0])
                    self.logger.info("processing timezones: %s", platform_timezone_list[:-2])
                    platform_timezone_clause = "AND sd.platform_timezone in (" + platform_timezone_list[:-2] + ")"
                    self.logger.debug("platform_timezone_clause: %s", platform_timezone_clause)
                    self.platform_timezones = platform_timezones

                    aggregate_sql = self.config['GET_BASE_FACT']
                    aggregate_sql_parsed = aggregate_sql % (
                        start_utc_date_sid, start_utc_hour_sid, end_utc_date_sid, end_utc_hour_sid,
                        platform_timezone_clause)
                    self.logger.info("aggregate_sql_parsed\n\n%s\n\n", aggregate_sql_parsed)
                    aggregate_df = sql_context.sql(aggregate_sql_parsed)

                    load_query = self.config['FACT_LOAD_PLATFORM_QUERY'] % (rs_utc_hour, rs_utc_hour, rs_utc_hour,
                                                                            start_utc_date_sid, end_utc_date_sid)
                    load_query = "( " + load_query + ") platform"

                    vertica_df = self.get_vertica_df(sql_context, load_query)
                    vertica_df.cache()

                    update_column_list = ['tot_publisher_revenue', 'tot_network_revenue',
                                          'tot_publisher_cpd_revenue',
                                          'tot_network_cpd_revenue', 'tot_spend']
                    columns = sql_context.table(self.config['FACT_TABLE']).columns

                    select_columns = []
                    for column_name in columns:
                        if column_name not in update_column_list:
                            select_columns.append(col('base.' + column_name))

                    # join with base table and modify the existing metrics
                    fact_df = aggregate_df.alias('base'
                                                 ).join(vertica_df.alias('tmp_new_revenue'
                                                                         ), ['rowid'], 'inner'
                                                        ).select(select_columns
                                                                 + [col('tmp_new_revenue.tot_publisher_revenue'),
                                                                    col('tmp_new_revenue.tot_network_revenue'),
                                                                    col('tmp_new_revenue.tot_publisher_cpd_revenue'),
                                                                    col('tmp_new_revenue.tot_network_cpd_revenue'),
                                                                    col('tmp_new_revenue.tot_spend')])

                    hdfs_path = self.env['HIVE_BASE_DIR'] + self.config['FACT_TABLE'].split('.')[1]
                    partition_value = ""
                    self.write_dataset(
                        fact_df,
                        partition_value,
                        hdfs_path,
                        vertica_republishing_data,
                        ['utc_date_sid', 'utc_hour_sid', 'platform_timezone'],
                        ['utc_date_sid', 'utc_hour_sid'],
                        write_mode,
                    )

                self.update_rollup_state(
                    rs_utc_hour,
                    0,
                    vertica_has_oxts,
                    vertica_has_conv,
                    vertica_needs_rs_run,
                    1,
                    vertica_republishing_data,
                )
                self.logger.info("load complete for %s", str(rs_utc_hour))

    def update_rollup_state(self, rs_utc_hour, new_input_data, has_oxts, has_conv,
                            needs_rs_run, new_rollup_data, republishing_data):
        """
        upadte rollup state table
        :param rs_utc_hour, new_input_data, has_oxts, has_conv,needs_rs_run, new_rollup_data, republishing_data
        :return: None
        """
        db = OXDB(self.env['FMYSQL_META_DSN'])
        db.execute("SET tx_isolation = 'READ-COMMITTED'")
        db.execute(self.config['INSERT_ROLLUP_STATE'], (rs_utc_hour,
                                                        new_input_data,
                                                        has_oxts,
                                                        has_conv,
                                                        needs_rs_run,
                                                        new_rollup_data,
                                                        republishing_data,
                                                        new_input_data,
                                                        has_oxts,
                                                        has_conv,
                                                        needs_rs_run,
                                                        new_rollup_data,
                                                        republishing_data))
        db.commit()
        db.close()




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''Sync Fact table based upon query and check'''))

    parser.add_argument('--table_name', help='Name of the table')
    parser.add_argument('--ds_interval', help="ds interval")
    options = parser.parse_args()

    yaml_file = options.table_name + ".yaml"

    if options.ds_interval:
        if options.ds_interval.find('_') > 0:
            feed_date = datetime.strptime("%s UTC" % options.ds_interval, '%Y-%m-%d_%H %Z')
        else:
            feed_date = datetime.strptime("%s UTC" % options.ds_interval, '%Y-%m-%d %Z')

    sync_fact_obj = sync_fact_table(yaml_file, options.ds_interval)
    try:
        job_start = datetime.now()

        if not sync_fact_obj.lock.getLock():
            sync_fact_obj.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        if options.ds_interval:
            sync_fact_obj.logger.info("Start running Sync Fact at  %s", job_start)
            sql_context = get_sql_context(options.table_name + options.ds_interval)
            sql_context.setConf("hive.exec.dynamic.partition", "true")
            sql_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            sync_fact_obj.load_data(sql_context, feed_date)
        else:
            sql_context = get_sql_context(options.table_name)
            sql_context.setConf("hive.exec.dynamic.partition", "true")
            sql_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            sync_fact_obj.load_data(sql_context)
        job_end = datetime.now()
        sync_fact_obj.logger.info("Finished running Sync Fact at  %s", job_end)
        sync_fact_obj.logger.info("Total elapsed time = %s seconds", (job_end - job_start))

    except SystemExit:
        pass
    except:
        exc_type, exc_val = sys.exc_info()[:2]
        sync_fact_obj.logger.error("Error: %s, %s", exc_type, exc_val)
        raise
    finally:
        sync_fact_obj.lock.releaseLock()