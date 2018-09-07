#!/bin/python -u

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
from util.parquet_utils import parquet_utils
from util.spark_utils import get_sql_context, get_missing_values, convert_to_impala_schema
from util.impala_utils import get_hive_create_table_sql, get_drop_table_sql
from util.impala_utils import analyze_stats, invalidate_metadata

ENV = '/var/dw-grid-etl/python/env.yaml'


class SyncDimTable(object):
    """
    Perform sync operation from vertica onto impala for specified dimension.
    """
    def __init__(self, dimension, config, env, debug_level):
        """
        Initialization method.
        :param dimension : Dimension name.
        :param config: Dimension configuration details.
        :param env: Environment configuration details.
        :param debug_level: Debugging level.
        """
        self.dim_config = config
        self.env = env
        self.name = '_'.join([os.path.basename(sys.argv[0].replace(".py", "")), dimension])
        self.lock = JobLock(self.name)
        self.logger = EtlLogger.get_logger(log_name=self.name, log_level=debug_level)

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
                         'password': cfg_parser.get(self.env['VERTICA_DSN'], 'password')}
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
        db_settings = self.get_vertica_jdbc_settings("/etc/odbc.ini")
        vertica_df = sql_context.read.jdbc(db_settings['jdbc_url'],
                                           sql_query,
                                           properties=db_settings['db_properties'])
        return vertica_df

    def get_source_query(self, dimension, last_sync_value):
        select_query = "SELECT * FROM " + self.dim_config['SOURCE_TABLE_NAME']
        filter_value = "epoch >= " + str(last_sync_value)
        source_query = "(" + select_query + " WHERE " + filter_value + ") as " + dimension + "_dim"
        return source_query

    def load_data(self, sql_context, dimension, table_name, target_df):
        """
        Persists the target dataset into a table.
        :param sql_context: hive sql context object.
        :param dimension: Dimension name.
        :param table_name: Table name to persist data.
        :param target_df: target data set.
        :return:
        """
        hdfs_path = self.env['HIVE_BASE_DIR'] + dimension
        parquet_file_obj = parquet_utils(sql_context, self.env, self.logger)
        self.logger.info("Writing dataset for %s at %s", dimension, hdfs_path)
        parquet_file_obj.write_data(table_name, target_df, None, None, hdfs_path)
        self.logger.info("Writing dataset for %s completed.", dimension)

    def modify_table(self, dimension, sql_context, new_columns_schema):
        self.logger.info("Adding columns to table %s for dimension %s",
                         self.dim_config['TARGET_TABLE_NAME'], dimension)
        columns_definition = ','.join([' '.join(value) for value in new_columns_schema])
        alter_sql = 'ALTER TABLE ' + self.dim_config['TARGET_TABLE_NAME'] + " ADD COLUMNS (" \
                    + columns_definition + ")"
        self.logger.info("Adding columns sql : %s", alter_sql)
        sql_context.sql(alter_sql)

    def create_table(self, dimension, sql_context, table_name, table_schema):
        self.logger.info("Creating % table for %s", table_name, dimension)
        table_schema = ','.join([' '.join(value) for value in table_schema])
        create_sql = get_hive_create_table_sql(table_name, table_schema,
                                               self.env['HIVE_BASE_DIR'] + dimension)
        drop_sql = get_drop_table_sql(table_name)
        self.logger.info("Drop table sql: %s", drop_sql)
        sql_context.sql(drop_sql)
        self.logger.info("Create table sql : %s", create_sql)
        sql_context.sql(create_sql)

    def get_merged_dataset(self, dimension, source_df, destination_df):
        self.logger.info("Creating alias for source and destination data frames")
        src_df = source_df.alias("src_df")
        dest_df = destination_df.alias("dest_df")
        src_on_condition = []
        dest_on_condition = []
        where_condition = []
        if not self.dim_config.get('KEYS') or len(self.dim_config['KEYS'].strip()) == 0:
            self.logger.info("Join condition keys cannot be empty.")
            raise Exception("Join condition keys are empty for %s dimension configuration."
                            % dimension)
        for key in self.dim_config['KEYS'].split(','):
            src_on_condition.append("src_df." + key.strip())
            dest_on_condition.append("dest_df." + key.strip())
            where_condition.append("src_df." + key.strip() + " IS NULL ")
        on_condition = [col(dest) == col(src) for (dest, src) in zip(dest_on_condition, src_on_condition)]
        self.logger.info("where condition %s", where_condition)
        self.logger.info("on condition %s", on_condition)
        self.logger.info("Performing LEFT JOIN operation.")
        joined_df = dest_df.join(
            src_df,
            on_condition,
            'left_outer'
        ).where(' and '.join(where_condition)
                ).select("dest_df.*")
        self.logger.info("Performing UNION operation.")
        union_df = joined_df.unionAll(src_df)
        return union_df

    def sync_dimension(self, dimension, sql_context):
        """
        Syncs the dimension from vertica to impala.
        :param dimension: Dimension to sync
        :param sql_context: Hive sql_context object.
        :return:
        """
        staging_table_name = None
        sync_label_name = dimension + '_sync_value'
        if self.dim_config.get('IS_FULL_SYNC'):
            self.logger.info("Full sync parameter for %s set to True.", dimension)
            last_sync_value = 0
        else:
            self.logger.info("Retrieving last sync value for %s", sync_label_name)
            db = OXDB(self.env['FMYSQL_META_DSN'])
            last_sync_value = LoadState(db, variable_name=sync_label_name).select()
            last_sync_value = last_sync_value.variable_value if last_sync_value else 0
            self.logger.info("Last sync value for %s is %s", sync_label_name, last_sync_value)
            db.close()

        self.logger.info("Retrieving the current sync value.")
        sync_value_query = "( SELECT MAX(epoch) as value FROM " + \
                           self.dim_config['SOURCE_TABLE_NAME'] + " ) as sync_value"
        self.logger.info("Current sync value SQL : %s", sync_value_query)
        current_sync_value = self.get_vertica_df(sql_context, sync_value_query).collect()[0][0]
        self.logger.info("Current source sync value %s", current_sync_value)

        self.logger.info("Verifying if there is a change in source data.")
        if int(current_sync_value) == int(last_sync_value):
            self.logger.info("No change in source data. "
                             "Source Sync value :%s  Destination sync value : %s", current_sync_value, last_sync_value)
            return
        sync_value = last_sync_value if int(last_sync_value) < int(current_sync_value) else current_sync_value

        self.logger.info("Creating source %s data frame", dimension)
        source_query = self.get_source_query(dimension, sync_value)
        self.logger.info("Source data frame query :%s", source_query)
        source_df = self.get_vertica_df(sql_context, source_query)
        self.logger.info("Converting source columns into impala compatible columns.")
        cols = convert_to_impala_schema(source_df.schema)
        source_df = source_df.select(*cols)
        self.logger.info("Source schema: %s", source_df.dtypes)

        self.logger.info("Creating destination table data frame for %s", dimension)
        try:
            destination_df = sql_context.table(self.dim_config['TARGET_TABLE_NAME'])
            is_table_created = False
        except Exception:
            self.logger.info("Creating %s table.", self.dim_config['TARGET_TABLE_NAME'])
            self.create_table(dimension, sql_context, self.dim_config['TARGET_TABLE_NAME'],
                              source_df.dtypes)
            destination_df = sql_context.table(self.dim_config['TARGET_TABLE_NAME'])
            is_table_created = True
        self.logger.info("Destination schema: %s", destination_df.dtypes)

        self.logger.info("Comparing source %s and destination schema %s",
                         self.dim_config['SOURCE_TABLE_NAME'], self.dim_config['TARGET_TABLE_NAME'])
        modified_columns = get_missing_values(destination_df.dtypes, source_df.dtypes)
        new_columns = get_missing_values(source_df.dtypes, destination_df.dtypes)
        is_columns_modified = True if len(modified_columns) > 0 else False
        is_columns_added = True if len(new_columns) > 0 else False

        if (is_columns_modified or is_columns_added or is_table_created) and last_sync_value > 0:
            self.logger.info("Columns are modified.Performing full sync on dimension %s", dimension)
            last_sync_value = 0
            source_query = self.get_source_query(dimension, last_sync_value)
            self.logger.info("Source data frame query : %s", source_query)
            source_df = self.get_vertica_df(sql_context, source_query)

        if is_columns_added and not is_columns_modified:
            self.modify_table(dimension, sql_context, new_columns)
            self.logger.info("Table schema changed.Creating destination data frame")
            destination_df = sql_context.table(self.dim_config['TARGET_TABLE_NAME'])
            self.logger.info("Schema after adding columns \n %s", destination_df.dtypes)

        if is_columns_modified:
            staging_table_name = self.dim_config['TARGET_TABLE_NAME'] + '_staging'
            self.create_table(dimension, sql_context, staging_table_name, source_df.dtypes)

        self.logger.info("Source data row count : %s", source_df.count())
        self.logger.info("Destination data row count : %s", destination_df.count())

        self.logger.info("Creating transformed dataset for %s", dimension)
        if (not (is_columns_added or is_columns_modified)) and last_sync_value > 0:
            transformed_df = self.get_merged_dataset(dimension, source_df, destination_df
                                                     ).repartition(1)
        else:
            transformed_df = source_df

        self.logger.info("Transformed data set row count %s", transformed_df.count())

        self.logger.info("Creating target data frame for %s", dimension)
        if is_columns_modified or is_table_created:
            target_df = transformed_df.select('*')
        else:
            target_df = transformed_df.select(*destination_df.columns)

        self.logger.info("Target data set row count %s", target_df.count())

        table_name = self.dim_config['TARGET_TABLE_NAME'] \
            if not staging_table_name else staging_table_name
        self.logger.info("Persisting %s dimension dataset in %s", dimension, table_name)
        self.load_data(sql_context, dimension, table_name, target_df)
        self.logger.info("Data persistence for %s dimension completed.", dimension)

        if is_columns_modified:
            # sql_context affects only hive schema.
            # Schema change done by sql context is not reflected until impala table
            # metadata invalidation is performed.
            drop_sql = get_drop_table_sql(self.dim_config['TARGET_TABLE_NAME'])
            rename_sql = "ALTER TABLE " + staging_table_name + " RENAME TO " \
                         + self.dim_config['TARGET_TABLE_NAME']
            self.logger.info("Dropping table %s", self.dim_config['TARGET_TABLE_NAME'])
            sql_context.sql(drop_sql)
            self.logger.info("Renaming table %s to %s", staging_table_name,
                             self.dim_config['TARGET_TABLE_NAME'])
            sql_context.sql(rename_sql)
            self.logger.info("Invalidating and computing stats for table %s",
                             self.dim_config['TARGET_TABLE_NAME'])
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            invalidate_metadata(impala_db, self.dim_config['TARGET_TABLE_NAME'])
            analyze_stats(impala_db, self.dim_config['TARGET_TABLE_NAME'])
            invalidate_metadata(impala_db, staging_table_name, is_refresh=False)
            impala_db.close()

        db = OXDB(self.env['FMYSQL_META_DSN'])
        if is_columns_modified:
            update_sql = "UPDATE partition_status SET table_name = ? " \
                         + "WHERE table_name = ? AND is_used_by_impala = 1"
            self.logger.info("Updating partition status table. Query %s", update_sql)
            db.execute(update_sql, self.dim_config['TARGET_TABLE_NAME'], staging_table_name)
        self.logger.info("Updating the last sync value for %s ", dimension)
        LoadState(db, variable_name=sync_label_name
                  ).upsert(variable_value=current_sync_value)
        self.logger.info("Updating %s value.", dimension + '_dim_updated')
        LoadState(db, variable_name=dimension + '_updated').upsert(commit=True)
        db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent(''' Sync the specified dimensions from source to destination.
         Specify either config_file_path to load all the configured dimensions sequentially or
         specify dim_config to sync only a single specified dimension.'''))

    parser.add_argument('--config_file_path', help='Name of the configuration file path.')
    parser.add_argument('--dim_config',
                        help=textwrap.dedent('''Dimension configuration in the following format
                        {<<DIM_NAME>>:<<DIM_CONFIG>>}
                        DIM_CONFIG =
                        {'SOURCE_TABLE_NAME':<src_schema.src_tbl_name>,
                         'TARGET_TABLE_NAME':<tgt_schema.tgt_tbl_name>,
                         'KEYS':<key1,key2>,'IS_FULL_SYNC':<True/False>
                        } '''))
    parser.add_argument('--log_level', type=str, choices=['INFO', 'DEBUG'],
                        default='INFO', help='Logging level. Default is INFO.')
    options = parser.parse_args()

    try:
        err_msg = ""
        if options.config_file_path:
            dimensions = yaml.load(open(options.config_file_path))['DIMENSIONS']
            name = os.path.basename(options.config_file_path.replace(".yaml", "")).lower()

        if options.dim_config:
            err_msg = "Unable to load supplied json configuration value."
            dimensions = json.loads(options.dim_config)
            err_msg = ""
            name = '_'.join(dimensions.keys()).lower()

        app_name = '_'.join([os.path.basename(sys.argv[0].replace(".py", "")), name])

        sql_context = get_sql_context(app_name)

        env_config = yaml.load(open(ENV))
        for dim in sorted(dimensions.keys()):
            try:
                sync_dim_obj = SyncDimTable(dim.lower(), dimensions.get(dim), env_config, options.log_level)
                job_start = datetime.now()
                sync_dim_obj.logger.info("Starting sync %s dimension job at %s", dim.lower(), job_start)
                if sync_dim_obj.lock.get_lock():
                    sync_dim_obj.sync_dimension(dim.lower(), sql_context)
                    sync_dim_obj.logger.info("Sync %s dimension job completed in %s seconds", dim.lower(),
                                         (datetime.now()-job_start).seconds)
                else:
                    sync_dim_obj.logger.info("Unable to get lock for %s dimension. Exiting..", dim.lower())
            except Exception as error:
                sync_dim_obj.logger.error("Sync %s dimension job FAILED. Error: %s", dim.lower(), error)
            finally:
                sync_dim_obj.lock.release_lock()
    except ValueError as err:
        raise Exception("Invalid parameter value. %s ERROR : %s " % (err_msg, err))
    except Exception as err:
        raise Exception("Dimensional sync job FAILED. ERROR :%s" % err)
