"""This class is responsible for reading the data from either hive table or ODFI and tranform the data to create a result dataframe
:
Load from ODFI
> spark-submit --jars /var/dw-grid-etl/lib/spark-odfi-datasource-assembly-1.0.jar --master yarn-client  --num-executors 17 --executor-cores 1 --executor-memory 10G /var/dw-grid-etl/python/fact_load.py ox_openrtb_sum_hourly 2016-08-02_10LOAD FROM hive table
> spark-submit --jars /var/dw-grid-etl/lib/spark-odfi-datasource-assembly-1.0.jar --master yarn-client  --num-executors 17 --executor-cores 1 --executor-memory 10G /var/dw-grid-etl/python/fact_load.py ox_openrtb_sum_hourly 2016-08-02_10 --hive_table odfi.ox_openrtb_sum_hourly_fact
-----------------------------------------------------------------------------------------------------------------------
"""
# !/bin/python -u
import argparse
import sys
import yaml
import re
import ast
import sqlparse
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import lit

from pyspark.sql import functions as F
from datetime import datetime, timedelta
from util.load_state import LoadState
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.fact_rollup import fact_rollup
from util.oxdb import OXDB
from util.parquet_utils import parquet_utils
from util.odfi_utils import get_datasets_schema

class FactLoad(object):
    def __init__(self, cmd_options):
        """
        Initializes the class variables
        :param cmd_options: User supplied console parameters.
        """
        self.job_name = cmd_options.job_name
        self.hive_table = cmd_options.hive_table
        fact_load_name = sys.argv[0]
        fact_load_config_name = fact_load_name.replace(".py", ".yaml")
        self.fact_config = yaml.load(open(fact_load_config_name))
        self.job_config = yaml.load(open(self.job_name + ".yaml"))
        self.env = yaml.load(open(self.job_config['ENV']))
        self.logger = EtlLogger.get_logger(self.job_name, log_level = self.job_config['LOG_LEVEL'])
        self.lock = JobLock(self.job_name)
        if not self.job_config.get('FEED_NAME'):
            raise Exception("Invalid feed name defined for %s job", self.job_name)
        else:
            self.feed_name = self.job_config['FEED_NAME']

    def get_feed_stats(self, sql_label, *params):
        """
        Executes the specified sql label info to retrieve the stats information of executing feed.
        :param: sql_label - sql statement to be executed
        :param: params - list of parameters for the sql statement.
        :return: The result from the sql_label execution.
        """
        try:
            self.logger.info("Executing label %s to retrieve stats for job %s", sql_label, self.job_name)
            db = OXDB(self.env['FMYSQL_META_DSN'])
            db_result = db.get_executed_cursor(
                self.fact_config[sql_label],
                *params
            ).fetchall()
            self.logger.info("%s job %s label execution result %s", self.job_name, sql_label, str(db_result))
            return db_result

        except Exception as err:
            raise Exception("%s job %s label execution failed. Error %s" % (self.job_name, sql_label, err))
        finally:
            db.close()

    def store_ds_metadata(self, ds_metadata, load_start_time, load_end_time):
        """
        Stores the last loaded dataset details in checkpoint and load state table.
        :param ds_metadata: ODFI dataset metadata
        :param load_start_time: Dataset load start time
        :param load_end_time: Dataset load end time
        :return: Success: Inserts a record into checkpoint table.
                 Failure: Job exists with an exception.
        """
        db = OXDB(self.env['FMYSQL_META_DSN'])
        try:
            self.logger.info("Updating load state variable %s", self.job_name)
            LoadState(
                db, variable_name=self.job_name
            ).update_variable_datetime(ds_metadata['startTimestamp'])

            self.logger.info(
                "Inserting entry into checkpoint table for feed %s dataset interval %s",
                self.feed_name, ds_metadata['readableInterval']
            )
            get_checkpoint_data=db.get_rows(self.fact_config['CHECK_IF_EXISTS'],self.job_name,
                ds_metadata['name'], ds_metadata['startTimestamp'], ds_metadata['serial'],
                ds_metadata['id'])

            if get_checkpoint_data[0][0]==None:
                self.logger.info("inserting new checkpoint info")
                db.execute(
                    self.fact_config['INSERT_CHECK_POINT'], self.job_name,
                    ds_metadata['name'], ds_metadata['startTimestamp'], ds_metadata['serial'],
                    ds_metadata['id'], ds_metadata['revision'],
                    ds_metadata['recordCount'], ds_metadata['dataSize'],
                    ds_metadata['recordCount'], load_start_time,
                    load_end_time, ds_metadata['dateCreated'],
                    commit=True
                )

            else:
                self.logger.info("updating existing checkpoint info")
                db.execute(
                    self.fact_config['UPDATE_CHECK_POINT'], self.job_name,
                    ds_metadata['name'], ds_metadata['startTimestamp'], ds_metadata['serial'],
                    ds_metadata['id'], ds_metadata['revision'],
                    ds_metadata['recordCount'], ds_metadata['dataSize'],
                    ds_metadata['recordCount'], load_start_time,
                    load_end_time, ds_metadata['dateCreated'],
                    self.job_name,
                    ds_metadata['name'],
                    ds_metadata['startTimestamp'],
                    ds_metadata['serial'],
                    ds_metadata['id'],
                    commit=True
                )


            self.logger.info(
                "Successfully recorded entries into checkpoint and load state table for job %s " +
                " dataset interval %s", self.job_name, ds_metadata['startTimestamp']
            )
        except Exception as err:
            raise Exception(
                "Checkpoint and load state table entry failed for %s job dataset interval %s. Error: %s"
                % (self.job_name, ds_metadata['readableInterval'], err)
            )
        finally:
            db.close()

    def get_odfi_metadata(self, ds_serial_no):
        """
        Retrieves odfi metadata from serial no greater than specified serial number.
        :param ds_serial_no: serial number from which odfi metadata is retrieved.
        :return: ODFI datasets for running feed.
        """
        odfi_host = self.env['ODFI_HOST']
        with open(self.env['ODFI_CRED_PATH'], 'r') as odfi_cred_file:
            odfi_user, odfi_passwd = \
                odfi_cred_file.read().strip('\n').split(':')
        ds_metadata = get_datasets_schema(
            odfi_host, odfi_user, odfi_passwd, self.feed_name,
            ds_serial_no, self.logger
        )
        return ds_metadata


    def check_dataset_republish(self, ds_interval):
        """
        Performs the following checks:
            - Identifies if the feed is daily or hourly feed.
            - Verifies if the dataset load is in sequential order.
            - Verifies if the load is for republished dataset.
        :param ds_interval: current loading dataset interval
        :return: Returns true/false for dataset republish value.
        """
        try:
            ds_republish = False
            self.logger.info("Retrieving last loaded hour for job %s", self.job_name)
            last_ds_hour = self.get_feed_stats('GET_LOAD_STATE', self.job_name)[0][0]
            if last_ds_hour:
                last_ds_hour = datetime.strptime("%s UTC" % last_ds_hour, '%Y-%m-%d %H:%M:%S %Z')

            self.logger.info("Identifying %s is daily/hourly feed", self.feed_name)
            if ds_interval.find('_') >= 0:
                self.logger.info("%s is an hourly feed", self.feed_name)
                date_format = '%Y-%m-%d_%H %Z'
                minutes_value = 60
            else:
                self.logger.info("%s is a daily feed", self.feed_name)
                date_format = '%Y-%m-%d %Z'
                minutes_value = 1440

            self.logger.info(
                "Verifying %s job sequential load and republish for dataset interval %s",
                self.job_name, ds_interval
            )
            if last_ds_hour is not None:
                current_ds_hour = datetime.strptime("%s UTC" % ds_interval, date_format)
                self.logger.info("Verifying sequential load for job %s", self.job_name)
                if current_ds_hour - last_ds_hour > timedelta(minutes=minutes_value):
                    self.logger.info(
                        "Gap detected for job %s loading from feed %s. Last loaded hour %s and current loaded hour %s"
                        % (self.job_name, self.feed_name, last_ds_hour, current_ds_hour)
                    )
                self.logger.info("Verifying republish for job %s loading from feed %s", self.job_name, self.feed_name)
                if current_ds_hour - last_ds_hour < timedelta(minutes=minutes_value):
                    self.logger.info("%s feed has republished hour %s", self.feed_name, current_ds_hour)
                    ds_republish = True
            self.logger.info(
                "%s feed feed_date is %s and republish value is %s",
                self.feed_name, current_ds_hour, ds_republish
            )
            return ds_republish
        except Exception as err:
            raise Exception(
                "Verifying republish of %s job loading from %s feed for dataset interval %s FAILED. Error: %s"
                % (self.job_name, self.feed_name, ds_interval, err)

            )

    def get_sql_context(self):
        """
        Creates a spark sql context.
        :return: Spark SQL Context object.
        """
        self.logger.info("Creating spark sql context for feed %s", self.feed_name)
        spark_conf = SparkConf().setAppName(self.job_name)
        spark_context = SparkContext(conf=spark_conf).getOrCreate()

        sql_context = HiveContext(spark_context)
        sql_context.setConf("hive.exec.dynamic.partition", "true")
        sql_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


        return sql_context

    def check_transformation_condition(self, check_type,sql_context, feed_date):
        """
        Executes all precondition checks and validates fact load preconditions
        :param sql_context: spark sql context
        :param feed_date: executing feed hour
        :return: Raises exception on precondition check failure.
        """
        if 'CHECK' in self.job_config:
            for pre_check in self.job_config['CHECK']:
                self.logger.info(
                    "Executing pre-check %s for interval %s",
                    pre_check['check']['sql'], feed_date
                )
                row_count = sql_context.sql(
                    pre_check['check']['sql'] % feed_date.strftime('%Y%m%d')
                ).collect()

                if row_count[0][0] == 0:
                    self.logger.info("Result of executing precondition sql is %d", row_count)
                    raise Exception("Precondition check for feed %s FAILED." % self.feed_name)

    def get_odfi_df(self, sql_context, odfi_ds_interval):
        """
        Returns data frame for the specified odfi dataset interval
        :param sql_context: spark sql context
        :param odfi_ds_interval: the odfi dataset interval
        :return: data frame for specified dataset interval
        """
        try:
            self.logger.info(
                "Creating %s odfi interval %s data frame", self.feed_name, odfi_ds_interval
            )
            odfi_host = self.env['ODFI_HOST']
            with open(self.env['ODFI_CRED_PATH'], 'r') as odfi_cred_file:
                odfi_user, odfi_passwd = odfi_cred_file.read().strip('\n').split(':')
            odfi_base_url = odfi_host + "/ODFI/rest"
            odfi_path = "/" + self.feed_name + "/"
            odfi_df = sql_context.read.format("com.openx.spark.odfi"). \
                option("odfibaseurl", odfi_base_url). \
                option("username", odfi_user). \
                option("password", odfi_passwd).load(odfi_path + odfi_ds_interval)
            self.logger.info(
                "Successfully created %s odfi interval %s data frame",
                self.feed_name, odfi_ds_interval
            )
            return odfi_df
        except Exception as err:
            raise Exception(
                "%s odfi interval %s data frame creation failed. Error"
                % (self.feed_name, odfi_ds_interval, err)

            )

    def get_missing_columns(self, current_data_columns, latest_data_columns):
        """
        Compares current and latest data columns and returns missing columns from current dataset.
        :param current_data_columns: list of tuples containing tuples(column_name, column_data_type)
        :param latest_data_columns:  list of tuples containing tuples(column_name, column_data_type)
        :return: Dictionary of "column"/"type" for the columns present only in latest dataset.
        """
        self.logger.info('Converting the tuples into a dictionary(column_name: data_type) values.')
        latest_data_dict = {}
        for tuple_row in latest_data_columns:
            latest_data_dict[tuple_row[0]] = tuple_row[1]

        current_data_dict = {}
        for tuple_row in current_data_columns:
            current_data_dict[tuple_row[0]] = tuple_row[1]

        self.logger.info("Retrieving missing columns from current data set.")
        missing_column_dict = {}
        missing_keys = set(latest_data_dict.keys()) - set(current_data_dict.keys())
        for key in missing_keys:
            missing_column_dict[key] = latest_data_dict[key]
        if len(missing_column_dict) > 0:
            self.logger.info("Missing columns: \n %s", missing_column_dict)

        return missing_column_dict

    def run_sql(self, run_type,sql_context,ds_interval=None):
        try:
            if run_type in self.job_config:
                for checks in self.job_config[run_type]:
                    sql = checks['SQL']
                    if 'SQL_DSN' in checks:
                        dsn=checks['SQL_DSN']
                        db=OXDB(self.env[dsn])
                        sql=sql.replace("?","'"+ds_interval+"'")
                        self.logger.info(sql)
                        db.execute(sql)
                        db.commit()
                        db.close()
                    else:
                        pre_run_df=sql_context.sql(sql)
                        if len(pre_run_df.take(1))>0:
                            content_topic_group_sid = sql_context.sql(
                                    self.job_config['GET_MAX_CONTENT_ID']).collect()
                            content_topic_group_sid = content_topic_group_sid[0][0]
                            pre_run_df = pre_run_df.withColumn('content_topic_group_sid', F.monotonically_increasing_id() + content_topic_group_sid)
                            parquet_file_obj = parquet_utils(sql_context, self.env, self.logger)

                            dest_df = sql_context.table(checks['TABLE'])
                            sql = parquet_file_obj.arrange_column_type(pre_run_df, dest_df)
                            pre_run_df = sql_context.sql(sql)

                            pre_run_df=pre_run_df.unionAll(dest_df)
                            pre_run_df=pre_run_df.coalesce(1)
                            hdfs_path = self.env['HIVE_BASE_DIR'] + checks['TABLE'].split('.')[1]
                            self.logger.info("Writing data content topic")
                            parquet_file_obj.write_data(
                                checks['TABLE'], pre_run_df,
                                None, None, hdfs_path)
                            self.logger.info("finishing Writing Content Topic")

                    self.logger.info("running check query: %s", sql)
        except Exception as exp:
            error = "ETL terminating because of the following exception occurred during the ETL execution: %s" % exp
            self.logger.error(self.logger, error)
            raise exp

    def get_query_column_mapping(self):
        """
        Reads the transformation query and creates mapping dictionary of source to destination.
        :return: Dictionary containing (source_column_name:destination_column_name)
        """
        self.logger.info("Creating staging:destination mapping table for %s", self.feed_name)
        query_map = {}

        acquiring_identifiers = True
        identifiers, aliases = [], []
        res = sqlparse.parse(self.job_config['TRANSFORMATION_QUERY'])
        table_alias=''
        for item in res[0].tokens:
            str_item = re.sub("[ \\t]+", "", str(item))
            if len(str_item) == 0: continue
            if isinstance(item, sqlparse.sql.IdentifierList) and acquiring_identifiers:
                identifiers.extend([i for i in item.get_identifiers()])
                continue
            if item.is_keyword:
                keyword = str(item).upper()
                if keyword == 'FROM':
                    acquiring_identifiers = False
                    continue
            if not acquiring_identifiers:
                aliases.extend(str_item.split(' '))
                table_alias = str(item).split()
                break

        src_column_names, dst_column_names = [], []
        temp_table=table_alias[0]

        for identifier in identifiers:
            dst_column_name = identifier.get_name()
            identifier=str(identifier)
            expression_list=[identifier.split()[0]]
            for expression in expression_list:
                if len(table_alias) > 1:
                    if table_alias[1]+'.' in expression:
                        index_i=expression.index(table_alias[1]+'.')
                        expression=expression[index_i:]
                        expression=re.split('[0-9(),+-/]', expression)
                        expression = expression[1]
                        src_column_name = expression
                        query_map[src_column_name] = dst_column_name

                else:
                    expression = re.split('[0-9(),+-/]', expression)
                    if len(expression) == 1:
                        expression = expression[0]
                    else:
                        expression = expression[1]
                    src_column_name = expression.lower()
                    query_map[src_column_name] = dst_column_name
        return query_map

    def module_import(self, name):
        """
        Dynamically loads the specified module
        :param name: Name of the feed
        :return: Imported module.
        """
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def pre_processing(self, sql_context, ds_interval, feed_date, is_republish):
        """
        Load and execute the modules specified in the pre-processing section.
        :return: Raises an exception on error.
        """
        if 'METHOD_NAME' in self.job_config:
            self.logger.info(
                "Running method %s for feed %s",self.job_config['METHOD_NAME'], self.feed_name
            )
            mod = self.module_import(self.job_name)
            func = getattr(mod, self.job_name)
            method_class = func(sql_context, self.job_config, self.job_name, self.job_name + '.yaml',
                                ds_interval, feed_date, is_republish, self.logger, self.lock)
            method_name = getattr(method_class, self.job_config['METHOD_NAME'])
            method_name()
            self.logger.info(
                "Completed method %s execution for feed %s", self.job_config['METHOD_NAME'], self.feed_name
            )
        else:
            self.logger.info("No pre-processing needed for feed %s", self.feed_name)

    def load_staging_data(self, sql_context, ds_interval, is_republish):
        """
        Loads the ODFI data into staging table for executing feed hour
        :param sql_context: spark sql context object
        :param ds_interval: dataset interval
        :param is_republish: Boolean value specifying if dataset is republished.
        :return:
        """
        if self.hive_table:
            self.logger.info(
                "Creating data frame from hive table %s for interval %s",
                self.hive_table, ds_interval
            )
            source_df = sql_context.sql(
                "SELECT * FROM " + self.hive_table + "WHERE interval = %s" % ds_interval
            )
        else:
            self.logger.info(
                "Loading data from ODFI for %s feed", self.feed_name
            )
            source_df = self.get_odfi_df(sql_context, ds_interval)

        odfi_latest_df = self.get_odfi_df(sql_context, 'latest')

        self.logger.info(
            "Comparing %s current %s interval and latest dataset for missing columns.",
            self.feed_name, ds_interval
        )
        missing_columns = self.get_missing_columns(source_df.dtypes, odfi_latest_df.dtypes)
        for col_name in missing_columns:
            self.logger.info("Adding missing column %s", col_name)
            source_df = source_df.withColumn(col_name, lit(None).cast(missing_columns[col_name]))

        self.logger.info("Creating %s table data frame", self.job_config['FACT_TABLE'])
        dest_df = sql_context.table(self.job_config['FACT_TABLE'])
        self.logger.info("Comparing source and transformation columns for backward compatibility.")

        query_map = self.get_query_column_mapping()

        dest_tbl_columns_type_map = {}

        for tuple_row in dest_df.dtypes:
            dest_tbl_columns_type_map[tuple_row[0]] = tuple_row[1]

        source_columns = source_df.columns
        for column_name in query_map:
            if column_name not in source_columns:
                self.logger.info("Adding column %s", query_map[column_name])
                source_df = source_df.withColumn(
                    query_map[column_name],
                    lit(None).cast(dest_tbl_columns_type_map[query_map[column_name]])
                )

        self.logger.info("Persisting source data as %s temporary table.", 'tmp_'+self.job_name)
        sql_context.registerDataFrameAsTable(source_df, 'tmp_' + self.job_name)
        parquet_file_obj = parquet_utils(sql_context, self.env, self.logger)

        if ds_interval.find('_') > 0:
            feed_date = datetime.strptime("%s UTC" % ds_interval, '%Y-%m-%d_%H %Z')
            feed_partition_value = feed_date.strftime('%Y%m%d%H')
        else:
            feed_date = datetime.strptime("%s UTC" % ds_interval, '%Y-%m-%d %Z')
            feed_partition_value = feed_date.strftime('%Y%m%d')

        self.logger.info('Executing pre-processing methods for dataset %s', ds_interval)
        self.pre_processing(sql_context, ds_interval, feed_date, is_republish)

        self.run_sql('PRE_RUN', sql_context)

        self.logger.info('Creating transformation SQL data frame for dataset %s', ds_interval)

        if ds_interval.find('_') > 0:
            tranform_sql=self.job_config['TRANSFORMATION_QUERY'].replace('%s', feed_date.strftime('%Y-%m-%d %H'))
            self.logger.info(tranform_sql)
            transform_df = sql_context.sql(tranform_sql)
            is_daily_feed = False
        else:
            transform_df = sql_context.sql(
                self.job_config['TRANSFORMATION_QUERY'].replace('%s', feed_date.strftime('%Y-%m-%d'))
            )
            is_daily_feed = True
        self.logger.info("Adding %s partition key for interval %s", self.feed_name, ds_interval)

        if 'utc_timestamp' not in transform_df.columns:
            transform_df = transform_df.withColumn('utc_timestamp', lit(feed_date))

        if self.job_config['FACT_PARTITION_KEY'] == 'utc_datehour_sid':
            transform_df = transform_df.withColumn(
                'utc_date_sid', lit(int(feed_date.strftime('%Y%m%d')))
            )
            transform_df = transform_df.withColumn(
                'utc_datehour_sid', lit(int(feed_date.strftime('%Y%m%d%H')))
            )

        for key in self.job_config['FACT_PARTITION_KEY'].split(','):
            if key.strip() == 'utc_date_sid':
                transform_df = transform_df.withColumn('utc_date_sid', lit(int(feed_date.strftime('%Y%m%d'))))
                feed_partition_value = feed_date.strftime('%Y%m%d')
            if key.strip() == 'utc_hour_sid':
                transform_df = transform_df.withColumn('utc_hour_sid', lit(int(feed_date.strftime('%H'))))
                feed_partition_value = feed_partition_value + ',' + str(int(feed_date.strftime('%H')))

        if 'FACT_SEQUENCE_INFO' in self.job_config:
            if self.job_config['FACT_SEQUENCE_INFO']['SEQUENCE_TYPE']=='negative':
                sql="select min(" + self.job_config['FACT_SEQUENCE_INFO']['SEQUENCE_ID'] + ") rowid from " + \
                self.job_config['FACT_TABLE']
                self.logger.info("running %s",sql)
                rowid = sql_context.sql(sql).collect()
                rowid = rowid[0][0]
                if rowid==None:
                    rowid=0
                transform_df = transform_df.withColumn(self.job_config['FACT_SEQUENCE_INFO']['SEQUENCE_ID'], -1*F.monotonically_increasing_id()-rowid)
            else:
                sql = "select max(" + self.job_config['FACT_SEQUENCE_INFO']['SEQUENCE_ID'] + ") rowid from " + \
                      self.job_config['FACT_TABLE']
                self.logger.info("running %s", sql)
                rowid = sql_context.sql(sql).collect()
                rowid = rowid[0][0]
                if rowid==None:
                    rowid=0
                transform_df = transform_df.withColumn(self.job_config['FACT_SEQUENCE_INFO']['SEQUENCE_ID'], F.monotonically_increasing_id() + rowid)

        sql = parquet_file_obj.arrange_column_type(transform_df, dest_df, self.job_config['FACT_PARTITION_KEY'])
        transform_df = sql_context.sql(sql)

        if 'MULTI_PARTITION' in self.job_config:

            parquet_file_obj.create_hive_table(transform_df, "mstr_datamart", 'tmp_' + self.feed_name)
            transform_df = sql_context.table('mstr_datamart.tmp_' + self.feed_name)

            platform_sql='select distinct platform_timezone from mstr_datamart.tmp_' + self.feed_name
            platform_timezone = sql_context.sql(platform_sql).collect()

            for timezone in platform_timezone:
                timezone = timezone[0]
                self.logger.info(timezone)
                if is_republish and self.job_config['FEED_NAME'] == 'ox_traffic_sum_hourly':
                    move_df=dest_df.where("utc_date_sid =  " + feed_date.strftime('%Y%m%d') + " and utc_hour_sid = " + str(int(
                            feed_date.strftime('%H'))) + " and platform_timezone='" + timezone + "' and tot_view_conversions+tot_click_conversions > 0")
                    transform_df=transform_df.unionAll(move_df)
                elif is_republish and self.job_config['FEED_NAME'] == 'ox_conversion_site_sum_hourly':
                    move_df = dest_df.where("utc_date_sid =  " + feed_date.strftime('%Y%m%d') + " and utc_hour_sid = " + str(int(
                            feed_date.strftime('%H'))) + " and platform_timezone='" + timezone + "' and tot_view_conversions+tot_click_conversions=0")
                    transform_df = transform_df.unionAll(move_df)

                hdfs_path = self.env['HIVE_BASE_DIR'] + self.job_config['FACT_TABLE'].split('.')[1] + \
                            "/utc_date_sid=" + feed_date.strftime('%Y%m%d') + "/" + "utc_hour_sid=" + \
                            feed_date.strftime('%H') + "/platform_timezone=" + timezone.replace("/",'%2F')
                self.partition_value = feed_date.strftime('%Y%m%d') + "," + \
                                       str(int(feed_date.strftime('%H'))) + "," + timezone
                self.logger.info("start writing dataset")
                parquet_file_obj.write_data(self.job_config['FACT_TABLE'],
                                       transform_df.where("platform_timezone='" + timezone + "'"),
                                       self.job_config['FACT_PARTITION_KEY'],
                                       self.partition_value,
                                       hdfs_path,is_republish)

            self.logger.info("%s hour %s data persistence completed.", self.feed_name, ds_interval)

            for timezone in platform_timezone:
                timezone = timezone[0]
                partition_value=feed_date.strftime('%Y%m%d') + "," + str(int(feed_date.strftime('%H')))  + "," + timezone
                keys_list =self.job_config['FACT_PARTITION_KEY'].split(',')
                values_list = partition_value.split(',')
                final_key = ""
                for key_index in range(len(keys_list)):
                    final_key = keys_list[key_index] + "=" + values_list[key_index] + ","
                final_key = final_key[:-1]
                self.logger.info("Partition Key Status %s", final_key)
                parquet_file_obj.invalidate_metadata_and_analyze_stats(self.job_config['FACT_TABLE'], self.job_config['FACT_PARTITION_KEY'], partition_value, final_key)

        elif 'DYNAMIC_PARTITION' in self.job_config:
            parquet_file_obj.create_hive_table(transform_df, "mstr_datamart", "tmp_"+self.feed_name)
            transform_df = sql_context.table("mstr_datamart."+"tmp_"+self.feed_name)

            hdfs_path = self.env['HIVE_BASE_DIR'] + self.job_config['FACT_TABLE'].split('.')[1] + \
                            "/utc_date_sid=" + feed_date.strftime(
                    '%Y%m%d') + "/" + "utc_hour_sid=" + str(int(feed_date.strftime('%H')))

            self.partition_value = feed_date.strftime('%Y%m%d') + "," + str(int(feed_date.strftime('%H')))

            parquet_file_obj.write_data(self.job_config['FACT_TABLE'],
                                   transform_df,
                                   self.job_config['FACT_PARTITION_KEY'],
                                   self.partition_value,
                                   hdfs_path,is_republish,dynamic_partition=True)

            self.logger.info("%s data transformation completed for hour %s", self.feed_name, ds_interval)
        else:
            self.logger.info("Persisting %s dataset %s", self.feed_name, ds_interval)
            parquet_file_obj = parquet_utils(sql_context, self.env, self.logger)
            load_query = parquet_file_obj.arrange_column_type(
                transform_df, dest_df, self.job_config['FACT_PARTITION_KEY']
            )

            transform_df = sql_context.sql(load_query)

            partition_key = []
            keys = self.job_config['FACT_PARTITION_KEY'].split(',')
            values = feed_partition_value.split(',')
            for key_index in range(0, len(keys)):
                partition_key.append(keys[key_index].strip() + "="+ values[key_index].strip())

            if len(partition_key) == 0:
                self.logger.error("Partition key cannot be empty.")
                raise Exception("Partition specification cannot be empty")

            hdfs_path = self.env['HIVE_BASE_DIR'] + self.job_config['FACT_TABLE'].split('.')[1] \
                + "/" + "/".join(partition_key)
            self.logger.info("Writing %s hour dataset at %s", ds_interval, hdfs_path)
            shuffle_value = self.job_config.get('SPARK_SHUFFLE_VALUE', None)
            if shuffle_value:
                query = "set spark.sql.shuffle.partitions=" + str(shuffle_value)
                sql_context.sql(query)

            parquet_file_obj.write_data(
                self.job_config['FACT_TABLE'], transform_df,
                self.job_config['FACT_PARTITION_KEY'], feed_partition_value, hdfs_path,
                target_file_count=self.job_config.get('TARGET_FILE_COUNT', None), is_republish=is_republish
            )
            self.logger.info("%s hour %s data persistence completed.", self.feed_name, ds_interval)
            self.logger.info("Executing rollup for %s dataset hour %s", self.feed_name, ds_interval)

        last_ds_hour = self.get_feed_stats('GET_LOAD_STATE', self.job_name)[0][0]

        if last_ds_hour:
            last_ds_hour = datetime.strptime("%s UTC" % last_ds_hour, '%Y-%m-%d %H:%M:%S %Z')
        else:
            last_ds_hour = feed_date

        try:
            if 'ROLLUP_TYPE' in self.job_config:
                rollup_instance = fact_rollup(sql_context, self.job_name+'.yaml', feed_date, last_ds_hour, self.logger)
                rollup_instance.check_rollup_condition(is_daily_feed=is_daily_feed)
        except Exception as error:
            exc_type, exc_val = sys.exc_info()[:2]
            self.logger.error("Error: %s, %s", exc_type, exc_val)
            raise
        self.logger.info("%s data transformation completed for hour %s", self.feed_name, ds_interval)

    def etl_load(self):
        """
        Executes the data load for the specified feed
        :return:
        """
        self.logger.info("Executing etl process for feed %s", self.feed_name)
        sql_context = self.get_sql_context()

        load_start_date = datetime.now()
        self.logger.info("Retrieving last processed dataset serial for job %s loading from %s feed",
                         self.job_name, self.feed_name)
        ds_serial = self.get_feed_stats('GET_LATEST_SERIAL', 'odfi_ds_status_'+self.job_name)[0][0]
        if ds_serial is None:
            # new feed load. Initialize the dataset serial to 0
            ds_serial = 0
        else:
            ds_serial = int(ds_serial)

        self.logger.info("Retrieving loaded datasets from checkpoint table for job %s", self.job_name)

        odfi_ds = self.get_feed_stats('GET_LOADED_DATASETS', self.job_name, self.feed_name, ds_serial)

        loaded_odfi_ds = {}
        for ds in odfi_ds:
            loaded_odfi_ds[ds.dataset_serial] = ds.date_time

        self.logger.info("Retrieving %s dataset metadata from serial %s", self.feed_name, ds_serial)
        odfi_ds_metadata = self.get_odfi_metadata(ds_serial)

        for odfi_key in sorted(odfi_ds_metadata.keys()):
            self.logger.info("Loading interval %s", odfi_key)
            ds_metadata = odfi_ds_metadata.get(odfi_key)

            if loaded_odfi_ds.get(ds_metadata['serial']):
                self.logger.info("%s feed dataset interval %s with serial %s already loaded.",
                                 self.feed_name, odfi_key, ds_metadata['serial'])
                continue

            if len(ds_metadata['parts']) > 0:
                # deductive reporting doesn't publish data parts, if there is no data for that hour.
                self.logger.info(
                    "Checking %s interval %s is republished dataset.",
                    self.feed_name, ds_metadata['readableInterval']
                )
                is_republish = self.check_dataset_republish(ds_metadata['readableInterval'])

                self.logger.info("Checking %s preconditions", self.feed_name)
                self.check_transformation_condition('CHECK',sql_context, ds_metadata['startTimestamp'])

                self.logger.info(
                    "Starting data transformation for feed %s dataset interval %s",
                    self.feed_name, ds_metadata['startTimestamp']
                )

                try:
                    self.load_staging_data(sql_context, ds_metadata['readableInterval'], is_republish)
                except Exception as error:
                    exc_type, exc_val = sys.exc_info()[:2]
                    fl.logger.error("Error: %s, %s", exc_type, exc_val)
                    raise

                self.logger.info(
                    "Recording status information for %s job dataset interval %s",
                    self.job_name, ds_metadata['startTimestamp']
                )
            else:
                self.logger.info("No data parts for feed %s interval %s ",
                                 self.feed_name, ds_metadata['readableInterval'])
            self.store_ds_metadata(ds_metadata, load_start_date, datetime.now())
            ds_serial = ds_metadata['serial']
            self.run_sql('POST_RUN', sql_context, ds_metadata['readableInterval'])

        self.logger.info("Updating the label %s for %s job", 'odfi_ds_status_'+self.job_name, self.job_name)
        db = OXDB(self.env['FMYSQL_META_DSN'])
        LoadState(
            db, variable_name='odfi_ds_status_'+self.job_name
        ).upsert(ds_serial, commit=True)
        db.close()


if __name__ == "__main__":
    # Parse the command line arguments.
    parser = \
        argparse.ArgumentParser(
            description="Loads fact data.")
    parser.add_argument(
        "job_name",
        help="Name of job to execute.")
    parser.add_argument(
        "--hive_table",
        help="Load the data from hive table"
    )
    parser.add_argument("--ds_interval", help="To Load particular interval")

    options = parser.parse_args()
    # Initialize the object
    fl = FactLoad(options)
    try:
        fl.logger.info(
            "Starting %s job execution at %s",
            options.job_name, datetime.now()
        )
        if fl.lock.get_lock():
            if options.ds_interval:
                sql_context = fl.get_sql_context()
                is_republish = fl.check_dataset_republish(options.ds_interval)
                fl.load_staging_data(sql_context, options.ds_interval, is_republish)
            else:
                fl.etl_load()
            fl.logger.info(
                "%s job execution completed at %s",
                options.job_name, datetime.now()
            )
        else:
            fl.logger.info("Unable to get lock, exiting...")
    except Exception as error:
        exc_type, exc_val = sys.exc_info()[:2]
        fl.logger.error("Error: %s, %s", exc_type, exc_val)
        raise
    finally:
        fl.lock.releaseLock()
