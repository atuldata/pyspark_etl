#!/opt/bin/python -u

import json
import os
import re
import sys
import time
import xml.etree.ElementTree as xmle
from datetime import datetime
from optparse import OptionParser
from subprocess import Popen, PIPE
from xml.etree.ElementTree import XMLParser
from oxdb import OXDB
from pyspark.sql.functions import *
from util.hdfs_utils import hdfs_utils
from util.impala_utils import get_column_type
from util.spark_utils import get_existing_path
import urllib2
from partition_status import partition_status

class parquet_utils:
    def __init__(self, sqlContext, env, logger):
        self.logger = logger
        self.sqlContext = sqlContext
        self.hdfsClient = hdfs_utils(self.sqlContext, self.logger)
        self.env = env
        self.batchid = str(int(time.time()))

    def invalidate_metadata_and_analyze_stats(self, table_name, partition_spec=None, analyze_using='impala',do_compute_stats=True):
        # refresh table in HiveContext in case something was changed outside of Spark SQL
        # sqlContext.refreshTable() does not work with spark2, so using sqlContext.sql().collect() statement:
        self.sqlContext.sql("REFRESH TABLE " + table_name).collect()
        self.logger.debug("refresh of metadata in Spark's HiveContext complete")
        self.logger.info("invalidate Impala metadata and analyze stats for table or partition")
        try:
            impala_db = OXDB(self.env['IMPALA_DSN'], None, False)
            impala_db.execute('set MT_DOP=8;')
            # execute Impala refresh and invalidate metadata regardless of analyze_using type
            if partition_spec:
                sql ="REFRESH %s PARTITION ( %s )"  % (table_name, partition_spec)
                self.logger.info(sql)
                impala_db.execute(sql)
                if do_compute_stats:
                    sql = 'COMPUTE INCREMENTAL STATS %s PARTITION( %s )' % (table_name, partition_spec)
                    self.logger.info("analyze stats method: %s and sql: %s", analyze_using, sql)
                    impala_db.execute(sql)
            else:
                # incremental stats have no meaning for a non-partitioned table
                # so use normal compute stats via Impala
                sql = "REFRESH %s" % (table_name)
                self.logger.info(sql)
                impala_db.execute(sql)
                if do_compute_stats:
                    sql = 'COMPUTE STATS %s' % (table_name)
                    self.logger.info("analyze stats method: %s and sql: %s", 'impala', sql)
                    impala_db.execute(sql)
            impala_db.close()
        except Exception as error:
            if partition_spec:
                err_msg = "%s refresh metadata and/or stats collection failed for partition specification %s" % (
                table_name, partition_spec)
            else:
                err_msg = "%s refresh metadata and/or stats collection failed." % (table_name)
            self.logger.error(err_msg + " Error %s" % error)
            # even if invalidate metadata and stats collection failed, don't fail the ETL job.
            # next run of job will invalidate the metadata for that table and compute_stats job
            # job will go back and compute stats for partitions missing stats
            raise

    def repartition_for_dynamic_partition_write(self, df, dynamic_partition_column, target_partitioncount_dict,
                                                default_target_partitioncount, additional_partition_columns=[]):
        """Repartitions the provided df based on the specified dynamic partition column, a dict containing the
        target number of partitions for specific dynamic partition column values, and a default target number
        of partitions for unspecified dynamic partition column values.  (Use this to perform an in-memory
        repartition before writing out to partitioned Parquet directories, to avoid the common problem of
        multiple Spark tasks attempting to write to multiple output directories simultaneously, resulting in
        memory pressure and small files.)

        This function only generates the target number of partitions based on a single dynamic partition column,
        so the additional_partition_columns parameter can be used to specify other static/dynamic partition
        columns that will be written to Parquet.

        A typical target_partitioncount_dict might look like this: {'America/New_York': 5, 'UTC': 3}, specifying
        5 and 3 output partitions respectively, with all other values getting `default_target_partitioncount'.

        Note that spark.sql.shuffle.partitions/spark.default.parallelism are set to 200 by default, which will
        manifest as the number of tasks used to perform this repartition.  If the total number of output
        partitions is less than this number, you will see empty tasks to make up the difference, which is
        expected behavior.  If the intended total number of output partitions is greater than this number,
        however, you may see fewer output files than you requested, and you should increase the parameters
        mentioned above."""

        def custom_partition_expr(partition_column, num_values_dict, default_num_values):
            """Builds an expression for dynamic repartitioning based on the specified column"""
            import pyspark.sql.functions as F
            def build_expr(num_values_entry):
                key, num_values = num_values_entry
                if num_values > 1:
                    partid_expr = (F.rand() * num_values).cast("int")
                else:
                    partid_expr = F.lit(0).cast("int")
                return (F.col(partition_column) == F.lit(key), partid_expr)

            def combine_expr(acc, expr_parts):
                condition, value = expr_parts
                if acc is None:
                    result = F.when(condition, value)
                else:
                    result = acc.when(condition, value)
                return result

            target_exprs = map(build_expr, num_values_dict.iteritems())
            partition_expr = reduce(combine_expr, target_exprs, None)
            if default_num_values > 1:
                partid_expr = (F.rand() * default_num_values).cast("int")
            else:
                partid_expr = F.lit(0).cast("int")
            return partition_expr.otherwise(partid_expr)

        dummy_partition_expr = custom_partition_expr(dynamic_partition_column, target_partitioncount_dict,
                                                     default_target_partitioncount)
        print(dummy_partition_expr)
        # the actual repartitioning is done on the specified dynamic partition column, a "__dummypartition"
        # column containing the dummy value constructed above, and any other partitioning columns
        # specified by the additional_partition_columns list (if specified).
        repartition_columns = additional_partition_columns + [dynamic_partition_column, "__dummypartition"]
        repartitioned_df = (df.withColumn("__dummypartition", dummy_partition_expr)
                            .repartition(*repartition_columns)
                            .drop("__dummypartition"))
        return repartitioned_df

    def get_partition_spec(self, table_name, partition_key, partition_value, with_cast=None):
        keys_list = partition_key.split(',')
        values_list = partition_value.split(',')
        partition_spec = ""
        for key_index in range(len(values_list)):
            if values_list[key_index].isdigit():
                partition_spec += keys_list[key_index] + "=" + values_list[key_index] + ","
            else:
                if with_cast:
                    column_type = get_column_type(table_name, keys_list[key_index])
                    partition_spec += keys_list[key_index] + '=CAST(' + values_list[
                        key_index] + ' AS ' + column_type + '),'
                else:
                    partition_spec += keys_list[key_index] + "=" + values_list[key_index] + ","
        partition_spec = partition_spec[:-1]
        return partition_spec

        # this function is responsible for dropping and adding partitions or altering table location
    def add_partition_location(self,dest_table, new_path,partition_key,partition_value,job_name):
        if partition_key:
            partition_spec = self.get_partition_spec(dest_table,partition_key,partition_value)
            move_ddl = "ALTER TABLE " + dest_table + " DROP IF EXISTS PARTITION  (" + partition_spec + ")"
            self.logger.info(move_ddl)
            result = self.sqlContext.sql(move_ddl).collect()
            move_ddl = "ALTER TABLE " + dest_table + " ADD PARTITION (" + partition_spec + ") LOCATION \"" + new_path + "\""
            self.logger.info(move_ddl)
            result = self.sqlContext.sql(move_ddl).collect()
        else:
            move_ddl = "ALTER TABLE %s SET LOCATION '%s'" % (dest_table, new_path)
            self.logger.info(move_ddl)
            result = self.sqlContext.sql(move_ddl).collect()
            move_ddl = "ALTER TABLE %s SET SERDEPROPERTIES ('path' = '%s')" % (dest_table, new_path)
            self.logger.info(move_ddl)
            result = self.sqlContext.sql(move_ddl).collect()

            # this function validate the dataframe schema against destination table.

    def validate_schema(self, source_schema, target_schema, partition_key, dynamic_partition_key=None):
        # break each schema into data and partition columns to validate separately.
        # static (batch) partition columns are not validated, so these expressions only consider
        # non-partition columns (called "data columns" in the error message) and dynamic partition columns.
        if partition_key:
            partition_cols = [c.strip() for c in partition_key.split(",")]
            source_data_schema = [c for c in source_schema if c.name not in partition_cols]
            target_data_schema = [c for c in target_schema if c.name not in partition_cols]
            # check data columns first
            if len(source_data_schema) != len(target_data_schema):
                raise ValueError("Target data schema has %d columns while DataFrame has %d"
                                 % (len(target_data_schema), len(source_data_schema)))
            for (sourcef, targetf) in zip(source_data_schema, target_data_schema):
                if sourcef.name != targetf.name:
                    raise ValueError("DataFrame data column %s does not match expected name in target schema %s"
                                     % (sourcef.name, targetf.name))
                if sourcef.dataType != targetf.dataType:
                    raise ValueError(
                        "DataFrame data column %s with type %r does not match expected type in target schema %r"
                        % (sourcef.name, sourcef.dataType, targetf.dataType))
        else:
            for (sourcef, targetf) in zip(source_schema, target_schema):
                if sourcef.name != targetf.name:
                    raise ValueError("DataFrame data column %s does not match expected name in target schema %s"
                                     % (sourcef.name, targetf.name))
                if sourcef.dataType != targetf.dataType:
                    raise ValueError(
                        "DataFrame data column %s with type %r does not match expected type in target schema %r"
                        % (sourcef.name, sourcef.dataType, targetf.dataType))

        if dynamic_partition_key:
            dynamic_partition_cols = [c.strip() for c in dynamic_partition_key.split(",")]
            source_dynpart_schema = [c for c in source_schema if c.name in dynamic_partition_cols]
            target_dynpart_schema = [c for c in target_schema if c.name in dynamic_partition_cols]
            # check dynamic partition columns, if any are defined
            if len(source_dynpart_schema) != len(target_dynpart_schema):
                raise ValueError("Target schema has %d dynamic partition columns while DataFrame has %d"
                                 % (len(target_dynpart_schema), len(source_dynpart_schema)))
            for (sourcef, targetf) in zip(source_dynpart_schema, target_dynpart_schema):
                if sourcef.name != targetf.name:
                    raise ValueError(
                        "DataFrame dynamic partition column %s does not match expected name in target schema %s"
                        % (sourcef.name, targetf.name))
                if sourcef.dataType != targetf.dataType:
                    raise ValueError(
                        "DataFrame dynamic partition column %s with type %r does not match expected type in target schema %r"
                        % (sourcef.name, sourcef.dataType, targetf.dataType))

    def write_data(self,
                   dest_table,
                   source_df,
                   partition_key,
                   partition_value,
                   path,
                   target_file_count=None,
                   validate_schema=True,
                   is_republish=False,
                   dynamic_partition_column=None,
                   job_name=None,
                   write_mode='overwrite',
                   analyze_using='impala',
                   compute_stats=True,
                   insert_partition_info=True
                   ):

        source_schema = source_df.schema.fields
        target_schema = self.sqlContext.table(dest_table).schema.fields
        # validate schema
        self.logger.info(partition_key)
        if dynamic_partition_column:
            self.logger.info(str(dynamic_partition_column))
            validate_dynamic_column=','.join(dynamic_partition_column)
        else:
            validate_dynamic_column=None

        if validate_schema:
            self.validate_schema(source_schema,target_schema,partition_key,validate_dynamic_column)
        self.logger.info("Schema is valid")

        #add batchid to new path
        new_path = path + "/" + "data_" + self.batchid + "/"
        self.logger.info("writing the data into %s using snappy" % (new_path))
        try:
            if target_file_count:
                source_df = source_df.coalesce(target_file_count)
            if dynamic_partition_column:
                self.logger.info("dynamic partition by %s", str(dynamic_partition_column))
                source_df.write.option("compression", "snappy").parquet(new_path,
                                                                      mode="overwrite",
                                                                      partitionBy=dynamic_partition_column)

            else:
                source_df.write.option("compression", "snappy").parquet(new_path, mode="overwrite")

        except Exception as error:
            err_msg = "%s write operation failed" % (dest_table)
            self.logger.error(err_msg + " Error %s" % error)
            raise

        if dynamic_partition_column:
            self.logger.info('Finding out the partition to change the location')
            explore_path=new_path

            # we are expecting a list for dynamic_partition column in case of multiple partitions
            # so if we don't have a list (probably a single string), change to a list
            if isinstance(dynamic_partition_column, list):
                self.logger.debug("dynamic_partition_column is a list already")
            else:
                # remove any spaces inside string and split on "," to create a list
                self.logger.debug("dynamic_partition_column is NOT a list - turn it into a list")
                dynamic_partition_column = dynamic_partition_column.replace(" ","").split(",")

            for x in dynamic_partition_column:
                explore_path+="/*"
            self.logger.info("looking for new partitions under this path: %s", explore_path)

            meta_file = ['_SUCCESS', '_common_metadata', '_metadata']
            partition_location = [(f.getPath().getName(), urllib2.unquote(f.getPath().toUri().toString()),
                                   f.getPath().getName(),
                                   urllib2.unquote(f.getPath().getName()))
                                  for f in self.hdfsClient.ls(explore_path) if
                                  f.getPath().getName() not in meta_file]
            self.logger.info(partition_location)
            partition_spec_list=[]
            for (folder_name, location, file_name, platform_partition) in partition_location:
                self.logger.info(folder_name, location, file_name, platform_partition)
                dynamic_spec = location[location.index(new_path)+len(new_path):].split('/')
                self.logger.info(dynamic_spec)

                if len(partition_value)==0:
                    updated_partition_value=partition_value
                else:
                    updated_partition_value = partition_value+","
                for dynamic_key in dynamic_spec:
                    value=dynamic_key.replace('%2F','/').replace('%2f','/').split("=")[1]

                    if value.isdigit():
                        updated_partition_value+=value+","
                    else:
                        updated_partition_value+="'"+value+"',"

                updated_partition_value=updated_partition_value[:-1]
                self.logger.info(" key %s - value %s, write_mode: %s", partition_key, updated_partition_value,
                                 write_mode)

                if write_mode=='append':
                    self.logger.info(dest_table, partition_key, updated_partition_value)
                    existing_location = get_existing_path(self.sqlContext,dest_table,partition_key, updated_partition_value)
                    if existing_location:
                        self.logger.info('change %s to existing location %s ', location, existing_location)
                        files = [urllib2.unquote(f.getPath().toUri().toString())
                                 for f in self.hdfsClient.ls(location + "/*")]
                        for file_name in files:
                            self.hdfsClient.rename(file_name, existing_location)
                            location = existing_location

                partition_spec_list.append((dest_table, location,partition_key,updated_partition_value,job_name))
                self.add_partition_location(dest_table, location,partition_key,updated_partition_value,job_name)

            if insert_partition_info:
                for (dest_table, location, partition_key, updated_partition_value, job_name) in partition_spec_list:
                    part_status = partition_status(self.env, self.logger)
                    part_status.insert_partition_info(partition_key, updated_partition_value, dest_table, new_path, 1, job_name)
                    part_status.commit_insert_partition_info()


            for (dest_table, location, partition_key, partition_value_with_partition, job_name) in partition_spec_list:
                if analyze_using=='impala':
                    partition_spec = self.get_partition_spec(dest_table, partition_key, partition_value_with_partition,
                                                         with_cast=True)
                else:
                    partition_spec = self.get_partition_spec(dest_table, partition_key, partition_value_with_partition)
                self.invalidate_metadata_and_analyze_stats(dest_table, partition_spec, analyze_using=analyze_using,do_compute_stats=compute_stats)

        else:
            self.add_partition_location(dest_table, new_path,partition_key,partition_value,job_name)
            part_status = partition_status(self.env, self.logger)
            part_status.insert_partition_info(partition_key, partition_value, dest_table, new_path, 1, job_name)
            part_status.commit_insert_partition_info()
            if partition_key:
                partition_spec = self.get_partition_spec(dest_table, partition_key, partition_value)
            else:
                partition_spec = None
            self.invalidate_metadata_and_analyze_stats(dest_table, partition_spec=partition_spec, analyze_using=analyze_using,do_compute_stats=compute_stats)

        self.logger.info("writing new dataset complete")

    # rearrange source data frame to match the the destination dataframe order and column types
    def arrange_column_type(self, source_df, dest_df, partition_key=None, dynamic_partition_key=None):
        columns = source_df.dtypes
        source_type_change = {}
        for col in columns:
            source_type_change[col[0]] = col[1]
        columns = dest_df.dtypes
        dest_type_change = {}
        for col in columns:
            dest_type_change[col[0]] = col[1]
        column_names = ""
        if dynamic_partition_key:
            dynamic_key_list = [key.strip() for key in dynamic_partition_key.split(',')]
        else:
            dynamic_key_list = []
        for dest_column_name in dest_df.columns:
            if (dest_column_name not in [key.strip() for key in partition_key.split(',')]
                or dest_column_name in dynamic_key_list):
                if source_type_change[dest_column_name] <> dest_type_change[dest_column_name]:
                    column_names += " CAST( " + dest_column_name + " as " + dest_type_change[dest_column_name] + " ),"
                else:
                    column_names += dest_column_name + ","
        column_names = column_names[:-1]
        source_df = self.sqlContext.registerDataFrameAsTable(source_df, 'tmp_transform')
        sql = "SELECT " + column_names + " FROM tmp_transform"
        self.logger.debug("running following query: %s", sql)
        return sql

    def get_matched_type(self, source_type, dest_type,dest_columns):
        source_type_change = {}
        for col in source_type:
            source_type_change[col[0]] = col[1]

        dest_type_change = {}
        for col in dest_type:
            dest_type_change[col[0]] = col[1]

        column_names = []
        for dest_column_name in dest_columns:
            if source_type_change[dest_column_name] <> dest_type_change[dest_column_name]:
                column_names.append('CAST( ' + dest_column_name + ' as ' + dest_type_change[
                    dest_column_name] + ' ) as ' + dest_column_name)
            else:
                column_names.append(dest_column_name)
        return column_names

    def create_hive_table(self, table_df,table_schema,table_name):
        columns = table_df.dtypes
        source_type_change = {}
        sql = ""
        for column_name in columns:
            t=column_name[0]
            if 'date' in column_name[0] or 'timestamp' in column_name[0]:
                t="`"+column_name[0]+"`"
            sql += t + " " + column_name[1] + ","
        sql = sql[:-1]

        path="/user/hive/warehouse/"+table_schema+".db/"+table_name
        result = self.sqlContext.sql("DROP TABLE "+table_schema+"."+table_name)
        sql = "CREATE TABLE IF NOT EXISTS " + table_schema+"."+table_name + " (" + sql
        sql += ") STORED AS PARQUET LOCATION '%s'" % (path)
        self.logger.info(sql)
        result=self.sqlContext.sql(sql)
        self.write_data(table_schema+"."+table_name, table_df, None, None, path)
        self.invalidate_metadata_and_analyze_stats(table_schema+"."+table_name)
