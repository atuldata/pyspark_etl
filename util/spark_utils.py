"""
Contains all utility methods related to spark functionality.
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import functions as F
from pyspark.sql import types as Types


def get_sql_context(app_name, broadcast_threshold=None):
    """
    Creates and returns Hive SQL context object with app name as syncing dimension.
    :param app_name: Name of the application.
    :param broadcast_threshold: broadcast threshold value.
    :return: Hive sql context object.
    """
    spark_conf = SparkConf().setAppName(app_name)
    spark_context = SparkContext(conf=spark_conf).getOrCreate()
    sql_context = HiveContext(spark_context)
    if broadcast_threshold:
        sql_context.setConf("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)
    return sql_context


def get_missing_values(source, destination):
    """
    Returns the missing values from destination.
    :param source: source list.
    :param destination: destination list.
    :return: list containing values missing from destination.
    """
    missing_values = []
    for value in source:
        if value not in destination:
            missing_values.append(value)
    return missing_values


def convert_to_impala_schema(df_schema):
    """
    Converts the spark data frame schema to impala specific data types.
    :param df_schema: data frame schema in the form of struct(name,type)
    :return: schema with data types compatible with impala
    """
    cols = []
    for f in df_schema.fields:
        if f.dataType == Types.DateType():
            cols.append(F.col(f.name.lower()).cast('timestamp'))
        else:
            cols.append(F.col(f.name.lower()))
    return cols

def get_existing_path(spark_context,table_name,partition_key=None,partition_value=None):
    partition_spec = ""
    if partition_key and partition_value:
        key_split = partition_key.replace(" ", "").split(",")
        value_split = partition_value.replace(" ", "").split(",")
        for index in range(len(key_split)):
           partition_spec += key_split[index] + "=" + value_split[index] + ','
        partition_spec = partition_spec[:-1]
    try:
        sql = "SHOW TABLE EXTENDED like '" + table_name + "' partition (" + partition_spec + ")"
        print sql
        file_list=spark_context.sql(sql).collect()
        file_path=file_list[2][0].replace("location:","")
    except Exception as error:
        file_path=None
        pass
    return  file_path
