"""
Contains all utility methods related to impala functionality.
"""

from oxdb import OXDB
import yaml
import os
ENV="/var/dw-grid-etl/python/env.yaml"


def get_hive_create_table_sql(table_name, table_schema, location,
                              partition_key=None, partition_key_type=None):
    """
    Returns hive table creation script.
    :param table_name: Table name to be created.
    :param table_schema: table schema in string format(col1_name col1_type, col2_name col2_type)
    :param location: parquet files location.
    :param partition_key: table partition key
    :param partition_key_type: table partition key type
    :return: hive table creation sql from parquet files.
    """
    sql = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table_name + \
          " ( " + table_schema + " ) "
    if partition_key and partition_key_type:
        sql += " PARTITIONED BY ( " + partition_key + " " + partition_key_type + " ) "
    sql += " STORED AS PARQUET LOCATION '%s'" % location
    return sql


def get_column_type(table_name, column_name):
    env = yaml.load(open(ENV))
    impala_db = OXDB(env['IMPALA_DSN'], None, False)
    sql_impala = 'SHOW CREATE TABLE ' + table_name
    data = impala_db.get_executed_cursor(sql_impala).fetchall()
    impala_db.close()
    if column_name in data[0][0]:
        data = data[0][0].split(column_name)[1]
        data = data.split()
        column_type = data[0]
        return column_type
    else:
        return None

def get_drop_table_sql(table_name, is_delete_files=False):
    """
    Returns hive table drop script.
    :param table_name: Table name to be dropped.
    :param is_delete_files: indicates whether hdfs files to be removed or not.
    :return: hive table drop script.
    """
    sql = "DROP TABLE IF EXISTS " + table_name
    if is_delete_files:
        sql += " PURGE "
    return sql

def get_partition_column(table_name):
    env = yaml.load(open(ENV))
    impala_db = OXDB(env['IMPALA_DSN'], None, False)
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

def invalidate_metadata(impala_db, table_name, is_refresh=True):
    """
    Invalidates the metadata of the specified table name.
    :param impala_db: impala db connection object.
    :param table_name: impala table name. Ex format <schema_name>.<table_name>
    :param is_refresh: if metadata refresh is required
    :return: invalidates the specified table metadata
    """

    try:
        impala_db.execute("INVALIDATE METADATA %s" % table_name)
        if is_refresh:
            impala_db.execute("REFRESH %s" % table_name)
    except Exception as error:
        raise Exception("%s metadata invalidation failed. Error %s" % (table_name, error))


def analyze_stats(impala_db, table_name, partition_spec=None):
    """
    Analyze status for specified table.
    :param impala_db: impala db connection object.
    :param table_name: impala table name. Ex format <schema_name<.<table_name>
    :param partition_spec: string of partition keys and constant values.
           partition_spec ::= partition_col=constant_value Ex: p1=k1,p2=k2
    :return: Collects either incremental or full stats of the specified table.
    """
    try:
        if partition_spec:
            impala_db.execute('set MT_DOP=8;')
            sql="COMPUTE INCREMENTAL STATS %s PARTITION (%s)" % (table_name, partition_spec)
            impala_db.execute(sql)
        else:
            impala_db.execute("COMPUTE STATS %s" % table_name)
    except Exception as error:
        if partition_spec:
            err_msg = "%s stats collection failed for partition %s specification " % (table_name, partition_spec)
        else:
            err_msg = "%s stats collection failed." % table_name
        raise Exception(err_msg + " Error %s" % error)


def get_existing_path(table_name,partition_key=None,partition_value=None):
    env=yaml.load(open(ENV))
    partition_spec = ""
    if partition_key and partition_value:
        key_split = partition_key.replace(" ", "").split(",")
        value_split = partition_value.replace(" ", "").split(",")
        print key_split
        print value_split
        for index in range(len(key_split)):
            if value_split[index].isdigit():
                partition_spec += key_split[index] + "=" + value_split[index] + ','
            else:
                column_type = get_column_type(table_name, key_split[index])
                partition_spec += key_split[index] + '=CAST(' + value_split[index] + ' AS ' + column_type + ')' + ','

        partition_spec = partition_spec[:-1]
    try:
        sql = "SHOW FILES IN " + table_name + " partition (" + partition_spec + ")"
        impala_db = OXDB(env['IMPALA_DSN'], None, False)
        file_list=impala_db.get_executed_cursor(sql).fetchall()
        file_path=os.path.dirname(file_list[0][0])
        impala_db.close()
    except Exception as error:
        file_path=None
        pass
    return  file_path

def get_table_list():
    """
        Get list of tables

    """
    env = yaml.load(open(ENV))
    impala_db = OXDB(env['IMPALA_DSN'], None, False)
    table_list = impala_db.get_executed_cursor("show tables").fetchall()
    impala_db.close()

    result_list = []
    for name in table_list:
        result_list.append(name[0])
    return result_list

def get_table_used_paths(table_name):
    """
        Get HDFS paths in use by a table

    """
    env = yaml.load(open(ENV))
    impala_db = OXDB(env['IMPALA_DSN'], None, False)
    data = impala_db.get_executed_cursor("show table stats " + table_name).fetchall()
    impala_db.close()

    partition_key = get_partition_column(table_name)

    path_list = []

    for row in data:
        if row[0]!='Total':
            if partition_key:
                # location column is 7th after the partition keys
                path_list.append(row[len(partition_key)+7]) 
            else:
                path_list.append(row[7])

    return path_list

