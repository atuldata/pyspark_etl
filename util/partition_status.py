import yaml
from datetime import datetime
from oxdb import OXDB
from hdfs_utils import hdfs_utils



'''------------------------------------------Class Intialization-------------------------------------------------------'''
ENV="/var/dw-grid-etl/python/env.yaml"
import yaml
class partition_status:
    def __init__(self, env, logger):
        self.env = env
        self.logger = logger
        self.mysql_db = OXDB(self.env['FMYSQL_META_DSN'])

    def insert_partition_info(self,partition_key,partition_value,dest_table,new_path,num_of_files,job_name=None):
        self.logger.info("inserting partition info")
        if partition_key:
            sql = "update partition_status set is_used_by_impala=0, modified_datetime=now() where table_name = ? and partition_key=? and partition_value=? and is_used_by_impala = 1"
            self.logger.info("running %s", sql)
            self.mysql_db.execute(sql,(dest_table, partition_key, partition_value))

            sql = "insert into partition_status(job_name,table_name,file_path,is_used_by_impala,partition_key,partition_value,number_of_files) " \
                  "  values(?,?,?,?,?,?,?)"

            self.logger.info("running %s", sql)
            self.mysql_db.execute(sql,(job_name,dest_table, new_path, 1, partition_key, partition_value, num_of_files))
        else:
            sql = "update partition_status set is_used_by_impala=0, modified_datetime=now() where table_name=? and is_used_by_impala = 1"
            self.logger.info("running %s", sql)
            self.mysql_db.execute(sql,(dest_table))

            sql = "insert into partition_status(table_name,file_path,is_used_by_impala,partition_key,partition_value,number_of_files) " \
                  "  values(?,?,?,NULL,NULL,?)"

            self.logger.info("running %s", sql)
            self.mysql_db.execute(sql,(dest_table, new_path, 1,num_of_files))

    def commit_insert_partition_info(self):
        self.mysql_db.commit()

    def delete_orphan_file(self, hdfs_client, file_path):
        try:
            hdfs_client.moveToTrash(file_path)
            del_sql="DELETE FROM partition_status WHERE file_path = ? AND is_used_by_impala = 0"
            self.mysql_db.execute(del_sql, file_path)
            self.mysql_db.commit()
            self.logger.info("Removed orphan partition file %s from partition_status table", file_path)
        except:
            self.logger.error("Failed to remove file %s from partition_status table", file_path)
            raise

    def get_orphan_parquet_files(self, hrs):
        """
        Retrieve a list of parquet files that were orphaned more than hrs ago
        """
        try:
            sql="select table_name, file_path from partition_status where modified_datetime < DATE_SUB(CURRENT_TIMESTAMP(), interval %d hour) and is_used_by_impala=0;"%(hrs)
            self.logger.info("Running:  %s", sql)
            orphan_file_list=self.mysql_db.get_executed_cursor(sql).fetchall()
            self.logger.info("Fetched %d orphan parquet files from partition_status table.", len(orphan_file_list))
            return orphan_file_list
        except: 
            self.logger.error("Error in get_orphan_parquet_files()")
            raise

def get_existing_path(partition_key,partition_value):
    env=yaml.load(open(ENV))
    sql="select job_name,file_path from partition_status where partition_key=? and partition_value=? and is_used_by_impala=1"
    mysql_db = OXDB(env['FMYSQL_META_DSN'])
    orphan_file_list=mysql_db.get_executed_cursor(sql,(partition_key,partition_value)).fetchall()
    mysql_db.close()
    return orphan_file_list


