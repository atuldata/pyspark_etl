#!/bin/python -u

import sys
import os
import yaml
import time
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.oxdb import OXDB
from util.load_state import LoadState


# Job_queue_lib is a framework for queuing up and processing jobs after the ETL is run.
# Example jobs that could be queued: variable dealtype revenue adjustments, rollups etc.
# A detailed description of the framework behaviour can be found in https://wiki.corp.openx.com/pages/viewpage.action?spaceKey=~rashmi.naik&title=Generic+Adjustments+framework#?lucidIFH-viewer-6aa01c1c=1
#
# Use Cases:
# 1. A base hourly job can call queue_job(...) to do revenue adjustments after loading the data for the given hour. The process_queue job will call the
#    job_queue_lib with the job name and key that needs to processesed.  See ox_video_traffic_hourly.yaml for the configurations that will be used by job_queue_lib framework.
#    Detailed description about yaml file can be found in: https://wiki.corp.openx.com/display/~rashmi.naik/Sample+yaml+file+for+job+queue+lib+framework
#    The job_queue_lib can be explicitly called as: python job_queue_lib.py "/var/feeds/python/ox_video_traffic_hourly.py" "REVENUE_ADJUSTMENT"
# 2. If we want to run an adjustment manually, just add an entry to mstr_datamart.queue_state table and call the job_queue_lib framework as: python job_queue_lib.py <job_path/job_name.py> <KEY>
#    The configuration file metioned in the queue_state table will be used for job processing.


#Framework variables:
PRECONDITION = "PRECONDITION"
PRECONDITION_FALIED = "PRECONDITION_FALIED"
MODULES = "MODULES"
ACTIONS = "ACTIONS"
ACTION = "ACTION"
TASKS = "TASKS"
VARIABLE_ASSIGNMENT = "VARIABLE_ASSIGNMENT"
VAR_NAMES = "VAR_NAMES"
VAR_SRC = "VAR_SRC"
FUNCTION = "FUNCTION"
FUNC_NAME = "FUNC_NAME"
FUNC_ARGS = "FUNC_ARGS"
SQLS = "SQLS"
SQL = "SQL"
SQL_ARGS = "SQL_ARGS"
UPDATE_LOAD_STATE = "UPDATE_LOAD_STATE"

class job_queue_lib:
    def __init__(self,yaml_file,job_name,key):
        if "/" in job_name: # just in case someone sends job name without path! 
            log_name = key.lower()+"_"+job_name.rsplit('/',1)[1].split(".")[0] # ex: revenue_adjustment_ox_video_traffic_houtly.log
        else:
            log_name = key.lower()+"_"+job_name.split(".")[0]
        self.lock = JobLock(log_name) #  revenue_adjustment_ox_video_traffic_houtly.lock ( can date and hour can be added to lock name later)
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(log_name, log_level = self.config['LOG_LEVEL'])

        self.job = dict() # this map will have all the varaibles related to the job that is executing currently
        self.job["jq_job_name"]=job_name
        self.job["jq_key"] = key
        self.env = yaml.load(open(self.config['ENV']))
        self.db = OXDB(self.env['FMYSQL_META_DSN']) 

    def process_queue_job(self):
        self.logger.info("Getting queued entry for the job: "+self.job["jq_job_name"] + " key: " +self.job["jq_key"] )
        job_details = self.db.get_executed_cursor(self.config['GET_QUEUED_JOB'],self.job["jq_job_name"],self.job["jq_key"] )
        try:
            for each_job in job_details:
                #get the job details for the hour we are processing
                self.initialize_job(each_job)
                self.logger.info("Processing job with jq_instance_sid %s", self.job["jq_instance_sid"])

                #Check precondition
                execute_job=self.check_precondition(self.job["jq_config_file"][self.job["jq_key"]][PRECONDITION])

                #Execute prcondition failed action if any precon fails and failed action exists!
                if not execute_job:
                    self.logger.info("Precondition failed!")
                    if PRECONDITION_FALIED in self.job["jq_config_file"][self.job["jq_key"] ]:
                        self.logger.info("Executing precondition failed action...")
                        self.precondition_failed()
                        self.db.commit()                   
                    break

                # change jq_state to running
                self.change_state('RUNNING')
                self.logger.info("Chaning  state to RUNNING for jq_instance_sid %s", self.job["jq_instance_sid"])
                self.db.commit()

                #Dynamically import the required modules
                if MODULES in self.job["jq_config_file"][self.job["jq_key"] ]:
                    self.logger.info("Importing modules...")
                    self.import_modules(self.job["jq_config_file"][self.job["jq_key"] ][MODULES])

                #Execute actions
                self.logger.info("Starting to execute actions...")
                self.execute_actions()
                self.change_state('SUCCESS')
                self.logger.info("Changed state to SUCCESS for jq_instance_sid %s", self.job["jq_instance_sid"])        
                self.db.commit() 
                self.job["jq_job_initialized"]  = False 
            
        except:
            self.logger.error("Exception %s",sys.exc_info()[0])
            if "jq_job_initialized" in self.job and self.job["jq_job_initialized"] :
                self.db.rollback() 
                self.change_state('FAILED') 
                self.logger.info("Changed state to FAILED for jq_instance_sid %s", self.job["jq_instance_sid"])
                self.change_state('QUEUED')
                self.logger.info("Changed state to QUEUED for jq_instance_sid %s", self.job["jq_instance_sid"])
                self.db.commit() 
            raise

    def initialize_job(self, each_job):
        self.job["jq_instance_sid"] = each_job[0]
        self.job["jq_config_file_name"] = each_job[1]
        self.job["jq_config_file"] = yaml.load(open(self.job["jq_config_file_name"] ))
        self.job["jq_utc_date_sid"] = each_job[2]
        self.job["jq_utc_timestamp"] = each_job[3]
        self.job["jq_state"] = each_job[4]
        self.job["jq_host"] = each_job[5]
        self.job["jq_is_republish"] = each_job[6]
        self.job["jq_rollup_config"] = yaml.load(open(self.job["jq_config_file"]['ROLLUP_CONFIG'])) #needs to be changed when we move rollup under this framework
        self.job["jq_pid"] = os.getpid()
        self.job["jq_db"] = self.db
        self.job["jq_job_initialized"] = True 

    def import_modules(self, module_names):
        for each_module in module_names:
            m = __import__(each_module)
            self.job["jq_module_"+each_module] = m


    def execute_sqls(self, sql_list):
        for sqls in sql_list:
            self.logger.info("Executing SQL: %s",sqls[SQL])
            if SQL_ARGS in sqls:
                args = [] # create the list of args that is required to execute this sql
                for each_var in sqls[SQL_ARGS]:
                    args.append(self.job[each_var])
                self.db.execute(sqls[SQL],args)
            else:
                self.db.execute(sqls[SQL])

    def precondition_failed(self):
        post_sql_list = self.job["jq_config_file"][self.job["jq_key"] ][PRECONDITION_FALIED]
        self.execute_sqls(post_sql_list)

    def check_precondition(self, precondition):
        for sqls in precondition:
            if SQL_ARGS in sqls:
                args = [] # create the list of args that is required to execute this sql
                for each_var in sqls[SQL_ARGS]:
                    args.append(self.job[each_var])

                status = self.db.get_executed_cursor(sqls[SQL],args).fetchall()[0][0]
            else:
                status = self.db.get_executed_cursor(sqls[SQL]).fetchall()[0][0]

            self.logger.info("Precondition %s return value:%s",sqls[SQL],status)

            #returning anything other than true will be treated as failed precon
            if not status: #if any pre condition fails return false without further check
                return status

        return status

    def change_state(self, jq_state):
        if jq_state=='QUEUED':
            self.job["jq_instance_sid"] = self.db.get_executed_cursor(self.config['NEXT_INSTANCE_SID']).fetchall()[0][0]
        self.db.execute(self.config['CHANGE_STATE'], (self.job["jq_instance_sid"]),(self.job["jq_job_name"]),(self.job["jq_config_file_name"] ),(self.job["jq_key"] ),(self.job["jq_utc_date_sid"] ),(self.job["jq_utc_timestamp"] ),jq_state,(self.job["jq_host"] ),(self.job["jq_pid"] ),(self.job["jq_is_republish"] ))


    def assign_variables(self, var_src, var_names):
        self.logger.info("Assigning variables:"+ ', '.join(var_names))
        cursor = self.db.get_executed_cursor(var_src)
        i = 0
        for row in cursor:
            for each_val in row: # assign values to the variables
                self.job[var_names[i]] = each_val
                i += 1

    def call_functions(self,class_name, func, params):
        self.logger.info("Calling function %s from %s", func,class_name)
        obj = self.job["jq_module_"+class_name] # get the module that is imported in the begining
        func = getattr(obj, func)
        if params is not None:
            args = [] # create the list of args that is required by func
            for each_var in params:
                args.append(self.job[each_var])
          
            func(*args) # call the function with parameters 
        else:
            func()

    def update_load_state(self,var_name):
        LoadState(
            self.db, variable_name=var_name
        ).update_variable_datetime(variable_value=self.job["jq_utc_timestamp"])

    def execute_actions(self):
        actions = self.job["jq_config_file"][self.job["jq_key"] ][ACTIONS]
        for each_action in actions:
            action = each_action[ACTION]
            if PRECONDITION in action:
                execute_action = self.check_precondition(action[PRECONDITION])
                if not execute_action:
                    self.logger.info("Precondition failed, hence skipping this action...")
                    continue

            # optional precondition 
            self.logger.info("Executing task...")
            tasks = action[TASKS]
            for each_task in tasks:
                #determine the task and do the appropriate thing
                if FUNCTION in each_task:
                    func = each_task[FUNCTION][FUNC_NAME] # expecting classname.funcname
                    class_name = func.split(".")[0]
                    fun_name = func.split(".")[1] 
                    if FUNC_ARGS in each_task[FUNCTION]:
                        params = each_task[FUNCTION][FUNC_ARGS]
                    else:
                        params = None
                    self.call_functions(class_name,fun_name, params)

                elif VARIABLE_ASSIGNMENT in each_task:
                    var = each_task[VARIABLE_ASSIGNMENT]
                    self.assign_variables(var[VAR_SRC],var[VAR_NAMES])

                elif SQLS in each_task:
                    self.execute_sqls(each_task[SQLS])

                elif UPDATE_LOAD_STATE in each_task:
                    self.update_load_state(each_task[UPDATE_LOAD_STATE])

                else:
                    self.logger.error("Unknown TASK %s", each_task)
                    raise Exception("Unknown TASK found in action!")

        self.logger.info("Done executing all actions!") 


#Famework utils
def queue_job(yaml_file,job,key,config_file_name,utc_timestamp,host,pid,is_republish):
    try:
        utc_date_sid = str(utc_timestamp).split(" ")[0].replace("-",""); # compute utc date sid from timestamp
        print "Queue %s %s job for %s" % (job,key, utc_timestamp)
        dh = job_queue_lib(yaml_file,job,key)
        dh.job["jq_config_file_name"] = config_file_name
        dh.job["jq_utc_date_sid"] = utc_date_sid
        dh.job["jq_utc_timestamp"] = utc_timestamp
        dh.job["jq_host"] = host
        dh.job["jq_pid"] = pid
        dh.job["jq_is_republish"] = is_republish
        dh.change_state('QUEUED')
        dh.db.commit()
    except:
        # printing exceptions here because python job that will call this will log it
        eType, eVal = sys.exc_info()[:2]
        print eType
        print eVal


if __name__ == "__main__":
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    # python job_queue_lib.py "/var/feeds/python/ox_video_traffic_hourly.py" "REVENUE_ADJUSTMENT"
    if len(sys.argv) != 3:
        print("Incorrect number of arguments! Expecting 2 args(job name and key) but got %d args" % (len(sys.argv) - 1))
        sys.exit(0)        
    job_name = sys.argv[1] # expecting job_name as /var/feeds/python/ox_video_traffic_hourly.py
    key = sys.argv[2] # ex: REVENUE_ADJUSTMENT, DAILY_ROLLUP etc.
    dh = job_queue_lib(yaml_file,job_name,key)

    try:
        if not dh.lock.getLock():
            dh.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        jobStart = time.time()
        dh.logger.info("Start processing queue...")
        dh.process_queue_job()
        jobEnd = time.time()
        dh.logger.info("Finished running process_queue, total elapsed time: %s seconds", (jobEnd-jobStart))
    except SystemExit:
        pass
    except:
        eType, eVal = sys.exc_info()[:2]
        dh.logger.error("Exception details: %s, %s", eType, eVal)
    
    dh.lock.releaseLock()
        
