#!/bin/python -u

import sys
import os
import yaml
import time
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
from util.oxdb import OXDB
import subprocess


# Process_queue is a job that looks at queue_state table and calls job_queue_lib framework to process the queued job.
# Process_queue job acts like a scheduler for the jobs that are queued by the base hourly jobs for certain extra processing such as revenue adjustments, rollups etc.
# Base hourly jobs adds an entry to mstr_datamart.queue_state for the current hour that needs some extra processing. Process_queue finds distinct <job_name, key> combinations and calls
# job_queue_lib for processing. Calls are made in parallel hence distinct <job_name, key> can run at the same time

class process_queue:
    def __init__(self,yaml_file):
        #Reading configuration file ( YAML file )
        self.lock = JobLock(self.__class__.__name__)
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.env = yaml.load(open(self.config['ENV']))
        self.db = OXDB(self.env['FMYSQL_META_DSN']) 

    def process_queue_job(self):
        job_details = self.db.get_executed_cursor(self.config['GET_QUEUED_JOB'])
        pq.logger.info("Getting all job's in queue...")
        #call each job for processing based on its type
        for each_queued_job in job_details:
            #call the Job_queue_lib job
            job = "/bin/python "+self.config['JOB_QUEUE_FRAMEWORK']+" "+each_queued_job[0]+" "+each_queued_job[1]
            pq.logger.info("Calling job: "+ job)
            subprocess.Popen(job,shell=True)
            
if __name__ == "__main__":
    yaml_file = sys.argv[0].replace(".py", ".yaml")
    pq = process_queue(yaml_file)
    try:
        if not pq.lock.getLock():
            pq.logger.info("Unable to get lock, exiting...")
            sys.exit(0)

        jobStart = time.time()
        pq.logger.info("Start processing queue...")
        pq.process_queue_job()
        jobEnd = time.time()
        pq.logger.info("Finished running process_queue, total elapsed time: %s seconds", (jobEnd-jobStart))
    except SystemExit:
        pass
    except:
        pq.logger.error("Exception %s",sys.exc_info()[0])
    finally:
        pq.lock.releaseLock()

    
