#!/opt/bin/python -u

import os
import sys
import time
import yaml
import shutil
import requests
import json
import pyodbc
from datetime import datetime
from time import time, ctime
from optparse import OptionParser
from util.oxdb import OXDB
from util.EtlLogger import EtlLogger
from util.JobLock import JobLock
import util.mondemand as mondemand
import subprocess


class GrafanaEvents:
    def __init__(self, tag, path):
        self.offset = 0
        self.description = ""
        self.frequency = 60
        self.command = ""
        self.label = ""
        self.source = ""

        if 'label' in tag:
            self.label = tag['label']

        if 'frequency' in tag:
            self.frequency = tag['frequency']

        if 'description' in tag:
            self.description = tag['description']

        if 'offset' in tag:
            self.offset = tag['offset']

        if 'command' in tag:
            self.command = tag['command']

        if 'source' in tag:
            self.source = tag['source']

        self.program_name = self.label
        self.mondemand_sender = mondemand.MondemandSender(self.program_name, logger=logger)

    @staticmethod
    def load(config, path=[]):
        events = list()

        if config and 'event' in config:
            e = config['event']
            events.append(GrafanaEvents(e, path))
            return events
        else:
            if type(config) == dict:
                for key in config:
                    tpath = list(path)
                    tpath.append(key)
                    events.extend(GrafanaEvents.load(config[key], tpath))
            elif type(config) == list:
                for val in config:
                    events.extend(GrafanaEvents.load(val, path))

        events.reverse()
        return events

    def query_druid(self, command, env, logger):
        host = env['DRUID_HOST']
        port = env['DRUID_PORT']
        path = env['DRUID_PATH']
        url = 'http://' + host + ':' + str(port) + '/' + path
        post_data = json.dumps(command)
        try:
            httpResult = requests.post(url, data=post_data)
            if httpResult.text is not None:
                json_data = json.loads(httpResult.text)
                druid_max_time = json_data[0]['result']['maxTime']
                if druid_max_time is not None:
                    druid_max_time= druid_max_time.partition(".")[0]
                    druid_max_time_obj = datetime.strptime(druid_max_time,"%Y-%m-%dT%H:%M:%S")
                    delta = datetime.utcnow() - druid_max_time_obj
                    hours = delta.days*24 + delta.seconds/3600
                    self.mondemand_sender.send_gauge('axb', hours)

        except:
            logger.error("Http post request to the following druid broker " + url + "failed")

    def run(self, db, logger):

        for row in db.get_executed_cursor(self.command):
            if len(row) != 2:
                # print "Invalid SQL query, check the format rules in yaml file."
                logger.info("Invalid command query, check the format rules in yaml file.")
                self.mondemand_sender.send_gauge(row)
            else:
                gauge_name = row[0]
                gauge_value = row[1]
                if type(gauge_value) is str:
                    self.mondemand_sender.send_gauge_string(gauge_name, gauge_value)
                else:
                    self.mondemand_sender.send_gauge(gauge_name, gauge_value)

    def run_command(self, logger):
        proc = subprocess.Popen([self.command], stdout=subprocess.PIPE, shell=True)
        (rows, err) = proc.communicate()
        rows=eval(rows)
        for row in rows:
            if len(row) != 2:
                # print "Invalid SQL query, check the format rules in yaml file."
                logger.info("Invalid command query, check the format rules in yaml file.")
                self.mondemand_sender.send_gauge(row)
            else:
                gauge_name = row[0]
                gauge_value = row[1]
                if type(gauge_value) is str:
                    self.mondemand_sender.send_gauge_string(gauge_name, gauge_value)
                else:
                    self.mondemand_sender.send_gauge(gauge_name, gauge_value)

def send_event(events,  env, logger, run_all=False):
    run_number = int(time() / 900)
    base_time = 900 * run_number
    run_number = base_time / 60

    for e in events:
        logger.info("Sending event: %s, run_all: %s, run_number: %s, frequency: %s, offset: %s", e, run_all, run_number,
                    e.frequency, e.offset)

        if not run_all and run_number % e.frequency != e.offset:
            logger.inf("Skipping event sending %s due to frequency", e.label)
            continue
        if e.source == 'druid':
            e.query_druid(e.command,env,logger)
        if e.source=='python':
            e.run_command(logger)

        else:
            try:
                db = OXDB(env[e.source])
            except:
                logger.info("FAILURE: Connection Failure")
                logger.info("Check Complete")
                sys.exit(-1)
            e.run(db, logger)


if __name__ == "__main__":
    parser = OptionParser(usage="%program filename")
    (options, args) = parser.parse_args()

    config = yaml.load(open(sys.argv[0].replace(".py", ".yaml")))
    env = yaml.load(open(config['ENV']))
    logger = EtlLogger.get_logger('grafana_events', log_level = config['LOG_LEVEL'])
    joblock = JobLock('grafana_events')

    try:
        logger.info("Starting to generate grafana events...")
        if not joblock.getLock():
            logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        events = GrafanaEvents.load(config)

        send_event(events, env, logger, True)

    except SystemExit:
        pass
    except:
        logger.error("Error: %s", sys.exc_info()[0])
        raise
    finally:
        joblock.releaseLock()
