"""
Read in all of the log files and can return the errors since the last read.
"""
from __future__ import print_function

import json

import os
import re
import yaml
import time
import sys
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.oxdb import OXDB
LOG_DATE_REGEX = r'^\[\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\]'
ERROR_HEAD_REGEX = r'%s.+Error:' % LOG_DATE_REGEX


class LogErrors():
    """
    Usage:
    for html_row in LogErrors().get_html_rows():
        ...
    """

    def __init__(self, save_pos=True):
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.save_pos = save_pos
        self.config_file = __file__.replace('.pyc', '.yaml').replace('.py', '.yaml')
        self.config = yaml.load(open(self.config_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.state_file = __file__.replace('.pyc', '_state.json').replace('.py', '_state.json')
        self.env = yaml.load(open(self.config['ENV']))
        self.state = json.load(open(self.state_file)) \
            if os.path.exists(self.state_file) and os.stat(self.state_file).st_size else {}

    def get_error_messages(self, log_file):
        """
        Yields the error messages from the log file from a given position.
        """
        result = []
        statinfo = os.stat(log_file)
        if log_file not in self.state or statinfo.st_size<self.state[log_file]:
            self.state[log_file] = 0
        with open(log_file) as fh_:
            error_msg = None
            fh_.seek(self.state[log_file])
            for line in fh_:
                if re.match(LOG_DATE_REGEX, line):
                    # if any([item in line for item in self.config['TO_FIND']]):
                    if re.match(ERROR_HEAD_REGEX, line):
                        if error_msg is not None:
                            result.append(error_msg)
                        error_msg = line
                    elif error_msg is not None:
                        yield error_msg
                        error_msg = None
                elif error_msg is not None:
                    error_msg += line
            if error_msg is not None:
                result.append(error_msg)
            self.state[log_file] = fh_.tell()

    def get_html_rows(self):
        """
        Yields log_file_name as the first
        Then yields the error_messages.
        Only yields the log_file_name if there are new error messages.
        Yields back line by line with html
        """
        output=[]
        result = {}
          # Track log files already seen to ensure printing them only once.
        rows=self.get_rows()
        for log_file_name, error_message in rows:
                if log_file_name not in result:
                    result[log_file_name]=[]
                result[log_file_name].append( '<pre>%s</pre><br />' % error_message.replace('<', '&lt;').replace('>', '&gt;'))

        self.db = OXDB(self.env['FMYSQL_META_DSN'])
        instance_id = str(int(time.time()))
        sql = self.config['INSERT_ERROR']
        if len(result)==0:
            self.db.execute(sql, (instance_id, 'IMPALA_DSN', None, None))

        for log_file_name in result:
            result[log_file_name] = dict.fromkeys(result[log_file_name]).keys()
            log_error=""
            for x in result[log_file_name][len(result[log_file_name])-5:]:
                log_error+=x[:2000]
            #result[log_file_name]=''.join(result[log_file_name][len(result[log_file_name])-5:])
            result[log_file_name] = log_error
            #print ('IMPALA_DSN',log_file_name,result[log_file_name])
            self.db.execute(sql, (instance_id,'IMPALA_DSN',log_file_name,result[log_file_name]))
            output.append([log_file_name,result[log_file_name]])
        self.db.commit()
        return output

    def get_rows(self):
        """
        Iterates through all of the log files in the path supplied.
        Yields log_file_name, error_message
        """
        result=[]
        log_directory=self.config['LOGS_DIR']
        for directory in log_directory:
            if os.path.exists(directory):
                for log_file_name in os.listdir(directory):
                    if log_file_name.endswith('.log') and not log_file_name.startswith('.'):
                        for error_message in \
                                self.get_error_messages(
                                    os.path.join(directory, log_file_name)):
                            error_message= re.sub(LOG_DATE_REGEX,'',error_message)
                            error_message=error_message.strip()
                            if [log_file_name,error_message] not in result:
                                result.append([log_file_name, error_message])
                        # Save after each file is read
                        if self.save_pos:
                            self.save_state()
        return result

    def save_state(self):
        """
        Write out the state to the state_file.
        """
        json.dump(self.state, open(self.state_file, 'w'))

if __name__ == "__main__":

    le = LogErrors()
    try:
        if not le.lock.getLock():
            le.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        row=le.get_html_rows()
        print (row)
    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        le.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        le.lock.releaseLock()
