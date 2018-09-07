#!/bin/python -u

import yaml
import sys
import os
import subprocess

from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.oxdb import OXDB
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

class check:
    def __init__(self, yaml_file):

        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.env = yaml.load(open(self.config['ENV']))
        self.msg = MIMEMultipart('alternative')
        subject=self.env['IMPALA_DSN']
        self.msg['Subject'] = subject+"  Alerts"
        self.msg['From'] = self.config["MAIL_FROM"]
        self.msg['To'] = self.config["MAIL_TO"]
        self.mail_result = '<html><head><style> body {background-color: powderblue;} h1   {color: blue;}p    {color: red;} td, th {border: 1px solid #999;padding: 0.5rem;}</style></head><body>'

    def processor(self):
        self.metadb = OXDB(self.env['FMYSQL_META_DSN'])
        checks=self.config['CHECK_LIST']
        checks_data= self.metadb.get_executed_cursor(self.config['UPDATED_CHECK_LIST']).fetchall()
        checks_tuple=[]
        if checks_data<>checks:
            self.metadb.execute(self.config['TRUNCATE_CHECK_LIST'])
            for check in checks:
                check=check['check']
                if 'to' in check:
                    send_to=check['to']
                else:
                    send_to=None

                checks_tuple.append((check['check_id'],
                                     check['source_dsn'],
                                     check['check_type'],
                                     check['description'],
                                     check['check_result'],
                                     check['on_change'],
                                     check['priority'],
                                     send_to))

            self.metadb.executemany(self.config['INSERT_CHECK_LIST'], checks_tuple)
            self.metadb.commit()
        checks = self.metadb.get_executed_cursor(self.config['GET_CHECK_LIST']).fetchall()
        print checks
        self.metadb.close()
        content=""
        row_data=""
        for check in checks:
            check_id = check[0]
            check_type = check[2]
            des = check[3]
            command = check[4]
            priority = check[6]
            table_names = command
            if priority=='high':
                color_code='red'
            elif priority=='medium':
                color_code='orange'
            else:
                color_code='blue'

            if check[1]=='PYTHON':
                proc = subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
                (rows, err) = proc.communicate()
                fields=['log_name','log_error']
                rows = eval(rows)
                if len(rows)>0:
                    row_data = "<p><b><font color='" + color_code + "'> check_id - " + str(
                        check_id) + " | " + des + "</font></b></p>"
                    row_data += "<table><tr>"

                    for f in fields:
                        row_data = row_data + "<td><b>" + f + "</b></td>"
                    row_data += "</tr>"

                    for r in rows:
                        row_data += "<tr>"
                        for row_value in r:
                            row_data = row_data + "<td>" + str(row_value) + "</td>"
                        row_data += "</tr>"

                    row_data += "</table>"
                    content += row_data

            else:
                self.oxdb = OXDB(self.env[check[1]], None, False)
                self.logger.info(command)
                rows = self.oxdb.get_executed_cursor(command).fetchall()
                fields=self.oxdb.get_executed_cursor(command).description
                self.oxdb.close()

                self.metadb = OXDB(self.env['FMYSQL_META_DSN'])
                if len(rows)>0:
                    self.metadb.execute(self.config['CHECK_RUN_HISTORY'], check_id, rows[0][0])
                else:
                    self.metadb.execute(self.config['CHECK_RUN_HISTORY'], check_id, 0)
                self.metadb.commit()
                self.metadb.close()
                if check[1]!='PYTHON' and len(rows)>0:
                    if rows[0][0]>0:
                    # Compose the header of the table
                        row_data="<p><b><font color='"+color_code+"'> check_id - "+str(check_id)+" | "+des+"</font></b></p>"
                        row_data += "<table><tr>"
                        if len(fields)==1:
                            row_data = row_data + "<td><b>" + "check_id" + "</b></td>"
                            row_data = row_data + "<td><b>" + "check_type" + "</b></td>"
                            row_data = row_data   + "<td><b>" + "table_involved" + "</b></td>"
                            row_data = row_data + "<td><b>" + "bad_data_count" + "</b></td>"
                        else:
                            for f in fields:
                                row_data = row_data + "<td><b>" + f[0] + "</b></td>"
                        row_data += "</tr>"

                        # Fill the table with data
                        for r in rows:
                            row_data+="<tr>"
                            if len(fields)==1:
                                row_data = row_data + "<td>" + str(check_id) + "</td>"
                                row_data = row_data + "<td>" + str(check_type) + "</td>"
                                row_data = row_data + "<td>" + str(table_names) + "</td>"
                                row_data = row_data + "<td>" + str(r[0]) + "</td>"
                            else:
                                for i in r:
                                    row_data = row_data + "<td>" + str(i) + "</td>"
                            row_data+="</tr>"
                        row_data += "</table>"
                        content += row_data

        self.mail_result=self.mail_result+content+"</body></html>"
        part1 = MIMEText(self.mail_result, 'html')
        self.msg.attach(part1)
        s = smtplib.SMTP('localhost')
        if len(content) > 0:
            s.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())

if __name__=='__main__':
    yaml_file = sys.argv[0].replace('.py', '.yaml')
    checker = check(yaml_file)

    try:
        if not checker.lock.getLock():
            checker.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        checker.processor()

    except SystemExit:
        pass
    except:
        exc_type,exc_val = sys.exc_info()[:2]
        checker.logger.error("Error: %s, %s", exc_type,exc_val)
        raise
    finally:
        checker.lock.releaseLock()
