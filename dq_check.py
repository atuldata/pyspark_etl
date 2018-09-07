#!/opt/bin/python -u

import yaml
import sys
import os
import subprocess

from monitor_log import LogErrors
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger
from util.oxdb import OXDB
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import json

class dq_check:
    def __init__(self, yaml_file):

        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.env = yaml.load(open(self.config['ENV']))
        self.msg = MIMEMultipart('alternative')
        subject=self.env['IMPALA_DSN']
        self.msg['Subject'] = subject.replace('_Impala','')+" DQ Framework Checks"
        self.msg['From'] = self.config["MAIL_FROM"]
        self.msg['To'] = self.config["MAIL_TO"]
        self.mail_result = '<html><head><style> body {background-color: powderblue;} h1   {color: blue;}p    {color: red;} td, th {border: 1px solid #999;padding: 0.5rem;}</style></head><body>'

    def processor(self):
        self.metadb = OXDB(self.env['FMYSQL_META_DSN'])
        checks=self.config['CHECK_LIST']
        checks_data= self.metadb.get_executed_cursor('select * from dq_check_list').fetchall()
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
                                     send_to
                                                  ))

            self.metadb.executemany(self.config['INSERT_CHECK_LIST'], checks_tuple)
            self.metadb.commit()
        checks = self.metadb.get_executed_cursor(self.config['GET_CHECK_LIST']).fetchall()


        self.metadb.close()
        content=""
        result_set=[]
        file = open('data.js', 'w')
        file_output = open('index.html', 'w')
        file_index = open('index_pre.html', 'r')
        file_output.writelines(file_index.readlines())
        file_output.writelines('<div style="width:100%; height: 200px; ">')
        file.write("$(function() {\n")
        file_index.close()
        for check in checks:
            dict_data=[]
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
            else:
                self.oxdb = OXDB(self.env[check[1]], 'mstr_datamart', False)
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
                column_list=[]
                if check[1]!='PYTHON' and len(rows)>0:
                    if rows[0][0]>0:
                    # Compose the header of the table
                        row_data="<p><b><font color='"+color_code+"'> check_id - "+str(check_id)+" | "+des+"</font></b></p>"
                        row_data += "<table><tr>"
                        file_output.writelines('<div class="cellContainer">')
                        file_output.writelines('<div id="check_'+str(check_id)+' style="margin: 5px; "></div>')
                        file_output.writelines('</div>')

                        if len(fields)==1:
                            row_data = row_data + "<td><b>" + "check_id" + "</b></td>"
                            row_data = row_data + "<td><b>" + "check_type" + "</b></td>"
                            row_data=row_data   + "<td><b>" + "table_involved" + "</b></td>"
                            row_data = row_data + "<td><b>" + "bad_data_count" + "</b></td>"

                        else:
                            for f in fields:
                                row_data = row_data + "<td><b>" + f[0] + "</b></td>"
                                column_list.append({'title': f[0], 'index': f[0],'flex': 1})
                        row_data += "</tr>"

                        # Fill the table with data
                        for r in rows:
                            row_data+="<tr>"
                            if len(fields)==1:
                                row_data = row_data + "<td>" + str(check_id) + "</td>"
                                row_data = row_data + "<td>" + str(check_type) + "</td>"
                                row_data = row_data + "<td>" + str(table_names) + "</td>"
                                row_data = row_data + "<td>" + str(r[0]) + "</td>"
                                dict_data.append({'check_id': str(check_id),'check_type': str(check_type),'table_names': str(table_names),'bad_data_count': str(r[0])})
                                column_list.append(
                                    {'title': 'check_id', 'index': 'check_id','flex': 1})
                                column_list.append(
                                    {'title': 'check_type', 'index': 'check_type','flex': 1})
                                column_list.append(
                                    {'title': 'table_names', 'index': 'table_names','flex': 1})
                                column_list.append(
                                    {'title': 'bad_data_count', 'index': 'bad_data_count','flex': 1})
                                dict_data.append({'check_id': str(check_id), 'check_type': str(check_type),
                                                  'table_names': str(table_names), 'bad_data_count': str(r[0])})
                            else:
                                item={}
                                for f in fields:
                                   item[f[0]]=''
                                for i in range(len(r)):
                                    row_data = row_data + "<td>" + str(r[i]) + "</td>"
                                    item[fields[i][0]]=str(r[i])
                                dict_data.append(item)
                            row_data+="</tr>"
                    row_data += "</table>"


                    file.write("var check_"+str(check_id)+" = ")
                    file.write(json.dumps(dict_data).replace("},","},\n"))
                    file.write(";\n")
                    file.write("\n")
                    fancy_grid="new FancyGrid({\n"
                    fancy_grid += "title: "+"'check_"+str(check_id)+" "+str(des)+"',\n"
                    fancy_grid+="renderTo: "+"'check_"+str(check_id)+"',\n"
                    fancy_grid += "width: 'fit',\n"
                    fancy_grid += "height: 'fit',\n"
                    fancy_grid += "theme: 'blue',\n"
                    fancy_grid += "selModel: 'row',\n"
                    fancy_grid += "trackOver: true,\n"
                    fancy_grid += "data: "+"check_"+str(check_id)+",\n"
                    fancy_grid += "clicksToEdit: 1,\n"
                    fancy_grid += "defaults: {\n"
                    fancy_grid += "type: 'string',\n"
                    fancy_grid += "editable: true,\n"
                    fancy_grid += "filter: {\n"
                    fancy_grid += "header: true\n"
                    fancy_grid += "}\n"
                    fancy_grid += "},\n"
                    fancy_grid += "paging: true,\n"
                    fancy_grid += "columns: "
                    fancy_grid += json.dumps(column_list).replace("},","},\n").replace('"title"','title').replace('"index"','index').replace('"flex"','flex')+"\n"
                    fancy_grid+="});\n"
                    file.write(fancy_grid)
                    file.write("\n")

                content += row_data
                if check[1]=='PYTHON':
                    row_data = "<p><b><font color='" + color_code + "'> check_id - " + str(
                        check_id) + " | " + des + "</font></b></p>"
                    row_data += "<table><tr>"
                    for f in fields:
                        row_data = row_data + "<td><b>" + f + "</b></td>"
                        print row_data
                    for r in rows:
                        for i in range(len(fields)):
                            row_data = row_data + "<td>" + str(r[i]) + "</td>"
                            print row_data
                    content += row_data
                if check[7]:
                    mail_result=self.mail_result+row_data+"</body></html>"
                    separate_msg=MIMEMultipart('alternative')
                    part1 = MIMEText(mail_result, 'html')
                    separate_msg['From']=self.msg['From']
                    separate_msg['Subject']=self.env['IMPALA_DSN']+" Alerts"
                    separate_msg.attach(part1)
                    new = smtplib.SMTP('localhost')
                    new.sendmail(separate_msg['From'], check[7], separate_msg.as_string())
                    new.close()

        self.mail_result=self.mail_result+content+"</body></html>"
        file.write("});\n")
        file.close()
        file_output.writelines('</div></body></html>')
        file_output.close()
        part1 = MIMEText(self.mail_result, 'html')
        self.msg.attach(part1)
        s = smtplib.SMTP('localhost')
        if len(content) > 0:
            s.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())

if __name__=='__main__':
    yaml_file = sys.argv[0].replace('.py', '.yaml')
    checker = dq_check(yaml_file)

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
