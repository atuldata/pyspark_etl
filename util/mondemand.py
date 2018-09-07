#!/opt/bin/python -u
# Note: scripts using this code should make sure to send out
# mondemand stats each minute, otherwise the collected data will not
# look as expected on the grafana side.
from __future__ import print_function

import os
import subprocess
import socket
from optparse import OptionParser

def strip_quotes(s):
   if len(s)==0:
      return s
   if s[0]=='"':
      s = s[1:]
   if len(s)==0:
      return s
   if s[-1]=='"':
      return s[:-1]

def parse_config(filename="/etc/mondemand/mondemand.conf", logger=None):
   if not os.path.isfile(filename):
      if logger:
         logger.info("Did not find mondemand config file %s" %(filename))
      else:
         print("Did not find mondemand config file %s" % filename)
      return None
   (addr, port, ttl) = (None, None, None)
   with open(filename, "r") as f:
	  for line in f:
	     line = line[:-1]
	     parts = line.split("=")
	     if len(parts) != 2:
		    continue
	     # result[parts[0]] = strip_quotes(parts[1])
	     key = parts[0]
	     value = strip_quotes(parts[1])
	     if key == "MONDEMAND_ADDR":
		    addr = value
	     elif key == "MONDEMAND_PORT":
		    port = value
	     elif key == "MONDEMAND_TTL":
		    ttl = value
   if addr and port:
      if ttl:
	     return ("lwes::%s:%s:%s" % (addr, port,  ttl), addr, port)
      else:
	     return ("lwes::%s:%s" % (addr, port), addr, port)
   else:
      return (None, None, None)

# Counters are meant to keep a running count over time (total number of x seen for the life of y) where as gauges represent the current number of something for the given time.

class MondemandSender(object):

   def __init__(self, program, context=None, logger=None):
      (self.config, self.mondemand_host, self.mondemand_port) = \
          parse_config(logger=logger)
      self.program = program
      self.logger = logger
      if not context:
         # we want the equivalent of hostname -s, see the OX mondemand wiki page
         context = 'host:' + socket.getfqdn().split('.')[0]
      self.context = context

   def send(self, string):
      args = ['mondemand-tool', '-o', self.config, '-p', self.program, '-c', self.context, '-s', string]
      command = " ".join(args)
      if self.logger:
         log = "calling "+command
         self.logger.info(log)
      ret = subprocess.call(args)
      return ret

   def send_counter(self, counter_name, value):
	  return self.send('counter:%s:%d' % (counter_name, value))

   def send_gauge(self, gauge_name, value):
	  return self.send('gauge:%s:%d' % (gauge_name, value))

   def send_gauge_string(self, gauge_name, value):
       return self.send('gauge:%s:%s' % (gauge_name, value))

if __name__=="__main__":
   parser = OptionParser()
   parser.add_option("-p", dest="program", type="string")
   parser.add_option("-c", dest="context", type="string")
   parser.add_option("-n", dest="gauge_name", type="string")
   parser.add_option("-v", dest="gauge_value", type="int")
   (options, args) = parser.parse_args()
   sender = MondemandSender(options.program, options.context)
   sender.send_gauge(options.gauge_name, options.gauge_value)
                                                                     
