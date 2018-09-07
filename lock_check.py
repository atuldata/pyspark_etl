#!/bin/python -u
import sys
import yaml
import errno
import re
from util.oxdb import OXDB
from datetime import datetime, timedelta
from os.path import isfile, join
from os import listdir, path, kill, sysconf, sysconf_names, remove
from util.JobLock import JobLock
from util.EtlLogger import EtlLogger

class lock_check:

    def __init__(self, yaml_file):
        self.config = yaml.load(open(yaml_file))
        self.logger = EtlLogger.get_logger(self.__class__.__name__, log_level = self.config['LOG_LEVEL'])  # use class name as the log name
        self.lock = JobLock(self.__class__.__name__)  # use class name as the lock name
        self.env = yaml.load(open(self.config['ENV']))
        self.db = OXDB(self.env['FMYSQL_META_DSN'])
        self.lock_directory = self.config['lock_directory']
        self.run_limit = self.parse_time(self.config.get('run_limit', '6h'))
        self.logger.info('Process run limit : %s' %self.run_limit)
        self.white_list = self.config.get('white_list', [])
        self.logger.info('Process white list : %s' %self.white_list)

    def parse_time(self, time_str):
        regex = re.compile(r'((?P<hours>\d+?)hr)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')
        parts = regex.match(time_str)
        if not parts:
            return
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.iteritems():
            if param:
                time_params[name] = int(param)
        return timedelta(**time_params)

    def pid_exists(self, pid):
        """Check whether pid exists in the current process table.
        UNIX only.
        """
        if pid < 0:
            return False
        if pid == 0:
            # According to "man 2 kill" PID 0 refers to every process
            # in the process group of the calling process.
            # On certain systems 0 is a valid PID but we have no way
            # to know that in a portable fashion.
            raise ValueError('invalid PID 0')
        try:
            kill(pid, 0)
        except OSError as err:
            if err.errno == errno.ESRCH:
                # ESRCH == No such process
                return False
            elif err.errno == errno.EPERM:
                # EPERM clearly means there's a process to deny access to
                return True
            else:
                # According to "man 2 kill" possible error values are
                # (EINVAL, EPERM, ESRCH)
                raise
        else:
            return True

    def proc_starttime(self, pid):
        # https://gist.github.com/westhood/1073585
        p = re.compile(r"^btime (\d+)$", re.MULTILINE)
        with open("/proc/stat") as f:
            m = p.search(f.read())
        btime = int(m.groups()[0])

        clk_tck = sysconf(sysconf_names["SC_CLK_TCK"])
        with open("/proc/%d/stat" % pid) as f:
            stime = int(f.read().split()[21]) / clk_tck

        return datetime.fromtimestamp(btime + stime)

    def silentremove(self, filename):
        try:
            remove(filename)
        except OSError as e:  # this would be "except OSError, e:" before Python 2.6
            if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
                raise  # re-raise exception if a different error occurred

    def check_locks(self):
        for lock_file_path in [path.join(self.lock_directory, f) for f in listdir(self.lock_directory) if isfile(join(self.lock_directory, f)) and f.endswith('.LOCK')]:
            try:
                base_name = path.splitext(path.basename(lock_file_path))[0]
                if base_name in self.white_list:
                    self.logger.info('Skipping file because it has been whitelisted: %s' %lock_file_path)
                    continue

                self.db.execute(self.config['CLEANUP_SQL'], lock_file_path)

                with open(lock_file_path) as lock_file:
                    pid = lock_file.read()
                    if not pid:
                        self.silentremove(lock_file_path)
                        continue
                    pid = int(pid)
                    if not self.pid_exists(pid):
                        self.silentremove(lock_file_path)
                        continue
                    process_uptime = datetime.now() - self.proc_starttime(pid)
                    if process_uptime < self.run_limit:
                        continue
                    self.db.execute(self.config['INSERT_SQL'], lock_file_path, '%s'%process_uptime)
                    self.logger.info('Lock File : %s pid : %s uptime: %s' %(lock_file_path, pid, process_uptime))

            except Exception as e:
                self.logger.warn('Exception while checking lock file: %s. %s' % (lock_file_path, e))
        self.db.commit()

if __name__ == "__main__":

    yaml_file = sys.argv[0].replace('.py', '.yaml')
    instance = lock_check(yaml_file)

    try:
        if not instance.lock.getLock():
            instance.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        instance.check_locks()
    except SystemExit:
        pass
    except:
        instance.logger.error("Error: %s", sys.exc_info()[0])
        raise
    finally:
        instance.lock.releaseLock()
