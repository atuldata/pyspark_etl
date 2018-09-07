import fcntl
import os

LOCK_FILE_PATH = '/var/dw-grid-etl/locks'
LOCK_FILE_EXTENSION = 'LOCK'


class JobLock:
    def __init__(self, job_name):
        self.lock_file_name = \
            os.path.join(
                LOCK_FILE_PATH, '.'.join([job_name, LOCK_FILE_EXTENSION]))
        self.got_lock = False
        self.lock_file = open(self.lock_file_name, 'a+')

        # Legacy properties below
        self.lockFile = self.lock_file
        self.lockFileName = self.lock_file_name
        self.gotLock = self.got_lock

    def get_lock(self):
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.got_lock = True
        except Exception:
            self.got_lock = False

        if self.got_lock:
            self.lock_file.truncate()
            self.lock_file.write(str(os.getpid()))
            self.lock_file.flush()

        return self.got_lock

    def release_lock(self):
        if self.got_lock:
            fcntl.flock(self.lock_file, fcntl.LOCK_UN)
            if os.path.exists(self.lock_file_name):
                os.remove(self.lock_file_name)
            self.got_lock = False

    # Legacy methods below
    getLock = get_lock
    releaseLock = release_lock

