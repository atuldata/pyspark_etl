"""
This is the recommended logger to use to ensure consistency in the way we log.
"""
import logging
import logging.handlers
import os.path

LOG_DIRECTORY = '/var/dw-grid-etl/logs'
LOG_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'
LOG_FORMAT_DEBUG = \
    '[%(asctime)s] %(levelname)s: [%(module)s:%(name)s.%(funcName)s:%(lineno)d] ' + \
    '%(message)s'
LOG_LEVEL = logging.INFO
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
MAX_BYTES = 100000000
BACKUP_COUNT = 5

class EtlLogger:

    def __init__(self):
        pass

    @staticmethod
    def get_logger(
            log_name='etl_logger',
            log_directory=LOG_DIRECTORY,
            log_format=None,
            log_level=LOG_LEVEL,
            log_date_format=LOG_DATE_FORMAT,
            max_bytes=MAX_BYTES,
            backup_count=BACKUP_COUNT):
        """
        This returns a logger with our default location and format.
        """
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level)

        # Initialize the logging formatter
        if log_format is None:
            log_format = \
                LOG_FORMAT_DEBUG if log_level == logging.DEBUG else LOG_FORMAT
        formatter = logging.Formatter(log_format, datefmt=log_date_format)
    
        # Initialize the logger
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)
    
        # Add a rotating file based log handler to the logger
        log_file = os.path.join(log_directory, log_name + '.log')
        rotating_file_handler = \
            logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count)
        rotating_file_handler.setFormatter(formatter)
        logger.addHandler(rotating_file_handler)
    
        # Also add console handler to output ERROR to console
        # XXX Why do we want to log errors to the console?
        # If the error is catastrophic then just raise it.
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console.setFormatter(formatter)
        logger.addHandler(console)
    
        return logger

