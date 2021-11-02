import logging
import sys
from logging.handlers import TimedRotatingFileHandler
FORMATTER = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(LOG_FILE):
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(logger_name,LOG_FILE):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG) # лучше иметь больше логов, чем их нехватку
    if (logger.hasHandlers()):
        logger.handlers.clear()
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler(LOG_FILE))
    logger.propagate = False
    return logger