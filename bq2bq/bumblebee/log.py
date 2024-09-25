import sys
import logging
import os

def get_log_level():
    log_level = str(os.environ.get("LOG_LEVEL", default="INFO")).upper()
    log_level = log_level if log_level in logging._nameToLevel else "INFO"
    return logging._nameToLevel.get(log_level)

def get_logger(name: str):
    logger = logging.getLogger(name)
    logformat = "[%(asctime)s] %(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(level=get_log_level(), stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

    return logger
