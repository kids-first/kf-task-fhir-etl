import os
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")

DEFAULT_LOG_LEVEL = logging.INFO
#DEFAULT_FORMAT = (
#    "%(asctime)s - %(name)s" " - %(threadName)s - %(levelname)s - %(message)s"
#)
DEFAULT_FORMAT = (
    "%(asctime)s - %(name)s" " - %(levelname)s - %(message)s"
)


def init_logger(
    log_level=DEFAULT_LOG_LEVEL,
):
    """
    Configure and create the logger

    :param log_level: a string specifying what level of log messages to record
    in the log file. Values are not case sensitive. The list of acceptable
    values are the names of Python's standard lib logging levels.
    (critical, error, warning, info, debug, notset)
    :type log_level: one of logging modules log levels
    """
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logging.Formatter(DEFAULT_FORMAT))

    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(consoleHandler)

