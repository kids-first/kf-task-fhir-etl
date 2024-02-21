import os
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")

DEFAULT_LOG_LEVEL = logging.INFO
# DEFAULT_FORMAT = (
#    "%(asctime)s - %(name)s" " - %(threadName)s - %(levelname)s - %(message)s"
# )
DEFAULT_FORMAT = (
    "%(asctime)s - %(name)s" " - %(levelname)s - %(message)s"
)

INDEXD_BASE_URL = os.environ.get("INDEXD_BASE_URL") or (
    "https://data.kidsfirstdrc.org"
)
# NCI Gen3 Service where file metadata is stored: DCF
DCF_BASE_URL = os.environ.get("DCF_BASE_URL") or (
    "https://nci-crdc.datacommons.io"
)
INDEXD_ENDPOINT = os.environ.get("INDEXD_ENDPOINT") or "index/index"

KF_API_DATASERVICE_URL = (
    os.getenv("KF_API_DATASERVICE_URL")
    or "https://kf-api-dataservice.kidsfirstdrc.org/"
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
