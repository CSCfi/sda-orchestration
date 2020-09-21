"""Logging formatting."""

import logging
import os

# Keeping it simple with the logging formatting

FORMAT = (
    "[%(asctime)s][%(name)s][%(process)d %(processName)s][%(levelname)-8s] (L:%(lineno)s) %(funcName)s: %(message)s"
)
logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
LOG = logging.getLogger("sda_orchestrator")
# By default the logging level would be INFO
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG.setLevel(log_level)
