"""SDA orchestrator configuration.

Configuration required for DOIs, REMS and other metadata.
"""

from os import environ, strerror
from pathlib import Path
from typing import Dict
import json
import errno

from ..utils.logger import LOG


def parse_config_file(config_file: str) -> Dict:
    """Load JSON schemas."""
    file_path = Path(config_file)
    if not file_path.is_file():
        LOG.error(f"File {file_path} not found")
        raise FileNotFoundError(errno.ENOENT, strerror(errno.ENOENT), file_path)
    with open(file_path, "r") as fp:
        return json.load(fp)


CONFIG_INFO = parse_config_file(
    environ.get("CONFIG_FILE", str(Path(__file__).resolve().parent.joinpath("config.json")))
)
