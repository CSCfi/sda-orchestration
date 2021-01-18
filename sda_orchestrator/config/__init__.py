"""SDA orchestrator configuration.

Configuration required for DOIs, REMS and other metadata.
"""

from os import environ
from pathlib import Path
from typing import Dict
import json


def parse_config_file(config_file: str) -> Dict:
    """Load JSON schemas."""
    file_path = Path(config_file)

    with open(file_path, "r") as fp:
        return json.load(fp)


CONFIG_INFO = parse_config_file(
    environ.get("CONFIG_FILE", str(Path(__file__).resolve().parent.joinpath("config.json")))
)
