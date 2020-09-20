"""Fetching IDs for files and datasets."""

from pathlib import Path
from .db_ops import map_file2dataset
from .logger import LOG
import secrets
import string


def map_dataset_file_id(msg: dict, decrypted_checksum: str, accessionID: str) -> None:
    """Map accession ID to dataset.

    Generate dataset id based on folder or user.
    """
    file_path = Path(msg["filepath"])
    file_path_parts = file_path.parts
    dataset = ""
    # if a file it is submited in the root directory the dataset
    # is the urn:default:<username>
    # otherwise we take the root directory and construct the path
    # urn:dir:<root_dir>
    if len(file_path_parts) < 2:
        dataset = f'urn:default:{msg["user"]}'
    else:
        dataset = f"urn:dir:{file_path_parts[0]}"

    map_file2dataset(msg["user"], msg["filepath"], decrypted_checksum, dataset)

    LOG.info(f'filepath: {msg["decrypted_checksums"]} mapped stableID {accessionID} and to dataset {dataset}.')


def generate_accession_id() -> str:
    """Generate Stable ID."""
    accessionID = "SDAF" + "".join(secrets.choice(string.digits) for i in range(16))
    return accessionID
