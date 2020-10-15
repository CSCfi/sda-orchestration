"""Fetching IDs for files and datasets."""

from pathlib import Path
from uuid import uuid4


def generate_dataset_id(user: str, inbox_path: str) -> str:
    """Map accession ID to dataset.

    Generate dataset id based on folder or user.
    """
    file_path = Path(inbox_path)
    file_path_parts = file_path.parts
    dataset = ""
    # if a file it is submited in the root directory the dataset
    # is the urn:default:<username>
    # otherwise we take the root directory and construct the path
    # urn:dir:<root_dir>
    if len(file_path_parts) <= 2:
        dataset = f"urn:default:{user}"
    else:
        # if it is / then we take the next value
        dataset = f"urn:dir:{file_path_parts[1]}"

    return dataset


def generate_accession_id() -> str:
    """Generate Stable ID."""
    accessionID = uuid4()
    return accessionID.urn
