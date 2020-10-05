"""Test ID consumers."""

import unittest
from unittest.mock import patch
import uuid
from sda_orchestrator.utils.id_ops import map_dataset_file_id, generate_accession_id


class IDOpsCalled(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch("logging.Logger.info")
    @patch("sda_orchestrator.utils.id_ops.map_file2dataset")
    def test_map_simple_file(self, mock, log):
        """Test if start a consumer was called."""
        data = {"filepath": "file.c4gh", "user": "test"}
        map_dataset_file_id(data, "checksum1", "accessionID")
        mock.assert_called_with("test", "file.c4gh", "checksum1", "urn:default:test")
        log.assert_called_with(
            "file with checksum: checksum1 mapped accessionID: accessionID and to dataset urn:default:test."
        )

    @patch("logging.Logger.info")
    @patch("sda_orchestrator.utils.id_ops.map_file2dataset")
    def test_map_simple_file_dir(self, mock, log):
        """Test if start a consumer was called."""
        data = {"filepath": "rooter/dir1/dir2/file.c4gh", "user": "test"}
        map_dataset_file_id(data, "checksum1", "accessionID")
        mock.assert_called_with("test", "rooter/dir1/dir2/file.c4gh", "checksum1", "urn:dir:rooter")
        log.assert_called_with(
            "file with checksum: checksum1 mapped accessionID: accessionID and to dataset urn:dir:rooter."
        )

    @patch(
        "sda_orchestrator.utils.id_ops.uuid4",
        return_value=uuid.UUID("urn:uuid:5fb82fa1-dcf9-431f-a5fc-fb72e2d2ee14"),
    )
    def test_generate_accession_id(self, mock):
        """Test generate accession id."""
        result = generate_accession_id()
        self.assertEqual(result, "urn:uuid:5fb82fa1-dcf9-431f-a5fc-fb72e2d2ee14")
