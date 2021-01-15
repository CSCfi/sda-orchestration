"""Test ID consumers."""

import unittest
from unittest.mock import patch
import uuid
from sda_orchestrator.utils.id_ops import generate_dataset_id, generate_accession_id


class IDOpsCalled(unittest.TestCase):
    """Test for IDs p[s]."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    def test_map_simple_file(self):
        """Test if we can map a single file."""
        result = generate_dataset_id("user", "user/txt1.c4gh")
        self.assertEqual("urn:neic:user", result)

    def test_map_simple_file_dir(self):
        """Test if dir scheme affects urn."""
        result = generate_dataset_id("user", "user/smth/smth2/txt9.c4gh")
        self.assertEqual("urn:neic:user-smth", result)

    @patch(
        "sda_orchestrator.utils.id_ops.uuid4",
        return_value=uuid.UUID("urn:uuid:5fb82fa1-dcf9-431f-a5fc-fb72e2d2ee14"),
    )
    def test_generate_accession_id(self, mock):
        """Test generate accession id."""
        result = generate_accession_id()
        self.assertEqual(result, "urn:uuid:5fb82fa1-dcf9-431f-a5fc-fb72e2d2ee14")
