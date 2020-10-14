"""Test DB ops."""

import unittest
from unittest.mock import patch
from sda_orchestrator.utils.db_ops import map_file2dataset


class DBOps(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch("psycopg2.connect")
    def test_validate_call(self, mock):
        """Test if pyscopg2 was called."""
        map_file2dataset("test", "rooter/dir1/dir2/file.c4gh", "checksum1")
        self.assertTrue(mock.called)
