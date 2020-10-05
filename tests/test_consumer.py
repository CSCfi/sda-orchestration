"""Test Consumer class."""

import unittest
from unittest.mock import patch
from sda_orchestrator.utils.consumer import Consumer


class ConsumerTest(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        self._mq = Consumer(password="")  # nosec

    @patch("sda_orchestrator.utils.consumer.Connection")
    def test_validate_call(self, mock):
        """Test if pyscopg2 was called."""
        self._mq.create_connection()
        mock.assert_called()
