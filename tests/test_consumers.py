"""Test AMQP consumers."""

import unittest
from unittest.mock import patch
from sda_orchestrator.complete_consume import main


class CompleteConsumerCalled(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch("sda_orchestrator.complete_consume.CompleteConsumer")
    def test_start_complete_consumer(self, mock):
        """Test if start a consumer was called."""
        main()
        self.assertTrue(mock.called)
