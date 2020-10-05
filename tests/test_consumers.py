"""Test AMQP consumers."""

import unittest
from unittest.mock import patch
from sda_orchestrator.inbox_consume import main as inbox_main
from sda_orchestrator.complete_consume import main as complete_main
from sda_orchestrator.verified_consume import main as verified_main


class ConsumersCalled(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch("sda_orchestrator.complete_consume.CompleteConsumer")
    def test_start_complete_consumer(self, mock):
        """Test if start a consumer was called."""
        complete_main()
        self.assertTrue(mock.called)

    @patch("amqpstorm.Connection")
    @patch("sda_orchestrator.inbox_consume.InboxConsumer")
    def test_start_inbox_consumer(self, mock, amqp_mock):
        """Test if start a consumer was called."""
        inbox_main()
        self.assertTrue(mock.called)

    @patch("amqpstorm.Connection")
    @patch("sda_orchestrator.verified_consume.VerifyConsumer")
    def test_start_verified_consumer(self, mock, amqp_mock):
        """Test if start a consumer was called."""
        verified_main()
        self.assertTrue(mock.called)
