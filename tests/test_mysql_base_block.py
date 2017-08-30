import logging
from nio.signal.base import Signal
from threading import Event
from unittest.mock import Mock

from nio.testing.block_test_case import NIOBlockTestCase
from nio.util.discovery import not_discoverable

from ..mysql_base_block import MySQLBase


class MySQLLikeException(Exception):

    def __init__(self):
        super().__init__("Could not connect to MySQL")


@not_discoverable
class MySQLBaseWithConnect(MySQLBase):

    def __init__(self, e, retries=1):
        super().__init__()
        self.e = e
        self.retries = retries
        self.retry_count = 0

    def _connect(self):
        self._db.open = Mock(side_effect=MySQLLikeException())
        super()._connect()

    def _reconnect(self):
        # retries and retry_count make sure that reconnect eventually stops.
        self.retry_count += 1
        if self.retry_count >= self.retries:
            self.e.set()
            return
        else:
            super()._reconnect()


class TestMySQLInsert(NIOBlockTestCase):

    def test_connect(self):
        blk = MySQLBase()
        blk._connect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk._connect.called)
        self.assertIsNone(blk._connection_job)

    def test_reconnect(self):
        """ tests that _reconnect is called on failed _connect """
        # also tests that subsequent calls to _reconnect continue to be made.
        e = Event()
        num_retries = 2
        blk = MySQLBaseWithConnect(e, num_retries)
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "retry_timeout": {"seconds": 0.01},
            "log_level": logging.DEBUG
        })
        blk.start()
        # wait for reconnect to be called num_retries times
        e.wait(1)
        self.assertEqual(blk.retries, blk.retry_count)
        self.assertIsNotNone(blk._connection_job)
        blk.stop()
        self.assertIsNone(blk._connection_job)

    def test_process_signals(self):
        blk = MySQLBase()
        blk._connect = Mock()
        blk._on_discarded_signals = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "retry_timeout": {"seconds": 0.01},
            "log_level": logging.DEBUG
        })
        self.assertFalse(blk.connected)
        blk.start()
        blk.process_signals([Signal()])
        self.assertTrue(blk._on_discarded_signals.called)

    def test_stop(self):
        blk = MySQLBase()
        blk._connect = Mock()
        blk._close_connection = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "retry_timeout": {"seconds": 0.01},
            "log_level": logging.DEBUG
        })
        self.assertFalse(blk.connected)
        blk.start()
        blk.process_signals([Signal()])
        blk.stop()

        self.assertTrue(blk._close_connection.called)
