import logging
from time import sleep
from nio.common.signal.base import Signal
from nio.util.support.fast_tests import sleeping_test
from ..mysql_insert_block import MySQLInsert
from unittest.mock import Mock
from nio.util.support.block_test_case import NIOBlockTestCase


class MySQLLikeException(Exception):
    def __init__(self):
        super().__init__("Could not connect to MySQL")


class MySQLInsertWithConnect(MySQLInsert):
    def _connect(self):
        self._db.open = Mock(side_effect=MySQLLikeException())
        super()._connect()


class TestMySQLInsert(NIOBlockTestCase):

    def test_connect(self):
        blk = MySQLInsert()
        blk._connect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk._connect.called)
        self.assertIsNone(blk._connection_job)

    @sleeping_test
    def test_reconnect(self):
        blk = MySQLInsertWithConnect()
        blk._reconnect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "retry_timeout": {"seconds": 0.01},
            "log_level": logging.DEBUG
        })
        blk.start()
        sleep(0.1)
        self.assertTrue(blk._reconnect.called)
        self.assertIsNotNone(blk._connection_job)
        blk.stop()
        self.assertIsNone(blk._connection_job)

    def test_process_signals(self):
        blk = MySQLInsert()
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
        blk = MySQLInsert()
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
