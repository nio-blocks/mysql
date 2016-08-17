import logging
from nio import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from ..mysql_insert_block import MySQLInsert
from . import mysql_running


class TestMySQLInsert(NIOBlockTestCase):

    def setUp(self):
        self._outcome.success = mysql_running("127.0.0.1", 3306,
                                              "root", "mysqlroot")
        if self._outcome.success:
            super().setUp()

    def test_process_signals(self):

        insert_blk = MySQLInsert()
        self.configure_block(insert_blk, {
            "host": "127.0.0.1",
            "credentials": {"username": "root", "password": "mysqlroot"},
            "log_level": logging.DEBUG
        })
        insert_blk.start()
        self._es_find_signals_notified = []
        insert_blk.process_signals([Signal({"test_field": True})])
        insert_blk.stop()
        # assert than a signal containing inserted items count was received
        self.assertEqual(len(self._es_find_signals_notified), 1)

    def signals_notified(self, signals, output_id='default'):
        if hasattr(self, "_es_find_signals_notified"):
            self._es_find_signals_notified.extend(signals)
