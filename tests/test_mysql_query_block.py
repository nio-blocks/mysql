import logging
from nio import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from ..mysql_insert_block import MySQLInsert
from ..mysql_query_block import MySQLQuery
from . import mysql_running


class TestMySQLQuery(NIOBlockTestCase):

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
        insert_blk.process_signals([Signal({"test_field": True})])
        insert_blk.stop()

        query_blk = MySQLQuery()
        self.configure_block(query_blk, {
            "host": "127.0.0.1",
            "credentials": {"username": "root", "password": "mysqlroot"},
            "log_level": logging.DEBUG
        })
        query_blk.start()

        self._es_find_signals_notified = []
        query_blk.process_signals([Signal({"table": "NIOSignal"})])
        self.assertGreater(len(self._es_find_signals_notified), 0)

    def signals_notified(self, signals, output_id='default'):
        if hasattr(self, "_es_find_signals_notified"):
            self._es_find_signals_notified.extend(signals)
