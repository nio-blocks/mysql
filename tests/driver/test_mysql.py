from nio.common.signal.base import Signal
# TODO, set this to False by default
from ...driver.mysql import MySQL

skip_tests = False
reason = ""

# ignore tests when pymysql is not installed
# noinspection PyBroadException
try:
    import pymysql
except:
    skip_tests = True
    reason = "pymysql is not installed"

from datetime import datetime
import unittest
import logging

from nio.modules.threading import spawn


class AttributeTypes(object):

    def __init__(self, int_field, string_field,
                 bool_field, float_field, datetime_field):
        super().__init__()
        self.int_field = int_field
        self.string_field = string_field
        self.bool_field = bool_field
        self.float_field = float_field
        self.datetime_field = datetime_field


class AttributeTypesWithToDict(Signal):

    def __init__(self, int_field, string_field,
                 bool_field, float_field, datetime_field):
        super().__init__()
        self.int_field = int_field
        self.string_field = string_field
        self.bool_field = bool_field
        self.float_field = float_field
        self.datetime_field = datetime_field


class Type1(object):

    def __init__(self, field1, field2):
        super().__init__()
        self.field1 = field1
        self.field2 = field2


class Type2(object):

    def __init__(self, field21, field22):
        super().__init__()
        self.field21 = field21
        self.field22 = field22


class IdItem(object):

    def __init__(self, _id):
        super().__init__()
        self.id = _id


@unittest.skipIf(skip_tests, reason)
class TestMySQL(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger("test_MySQL")
        logger.setLevel(logging.DEBUG)

        # ch = logging.StreamHandler()
        # create formatter
        # formatter = logging.Formatter(
        #     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # add formatter to ch
        # ch.setFormatter(formatter)
        # add ch to logger
        # logger.addHandler(ch)

        # every test starts with a clean slate
        try:
            self.connection = pymysql.connect(host="127.0.0.1",
                                              port=3306,
                                              user='root',
                                              passwd='mysqlroot')
        except Exception as e:
            if "connect to MySQL" not in str(e):
                raise e
            else:
                logger.warning("MySQL connection error, Test skipped")
                self._outcome.success = False
                return

        cursor = self.connection.cursor()
        cursor.execute("DROP DATABASE IF EXISTS nio_unittests")
        cursor.close()

        # create instance and connect
        self.my_sql = MySQL("127.0.0.1", 3306, "nio_unittests",
                            'root', 'mysqlroot',
                            10, logger, self.get_table_name)
        self.assertIsNone(self.my_sql.connection)

        self.my_sql.open()
        self.assertIsNotNone(self.my_sql.connection)

    def tearDown(self):

        self.my_sql.close()
        self.assertIsNone(self.my_sql.connection)

        # DROP DATABASE nio_unittests
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("DROP DATABASE nio_unittests")
            cursor.close()

    def get_table_name(self, item):
        # do default anyways
        return item.__class__.__name__

    def test_add_item(self):

        item = AttributeTypes(1, "string1", True, 2.2, datetime.now())
        table_name = self.my_sql.get_table_name(item)

        stored_items = self.my_sql.add_items([item])
        self.assertEqual(stored_items, 1)

        rows = self.my_sql.dump()
        self.assertEqual(len(rows[table_name]), 1)

    def test_add_item_with_to_dict(self):

        item = AttributeTypesWithToDict(
            1, "string1", True, 2.2, datetime.now())
        table_name = self.my_sql.get_table_name(item)

        stored_items = self.my_sql.add_items([item])
        self.assertEqual(stored_items, 1)

        rows = self.my_sql.dump()
        self.assertEqual(len(rows[table_name]), 1)

    def test_add_attribute_on_the_fly(self):

        item = Type1(1, "string1")
        table_name = self.my_sql.get_table_name(item)

        stored_items = self.my_sql.add_items([item])
        self.assertEqual(stored_items, 1)
        rows = self.my_sql.dump()
        self.assertEqual(len(rows[table_name][0]), 2)

        # create a new item of the same type but add a new attribute,
        # thus testing ability to modify table on the fly
        item = Type1(2, "string2")
        # make it a float attribute
        setattr(item, "field3", 3.3)
        stored_items = self.my_sql.add_items([item])
        self.assertEqual(stored_items, 1)
        rows = self.my_sql.dump()
        self.assertEqual(len(rows[table_name][0]), 3)

        rows = self.my_sql.dump()
        self.assertEqual(len(rows), 1)
        table_name = self.my_sql.get_table_name(item)
        self.assertEqual(len(rows[table_name]), 2)

    def test_two_item_types(self):

        item11 = Type1(11, "string11")
        item12 = Type1(12, "string12")
        item2 = Type2(21, "string21")

        table1_name = self.my_sql.get_table_name(item11)
        table2_name = self.my_sql.get_table_name(item2)

        stored_items = self.my_sql.add_items([item11, item2, item12])
        self.assertEqual(stored_items, 3)
        rows = self.my_sql.dump()
        # verify that table1 has two items
        self.assertEqual(len(rows[table1_name]), 2)
        # verify that table2 has one item
        self.assertEqual(len(rows[table2_name]), 1)

    def add_items(self, index, item_count, add_field=False):
        for i in range(item_count):
            _id = "item_{0}_{1}".format(index, i)
            item = IdItem(_id)
            if add_field:
                setattr(item, "added_field", _id)
            self.my_sql.add_items([item])

    def test_concurrency(self):

        table_name = self.my_sql.get_table_name(IdItem(1))
        thread_count = 1

        items_per_thread1 = 90
        threads = []
        for thread_index in range(thread_count):
            threads.append(
                spawn(self.add_items, thread_index, items_per_thread1))

        # add to the mixture additions requiring table structure modifications
        items_per_thread2 = 10
        for thread_index in range(thread_count):
            threads.append(
                spawn(self.add_items, thread_index, items_per_thread2,
                      True))

        items_per_thread3 = 100
        threads = []
        for thread_index in range(thread_count):
            threads.append(
                spawn(self.add_items, thread_index, items_per_thread3))

        # up to here we have 200 items per thread
        # must have 2000 items

        for thread in threads:
            thread.join()

        rows = self.my_sql.dump()
        items_total = items_per_thread1 + items_per_thread2 + items_per_thread3
        self.assertEqual(len(rows[table_name]), items_total * thread_count)

        # make sure items with extra field were populated as expected
        expected_count = items_per_thread2 * thread_count
        (count,), description = self.my_sql.execute_fetch_one_statement(
            "select count(*) from IdItem where added_field is not NULL;")
        self.assertEqual(expected_count, count)

    def test_get_value(self):
        mysql = MySQL(None, None, None, None, None, None, None)
        value = mysql.get_value(1, int)
        self.assertTrue(isinstance(value, int))
        value = mysql.get_value(False, bool)
        self.assertTrue(isinstance(value, bool))
        value = mysql.get_value(1.1, float)
        self.assertTrue(isinstance(value, float))
        value = mysql.get_value([1, 1], list)
        self.assertTrue(isinstance(value, str))
        value = mysql.get_value(datetime.now(), datetime)
        self.assertTrue(isinstance(value, datetime))
        value = mysql.get_value("a string", str)
        self.assertTrue(isinstance(value, str))


def test_suite():
    return unittest.TestSuite((unittest.makeSuite(TestMySQL)))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
