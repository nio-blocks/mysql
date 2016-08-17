from nio.util.discovery import discoverable
from nio import Signal
from nio.properties import Property

from .mysql_base_block import MySQLBase


@discoverable
class MySQLInsert(MySQLBase):

    """ A block for inserting data into a MySQL database.
    Properties:
        target_table = ExpressionProperty(
            title='Target table', default="{{($__class__.__name__)}}")
    """
    target_table = Property(
        title='Target table', default="{{($__class__.__name__)}}")

    def get_target_table(self):
        return self.target_table()

    def execute_query(self, signals):
        added_items = self._db.add_items(signals)
        return [Signal({"inserted": added_items})]
