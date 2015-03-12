from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.signal.base import Signal
from nio.metadata.properties import ExpressionProperty

from .mysql_base_block import MySQLBase


@Discoverable(DiscoverableType.block)
class MySQLInsert(MySQLBase):

    """ A block for inserting data into a MySQL database.
    Properties:
        target_table = ExpressionProperty(
            title='Target table', default="{{($__class__.__name__)}}")
    """
    target_table = ExpressionProperty(
        title='Target table', default="{{($__class__.__name__)}}")

    def get_target_table(self):
        return self.target_table

    def execute_query(self, signals):
        added_items = self._db.add_items(signals)
        return [Signal({"inserted": added_items})]
