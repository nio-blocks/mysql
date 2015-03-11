from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import ExpressionProperty, BoolProperty
from nio.common.signal.base import Signal

from .mysql_base_block import MySQLBase
from . import evaluate_expression


@Discoverable(DiscoverableType.block)
class MySQLQuery(MySQLBase):

    """ A block for inserting data into a MySQL database.
    Properties:
        query: mysql statement to execute
        commit: indicates if after running the query a commit is to be made
    """
    query = ExpressionProperty(
        title='Query', default="SELECT * from {{$table}}")
    commit = BoolProperty(default=False, title='Commit statement')

    def __init__(self):
        super().__init__()
        self._db = None
        self._connection_job = None

    def execute_query(self, signals):
        for signal in signals:
            # evaluate resulting statement
            query = evaluate_expression(self.query, signal, False)
            rows, description = self._db.execute_statement(query, "fetchall")
            # should a commit be made
            if self.commit:
                self._db.connection.commit()
            if rows:
                # grab field names from description
                field_names = [i[0] for i in description]
                output = []
                for row in rows:
                    # create signal with resulting data
                    signal_data = {field_names[i]: row[i]
                                   for i in range(len(field_names))}
                    output.append(Signal(signal_data))
                return output
