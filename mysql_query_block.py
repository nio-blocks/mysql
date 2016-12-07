from nio.util.discovery import discoverable
from nio.properties import Property
from nio.signal.base import Signal

from .mysql_base_block import MySQLBase
from . import evaluate_expression


@discoverable
class MySQLQuery(MySQLBase):

    """ A block for inserting data into a MySQL database.
    Properties:
        query: mysql statement to execute
    """
    query = Property(
        title='Query', default="SELECT * from {{$table}}")

    def execute_query(self, signals):
        for signal in signals:
            # evaluate resulting statement
            query = self.query(signal)
            rows, description = self._db.execute_statement(query, "fetchall")
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
