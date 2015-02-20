from nio.common.versioning.dependency import DependsOn
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.block.base import Block
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.object import ObjectProperty
from nio.metadata.properties.int import IntProperty
from nio.metadata.properties.holder import PropertyHolder
from .driver.mysql import MySQL


class Credentials(PropertyHolder):

    """ User credentials
    Properties:
        username (str): User to connect as
        password (str): User's password
    """
    username = StringProperty(title='User to connect as',
                              default='root')
    password = StringProperty(title='Password to connect with',
                              default='mysqlroot')


@DependsOn("nio.modules.communication", "1.0.0")
@Discoverable(DiscoverableType.block)
class MySQLDBInsert(Block):

    """ A block for inserting data into a MySQL database.
    Properties:
        host (str): location of the database
        port (int): open port served by database
        database (str): database name
    """
    host = StringProperty(title='MySQL Host',
                          default='[[MYSQL_HOST]]')
    port = IntProperty(title='Port',
                       default=3306)
    database = StringProperty(title='Database Name', default="signals")
    credentials = ObjectProperty(Credentials, title='Credentials')
    commit_interval = IntProperty(title='Commit Interval', default=50)
    tables_prefix = StringProperty(title='Tables prefix', default="")

    def __init__(self):
        super().__init__()
        self._db = None

    def configure(self, context):
        super().configure(context)
        try:
            self._connect_to_db()
            self._logger.debug("Connected")
        except Exception as e:
            self._logger.error(
                "Could not connect to MySQL instance: {}".format(e))

    def stop(self):
        if self._db:
            self._db.close()
        super().stop()

    def process_signals(self, signals, input_id='default'):
        self._db.add_items(signals)

    def _connect_to_db(self):
        print('host: {0}'.format(self.host))
        self._db = MySQL(self.host, self.port, self.database,
                         self.credentials.username, self.credentials.password,
                         self.commit_interval,
                         self._logger, self.tables_prefix)
        self._db.open()
