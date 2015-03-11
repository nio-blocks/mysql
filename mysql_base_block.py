from datetime import timedelta

from nio.common.versioning.dependency import DependsOn
from nio.common.block.base import Block
from nio.metadata.properties import TimeDeltaProperty
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.object import ObjectProperty
from nio.metadata.properties.int import IntProperty
from nio.metadata.properties.holder import PropertyHolder
from nio.modules.scheduler import Job
from .driver.mysql import MySQL


class Credentials(PropertyHolder):

    """ User credentials
    Properties:
        username (str): User to connect as
        password (str): User's password
    """
    username = StringProperty(title='User to connect as',
                              default='[[MYSQL_USER]]')
    password = StringProperty(title='Password to connect with',
                              default='[[MYSQL_PASSWORD]]')


@DependsOn("nio.modules.scheduler")
class MySQLBase(Block):

    """ A block for inserting data into a MySQL database.
    Properties:
        host (str): location of the database
        port (int): open port served by database
        database (str): database name
        commit_interval: Specifies how many records before a commit call.
        retry_timeout: When disconnected, this specifies how long to wait
                       before attempting to connect.
    """
    host = StringProperty(title='MySQL Host',
                          default='[[MYSQL_HOST]]')
    port = IntProperty(title='Port',
                       default=3306)
    database = StringProperty(title='Database Name', default="signals")
    credentials = ObjectProperty(Credentials, title='Credentials')
    commit_interval = IntProperty(title='Commit Interval', default=50)
    retry_timeout = TimeDeltaProperty(title="Retry Timeout",
                                      default={"seconds": 1})

    def __init__(self):
        super().__init__()
        self._db = None
        self._connection_job = None

    def configure(self, context):
        super().configure(context)
        self._db = MySQL(self.host, self.port, self.database,
                         self.credentials.username, self.credentials.password,
                         self.commit_interval,
                         self._logger,
                         target_table=self.get_target_table())
        self._connect()

    def stop(self):
        # Cancel pending reconnects if any
        if self._connection_job:
            self._connection_job.cancel()
            self._connection_job = None

        self._close_connection()
        super().stop()

    def process_signals(self, signals, input_id='default'):
        if self.connected:
            self.deliver_signals(signals)
        else:
            self._on_discarded_signals(signals)

    def deliver_signals(self, signals, retry=True):
        """ Allows for a retry when processing
        signals, currently if signals fail to be delivered
        the first time, and it was a connection issue, it will
        try, only once, to reconnect and deliver again.
        """
        try:
            output = self.execute_query(signals)
            # Check if we have anything to output
            if output:
                self.notify_signals(output)
        except Exception as e:
            exception_details = str(e)
            if "connect" in exception_details.lower() and retry:
                # attempt an immediate reconnect
                self._reconnect()
                # if reconnected fine
                if self.connected:
                    # attempt to add these signals again, specify not to retry
                    self.deliver_signals(signals, False)
                else:
                    self._logger.error('Exception, details {0}'.
                                       format(exception_details))

    def execute_query(self, signals):
        """ To be implemented by inheriting classes
        """
        raise NotImplementedError()

    def get_target_table(self):
        """ A target table is needed only when saving,
         allow an override in child classes.
        """
        return None

    def _connect(self):
        """ Connect to database, this method as built-in
        reconnection functionality
        """
        self._logger.debug("Connecting to: {0}:{1}".format(
            self.host, self.port))
        try:
            self._db.open()
            self._logger.debug("Connected to: {0}:{1}".format(
                self.host, self.port))
        except Exception as e:
            self._logger.debug("Failed to connect, details: {0}".
                               format(str(e)))
            self._connection_job = Job(
                self._reconnect,
                timedelta(seconds=self.retry_timeout.total_seconds()),
                repeatable=False)

    def _close_connection(self):
        if self.connected:
            try:
                self._logger.debug("Closing connection")
                self._db.close()
            except Exception as e:
                self._logger.debug("Failed to close connection, details".
                                   format(str(e)))

    def _reconnect(self):
        self._connection_job = None
        self._close_connection()
        self._connect()

    def _on_discarded_signals(self, signals):
        # TODO, good place to implement functionality for
        # "saving" signals hoping for a reconnect and have no-loss
        self._logger.warning('Block is not connected, discarding: {0} '
                             'signals'.format(len(signals)))

    @property
    def connected(self):
        return self._db and self._db.connected