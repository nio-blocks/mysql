from datetime import timedelta

from nio.util.versioning.dependency import DependsOn
from nio.util.discovery import not_discoverable
from nio.block.base import Block
from nio.properties import TimeDeltaProperty, StringProperty, \
    ObjectProperty, IntProperty, PropertyHolder, BoolProperty
from nio.modules.scheduler import Job

from .driver.mysql import MySQL


class Credentials(PropertyHolder):

    """ User credentials
    Properties:
        username (str): User to connect as
        password (str): User's password
    """
    username = StringProperty(title='Username', default='[[MYSQL_USER]]')
    password = StringProperty(title='Password', default='[[MYSQL_PASSWORD]]')


@not_discoverable
@DependsOn("nio.modules.scheduler")
class MySQLBase(Block):

    """ A block for inserting data into a MySQL database.
    Properties:
        host (str): location of the database
        port (int): open port served by database
        database (str): database name
        commit_after_query: Specifies how many records before a commit call.
        retry_timeout: When disconnected, this specifies how long to wait
                       before attempting to connect.
    """
    host = StringProperty(title='MySQL Host', default='[[MYSQL_HOST]]')
    port = IntProperty(title='Port', default=3306)
    database = StringProperty(title='Database Name', default="signals")
    credentials = ObjectProperty(Credentials, title='Connection Credentials')
    commit_after_query = BoolProperty(
        title='Commit After Query', default=False)
    retry_timeout = TimeDeltaProperty(title="Retry Timeout",
                                      default={"seconds": 1})

    def __init__(self):
        super().__init__()
        self._db = None
        self._connection_job = None

    def configure(self, context):
        super().configure(context)
        self._db = MySQL(self.host(), self.port(), self.database(),
                         self.credentials().username(),
                         self.credentials().password(),
                         self.commit_after_query(),
                         self.logger,
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
                    self.logger.exception('Unable to reconnect and send')
            else:
                self.logger.exception('Unable to execute query')

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
        self.logger.debug("Connecting to: {0}:{1}".format(
            self.host(), self.port()))
        try:
            self._db.open()
            self.logger.debug("Connected to: {0}:{1}".format(
                self.host(), self.port()))
        except Exception as e:
            self.logger.exception("Failed to connect to database, retrying")
            self._connection_job = Job(
                self._reconnect,
                timedelta(seconds=self.retry_timeout().total_seconds()),
                repeatable=False)

    def _close_connection(self):
        if self.connected:
            try:
                self.logger.debug("Closing connection")
                self._db.close()
            except Exception as e:
                self.logger.debug(
                    "Failed to close connection, details".format(str(e)))

    def _reconnect(self):
        self._connection_job = None
        self._close_connection()
        self._connect()

    def _on_discarded_signals(self, signals):
        # TODO, good place to implement functionality for
        # "saving" signals hoping for a reconnect and have no-loss
        self.logger.warning(
            'Block is not connected, discarding: {0} signals'.format(
                len(signals))
        )

    @property
    def connected(self):
        return self._db and self._db.connected
