import datetime
from .sql import SQL


class MySQL(SQL):
    """ Overrides methods to provide a MySQL implementation complementing
    SQL parent class.
    """
    def __init__(self, host, port,
                 database, user, password,
                 commit_interval, logger, target_table=None):
        super().__init__(database,
                         commit_interval,
                         logger,
                         target_table)
        self._host = host
        self._port = port
        self._user = user
        self._password = password

    def get_field_format(self):
        return "%s"

    def parse_field_names(self, fields_in):
        """ Obtain field names from a fields retrieval query

        Args:
            fields_in: list of tuples containing field definitions
        """
        fields_names = []
        for field_tuple in fields_in:
            list_fields = list(field_tuple)
            fields_names.append(list_fields[0].replace("`", ""))
        return fields_names

    def get_fields_statement(self, table):
        return "SHOW COLUMNS FROM {0};".format(table)

    def get_table_names(self):
        return "SELECT table_name FROM information_schema.tables " \
               "WHERE table_schema='{0}'".format(self._database)

    def get_table_exists_statement(self, table):
        return "SELECT COUNT(*) FROM information_schema.tables " \
               "WHERE table_schema='{0}' AND table_name='{1}'".\
            format(self._database, table)

    def get_type(self, primitive_type):
        type_out = "TEXT"
        if primitive_type == int:
            type_out = "INTEGER"
        elif primitive_type == bool:
            type_out = "BOOLEAN"
        elif primitive_type == datetime.datetime:
            type_out = "DATETIME"
        elif primitive_type == float:
            type_out = "FLOAT"

        return type_out

    def get_value(self, value, type_in):
        if type_in == int:
            value = int(value)
        elif type_in == bool:
            value = bool(value)
        elif type_in == float:
            value = float(value)
        elif type_in == list:
            value = "'{0}'".format(tuple(value))

        return value

    def setup_connection(self):
        import pymysql
        try:
            self.connection = pymysql.connect(host=self._host,
                                              port=self._port,
                                              user=self._user,
                                              passwd=self._password,
                                              db=self._database,
                                              charset='utf8')
        except Exception as e:
            self._logger.warning(
                'Trying to open database: {0}, details: {1}'.
                format(self._database, str(e)))
            # attempt to create the database
            self.connection = pymysql.connect(host=self._host,
                                              port=self._port,
                                              user=self._user,
                                              passwd=self._password)
            cursor = self.connection.cursor()
            statement = "CREATE DATABASE IF NOT EXISTS `{0}`".format(
                self._database)
            cursor.execute(statement)
            cursor.close()

            # now attempt to select database
            self.connection.select_db(self._database)
