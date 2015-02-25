from nio.modules.threading import RLock


class SQL(object):

    #TODO: this LUT substitution helper could come from configuration/setting
    TABLE_NAME_TRANSLATIONS = {'Signal': 'NIOSignal'}

    class FieldItem(object):
        def __init__(self, name, type_in):
            self.name = name
            self.type = type_in

    def __init__(self, database, commit_interval, logger, target_table):
        super().__init__()
        self._database = database
        self._commit_interval = commit_interval
        self._logger = logger
        self._target_table = target_table

        self._connection = None
        self._connection_lock = RLock()
        # caches field definitions for each table in the database
        self._tables = {}
        self._tables_lock = RLock()
        self._uncommitted = 0

    def open(self):
        """ Initiates a database connection
        """
        with self._connection_lock:
            self.setup_connection()
        table_names = self.execute_fetch_all_statement(self.get_table_names())
        for table_name, in table_names:
            self._update_field_definitions(table_name)

    def close(self):
        """ Terminates a database connection
        """

        # commit any unsaved changes if any
        self.commit()
        if self.connected:
            with self._connection_lock:
                try:
                    self.connection.close()
                except Exception as e:
                    self._logger.warning(
                        'Trying to close database: {0}, details: {1}'.
                        format(self._database, str(e)))
                finally:
                    self.connection = None

    def delete_table(self, table):
        self.execute_statement("DROP TABLE IF EXISTS `{0}`".format(table))

    def add_items(self, items):
        """ Add items to database, each item can potentially
        go to a different table depending of its class type

        Args:
            items: list of items to add to database, the table an
                item goes to is determined based on the item class
        """

        processed_items = 0
        if self.connected:
            # each item can potentially add columns to a table,
            # make sure potential new table structure can store item
            self._adjust_tables_structure(items, False)

            # determine for each table, the values that will be inserted to it
            value_tables = {}
            for e in items:
                table_name = self.get_table_name(e)
                if table_name not in value_tables:
                    value_tables[table_name] = []

                with self._tables_lock:
                    if table_name in self._tables:
                        processed_items += 1

                        # add item
                        value_tables[table_name].append(
                            self._get_values(
                                e,
                                self._tables[table_name]["field_item_list"]))

            # execute actual insert queries per table
            for table in value_tables:
                items_count = len(value_tables[table])
                if items_count:
                    with self._tables_lock:
                        statement = "INSERT INTO {0} ({1}) VALUES ({2});".\
                            format(table,
                                   self._tables[table]["field_names"],
                                   self._tables[table]["field_formats"])

                    with self._connection_lock:
                        cursor = self.connection.cursor()
                        # convert item values to tuple and insert
                        values = tuple(value_tables[table])
                        try:
                            cursor.executemany(statement, values)
                            # keep track of rows added
                            self._uncommitted += items_count

                            self._logger.debug(
                                'Inserted: {0} into: {1}, statement: {2}, '
                                'values:{3}'.format(items_count, table,
                                                    statement, values))
                        except Exception as e:
                            self._logger.warning(
                                'Executing: {0} with value: {1}, details: {2}'.
                                format(statement, tuple(value_tables[table]),
                                       str(e)))
                            raise e
                        finally:
                            cursor.close()

            if self._uncommitted >= self._commit_interval:
                # reset counter and commit
                self._uncommitted = 0
                self.commit()

        return processed_items

    def dump(self, rows_per_table=-1):
        """ Convenient method to dump database tables to a dictionary
        """
        self.commit()

        tables = {}
        with self._tables_lock:
            for table in self._tables:
                statement = \
                    "SELECT * FROM {0} {1}".format(
                        table, "" if rows_per_table == -1 else "LIMIT {0}".
                        format(rows_per_table))
                tables[table] = self.execute_fetch_all_statement(statement)

        return tables

    def clean_table(self, table):
        """ Remove all records in a table

        Args:
            table: table to clean
        """

        statement = "DELETE from {0};".format(table)
        self.execute_statement(statement)
        self.commit()

    def execute_statement(self, statement, cursor_call=None):
        """ Executes a statement

        Args:
            statement: statement to execute
        """

        # create table
        with self._connection_lock:
            cursor = self.connection.cursor()
            try:
                result = cursor.execute(statement)
                if cursor_call:
                    result = getattr(cursor, cursor_call)()
            finally:
                cursor.close()

        return result

    def execute_fetch_all_statement(self, statement):
        return self.execute_statement(statement, "fetchall")

    def execute_fetch_one_statement(self, statement):
        return self.execute_statement(statement, "fetchone")

    def execute_fetch_one_statement1(self, statement):
        """ Executes a statement

        Args:
            statement: statement to execute
        """

        # create table
        with self._connection_lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(statement)
                result = cursor.fetchone()
            finally:
                cursor.close()

        return result

    @property
    def connected(self):
        with self._connection_lock:
            return self.connection is not None

    def get_table_name(self, item):
        """ Finds out table name for a given item
        """
        try:
            if callable(self._target_table):
                table_name = self._target_table(item)
        except Exception as e:
            self._logger.warning(
                'Target table method failure: {0}, details: {1}'.
                format(self._target_table, str(e)))
            table_name = item.__class__.__name__

        if table_name in SQL.TABLE_NAME_TRANSLATIONS:
            table_name = SQL.TABLE_NAME_TRANSLATIONS[table_name]
        return table_name

    def commit(self):
        if self.connected:
            with self._connection_lock:
                try:
                    self.connection.commit()
                except Exception as e:
                    self._logger.warning(
                        'Trying to commit changes: {0}, details: {1}'.
                        format(self._database, str(e)))

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    def _get_field_names(self, table):
        """ Finds out field names for a given table
        """

        fields_def = self.execute_fetch_all_statement(
            self.get_fields_statement(table))
        field_names = self.parse_field_names(fields_def)
        return field_names

    def _get_field_definitions(self, table):
        """ Provides table definitions
        """
        fields = self._get_field_names(table)
        field_item_list = []
        field_formats = ""
        field_names = ""
        field_no = 0
        for field in fields:
            field_item_list.append(SQL.FieldItem(field, None))
            field_names += "{0}`{1}`".format("," if field_no else "",
                                             field)
            field_formats += "{0}{1}".format("," if field_no else "",
                                             self.get_field_format())
            field_no += 1
        return field_item_list, field_names, field_formats

    def _create_table(self, table, fields):
        fields_definition = ""
        field_no = 0
        for field in fields:
            primitive_type = type(fields[field])
            fields_definition += \
                "{0}`{1}` {2}".format("," if field_no else "",
                                      field,
                                      self.get_type(primitive_type))
            field_no += 1

        statement = "CREATE TABLE IF NOT EXISTS {0}({1})".\
            format(table, fields_definition)
        self._logger.debug('Creating table: {0}, statement: {1}'.
                           format(table, statement))
        try:
            self.execute_statement(statement)
        except Exception as e:
            self._logger.error("Error creating table, please make sure "
                               "table name: {0} is allowed".format(table))
            raise e

    def _alter_table(self, table, fields, item):
        self._logger.info('Altering table: {0}, fields: {1} need to be added'.
                          format(table, fields))

        for field in fields:
            field_type = type(getattr(item, field))
            statement = "ALTER TABLE {0} ADD COLUMN `{1}` {2};".\
                format(table, field, self.get_type(field_type))
            self.execute_statement(statement)

        self._update_field_definitions(table)

    def _get_values(self, item, field_item_list):
        """ Processes the values for a given item so that
        they can be saved as a row
        """
        values = []
        for field_item in field_item_list:
            # old value = getattr(item, field_item.name, None)
            value = self._get_field_value(item, field_item.name)
            if value is not None:
                if field_item.type is None:
                    field_item.type = type(value)

                try:
                    value = self.get_value(value, field_item.type)
                except Exception as e:
                    self._logger.error(
                        "Could not get value, field: {0}, type: {1} "
                        "from value: {2}, details: {3}".format(
                            field_item.name, field_item.type, value, str(e)))

            values.append(value)

        return values

    def _get_item_dict(self, item):
        try:
            return item.to_dict()
        except:
            return item.__dict__

    def _get_field_value(self, item, field):
        return getattr(item, field, None)

    def _update_field_definitions(self, table):
        """ Updates internal table definitions
        """
        try:
            with self._tables_lock:
                if not table in self._tables:
                    self._tables[table] = {}

                self._tables[table]["field_item_list"], \
                    self._tables[table]["field_names"], \
                    self._tables[table]["field_formats"] = \
                    self._get_field_definitions(table)

                # create a case insensitive field list
                self._tables[table]["field_list"] = \
                    [field.name.lower()
                     for field in self._tables[table]["field_item_list"]]

            self._logger.debug('Updated field definitions for table: {0}'.
                               format(table))
            self._logger.debug('field_names is: {0}'.
                               format(self._tables[table]["field_names"]))
            self._logger.debug('field_formats is: {0}'.
                               format(self._tables[table]["field_formats"]))
            self._logger.debug('field_list is: {0}'.
                               format(self._tables[table]["field_list"]))

        except Exception as e:
            self._logger.error('Updating field definitions: {0}, details: {1}'.
                               format(self._database, str(e)))
            raise e

    def _check_table(self, table, fields):
        """ Makes sure a table exists and updates internal table
        definitions
        """
        statement = self.get_table_exists_statement(table)
        try:
            with self._connection_lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute(statement)
                    (count,) = cursor.fetchone()
                finally:
                    cursor.close()
            if not count:
                self._logger.debug('Creating table {0}'.format(table))
                self._create_table(table, fields)
                self._update_field_definitions(table)

        except Exception as e:
            self._logger.error(
                'Failed to find out whether table exists: {0}, '
                'details: {1}'.format(self._database, str(e)))
            raise e

    def _adjust_tables_structure(self, items, in_error_mode=False):
        """ Makes sure table columns structure is up to date and can handle
            all item attributes
        """
        for e in items:
            item_dict = self._get_item_dict(e)
            table_name = self.get_table_name(e)
            self._check_table(table_name, item_dict)

            new_fields = []
            with self._tables_lock:
                if table_name in self._tables:
                    # any fields not in table?
                    new_fields = [field for field in item_dict
                                  if field.lower() not in
                                  self._tables[table_name]["field_list"]]
            if len(new_fields):
                try:
                    self._alter_table(table_name, new_fields, e)
                except Exception as e:
                    if not in_error_mode:
                        self._logger.warning(
                            'Table {0} might be out of sync, new fields '
                            'are: {1}'.format(table_name, new_fields))
                        # make a call to update field definitions in case
                        # it is out of sync.
                        self._update_field_definitions(table_name)
                        # and try again
                        self._adjust_tables_structure(items, True)
                        self._logger.info('Table: {0}, successfully '
                                          'recovered from out of sync '
                                          'condition'.format(table_name))
                    else:
                        self._logger.error(
                            'Table {0} structure could not be updated, '
                            'new fields are: {1}'.
                            format(table_name, new_fields))
                        raise e

    # implementation specific methods
    def setup_connection(self):
        pass

    def get_type(self, primitive_type):
        pass

    def get_field_format(self):
        pass

    def get_fields_statement(self, table):
        pass

    def parse_field_names(self, fields_def):
        return []

    def get_table_exists_statement(self, table):
        pass

    def get_value(self, value, type_in):
        pass

    def get_table_names(self):
        pass
