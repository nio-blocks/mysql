MySQLInsert
===========
Stores input signals in a MySQL database.

Properties
----------
- **commit_after_query**: Whether or not to issue a commit after a query is executed.
- **credentials**: MySQL user name and password to connect to database.
- **database**: Name of MySQL database to connect to.
- **host**: MySQL server host.
- **port**: MySQL server port.
- **retry_timeout**: When disconnected, this specifies how long to wait before attempting to connect.
- **target_table**: MySQL table to insert into.  Allows to specify/calculate table name from signal

Inputs
------
- **default**: A record will be inserted into the database for each input signal.

Outputs
-------
- **default**: A signal containing number of successfully added items.

Commands
--------
None

Dependencies
------------
-   [pymysql](https://pypi.python.org/pypi/PyMySQL/)

MySQLQuery
==========
Queries a MySQL database.

Properties
----------
- **commit_after_query**: Whether or not to issue a commit after a query is executed.
- **credentials**: MySQL user name and password to connect to database.
- **database**: Name of MySQL database to connect to.
- **host**: MySQL server host.
- **port**: MySQL server port.
- **query**: SQL query to execute.
- **retry_timeout**: When disconnected, this specifies how long to wait before attempting to connect.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: Data satisfying query in the form of 'Signal' instances.

Commands
--------
None

Dependencies
------------
-   [pymysql](https://pypi.python.org/pypi/PyMySQL/)
