MySQLInsert
===========

Stores input signals in a MySQL database.

Properties
--------------

-   **host**: MySQL server host.
-   **port**: MySQL server port.
-   **database**: Database name.
-   **commit_interval**: Specifies how many records before a commit call.
-   **retry_timeout**: When disconnected, this specifies how long to wait before attempting to connect.
-   **target_table**: Allows to specify/calculate table name from signal

Dependencies
----------------

-   [pymysql](https://pypi.python.org/pypi/PyMySQL/)

Commands
----------------
None

Input
-------
A record will be inserted into the database for each input signal.

Output
---------
None

----------------
