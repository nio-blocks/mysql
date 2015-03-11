MySQLBase
===========

Stores input signals in a MySQL database.

Properties
--------------

-   **host**: MySQL server host.
-   **port**: MySQL server port.
-   **database**: Database name.
-   **commit_interval**: Specifies how many records before a commit call.
-   **retry_timeout**: When disconnected, this specifies how long to wait before attempting to connect.

Dependencies
----------------

-   [pymysql](https://pypi.python.org/pypi/PyMySQL/)

Commands
----------------
None

Input
-------
None

Output
---------
None

----------------

MySQLInsert
===========

Stores input signals in a MySQL database.

Properties
--------------

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
A signal containing number of successfully added items

----------------

MySQLQuery
===========

Queries MySQL database.

Properties
--------------

-   **query**: Query statement
-   **commit**: Specifies if a commit statement should follow query execution

Dependencies
----------------

-   [pymysql](https://pypi.python.org/pypi/PyMySQL/)

Commands
----------------
None

Input
-------
Signals to be processed.

Output
---------
Data satisfying query in the form of 'Signal' instances.

----------------
