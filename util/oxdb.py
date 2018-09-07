"""
Base for db connections for OX.
"""
import pyodbc

# These are used to match against the driver name.
SCHEMA_SETTERS = {
    'vertica': 'set search_path=%s',
    'myodbc': 'use %s'
}


class OXDB(object):
    """
    This will cache and return a database connection.
    Currently only supports connection via DSN in odbc.ini
    Required:
        1) dsn
    Optional:
        1) schema - This in the case of Vertica is used for the search_path.
                    For mysql this would be the database name. Used in
                    initializing the connection.

        2) support_transacations - set to True by default.   Should be set to False
            if connecting to Hive, Impala, or other non-transactional data source.
    """
    _connection = None

    def __init__(self, dsn, schema=None, support_transactions=True):
        self.dsn = dsn
        self.schema = schema
        self.support_transactions = support_transactions
        self.connect_string = "DSN=%s" % self.dsn

    def close(self):
        """
        Closes the connection and removes it from the cached connections.
        """
        self.connection.close()

    def commit(self):
        """
        Calls commit on the connection.
        """
        self.connection.commit()

    @property
    def connection(self):
        """
        Connection is created once for the instance. Will recreate if destroyed.
        """
        if self._connection is None:
            if self.support_transactions:
                self._connection = pyodbc.connect(self.connect_string)
            else:
                # counterintuitive, but set autocommit=True so pyodbc does not try to disable transactional support
                # and thus thrown an error for dbs that do NOT support transacations
                self._connection = pyodbc.connect(self.connect_string, autocommit=True)
            if self.schema is not None:
                driver_name = \
                    self.connection.getinfo(pyodbc.SQL_DRIVER_NAME).lower()
                for key, setter in SCHEMA_SETTERS.items():
                    if key in driver_name:
                        self.execute(setter % self.schema)

        return self._connection

    def get_row(self, *args):
        """
        Will ALWAYS return a Tuple even if the results are None.
        Only ever returns one row. Cannot be used as an iterator.
        """
        cursor = self.get_executed_cursor(*args)
        default_row = tuple([None] * len(cursor.description))

        for row in cursor:
            return row

        return default_row

    def get_rows(self, *args):
        """
        Returns an array of tuples for the query.
        Can return a single tuple of None's if the result set is empty.
        Recommend using the get_executed_cursor since it returns an iterator
        and uses minimal RAM.
        """
        cursor = self.get_executed_cursor(*args)
        default_rows = [tuple([None] * len(cursor.description))]

        rows = [row for row in self.get_executed_cursor(*args)]
        if len(rows):
            return rows

        return default_rows

    def execute(self, *args, **kwargs):
        """
        Executes a statement and returns the row count affected.
        """
        cursor = self.get_executed_cursor(*args)
        if 'commit' in kwargs and isinstance(kwargs['commit'], bool) \
                and kwargs['commit']:
            self.commit()

        return cursor.rowcount

    def executemany(self, *args, **kwargs):
        """
        Executes a statement and returns the row count affected.
        """
        cursor = self.connection.cursor()
        cursor.executemany(*args)
        if 'commit' in kwargs and isinstance(kwargs['commit'], bool) \
                and kwargs['commit']:
            self.commit()

        return cursor.rowcount

    def get_executed_cursor(self, *args):
        """
        Returns a cursor with the executed statement.
        """
        cursor = self.connection.cursor()
        cursor.execute(*args)

        return cursor

    def rollback(self):
        """
        Calls rollack() on the connection.
        """
        self.connection.rollback()
