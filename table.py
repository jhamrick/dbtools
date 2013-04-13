import sqlite3 as sql
import numpy as np
import pandas as pd
import re


class Table(object):
    """A frame-like interface to a SQLite database table.

    """

    @classmethod
    def create(cls, db, name, dtypes, primary_key=None,
               autoincrement=False, verbose=False):
        """Create a table called `name` in the database `db`.

        Parameters

            db: path to the SQLite database

            name: name of the desired table

            dtypes: list of 2-tuples, where each tuple corresponds a
            desired column and has the format (column name, data type).

            primary_key: (default=None) name of the primary key
            column. If None, no primary key is set.

            autoincrement: (default=False) set the primary key column to
            automatically increment.

            verbose: (default=False) print out SQL command information.

        Returns: Table object

        """

        args = []

        for label, dtype in dtypes:
            # parse the python type into a SQL type
            if dtype is None:
                sqltype = "NULL"
            elif dtype is int or dtype is long:
                sqltype = "INTEGER"
            elif dtype is float:
                sqltype = "REAL"
            elif dtype is str or dtype is unicode:
                sqltype = "TEXT"
            elif dtype is buffer:
                sqltype = "BLOB"
            else:
                raise ValueError("invalid data type: %s" % dtype)

            # construct the SQL syntax for this column
            arg = "%s %s" % (label, sqltype)
            if primary_key is not None and primary_key == label:
                if sqltype != "INTEGER":
                    raise ValueError(
                        "invalid data type for primary key: %s" % dtype)
                arg += " PRIMARY KEY"
                if autoincrement:
                    arg += " AUTOINCREMENT"

            args.append(arg)

        # connect to the database and create the table
        conn = sql.connect(db)
        with conn:
            cur = conn.cursor()
            cmd = "CREATE TABLE %s(%s)" % (name, ', '.join(args))
            if verbose:
                print cmd
            cur.execute(cmd)

        tbl = cls(db, name, verbose=verbose)
        return tbl

    def __init__(self, db, name, verbose=False):
        """Create an interface to the table `name` in the database `db`.

        Parameters

            db: the path to the SQLite database

            name: the name of the table in the database

            verbose: (default=False) print out SQL command information

        """

        # save the parameters
        self.db = str(db)
        self.name = str(name)
        self.verbose = bool(verbose)

        # query the database for information about the table
        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT sql FROM sqlite_master "
                        "WHERE tbl_name='%s' and type='table'" % name)
            info = cur.fetchall()

        # parse the response -- it will look like 'CREATE TABLE
        # name(col1 TYPE, col2 TYPE, ...)'
        args = re.match("([^\(]*)\((.*)\)", info[0][0]).groups()[1]

        # compute repr string
        self.repr = "%s(%s)" % (self.name, args)

        # get the column names
        cols = args.split(", ")
        self.columns = tuple([x.split(" ")[0] for x in cols])

        # parse primary key, if any
        pk = [x.split(" ", 1)[1].find("PRIMARY KEY") > -1 for x in cols]
        primary_key = np.nonzero(pk)[0]
        if len(primary_key) > 1:
            raise ValueError("more than one primary key: %s" % primary_key)
        elif len(primary_key) == 1:
            self.primary_key = self.columns[primary_key[0]]
        else:
            self.primary_key = None

        # parse autoincrement, if applicable
        ai = [x.split(" ", 1)[1].find("AUTOINCREMENT") > -1 for x in cols]
        autoincrement = np.nonzero(ai)[0]
        if len(autoincrement) > 1:
            raise ValueError("more than one autoincrementing "
                             "column: %s" % autoincrement)
        elif self.primary_key is not None and len(autoincrement) == 1:
            if self.primary_key != self.columns[autoincrement[0]]:
                raise ValueError("autoincrement is different from primary key")
            self.autoincrement = True
        else:
            self.autoincrement = False

    def drop(self):
        """Drop the table from its database.

        Note that after calling this method, this Table object will no
        longer function properly, as it will correspond to a table that
        does not exist.

        """

        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cmd = "DROP TABLE %s" % self.name
            if self.verbose:
                print cmd
            cur.execute(cmd)

    def insert(self, values=None):
        """Insert values into the table.

        The `values` parameter should be a list of non-string sequences
        (or a single sequence, which will then be encased in a
        list). Each sequence is handled as follows:

            - If the sequence is a dictionary, then the keys should
              correspond to column names and the values should match the
              data types of the columns.

            - If the sequence is not a dictionary, it should have length
              equal to the number of columns and each element should be
              a value to be inserted in the corresponding column.

        If the table has an autoincrementing primary key, then this
        value should be excluded from every sequence as it will be
        filled in automatically.

        """

        # argument parsing -- `values` should be a list of sequences
        if values is None:
            values = {}
        if hasattr(values, 'keys') or not hasattr(values, "__iter__"):
            values = [values]
        elif not hasattr(values[0], "__iter__"):
            values = [values]

        # find the columns, excluding the primary key, that we need to
        # insert values for
        cols = list(self.columns)
        if self.autoincrement:
            cols.remove(self.primary_key)
        ncol = len(cols)

        # extract the entries from the values that were given
        entries = []
        for vals in values:
            if hasattr(vals, 'keys'):
                entry = tuple([vals.get(key, None) for key in cols])
            elif hasattr(vals, "__iter__"):
                if len(vals) != ncol:
                    raise ValueError("expected %d values, got %d" % (
                        ncol, len(vals)))
                entry = tuple(vals)
            else:
                raise ValueError(
                    "expected dict or list/tuple, got: %s" % type(vals))

            entries.append(entry)

        # target string of NULL and question marks
        qm = ["?"]*ncol
        if self.autoincrement:
            qm.insert(self.columns.index(self.primary_key), 'NULL')
        qm = ", ".join(qm)

        # perform the insertion
        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            for entry in entries:
                cmd = ("INSERT INTO %s VALUES (%s)" % (self.name, qm), entry)
                if self.verbose:
                    print ", ".join([str(x) for x in cmd])
                cur.execute(*cmd)

    def select(self, columns=None, where=None):
        # argument parsing
        if columns is None:
            cols = list(self.columns)
        else:
            if not hasattr(columns, '__iter__'):
                cols = [columns]
            else:
                cols = list(columns)

        # select primary key even if not given, so we can use the
        # correct index later
        if self.primary_key is not None and self.primary_key not in cols:
            cols.insert(0, self.primary_key)
        sel = ",".join(cols)

        # base query
        query = "SELECT %s FROM %s" % (sel, self.name)
        # add a selection filter, if specified
        if where is not None:
            where_str, where_args = where
            query += " WHERE %s" % where_str
            if not hasattr(where_args, "__iter__"):
                where_args = (where_args,)
            cmd = (query, where_args)
        else:
            cmd = (query,)

        if self.verbose:
            print ", ".join([str(x) for x in cmd])

        # connect to the database and execute the query
        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cur.execute(*cmd)
            rows = cur.fetchall()

        # now we need to parse the result into a DataFrame
        if self.primary_key in cols:
            index = self.primary_key
        else:
            index = None
        data = pd.DataFrame.from_records(
            rows, columns=cols, index=index,
            coerce_float=True)

        return data

    def __getitem__(self, key):
        if isinstance(key, int):
            # select a row
            if not self.autoincrement:
                raise ValueError("no autoincrementing primary key column")
            data = self.select(where=("%s=?" % self.primary_key, key))

        elif isinstance(key, slice):
            # select multiple rows
            if (key.start is None and
                    key.stop is None and
                    key.step in (None, 1)):
                where = None

            else:
                if not self.autoincrement:
                    raise ValueError("no autoincrementing primary key column")
                if key.step not in (None, 1):
                    raise ValueError("cannot handle step size > 1")
                elif key.start is None:
                    where = ("%s<?" % self.primary_key, key.stop)
                elif key.stop is None:
                    where = ("%s>=?" % self.primary_key, key.start)
                else:
                    where = ("%s<? AND %s>=?" % (
                        self.primary_key, self.primary_key),
                        (key.stop, key.start))
            data = self.select(where=where)

        elif isinstance(key, str):
            # select a column
            data = self.select(key)

        elif all(isinstance(k, str) for k in key):
            # select multiple columns
            data = self.select(key)

        else:
            raise ValueError("invalid key: %s" % key)

        return data

    def __repr__(self):
        return self.repr

    def __str__(self):
        return self.repr
