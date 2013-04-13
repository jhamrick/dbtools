import sqlite3 as sql
import numpy as np
import pandas as pd


class Table(object):

    def __init__(self, db, name, verbose=False):
        self.db = str(db)
        self.name = str(name)
        self.verbose = bool(verbose)

        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info('%s')" % self.name)
            rows = cur.fetchall()

        # id, column name, data type, null, default, primary key
        self.meta = np.array(rows)
        self.columns = tuple(self.meta[:, 1])

        # parse primary key, if any
        primary_key = np.nonzero(self.meta[:, 5])[0]
        if len(primary_key) > 1:
            raise ValueError("more than one primary key: %s" % primary_key)
        elif len(primary_key) == 1:
            self.primary_key = self.columns[primary_key[0]]
        else:
            self.primary_key = None

        # compute repr based on column names and types
        args = ["%s %s" % (m[1], m[2]) for m in self.meta]
        if self.primary_key is not None:
            idx = self.columns.index(self.primary_key)
            args[idx] += " PRIMARY KEY AUTOINCREMENT"
        self.repr = "%s(%s)" % (self.name, ", ".join(args))

    def __repr__(self):
        return self.repr

    def __str__(self):
        return self.repr

    @classmethod
    def create(cls, db, name, dtypes, primary_key=None,
               autoincrement=False, verbose=False):
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

        tbl = cls(db, name)
        return tbl

    def drop(self):
        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cmd = "DROP TABLE %s" % self.name
            if self.verbose:
                print cmd
            cur.execute(cmd)

    def insert(self, values=None):

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
        if self.primary_key is not None:
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
        if self.primary_key is not None:
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
        if self.primary_key not in cols:
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
            if self.primary_key is None:
                raise ValueError("no primary key column")
            data = self.select(where=("%s=?" % self.primary_key, key))

        elif isinstance(key, slice):
            # select multiple rows
            if self.primary_key is None:
                raise ValueError("no primary key column")
            if key.step not in (None, 1):
                raise ValueError("cannot handle step size > 1")
            if key.start is None and key.stop is None:
                where = None
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
