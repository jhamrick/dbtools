import sqlite3 as sql
import numpy as np
import pandas as pd


class Table(object):

    def __init__(self, db, name):
        self.db = str(db)
        self.name = str(name)

        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info('%s')" % self.name)
            rows = cur.fetchall()

        # id, column name, data type, null, default, primary key
        self.meta = np.array(rows)
        self.columns = tuple(self.meta[:, 1])

        # parse data types
        dtype = []
        for dt in self.meta[:, 2]:
            if dt == "INTEGER":
                dtype.append(int)
            elif dt == "TEXT":
                dtype.append(str)
            else:
                raise ValueError("unhandled dtype: %s" % dt)
        self.dtype = tuple(dtype)

        # parse primary key, if any
        pk = np.nonzero(self.meta[:, 5])[0]
        if len(pk) > 1:
            raise ValueError("more than one primary key: %s" % pk)
        elif len(pk) == 1:
            self.pk = self.columns[pk[0]]
        else:
            self.pk = None

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
        if self.pk not in cols:
            cols.insert(0, self.pk)
        sel = ",".join(cols)

        # base query
        query = "SELECT %s FROM %s" % (sel, self.name)
        # add a selection filter, if specified
        if where is not None:
            where_str, where_args = where
            query += " WHERE %s" % where_str
            if not hasattr(where_args, "__iter__"):
                where_args = (where_args,)
            args = (query, where_args)
        else:
            args = (query,)

        # connect to the database and execute the query
        conn = sql.connect(self.db)
        with conn:
            cur = conn.cursor()
            cur.execute(*args)
            rows = cur.fetchall()

        # now we need to parse the result into a DataFrame
        if self.pk in cols:
            index = self.pk
        else:
            index = None
        data = pd.DataFrame.from_records(rows, columns=cols, index=index)

        return data

    def __getitem__(self, key):
        if isinstance(key, int):
            # select a row
            if self.pk is None:
                raise ValueError("no primary key column")
            data = self.select(where=("%s=?" % self.pk, key))

        elif isinstance(key, slice):
            # select multiple rows
            if self.pk is None:
                raise ValueError("no primary key column")
            if key.step not in (None, 1):
                raise ValueError("cannot handle step size > 1")
            if key.start is None and key.stop is None:
                where = None
            elif key.start is None:
                where = ("%s<?" % self.pk, key.stop)
            elif key.stop is None:
                where = ("%s>=?" % self.pk, key.start)
            else:
                where = ("%s<? AND %s>=?" % (self.pk, self.pk),
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


tbl = Table("data.db", "Participants")
