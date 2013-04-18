import numpy as np
import os

from nose.tools import raises

from dbtools import Table
from . import DBNAME, RewriteDocstringMeta
from table_base import TestTable


class TestTablePrimaryKey(TestTable):

    __metaclass__ = RewriteDocstringMeta

    def setup(self):
        if os.path.exists(DBNAME):
            os.remove(DBNAME)
        self.tbl = Table.create(
            DBNAME, "Foo", self.dtypes,
            primary_key='id', autoincrement=True,
            verbose=True)

    def check_data(self, indata, outdata):
        out = True
        if not (indata[:, 1:] == outdata.as_matrix()).all():
            out = False
        return out

    def check_index(self, indata, outdata):
        out = True
        if not (indata[:, 0] == np.array(outdata.index)).all():
            out = False
        return out

    def test_create_autoincrement(self):
        """Check that autoincrement is set"""
        assert self.tbl.autoincrement

    def test_create_primary_key(self):
        """Check that the primary key is set"""
        assert self.tbl.primary_key == 'id'

    def test_create_from_dataframe(self):
        """Create a table from a dataframe"""
        self.insert()
        data = self.tbl.select()
        data.index.name = None
        tbl = Table.create(DBNAME, "Foo_2", data, verbose=True,
                           primary_key='id', autoincrement=True)
        self.check(self.idata, tbl.select())

    @raises(ValueError)
    def test_create_from_dataframe_invalid_pk(self):
        """Create a table from a dataframe with invalid primary key"""
        self.insert()
        data = self.tbl.select()
        Table.create(
            DBNAME, "Foo_2", data,
            primary_key='foo', verbose=True)

    def test_create_from_dicts(self):
        """Create a table from dictionaries"""
        cols = zip(*self.dtypes)[0]
        dicts = [dict([(cols[i], d[i]) for i in xrange(len(d))])
                 for d in self.idata]

        tbl = Table.create(
            DBNAME, "Bar", dicts, verbose=True,
            primary_key='id', autoincrement=True)

        self.check_index(self.idata, tbl.select())
        for idx, col in enumerate(cols):
            if col == 'id':
                continue
            self.check_data(self.idata[:, [0, idx]], tbl[col])

    def test_select_columns(self):
        """Make sure columns of selected data are correct"""
        self.insert()
        data = self.tbl.select()
        assert (u'id',) + tuple(data.columns) == self.tbl.columns

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name']
        assert self.check(self.idata[:, [0, 1]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check(self.idata[:, [0, 1, 2]], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check(self.idata[:, [0, 1, 3]], data)

    def test_index_0(self):
        """Index the zeroth row"""
        self.insert()
        data = self.tbl[0]
        assert self.check(self.idata[:0], data)

    def test_index_1(self):
        """Index the first row"""
        self.insert()
        data = self.tbl[2]
        assert self.check(self.idata[:1], data)

    def test_index_12(self):
        """Slice the first and second rows"""
        self.insert()
        data = self.tbl[2:6]
        assert self.check(self.idata[:2], data)

    def test_index_lt_3(self):
        """Slice up to the third row"""
        self.insert()
        data = self.tbl[:6]
        assert self.check(self.idata[:2], data)

    def test_index_geq_3(self):
        """Slice past the third row"""
        self.insert()
        data = self.tbl[6:]
        assert self.check(self.idata[2:], data)

    @raises(ValueError)
    def test_index_alternate(self):
        """Slice every other row"""
        self.insert()
        self.tbl[::2]
