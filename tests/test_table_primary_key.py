import numpy as np
import os

from .. import Table
from . import DBNAME
from test_table import TestTable


class TestTablePrimaryKey(TestTable):

    def setup(self):
        if os.path.exists(DBNAME):
            os.remove(DBNAME)
        self.tbl = Table.create(
            DBNAME, "Foo", self.dtypes,
            primary_key='id', autoincrement=False)

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

    def test_create_primary_key(self):
        """Check that the primary key is set"""
        assert self.tbl.primary_key == 'id'

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
