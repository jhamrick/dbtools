import numpy as np
import os

from nose.tools import raises

from .. import Table
from . import DBNAME
from test_table_primary_key import TestTablePrimaryKey


class TestTablePrimaryKeyAutoincrement(TestTablePrimaryKey):

    def setup(self):
        if os.path.exists(DBNAME):
            os.remove(DBNAME)
        self.tbl = Table.create(
            DBNAME, "Foo", self.dtypes,
            primary_key='id', autoincrement=True,
            verbose=True)

    def insert(self):
        self.idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(self.idata)

    def check_data(self, indata, outdata):
        out = True
        if not (indata == outdata.as_matrix()).all():
            out = False
        return out

    def check_index(self, indata, outdata):
        out = True
        if not (np.arange(1, indata.shape[0]+1) ==
                np.array(outdata.index)).all():
            out = False
        return out

    def test_insert_list(self):
        """Insert a list"""
        self.tbl.insert(['Alyssa P. Hacker', 25, 66.25])
        data = self.tbl.select()
        idata = np.array([['Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check(idata, data)

    def test_insert_dict(self):
        """Insert a dictionary"""
        self.tbl.insert({
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })
        data = self.tbl.select()
        idata = np.array([['Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check(idata, data)

    def test_insert_dictlist(self):
        """Insert a list of dictionaries"""
        self.tbl.insert([
            {
                'name': 'Alyssa P. Hacker',
                'age': 25,
                'height': 66.25
            },
            {
                'name': 'Ben Bitdiddle',
                'age': 24,
                'height': 70.1
            }])
        data = self.tbl.select()
        idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1]],
            dtype='object')
        assert self.check(idata, data)

    def test_index_0(self):
        """Index the zeroth row"""
        self.insert()
        data = self.tbl[0]
        assert self.check_data(self.idata[:0], data)

    def test_index_1(self):
        """Index the first row"""
        self.insert()
        data = self.tbl[1]
        assert self.check_data(self.idata[:1], data)

    def test_index_12(self):
        """Slice the first and second rows"""
        self.insert()
        data = self.tbl[1:3]
        assert self.check_data(self.idata[:2], data)

    def test_index_lt_3(self):
        """Slice up to the third row"""
        self.insert()
        data = self.tbl[:3]
        assert self.check_data(self.idata[:2], data)

    def test_index_geq_3(self):
        """Slice past the third row"""
        self.insert()
        data = self.tbl[3:]
        assert self.check_data(self.idata[2:], data)

    @raises(ValueError)
    def test_index_alternate(self):
        """Slice every other row"""
        self.insert()
        self.tbl[::2]

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name']
        assert self.check(self.idata[:, [0]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check(self.idata[:, :2], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check(self.idata[:, [0, 2]], data)
