import os
import numpy as np
from nose.tools import with_setup, raises
from table import Table
from sqlite3 import OperationalError


class TestTable(object):

    dtypes = (
        ('id', int),
        ('name', str),
        ('age', int),
        ('height', float)
    )

    def setup(self):
        self.tbl = Table.create(
            "test.db", "Foo", self.dtypes, primary_key='id')

    def teardown(self):
        os.remove("test.db")

    def insert(self):
        self.idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(self.idata)

    def test_create_name(self):
        """Check for correct table name"""
        assert self.tbl.name == "Foo"

    def test_create_columns(self):
        """Check that the columns are named correctly"""
        assert self.tbl.columns == zip(*self.dtypes)[0]

    def test_create_primary_key(self):
        """Check that the primary key is set"""
        assert self.tbl.pk == 'id'

    @raises(OperationalError)
    def test_drop(self):
        """Drop table"""
        self.tbl.drop()
        self.tbl.select()

    def test_insert_null(self):
        """Insert a null entry"""
        self.tbl.insert({})

    @raises(ValueError)
    def test_insert_string(self):
        """Insert just a string (should fail)"""
        self.tbl.insert('Alyssa P. Hacker')

    @raises(ValueError)
    def test_insert_int(self):
        """Insert just an integer (should fail)"""
        self.tbl.insert(25)

    @raises(ValueError)
    def test_insert_float(self):
        """Insert just a float (should fail)"""
        self.tbl.insert(66.25)

    @raises(ValueError)
    def test_insert_shortlist(self):
        """Insert a list that's too short (should fail)"""
        self.tbl.insert(['Alyssa P. Hacker', 25])

    @raises(ValueError)
    def test_insert_shortlists(self):
        """Insert a list of lists that are too short (should fail)"""
        self.tbl.insert([
            ['Alyssa P. Hacker', 66.25],
            ['Ben Bitdiddle', 24]
        ])

    def test_insert_list(self):
        """Insert a list"""
        self.tbl.insert(['Alyssa P. Hacker', 25, 66.25])
        data = self.tbl.select().as_matrix()
        idata = np.array(['Alyssa P. Hacker', 25, 66.25], dtype='object')
        assert (data == idata).all()

    def test_insert_lists(self):
        """Insert a list of lists"""
        self.insert()
        data = self.tbl.select().as_matrix()
        assert (data == self.idata).all()


    def test_select_columns(self):
        """Make sure columns of selected data are correct"""
        self.insert()
        data = self.tbl.select()
        assert (u'id',) + tuple(data.columns) == self.tbl.columns

    def test_select_index(self):
        """Make sure the index of selected data is correct"""
        self.insert()
        data = self.tbl.select()
        assert tuple(data.index) == (1, 2, 3, 4)

    def test_insert_dict(self):
        """Insert a dictionary"""
        self.tbl.insert({
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })
        data = self.tbl.select().as_matrix()
        idata = np.array(['Alyssa P. Hacker', 25, 66.25], dtype='object')
        assert (data == idata).all()

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
        data = self.tbl.select().as_matrix()
        idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1]],
            dtype='object')
        assert (data == idata).all()

    def test_index_0(self):
        """Index the zeroth row"""
        self.insert()
        data = self.tbl[0].as_matrix()
        assert (data == self.idata[:0]).all()

    def test_index_1(self):
        """Index the first row"""
        self.insert()
        data = self.tbl[1].as_matrix()
        assert (data == self.idata[:1]).all()

    def test_index_12(self):
        """Slice the first and second rows"""
        self.insert()
        data = self.tbl[1:3].as_matrix()
        assert (data == self.idata[:2]).all()

    def test_index_lt_3(self):
        """Slice up to the third row"""
        self.insert()
        data = self.tbl[:3].as_matrix()
        assert (data == self.idata[:2]).all()

    def test_index_geq_3(self):
        """Slice past the third row"""
        self.insert()
        data = self.tbl[3:].as_matrix()
        assert (data == self.idata[2:]).all()

    @raises(ValueError)
    def test_index_alternate(self):
        """Slice every other row (should fail)"""
        self.insert()
        data = self.tbl[::2].as_matrix()
        assert (data == self.idata[:2]).all()

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name'].as_matrix()
        assert (data == self.idata[:, :1]).all()

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age'].as_matrix()
        assert (data == self.idata[:, :2]).all()

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height'].as_matrix()
        assert (data == self.idata[:, [0, 2]]).all()

    def test_slice_all(self):
        """Slice all the data"""
        self.insert()
        data = self.tbl[:].as_matrix()
        assert (data == self.idata).all()
