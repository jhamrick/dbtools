import os
import numpy as np
from nose.tools import with_setup, raises
from table import Table
from sqlite3 import OperationalError


class TestTable(object):

    def setup(self):
        self.dtypes = (
            ('id', int),
            ('name', str),
            ('age', int),
            ('height', float)
        )
        self.tbl = Table.create(
            "test.db", "Foo", self.dtypes, primary_key='id')

    def teardown(self):
        os.remove("test.db")

    @with_setup(setup, teardown)
    def test_create_name(self):
        """Ensure the table name is correct"""
        assert self.tbl.name == "Foo"

    @with_setup(setup, teardown)
    def test_create_columns(self):
        """Check that the columns are named correctly"""
        assert self.tbl.columns == zip(*self.dtypes)[0]

    @with_setup(setup, teardown)
    def test_create_pk(self):
        """Check that the primary key is set"""
        assert self.tbl.pk == 'id'

    @with_setup(setup, teardown)
    @raises(OperationalError)
    def test_drop(self):
        """Make sure dropping the table works"""
        self.tbl.drop()
        self.tbl.select()

    @with_setup(setup, teardown)
    def test_insert_null(self):
        """See if we can insert a null entry"""
        self.tbl.insert({})

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_string(self):
        """Try to insert just a string (should fail)"""
        self.tbl.insert('Alyssa P. Hacker')

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_int(self):
        """Try to insert just an integer (should fail)"""
        self.tbl.insert(25)

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_float(self):
        """Try to insert just a float (should fail)"""
        self.tbl.insert(66.25)

    @with_setup(setup, teardown)
    def test_insert_dict(self):
        """Try to insert a dictionary"""
        self.tbl.insert({
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })
        data = self.tbl.select().as_matrix()
        idata = np.array(['Alyssa P. Hacker', 25, 66.25], dtype='object')
        assert (data == idata).all()

    @with_setup(setup, teardown)
    def test_insert_dictlist(self):
        """Try to insert a list of dictionaries"""
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

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_shortlist(self):
        """Try to insert a list that's too short (should fail)"""
        self.tbl.insert(['Alyssa P. Hacker', 25])

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_shortlists(self):
        """Try to insert a list of lists that are too short (should fail)"""
        self.tbl.insert([
            ['Alyssa P. Hacker', 66.25],
            ['Ben Bitdiddle', 24]
        ])

    @with_setup(setup, teardown)
    def test_insert_list(self):
        """Try to insert a list"""
        self.tbl.insert(['Alyssa P. Hacker', 25, 66.25])
        data = self.tbl.select().as_matrix()
        idata = np.array(['Alyssa P. Hacker', 25, 66.25], dtype='object')
        assert (data == idata).all()

    @with_setup(setup, teardown)
    def test_insert_lists(self):
        """Try to insert a list of lists"""
        idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(idata)
        data = self.tbl.select().as_matrix()
        assert (data == idata).all()

    @with_setup(setup, teardown)
    def test_select_columns(self):
        """Make sure columns of selected data are correct"""
        idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(idata)
        data = self.tbl.select()
        assert (u'id',) + tuple(data.columns) == self.tbl.columns

    @with_setup(setup, teardown)
    def test_select_index(self):
        """Make sure the index of selected data is correct"""
        idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(idata)
        data = self.tbl.select()
        assert tuple(data.index) == (1, 2, 3, 4)



