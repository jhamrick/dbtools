import os
import numpy as np
from nose.tools import raises
from table import Table
from sqlite3 import OperationalError


######################################################################
class TestTable(object):

    dtypes = (
        ('id', int),
        ('name', str),
        ('age', int),
        ('height', float)
    )

    def setup(self):
        self.tbl = Table.create(
            "test.db", "Foo", self.dtypes)

    def teardown(self):
        os.remove("test.db")

    def insert(self):
        self.idata = np.array([
            [2, 'Alyssa P. Hacker', 25, 66.25],
            [4, 'Ben Bitdiddle', 24, 70.1],
            [6, 'Louis Reasoner', 26, 68.0],
            [8, 'Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(self.idata)

    def check_equal(self, indata, outdata):
        out = True
        if not (indata == outdata.as_matrix()).all():
            out = False
        if not (np.arange(0, indata.shape[0]) ==
                np.array(outdata.index)).all():
            out = False
        return out

    def test_create_name(self):
        """Check for correct table name"""
        assert self.tbl.name == "Foo"

    def test_create_columns(self):
        """Check that the columns are named correctly"""
        assert self.tbl.columns == zip(*self.dtypes)[0]

    def test_create_primary_key(self):
        """Check that the primary key is not set"""
        assert self.tbl.primary_key is None

    def test_create_autoincrement(self):
        """Check that autoincrement is not set"""
        assert not self.tbl.autoincrement

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
        """Insert just a string"""
        self.tbl.insert('Alyssa P. Hacker')

    @raises(ValueError)
    def test_insert_int(self):
        """Insert just an integer"""
        self.tbl.insert(25)

    @raises(ValueError)
    def test_insert_float(self):
        """Insert just a float"""
        self.tbl.insert(66.25)

    @raises(ValueError)
    def test_insert_shortlist(self):
        """Insert a list that's too short"""
        self.tbl.insert(['Alyssa P. Hacker', 25])

    @raises(ValueError)
    def test_insert_longlist(self):
        """Insert a list of lists that are too short"""
        self.tbl.insert([1, 2, 'Alyssa P. Hacker', 25, 66.25])

    def test_insert_list(self):
        """Insert a list"""
        self.tbl.insert([1, 'Alyssa P. Hacker', 25, 66.25])
        data = self.tbl.select()
        idata = np.array([[1, 'Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check_equal(idata, data)

    def test_insert_lists(self):
        """Insert a list of lists"""
        self.insert()
        data = self.tbl.select()
        assert self.check_equal(self.idata, data)

    @raises(ValueError)
    def test_index_0(self):
        """Index the zeroth row"""
        self.insert()
        self.tbl[0]

    def test_select_columns(self):
        """Make sure columns of selected data are correct"""
        self.insert()
        data = self.tbl.select()
        assert tuple(data.columns) == self.tbl.columns

    def test_select_where_args(self):
        """Check where selection with one argument"""
        self.insert()
        data = self.tbl.select(where=("age=?", 25))
        assert self.check_equal(self.idata[[0]], data)

    def test_select_where_args2(self):
        """Check where selection with list of arguments"""
        self.insert()
        data = self.tbl.select(where=("age=?", (25,)))
        assert self.check_equal(self.idata[[0]], data)

    def test_select_where_no_args(self):
        """Check where selection with no arguments"""
        self.insert()
        data = self.tbl.select(where="age=25")
        assert self.check_equal(self.idata[[0]], data)

    def test_insert_dict(self):
        """Insert a dictionary"""
        self.tbl.insert({
            'id': 1,
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })
        data = self.tbl.select()
        idata = np.array([[1, 'Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check_equal(idata, data)

    def test_insert_dictlist(self):
        """Insert a list of dictionaries"""
        self.tbl.insert([
            {
                'id': 1,
                'name': 'Alyssa P. Hacker',
                'age': 25,
                'height': 66.25
            },
            {
                'id': 2,
                'name': 'Ben Bitdiddle',
                'age': 24,
                'height': 70.1
            }])
        data = self.tbl.select()
        idata = np.array([
            [1, 'Alyssa P. Hacker', 25, 66.25],
            [2, 'Ben Bitdiddle', 24, 70.1]],
            dtype='object')
        assert self.check_equal(idata, data)

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name']
        print self.idata[:, [1]], data
        assert self.check_equal(self.idata[:, [1]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check_equal(self.idata[:, [1, 2]], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check_equal(self.idata[:, [1, 3]], data)

    def test_slice_all(self):
        """Slice all the data"""
        self.insert()
        data = self.tbl[:]
        assert self.check_equal(self.idata, data)

    def test_update(self):
        """Update a single value"""
        self.insert()
        self.tbl.update({'name': 'Alyssa Hacker'},
                        where="name='Alyssa P. Hacker'")
        data = np.array(self.tbl.select()['name'])
        assert data[0] == 'Alyssa Hacker'

    def test_update_multiple(self):
        """Update multiple values"""
        self.insert()
        self.tbl.update({'name': 'Alyssa Hacker',
                         'age': 26},
                        where="name='Alyssa P. Hacker'")
        data = self.tbl.select()
        assert np.array(data['name'])[0] == 'Alyssa Hacker'
        assert np.array(data['age'])[0] == 26

    def test_update_arg(self):
        """Update a value using a WHERE argument"""
        self.insert()
        self.tbl.update({'name': 'Alyssa Hacker'},
                        where=("name=?", "Alyssa P. Hacker"))
        data = self.tbl.select()
        assert np.array(data['name'])[0] == 'Alyssa Hacker'

    def test_update_args(self):
        """Update a value using multiple WHERE arguments"""
        self.insert()
        self.tbl.update({'name': 'Alyssa Hacker'},
                        where=("name=? AND age=?", ("Alyssa P. Hacker", 25)))
        data = self.tbl.select()
        assert np.array(data['name'])[0] == 'Alyssa Hacker'

    def test_update_no_filter(self):
        """Update an entire column"""
        self.insert()
        self.tbl.update({'age': 0})
        data = self.tbl.select()
        assert (np.array(data['age']) == 0).all()

    @raises(ValueError)
    def test_update_fail(self):
        """Update with invalid values"""
        self.insert()
        self.tbl.update('name')


######################################################################
class TestTablePrimaryKey(TestTable):

    def setup(self):
        self.tbl = Table.create(
            "test.db", "Foo", self.dtypes,
            primary_key='id', autoincrement=False)

    def check_equal(self, indata, outdata):
        out = True
        if not (indata[:, 1:] == outdata.as_matrix()).all():
            out = False
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
        assert self.check_equal(self.idata[:, [0, 1]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check_equal(self.idata[:, [0, 1, 2]], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check_equal(self.idata[:, [0, 1, 3]], data)


######################################################################
class TestTablePrimaryKeyAutoincrement(TestTablePrimaryKey):

    def setup(self):
        self.tbl = Table.create(
            "test.db", "Foo", self.dtypes,
            primary_key='id', autoincrement=True)

    def insert(self):
        self.idata = np.array([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1],
            ['Louis Reasoner', 26, 68.0],
            ['Eva Lu Ator', 29, 67.42]
        ], dtype='object')
        self.tbl.insert(self.idata)

    def check_equal(self, indata, outdata):
        out = True
        if not (indata == outdata.as_matrix()).all():
            out = False
        if not (np.arange(1, indata.shape[0]+1) ==
                np.array(outdata.index)).all():
            out = False
        return out

    def test_create_autoincrement(self):
        """Check that autoincrement is set"""
        assert self.tbl.autoincrement

    def test_insert_list(self):
        """Insert a list"""
        self.tbl.insert(['Alyssa P. Hacker', 25, 66.25])
        data = self.tbl.select()
        idata = np.array([['Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check_equal(idata, data)

    def test_insert_dict(self):
        """Insert a dictionary"""
        self.tbl.insert({
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })
        data = self.tbl.select()
        idata = np.array([['Alyssa P. Hacker', 25, 66.25]], dtype='object')
        assert self.check_equal(idata, data)

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
        assert self.check_equal(idata, data)

    # def test_select_index(self):
    #     """Make sure the index of selected data is correct"""
    #     self.insert()
    #     data = self.tbl.select()
    #     assert tuple(data.index) == (1, 2, 3, 4)

    def test_index_0(self):
        """Index the zeroth row"""
        self.insert()
        data = self.tbl[0]
        assert (data.as_matrix() == self.idata[:0]).all()

    def test_index_1(self):
        """Index the first row"""
        self.insert()
        data = self.tbl[1]
        assert (data.as_matrix() == self.idata[:1]).all()

    def test_index_12(self):
        """Slice the first and second rows"""
        self.insert()
        data = self.tbl[1:3]
        assert (data.as_matrix() == self.idata[:2]).all()

    def test_index_lt_3(self):
        """Slice up to the third row"""
        self.insert()
        data = self.tbl[:3]
        assert (data.as_matrix() == self.idata[:2]).all()

    def test_index_geq_3(self):
        """Slice past the third row"""
        self.insert()
        data = self.tbl[3:]
        assert (data.as_matrix() == self.idata[2:]).all()

    @raises(ValueError)
    def test_index_alternate(self):
        """Slice every other row"""
        self.insert()
        self.tbl[::2]

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name']
        assert self.check_equal(self.idata[:, [0]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check_equal(self.idata[:, :2], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check_equal(self.idata[:, [0, 2]], data)
