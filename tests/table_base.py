import numpy as np
import os

from nose.tools import raises
from sqlite3 import OperationalError

from dbtools import Table
from . import DBNAME, RewriteDocstringMeta


class TestTable(object):

    __metaclass__ = RewriteDocstringMeta

    dtypes = (
        ('id', int),
        ('name', str),
        ('age', int),
        ('height', float)
    )

    idata = np.array([
        [2, 'Alyssa P. Hacker', 25, 66.25],
        [4, 'Ben Bitdiddle', 24, 70.1],
        [6, 'Louis Reasoner', 26, 68.0],
        [8, 'Eva Lu Ator', 29, 67.42]
    ], dtype='object')

    def setup(self):
        if os.path.exists(DBNAME):
            os.remove(DBNAME)
        self.tbl = Table.create(
            DBNAME, "Foo", self.dtypes, verbose=True)

    def teardown(self):
        os.remove(DBNAME)

    def insert(self):
        self.tbl.insert(self.idata)

    def check_data(self, indata, outdata):
        out = True
        if not (indata == outdata.as_matrix()).all():
            out = False
        return out

    def check_index(self, indata, outdata):
        out = True
        if not (np.arange(0, indata.shape[0]) ==
                np.array(outdata.index)).all():
            out = False
        return out

    def check(self, indata, outdata):
        data = self.check_data(indata, outdata)
        index = self.check_index(indata, outdata)
        return data and index

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

    def test_create_from_dataframe(self):
        """Create a table from a dataframe"""
        self.insert()
        data = self.tbl.select()
        tbl = Table.create(DBNAME, "Foo_2", data, verbose=True)
        self.check(self.idata, tbl.select())

    def test_create_from_dicts(self):
        """Create a table from dictionaries"""
        cols = zip(*self.dtypes)[0]
        dicts = [dict([(cols[i], d[i]) for i in xrange(len(d))])
                 for d in self.idata]

        tbl = Table.create(DBNAME, "Bar", dicts, verbose=True)

        self.check_index(self.idata, tbl.select())
        for idx, col in enumerate(cols):
            self.check_data(self.idata[:, [idx]], tbl[col])

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
        assert self.check(idata, data)

    def test_insert_lists(self):
        """Insert a list of lists"""
        self.insert()
        data = self.tbl.select()
        assert self.check(self.idata, data)

    def test_select_columns(self):
        """Make sure columns of selected data are correct"""
        self.insert()
        data = self.tbl.select()
        assert tuple(data.columns) == self.tbl.columns

    def test_select_where_args(self):
        """Check where selection with one argument"""
        self.insert()
        data = self.tbl.select(where=("age=?", 25))
        assert self.check(self.idata[[0]], data)

    def test_select_where_args2(self):
        """Check where selection with list of arguments"""
        self.insert()
        data = self.tbl.select(where=("age=?", (25,)))
        assert self.check(self.idata[[0]], data)

    def test_select_where_no_args(self):
        """Check where selection with no arguments"""
        self.insert()
        data = self.tbl.select(where="age=25")
        assert self.check(self.idata[[0]], data)

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
        assert self.check(idata, data)

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
        assert self.check(idata, data)

    def test_slice_name(self):
        """Slice the 'name' column"""
        self.insert()
        data = self.tbl['name']
        assert self.check(self.idata[:, [1]], data)

    def test_slice_name_age(self):
        """Slice the 'name' and 'age' columns"""
        self.insert()
        data = self.tbl['name', 'age']
        assert self.check(self.idata[:, [1, 2]], data)

    def test_slice_name_height(self):
        """Slice the 'name' and 'height' columns"""
        self.insert()
        data = self.tbl['name', 'height']
        assert self.check(self.idata[:, [1, 3]], data)

    def test_slice_all(self):
        """Slice all the data"""
        self.insert()
        data = self.tbl[:]
        assert self.check(self.idata, data)

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

    def test_delete_row(self):
        """Delete a row"""
        self.insert()
        self.tbl.delete(where="age=25")
        data = self.tbl.select()
        assert self.check_data(self.idata[1:], data)

    def test_delete_row_arg(self):
        """Delete a row with an argument"""
        self.insert()
        self.tbl.delete(where=("age=?", 25))
        data = self.tbl.select()
        assert self.check_data(self.idata[1:], data)

    def test_delete_rows(self):
        """Delete multiple rows"""
        self.insert()
        self.tbl.delete(where="age>25")
        data = self.tbl.select()
        assert self.check_data(self.idata[:2], data)

    def test_delete_rows_args(self):
        """Delete multiple rows with multiple arguments"""
        self.insert()
        self.tbl.delete(where=("age=? OR height>?", (25, 70)))
        data = self.tbl.select()
        assert self.check_data(self.idata[2:], data)

    def test_delete_all(self):
        """Delete all rows"""
        self.insert()
        self.tbl.delete()
        data = self.tbl.select()
        assert self.check_data(self.idata[:0], data)

    def test_csv(self):
        """Write a csv file"""
        self.insert()
        self.tbl.save_csv("test.csv")
        os.remove("test.csv")
