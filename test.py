import os
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
        assert self.tbl.name == "Foo"

    @with_setup(setup, teardown)
    def test_create_columns(self):
        assert self.tbl.columns == zip(*self.dtypes)[0]

    @with_setup(setup, teardown)
    def test_create_pk(self):
        assert self.tbl.pk == 'id'

    @with_setup(setup, teardown)
    @raises(OperationalError)
    def test_drop(self):
        self.tbl.drop()
        self.tbl.select()

    @with_setup(setup, teardown)
    def test_insert_null(self):
        self.tbl.insert({})

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_string(self):
        self.tbl.insert('Alyssa P. Hacker')

    @raises(ValueError)
    def test_insert_int(self):
        self.tbl.insert(25)

    @raises(ValueError)
    def test_insert_float(self):
        self.tbl.insert(66.25)

    @with_setup(setup, teardown)
    def test_insert_dict(self):
        self.tbl.insert({
            'name': 'Alyssa P. Hacker',
            'age': 25,
            'height': 66.25
        })

    @with_setup(setup, teardown)
    def test_insert_dictlist(self):
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

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_shortlist(self):
        self.tbl.insert(['Alyssa P. Hacker', 25])

    @with_setup(setup, teardown)
    @raises(ValueError)
    def test_insert_shortlists(self):
        self.tbl.insert([
            ['Alyssa P. Hacker', 66.25],
            ['Ben Bitdiddle', 24]
        ])

    @with_setup(setup, teardown)
    def test_insert_list(self):
        self.tbl.insert(['Alyssa P. Hacker', 25, 66.25])

    @with_setup(setup, teardown)
    def test_insert_lists(self):
        self.tbl.insert([
            ['Alyssa P. Hacker', 25, 66.25],
            ['Ben Bitdiddle', 24, 70.1]
        ])

    
