import os
from nose.tools import with_setup
from table import Table


class TestTable(object):

    def setup(self):
        self.dtypes = (
            ('id', int),
            ('name', str),
            ('age', int),
            ('height', float))
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
