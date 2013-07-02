import os

from dbtools import Table
from . import DBNAME


def test_not_exists_no_db():
    """Check that the table does not exist (db does not exist)"""
    if os.path.exists(DBNAME):
        os.remove(DBNAME)
    assert not Table.exists(DBNAME, 'foo', verbose=True)


def test_not_exists():
    """Check that the table does not exist (db does exist)"""
    tbl = Table.create(DBNAME, "foo", [('id', int)], verbose=True)
    tbl.drop()
    assert not Table.exists(DBNAME, 'foo', verbose=True)


def test_exists():
    """Check that the table does exist"""
    Table.create(DBNAME, "foo", [('id', int)], verbose=True)
    assert Table.exists(DBNAME, 'foo', verbose=True)
    os.remove(DBNAME)


def test_list_tables():
    """Check that the list of tables is correct"""
    Table.create(DBNAME, "foo", [('id', int)], verbose=True)
    Table.create(DBNAME, "bar", [('id', int)], verbose=True)
    tables = Table.list_tables(DBNAME, verbose=True)
    assert tables == ["foo", "bar"], tables
    assert Table.exists(DBNAME, 'foo', verbose=True)
    os.remove(DBNAME)
