# dbtools
A simple interface to SQLite databases.

**Version**: xxx  
**Date**: xxx  
**Author**: Jessica B. Hamrick  

## Overview

This module handles simple interfacing with a SQLite database.
Inspired by [ipython-sql](https://pypi.python.org/pypi/ipython-sql),
`dbtools` returns
[pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe)
objects from `SELECT` queries, and can handle basic forms of other SQL
statements (`CREATE`, `INSERT`, `UPDATE`, `DELETE`, and `DROP`).

The goal is *not* to replicate the full functionality of
[SQLAlchemy](http://www.sqlalchemy.org/) or really to be used for
object-relational mapping at all. This is meant to be used more for
scientific data collection (e.g., behavioral experiments) as
convenient access to a robust form of storage.

## Examples

### Create and load

```python
>>> from dbtools import Table
>>> tbl = Table.create("data.db", "People",
... [('id', int),
... ('name', str),
... ('age', int),
... ('height', float)],
... primary_key='id',
... autoincrement=True)
>>> tbl
People(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, height REAL)
>>> type(tbl)
<class 'dbtools.table.Table'>
```

If a table already exists, we can just directly create a Table object:

```python
>>> tbl = Table("data.db", "People")
>>> tbl
People(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, height REAL)
>>> tbl.columns
(u'id', u'name', u'age', u'height')
>>> tbl.primary_key
u'id'
>>> tbl.autoincrement
True
```

### Insert

Inserting with a list (excluding `id`, because it autoincrements):

```python
>>> tbl.insert(["Alyssa P. Hacker", 25, 66.24])
>>> tbl.select()
                name  age  height
id
1   Alyssa P. Hacker   25   66.24
>>> type(tbl.select())
<class 'pandas.core.frame.DataFrame'>
```

Inserting with a dictionary:

```python
>>> tbl.insert({
... 'name': 'Ben Bitdiddle',
... 'age': 24,
... 'height': 70.1})
>>> tbl.select()
                name  age  height
id
1   Alyssa P. Hacker   25   66.24
2      Ben Bitdiddle   24   70.10
```

You can insert as many things as you want as a time -- just pass them
in as a list of lists and/or dictionaries.

### Select

The previous two examples already used an instance of selection with
`tbl.select()`, which is the equivalent of doing `FROM People SELECT
*`. You can use slicing to select rows (but only if the primary key
column is an integer and autoincrements). Note that because SQLite
databases are one-indexed, indexing the zeroth element returns an
empty `DataFrame`.

```python
>>> tbl[1]
                name  age  height
id
1   Alyssa P. Hacker   25   66.24
>>> tbl[2:]
             name  age  height
id
2   Ben Bitdiddle   24    70.1
```

If you pass in a string or sequence of strings, it will treat them as
column names and select those columns:

```python
>>> tbl['name', 'height']
                name  height
id
1   Alyssa P. Hacker   66.24
2      Ben Bitdiddle   70.10
```

More advanced selection can be done through the `select` method by
specifying the `where` keyword argument (and you can use the `?`
syntax from the `sqlite3` library for untrusted inputs):

```python
>>> tbl.select(where='age>24')
                name  age  height
id
1   Alyssa P. Hacker   25   66.24
>>> tbl.select(columns=['name', 'height'], where=('age>?', 24))
                name  height
id
1   Alyssa P. Hacker   66.24
```

### Update

Updating data in the table works by taking a dictionary (with the keys
being columns, and values being new data) and (optionally) a `where`
keyword argument like in the `select` method to specify what data
should be updated.

```python
>>> tbl.update({'age': 26}, where='id=1')
>>> tbl.select()
                name  age  height
id
1   Alyssa P. Hacker   26   66.24
2      Ben Bitdiddle   24   70.10
```

### Delete

Deleting a row or rows requires specifying a `where` keyword argument
like in `select` and `update` (if it is not given, all rows are
deleted).

```python
>>> tbl.delete(where='height<70')
>>> tbl.select()
             name  age  height
id
2   Ben Bitdiddle   24    70.1
```

### Drop

Finally, the `drop` method is used to drop (delete) an entire table
from its database. Of course, this means it can't be accessed
afterwards because it no longer exists.

```python
>>> tbl.drop()
>>> tbl.select()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "dbtools/table.py", line 339, in select
    cur.execute(*cmd)
sqlite3.OperationalError: no such table: People
```
