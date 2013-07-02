import numpy as np
import sqlite3 as sql


def dict_to_dtypes(data, order=None):
    r"""
    Parses data types from a dictionary or list of dictionaries.

    The dictionary keys will be paired with the types of the
    corresponding values, e.g.::

        (key, type(data[key]))

    If there are multiple dictionaries that have the same keys, the
    value types should be the same across dictionaries (with the
    exception of NoneType).

    For example::

        dict_to_dtypes(data=[
            {'name': 'apple', 'fruit': True, 'tree': True},
            {'name': 'tomato', 'fruit': True, 'tree': False},
            {'name': 'cucumber', 'fruit': False, 'tree': False}
        ])

    will return::

        [('fruit', bool), ('name', str), ('tree', bool)]

    Parameters
    ----------
    data : dictionary or list of dictionaries
        Data to extract names and dtypes from.

    order : list of strings (optional)
        The order in which to return the dtypes in, by key. If None, the
        dtypes will be sorted alphabetically by key.

    Returns
    -------
    dtypes : list of 2-tuples
        Each tuple in the list has the form (key, dtype)

    """

    # if data is a dictionary, wrap it in a list
    if hasattr(data, 'keys'):
        data = [data]

    # build a dictionary mapping keys to sets of types
    all_types = {}
    for x in data:
        for key in x:
            if key not in all_types:
                all_types[key] = set()
            if x[key] is not None:
                all_types[key].add(type(x[key]))

    # make sure we have an ordering
    if order is None:
        order = sorted(all_types.keys())

    # make sure each key has a datatype associated with it and build
    # up the list of (key, dtype) tuples
    types = []
    for key in order:
        t = list(all_types[key])
        if len(t) != 1:
            raise ValueError("could not determine datatype "
                             "of column '%s'" % key)
        # convert numpy dtypes into native python types
        tt = type(np.asscalar(np.array([0], dtype=t[0])))
        types.append((key, tt))

    return types


def sql_execute(db, cmd, fetchall=False, verbose=False):
    r"""
    Execute a SQL command `cmd` in database `db`.

    Parameters
    ----------
    db : string
        Path to the SQLite database.
    cmd : string or list
        Command to be executed. Specifically, these are parameters to be passed
        to `sqlite3.Cursor.execute`. See:

        http://docs.python.org/2/library/sqlite3.html#sqlite3.Cursor.execute
    fetchall : bool (optional)
        Fetch the result of the command, and return it.
    verbose : bool (optional)
        Print the command that is run.

    Returns
    -------
    result : list or None
        The result of the executed command, if `fetchall` is True.

    """

    # wrap the command in a list, if it isn't one already
    if not hasattr(cmd, '__iter__'):
        cmd = [cmd]

    # connect to the database
    conn = sql.connect(db)
    with conn:
        # get the database cursor
        cur = conn.cursor()
        # optionally print the command we're running
        if verbose:
            print ", ".join([str(x) for x in cmd])
        # run the command
        cur.execute(*cmd)
        # optionally get the result
        if fetchall:
            result = cur.fetchall()
        else:
            result = None

    return result
