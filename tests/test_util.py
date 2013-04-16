from nose.tools import raises

from dbtools.util import dict_to_dtypes


def test_dict_to_dtypes_1():
    """Convert a dict to dtypes"""
    d = {'name': 'apple', 'fruit': True, 'tree': True}
    dtypes = dict_to_dtypes(d)
    expected = [('fruit', bool), ('name', str), ('tree', bool)]
    assert dtypes == expected


def test_dict_to_dtypes_2():
    """Convert a list of dicts to dtypes"""
    d = [{'name': 'apple', 'fruit': True, 'tree': True},
         {'name': 'tomato', 'fruit': True, 'tree': False},
         {'name': 'cucumber', 'fruit': False, 'tree': False}]
    dtypes = dict_to_dtypes(d)
    expected = [('fruit', bool), ('name', str), ('tree', bool)]
    assert dtypes == expected


def test_dict_to_dtypes_3():
    """Convert dicts with None values to dtypes"""
    d = [{'name': 'apple', 'fruit': True, 'tree': None},
         {'name': 'tomato', 'fruit': True, 'tree': False},
         {'name': None, 'fruit': None, 'tree': False}]
    dtypes = dict_to_dtypes(d)
    expected = [('fruit', bool), ('name', str), ('tree', bool)]
    assert dtypes == expected


@raises(ValueError)
def test_dict_to_dtypes_4():
    """Fail to convert dicts with inconsistent dtypes"""
    d = [{'name': 3, 'fruit': True, 'tree': None},
         {'name': 'tomato', 'fruit': True, 'tree': False},
         {'name': None, 'fruit': None, 'tree': False}]
    dict_to_dtypes(d)


@raises(ValueError)
def test_dict_to_dtypes_5():
    """Fail to convert dicts with unspecified dtypes"""
    d = [{'name': None, 'fruit': True, 'tree': None},
         {'name': None, 'fruit': True, 'tree': False},
         {'name': None, 'fruit': None, 'tree': False}]
    dict_to_dtypes(d)
