DBNAME = 'tests/test.db'


class RewriteDocstringMeta(type):

    def __init__(cls, name, parents, attrs):
        super(RewriteDocstringMeta, cls).__init__(name, parents, attrs)

        for key, attr in attrs.items():
            if key.startswith("__"):
                continue

            if hasattr(attr, '__call__') and attr.__doc__ is not None:
                doc = "%s: " % name + attr.__doc__
                attr.__doc__ = doc
