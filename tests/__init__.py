DBNAME = 'test.db'


def update_docstring(name, olddoc):
    # make sure it has a docstring
    if olddoc is None:
        return None

    # new docstring
    prefix = "%s: " % name
    if olddoc.split(": ")[0].startswith("Test"):
        newdoc = prefix + olddoc.split(": ")[1]
    else:
        newdoc = prefix + olddoc

    return newdoc


class RewriteDocstringMeta(type):
    """Modify docstrings to be prefixed with 'classname: '.

    To do this, we intercede before the class is created and modify the
    docstrings of its attributes.

    This will not affect inherited methods, however, so we also need to
    loop through the parent classes. We cannot simply modify the
    docstrings, because then the parent classes' methods will have the
    wrong docstring. Instead, we must actually copy the functions, and
    then modify the docstring.

    """

    def __new__(cls, name, parents, attrs):
        # hack -- skip nose (?) class
        if name == "C":
            obj = super(RewriteDocstringMeta, cls).__new__(
                cls, name, parents, attrs)
            return obj

        for attr_name in attrs:
            # skip special methods
            if attr_name.startswith("__"):
                continue

            # skip non-functions
            attr = attrs[attr_name]
            if not hasattr(attr, '__call__'):
                continue

            # update docstring
            attr.__doc__ = update_docstring(name, attr.__doc__)

        for parent in parents:
            for attr_name in dir(parent):

                # we already have this method
                if attr_name in attrs:
                    continue

                # skip special methods
                if attr_name.startswith("__"):
                    continue

                # get the original function and copy it
                a = getattr(parent, attr_name)

                # skip non-functions
                if not hasattr(a, '__call__'):
                    continue

                # copy function
                f = a.__func__
                attr = type(f)(
                    f.func_code, f.func_globals, f.func_name,
                    f.func_defaults, f.func_closure)
                doc = f.__doc__

                # update docstring and add attr
                attr.__doc__ = update_docstring(name, doc)
                attrs[attr_name] = attr

        # create the class
        obj = super(RewriteDocstringMeta, cls).__new__(
            cls, name, parents, attrs)
        return obj
