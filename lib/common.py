# -*- coding: utf-8 -*-

import re


def _make_split():
    regex = re.compile(r'([a-zA-Z0-9.@_]+)|("[a-zA-Z0-9.@_ \t\n]+")')
    def split(s):
        """
        Split given string by whitespaces.
        Content in double qoutes considered as a signle token.
        """
        return [item.group() for item in regex.finditer(s)]

    return split

split = _make_split()


def singleton(cls):
    class Holder: pass
    holder = Holder()

    def get_instance(*args, **kwargs):
        if not hasattr(holder, 'ref'):
            holder.ref = cls(*args, **kwargs)
        return holder.ref

    return get_instance


def uniq(seq):
    s = set()
    def unique(val):
        if not val in s:
            s.add(val)
            return True
        else:
            return False

    return filter(unique, seq)


def reorder(s):
    res = ''
    for ch in reversed(s):
        res += ch
    return res


def required_key(dct, key, **kwargs):
    failure_func = kwargs.get("failure_func", None)
    failure_msg = kwargs.get("failure_msg", "")
    default = kwargs.get("default", None)

    if not dct.has_key(key):
        if hasattr(failure_func, "__call__"):
            failure_func(failure_msg)
        return default

    return dct[key]


def required_type(value, type, **kwargs):
    failure_func = kwargs.get("failure_func", None)
    failure_msg = kwargs.get("failure_msg", None)
    default = kwargs.get("default", None)

    if isinstance(value, type):
        return value
    else:
        try:
            return type(value)
        except:
            if hasattr(failure_func, "__call__"):
                failure_func(failure_msg)
            return default


# vim:sts=4:ts=4:sw=4:expandtab:
