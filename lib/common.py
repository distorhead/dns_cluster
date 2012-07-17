# -*- coding: utf-8 -*-


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


# vim:sts=4:ts=4:sw=4:expandtab:
