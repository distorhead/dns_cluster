# -*- coding: utf-8 -*-


def singleton(cls):
    class Holder: pass
    holder = Holder()

    def get_instance(*args, **kwargs):
        if not hasattr(holder, 'ref'):
            holder.ref = cls(*args, **kwargs)
        return holder.ref

    return get_instance



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
