# -*- coding: utf-8 -*-


class Glossary(object):
    def __init__(self, **defs):
        self._values = set()
        self.add(**defs)

    def add(self, **defs):
        for key, val in defs.iteritems():
            setattr(self, key, val)
            self._values.add(val)

    def delete(self, key):
        if key in self and hasattr(self, key):
            delattr(self, key)
            self._values.remove(key)

    def __contains__(self, val):
        return val in self._values


# vim:sts=4:ts=4:sw=4:expandtab:
