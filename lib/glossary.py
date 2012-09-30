# -*- coding: utf-8 -*-


class Glossary(object):
    def __init__(self, **defs):
        self._defs = set()
        self.add(**defs)

    def add(self, **defs):
        for key, val in defs.iteritems():
            setattr(self, key, val)
            self._defs.add(key)

    def delete(self, key):
        if key in self and hasattr(self, key):
            delattr(self, key)
            self._defs.remove(key)

    def __contains__(self, key):
        return key in self._defs

    def defs(self):
        return self._defs


# vim:sts=4:ts=4:sw=4:expandtab:
