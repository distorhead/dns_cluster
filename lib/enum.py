# -*- coding: utf-8 -*-


class Enum(object):
    def __init__(self, *symbols):
        self._next_id = 0
        self._symbols = set()
        self.add(*symbols)

    def add(self, *symbols):
        for sym in symbols:
            setattr(self, sym, self._next_id)
            self._next_id += 1
            self._symbols.add(sym)

    def delete(self, sym):
        if sym in self and hasattr(self, sym):
            delattr(self, sym)
            self._symbols.remove(sym)

    def __contains__(self, sym):
        return sym in self._symbols


# vim:sts=4:ts=4:sw=4:expandtab:
