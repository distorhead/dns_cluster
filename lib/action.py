# -*- coding: utf-8 -*-

import exception
from bsddb3 import db


class Action(object):
    """
    Class represent single journal action.
    Action may be in {DO|UNDO} state.
    """

    class State:
        UNDO = 0
        DO = 1

    def __init__(self, name, dbenv, dbfile, dbname, state=None):
        if state is None:
            state = self.State.DO

        self._name = name
        self._dbenv = dbenv
        self._dbfile = dbfile
        self._dbname = dbname
        self._state = int(state)

    def set_state(self, state):
        self._state = int(state)

    def get_state(self):
        return self._state

    def get_name(self):
        return self._name

    def invert(self):
        self._state ^= 1

    def apply(self, txn=None):
        if self._state == self.State.DO:
            self._apply_do(txn)
        elif self._state == self.State.UNDO:
            self._apply_undo(txn)
        else:
            assert 0

    def _apply_do(self, txn):
        raise exception.NotImplementedError("do action not implemented")

    def _apply_undo(self, txn):
        raise exception.NotImplementedError("undo action not implemented")


class AddArea(Action):
    def __init__(self, name):
        super(self.__class__, self).__init__()


class AddSegment(Action): pass
class AddZone(Action): pass
class AddRecord_A(Action): pass
class AddRecord_PTR(Action): pass
class AddRecord_CNAME(Action): pass
class AddRecord_DNAME(Action): pass
class AddRecord_SOA(Action): pass
class AddRecord_NS(Action): pass
class AddRecord_MX(Action): pass
class AddRecord_SRV(Action): pass
class AddRecord_TXT(Action): pass


def make_action(dbrec):
    pass


# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
