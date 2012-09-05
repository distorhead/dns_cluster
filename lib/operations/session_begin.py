# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionBegin"]


class SessionBegin(Operation):
    def __init__(self, session):
        Operation.__init__(self)
        self._session = session

    def _do_run(self):
        sessid = self._session.start_session()
        return sessid


# vim:sts=4:ts=4:sw=4:expandtab:
