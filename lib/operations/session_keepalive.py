# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionKeepalive"]


class SessionKeepalive(Operation):
    def __init__(self, session, **kwargs):
        Operation.__init__(self)
        self._session = session
        self.sessid = self.required_data_by_key(kwargs, "sessid", int)

    def _do_run(self):
        self._session.keepalive_session(self.sessid)


# vim:sts=4:ts=4:sw=4:expandtab: