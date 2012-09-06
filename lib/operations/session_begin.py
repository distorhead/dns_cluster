# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionBeginOp"]


class SessionBeginOp(Operation):
    def __init__(self, session_srv, **kwargs):
        Operation.__init__(self, **kwargs)
        self.session_srv = session_srv

    def _do_run(self):
        return self.session_srv.begin_session()


# vim:sts=4:ts=4:sw=4:expandtab:
