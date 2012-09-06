# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionBeginOp"]


class SessionBeginOp(Operation):
    def _do_run(self):
        return self.session_srv.begin_session()


# vim:sts=4:ts=4:sw=4:expandtab:
