# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionCommitOp"]


class SessionCommitOp(Operation):
    def __init__(self, database_srv, session_srv, **kwargs):
        Operation.__init__(self, database_srv, session_srv, **kwargs)
        self.sessid = self.required_data_by_key(kwargs, "sessid", int)

    def _do_run(self):
        self.session_srv.commit_session(self.sessid)


# vim:sts=4:ts=4:sw=4:expandtab:
