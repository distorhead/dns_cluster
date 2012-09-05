# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionRollbackOp"]


class SessionRollbackOp(Operation):
    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.required_data_by_key(kwargs, "sessid", int)

    def _do_run(self, database, session):
        session.rollback_session(self.sessid)


# vim:sts=4:ts=4:sw=4:expandtab:
