# -*- coding: utf-8 -*-

from lib.operation import Operation
from lib.twisted_helpers import threaded


__all__ = ['SessionRollbackOp']


class SessionRollbackOp(Operation):
    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.required_data_by_key(kwargs, 'sessid', int)

    @threaded
    def _do_run(self, service_provider, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        session_srv.rollback_session(self.sessid)
        lock_srv.release_session(self.sessid)


# vim:sts=4:ts=4:sw=4:expandtab:
