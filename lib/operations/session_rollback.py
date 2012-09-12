# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionRollbackOp"]


class SessionRollbackOp(Operation):
    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.required_data_by_key(kwargs, 'sessid', int)

    def _do_run(self, service_provider, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        with database_srv.transaction() as txn:
            session_srv.rollback_session(self.sessid, txn=txn)
            lock_srv.release_session(self.sessid, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
