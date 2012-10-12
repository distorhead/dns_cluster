# -*- coding: utf-8 -*-

from lib.operation import Operation
from lib.operations.operation_helpers import OperationHelpersMixin


__all__ = ['SessionBeginOp']


class SessionBeginOp(Operation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.auth_arena = self.required_data_by_key(kwargs, 'auth_arena', str)
        self.auth_key = self.required_data_by_key(kwargs, 'auth_key', str)

    def _do_run(self, service_provider, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')
        database_srv = service_provider.get('database')

        with database_srv.transaction() as txn:
            self.check_authenticate(self.auth_arena, self.auth_key,
                                        database_srv, txn)
            sessid = session_srv.begin_session(self.auth_arena, txn=txn)
            d = session_srv.set_watchdog(sessid, txn=txn)
            d.addCallback(lambda x: lock_srv.release_session(sessid))
            return sessid


# vim:sts=4:ts=4:sw=4:expandtab:
