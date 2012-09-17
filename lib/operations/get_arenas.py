# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib import bdb_helpers


__all__ = ["GetArenasOp"]


class GetArenasOp(SessionOperation, OperationHelpersMixin):
    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        self._check_access(service_provider, sessid, session_data, None, txn)

        lock_srv.acquire('_global', sessid)

        adb = database_srv.dbpool().arena.dbhandle()
        return bdb_helpers.keys(adb, txn)

    def _has_access(self, service_provider, sessid, session_data, _, txn):
        return self.is_admin(session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
