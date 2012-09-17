# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin


__all__ = ["GetSegmentsOp"]


class GetSegmentsOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        # validate given parameters
        if self.is_admin(session_data):
            # admin has access to all arenas
            # concrete arena name required
            arena = self.required_data_by_key(self._kwargs, 'arena', str)
        else:
            # otherwise take from session
            arena = self.required_data_by_key(session_data, 'arena', str)

        lock_srv.acquire(arena, sessid)

        asdb = database_srv.dbpool().arena_segment.dbhandle()
        return bdb_helpers.get_all(asdb, self.arena, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
