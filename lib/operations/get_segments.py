# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation


__all__ = ["GetSegmentsOp"]


class GetSegmentsOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self.arena = self.required_data_by_key(kwargs, 'arena', str)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire(self.arena, sessid)
        with database_srv.transaction() as txn:
            asdb = database_srv.dbpool().arena_segment.dbhandle()
            return bdb_helpers.get_all(asdb, self.arena, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
