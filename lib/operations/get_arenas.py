# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation


__all__ = ["GetArenasOp"]


class GetArenasOp(SessionOperation):
    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire("_global", sessid)
        with database_srv.transaction() as txn:
            adb = database_srv.dbpool().arena.dbhandle()
            return bdb_helpers.keys(adb, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
