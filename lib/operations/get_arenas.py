# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetArenasOp"]


class GetArenasOp(Operation):
    def _do_run(self):
        with self.database_srv.transaction() as txn:
            adb = self.database_srv.dbpool().arena.dbhandle()
            return bdb_helpers.keys(adb, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
