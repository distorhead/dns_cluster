# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetArenasOp"]


class GetArenasOp(Operation):
    def _do_run(self, database, session):
        adb = database.dbpool().arena.dbhandle()
        return bdb_helpers.keys(adb)


# vim:sts=4:ts=4:sw=4:expandtab:
