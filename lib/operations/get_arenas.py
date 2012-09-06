# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetArenasOp"]


class GetArenasOp(Operation):
    def __init__(self, database_srv, **kwargs):
        Operation.__init__(self, **kwargs)
        self.database_srv = database_srv

    def _do_run(self):
        adb = self.database_srv.dbpool().arena.dbhandle()
        return bdb_helpers.keys(adb)


# vim:sts=4:ts=4:sw=4:expandtab:
