# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetSegmentsOp"]


class GetSegmentsOp(Operation):
    def __init__(self, database_srv, **kwargs):
        Operation.__init__(self, **kwargs)
        self.database_srv = database_srv
        self.arena = self.required_data_by_key(kwargs, "arena", str)

    def _do_run(self):
        asdb = self.database_srv.dbpool().arena_segment.dbhandle()
        return bdb_helpers.get_all(asdb, self.arena)


# vim:sts=4:ts=4:sw=4:expandtab:
