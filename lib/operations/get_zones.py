# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetZonesOp"]


class GetZonesOp(Operation):
    def __init__(self, database_srv, **kwargs):
        Operation.__init__(self, **kwargs)
        self.database_srv = database_srv
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.optional_data_by_key(kwargs, "segment", str, None)

    def _do_run(self):
        szdb = self.database_srv.dbpool().segment_zone.dbhandle()
        if not self.segment is None:
            key = self.arena + ' ' + self.segment
            return bdb_helpers.get_all(szdb, key)
        else:
            res = []
            as_zone_map = bdb_helpers.keys_values(szdb)
            for arena_segment, zone in as_zone_map:
                as_list = arena_segment.split(' ', 1)
                if len(as_list) == 2:
                    arena, segment = as_list
                    zone = {
                        "arena": arena,
                        "segment": segment,
                        "zone": zone
                    }
                    res.append(zone)

            return res


# vim:sts=4:ts=4:sw=4:expandtab:
