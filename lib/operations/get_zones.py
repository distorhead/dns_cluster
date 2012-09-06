# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operation import Operation


__all__ = ["GetZonesOp"]


class GetZonesOp(Operation):
    def __init__(self, database_srv, session_srv, **kwargs):
        Operation.__init__(self, database_srv, session_srv, **kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.optional_data_by_key(kwargs, "segment", str, None)

    def _do_run(self):
        with self.database_srv.transaction() as txn:
            szdb = self.database_srv.dbpool().segment_zone.dbhandle()
            if not self.segment is None:
                key = self.arena + ' ' + self.segment
                return bdb_helpers.get_all(szdb, key, txn)
            else:
                res = []
                as_zone_map = bdb_helpers.keys_values(szdb, txn)
                for arena_segment, zone in as_zone_map:
                    as_list = arena_segment.split(' ', 1)
                    if len(as_list) == 2:
                        arena, segment = as_list
                        if arena == self.arena:
                            zone = {
                                "segment": segment,
                                "zone": zone
                            }
                            res.append(zone)

                return res


# vim:sts=4:ts=4:sw=4:expandtab:
