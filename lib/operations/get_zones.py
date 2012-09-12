# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation


__all__ = ["GetZonesOp"]


class GetZonesOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self.arena = self.required_data_by_key(kwargs, 'arena', str)
        self.segment = self.optional_data_by_key(kwargs, 'segment', str, None)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        with database_srv.transaction() as txn:
            szdb = database_srv.dbpool().segment_zone.dbhandle()
            if not self.segment is None:
                resource = lock_srv.RESOURCE_DELIMITER.join([self.arena, self.segment])
                lock_srv.acquire(resource, sessid)

                key = self.arena + ' ' + self.segment
                return bdb_helpers.get_all(szdb, key, txn)
            else:
                lock_srv.acquire('_global', sessid)

                res = []
                as_zone_map = bdb_helpers.keys_values(szdb, txn)
                for arena_segment, zone in as_zone_map:
                    as_list = arena_segment.split(' ', 1)
                    if len(as_list) == 2:
                        arena, segment = as_list
                        if arena == self.arena:
                            zone = {
                                'segment': segment,
                                'zone': zone
                            }
                            res.append(zone)

                return res


# vim:sts=4:ts=4:sw=4:expandtab:
