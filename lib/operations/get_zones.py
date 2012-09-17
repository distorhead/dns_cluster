# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin


__all__ = ["GetZonesOp"]


class GetZonesOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs
        self.segment = self.optional_data_by_key(kwargs, 'segment', str, None)

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        if self.is_admin(session_data):
            arena = self.optional_data_by_key(self._kwargs, 'arena', str, None)

            if arena is None:
                # get all zones from all arenas
                return self._get_all_zones(service_provider, sessid, txn)
            elif self.segment is None:
                # get all zones from this arena
                return self._get_arena_zones(arena, service_provider, sessid, txn)
            else:
                # get zones from this arena and segment
                return self._get_arena_segment_zones(arena, self.segment,
                                                     service_provider, sessid, txn)

        else:
            arena = session_data['arena']

            if self.segment is None:
                # get all zones from this arena
                return self._get_arena_zones(arena, service_provider, sessid, txn)
            else:
                # get zones from this arena and segment
                return self._get_arena_segment_zones(arena, self.segment,
                                                     service_provider, sessid, txn)

    def _get_all_zones(service_provider, sessid, txn):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire('_global', sessid)

        szdb = database_srv.dbpool().segment_zone.dbhandle()
        as_zone_map = bdb_helpers.keys_values(szdb, txn)

        res = []
        for arena_segment, zone in as_zone_map:
            as_list = arena_segment.split(' ', 1)
            if len(as_list) == 2:
                arena, segment = as_list
                res.append({
                    'arena': arena,
                    'segment': segment,
                    'zone': zone
                })

        return res

    def _get_arena_zones(self, arena, service_provider, sessid, txn):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire(arena, sessid)

        asdb = database_srv.dbpool().arena_segment.dbhandle()

        res = []
        for segment in bdb_helpers.get_all(asdb, arena, txn):
            res += self._get_arena_segment_zones(arena, segment, service_provider,
                                                 sessid, txn)

        return res

    def _get_arena_segment_zones(self, arena, segment, service_provider, sessid, txn):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        resource = lock_srv.RESOURCE_DELIMITER.join([arena, segment])
        lock_srv.acquire(resource, sessid)

        szdb = database_srv.dbpool().segment_zone.dbhandle()

        res = []
        for zone in bdb_helpers.get_all(szdb, arena + ' ' + segment, txn):
            res.append({
                'arena': arena,
                'segment': segment,
                'zone': zone
            })

        return res


# vim:sts=4:ts=4:sw=4:expandtab:
