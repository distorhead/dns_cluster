# -*- coding: utf-8 -*-

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.database import transactional2


__all__ = ['GetZonesOp']


class GetZonesOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs
        self.segment = self.optional_data_by_key(kwargs, 'segment', str, None)

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        database_srv = service_provider.get('database')
        if self.is_admin(session_data):
            arena = self.optional_data_by_key(self._kwargs, 'arena', str, None)

            if arena is None:
                # get all zones from all arenas
                return self._get_all_zones(database_srv, service_provider, sessid)
            elif self.segment is None:
                # get all zones from this arena
                return self._get_arena_zones(database_srv, service_provider,
                                             arena, sessid)
            else:
                # get zones from this arena and segment
                return self._get_arena_segment_zones(database_srv, service_provider,
                                                     arena, self.segment, sessid)

        else:
            arena = session_data['arena']

            if self.segment is None:
                # get all zones from this arena
                return self._get_arena_zones(database_srv, service_provider,
                                             arena, sessid)
            else:
                # get zones from this arena and segment
                return self._get_arena_segment_zones(database_srv, service_provider,
                                                     arena, self.segment, sessid)

    @transactional2
    def _get_all_zones(self, database_srv, service_provider, sessid, **kwargs):
        txn = kwargs['txn']
        lock_srv = service_provider.get('lock')

        self._acquire_lock(service_provider, self.GLOBAL_RESOURCE, sessid)

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

    @transactional2
    def _get_arena_zones(self, database_srv, service_provider, arena, sessid, **kwargs):
        txn = kwargs['txn']
        lock_srv = service_provider.get('lock')

        self.check_arena_exists(database_srv, arena, txn=txn)

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena])
        self._acquire_lock(service_provider, resource, sessid)

        asdb = database_srv.dbpool().arena_segment.dbhandle()

        res = []
        for segment in bdb_helpers.get_all(asdb, arena, txn):
            res += self._get_arena_segment_zones(database_srv, service_provider,
                                                 arena, segment, sessid, False,
                                                 txn=txn)

        return res

    @transactional2
    def _get_arena_segment_zones(self, database_srv, service_provider, arena, segment,
                                 sessid, take_lock=True, **kwargs):
        txn = kwargs['txn']
        lock_srv = service_provider.get('lock')

        self.check_arena_exists(database_srv, arena, txn=txn)
        self.check_segment_exists(database_srv, arena, segment, txn=txn)

        if take_lock:
            resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                         arena,
                                                         segment])
            self._acquire_lock(service_provider, resource, sessid)

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
