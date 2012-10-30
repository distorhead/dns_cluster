# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib import bdb_helpers
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.database import transactional2
from lib.twisted_helpers import threaded


__all__ = ['GetZonesOp']


class GetZonesOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs
        self.segment = self.optional_data_by_key(kwargs, 'segment', str, None)

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        log.msg("_run_in_session")

        op_run_defer = defer.Deferred()

        if self.is_admin(session_data):
            arena = self.optional_data_by_key(self._kwargs, 'arena', str, None)

            if arena is None:
                # get all zones from all arenas
                self._get_all_zones_begin(op_run_defer,
                                          service_provider,
                                          sessid,
                                          session_data)
            elif self.segment is None:
                # get all zones from this arena
                self._get_arena_zones_begin(op_run_defer,
                                            service_provider,
                                            arena,
                                            sessid,
                                            session_data)
            else:
                # get zones from this arena and segment
                self._get_arena_segment_zones_begin(op_run_defer,
                                                    service_provider,
                                                    arena,
                                                    self.segment,
                                                    sessid,
                                                    session_data)

        else:
            arena = session_data['arena']

            if self.segment is None:
                # get all zones from this arena
                self._get_arena_zones_begin(op_run_defer,
                                            service_provider,
                                            arena,
                                            sessid,
                                            session_data)
            else:
                # get zones from this arena and segment
                self._get_arena_segment_zones_begin(op_run_defer,
                                                    service_provider,
                                                    arena,
                                                    self.segment,
                                                    sessid,
                                                    session_data)

        return op_run_defer


    # Begin operation

    def _get_all_zones_begin(self, op_run_defer, service_provider,
                                 sessid, session_data):
        log.msg("_get_all_zones_begin")

        # no prepare stage
        d = self._lock_stage(service_provider, self.GLOBAL_RESOURCE, sessid)
        d.addCallback(self._get_all_zones_lock_done, op_run_defer, service_provider,
                          sessid, session_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _get_arena_zones_begin(self, op_run_defer, service_provider, arena,
                                 sessid, session_data):
         log.msg("_get_arena_zones_begin")

         d = self._get_arena_zones_prepare(service_provider, arena,
                                               sessid, session_data)
         d.addCallback(self._get_arena_zones_prepare_done, op_run_defer,
                           service_provider, arena, sessid, session_data)
         d.addErrback(op_run_defer.errback)

    def _get_arena_segment_zones_begin(self, op_run_defer, service_provider,
                                           arena, segment, sessid, session_data):
         log.msg("_get_arena_segment_zones_begin")

         d = self._get_arena_segment_zones_prepare(service_provider, arena, segment,
                                                       sessid, session_data)
         d.addCallback(self._get_arena_segment_zones_prepare_done, op_run_defer,
                           service_provider, arena, segment, sessid, session_data)
         d.addErrback(op_run_defer.errback)


    # Prepare stage

    @threaded
    def _get_arena_zones_prepare(self, service_provider, arena, sessid, session_data):
        database_srv = service_provider.get('database')
        self.check_arena_exists(database_srv, arena)

    @threaded
    def _get_arena_segment_zones_prepare(self, service_provider, arena, segment,
                                             sessid, session_data):
        database_srv = service_provider.get('database')
        self.check_arena_exists(database_srv, arena)
        self.check_segment_exists(database_srv, arena, segment)


    # Prepare stage done

    def _get_arena_zones_prepare_done(self, _, op_run_defer, service_provider,
                                          arena, sessid, session_data):
        lock_srv = service_provider.get('lock')

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena])

        d = self._lock_stage(service_provider, resource, sessid)
        d.addCallback(self._get_arena_zones_lock_done, op_run_defer,
                          service_provider, arena, sessid, session_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _get_arena_segment_zones_prepare_done(self, _, op_run_defer, service_provider, 
                                                  arena, segment,
                                                  sessid, session_data):
        lock_srv = service_provider.get('lock')

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     arena,
                                                     segment])

        d = self._lock_stage(service_provider, resource, sessid)
        d.addCallback(self._get_arena_segment_zones_lock_done, op_run_defer,
                          service_provider, arena, segment, sessid, session_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)


    # Lock stage done

    def _get_all_zones_lock_done(self, _, op_run_defer, service_provider,
                                     sessid, session_data):
        d = self._get_all_zones_retrieve(service_provider, sessid, session_data)
        d.addCallback(op_run_defer.callback)
        d.addErrback(op_run_defer.errback)

    def _get_arena_zones_lock_done(self, _, op_run_defer, service_provider,
                                        arena, sessid, session_data):
        d = self._get_arena_zones_retrieve(service_provider, arena,
                                           sessid, session_data)
        d.addCallback(op_run_defer.callback)
        d.addErrback(op_run_defer.errback)

    def _get_arena_segment_zones_lock_done(self, _, op_run_defer, service_provider,
                                               arena, segment, sessid, session_data):
        d = self._get_arena_segment_zones_retrieve(service_provider, arena, segment,
                                                       sessid, session_data)
        d.addCallback(op_run_defer.callback)
        d.addErrback(op_run_defer.errback)


    # Retrieve stage

    @threaded
    def _get_all_zones_retrieve(self, service_provider, sessid, session_data):
        database_srv = service_provider.get('database')
        with database_srv.transaction() as txn:
            return self._get_all_zones(database_srv, txn)

    @threaded
    def _get_arena_zones_retrieve(self, service_provider, arena, sessid, session_data):
        database_srv = service_provider.get('database')
        with database_srv.transaction() as txn:
            return self._get_arena_zones(database_srv, arena, txn)

    @threaded
    def _get_arena_segment_zones_retrieve(self, service_provider, arena, segment,
                                              sessid, session_data):
        database_srv = service_provider.get('database')
        with database_srv.transaction() as txn:
            return self._get_arena_segment_zones(database_srv, arena, segment, txn)


    # DB helpers

    def _get_all_zones(self, database_srv, txn):
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

    def _get_arena_zones(self, database_srv, arena, txn):
        asdb = database_srv.dbpool().arena_segment.dbhandle()

        res = []
        for segment in bdb_helpers.get_all(asdb, arena, txn):
            res += self._get_arena_segment_zones(database_srv, arena, segment, txn)

        return res

    def _get_arena_segment_zones(self, database_srv, arena, segment, txn):
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
