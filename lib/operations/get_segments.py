# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib import bdb_helpers
from lib.operation import OperationError
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.twisted_helpers import threaded


__all__ = ['GetSegmentsOp']


class GetSegmentsOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        log.msg("_run_in_session called")

        op_run_defer = defer.Deferred()

        d = self._prepare_stage(service_provider, sessid, session_data)
        d.addCallback(self._prepare_stage_done, op_run_defer,
                          service_provider, sessid, session_data)
        # prepare stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

        return op_run_defer

    @threaded
    def _prepare_stage(self, service_provider, sessid, session_data):
        database_srv = service_provider.get('database')

        # validate given parameters
        if self.is_admin(session_data):
            # admin has access to all arenas
            # concrete arena name required
            arena = self.required_data_by_key(self._kwargs, 'arena', str)
        else:
            # otherwise take from session
            arena = self.required_data_by_key(session_data, 'arena', str)

        # check that arena exists in db
        self.check_arena_exists(database_srv, arena)

        return arena

    def _prepare_stage_done(self, arena, op_run_defer, service_provider,
                                sessid, session_data):
        log.msg("_prepare_stage_done")

        lock_srv = service_provider.get('lock')

        # setup lock stage
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena])
        d = self._lock_stage(service_provider, resource, sessid)
        d.addCallback(self._lock_stage_done, arena, op_run_defer, service_provider,
                          sessid, session_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _lock_stage_done(self, _, arena, op_run_defer, service_provider,
                             sessid, session_data):
        log.msg("_lock_stage_done")
        d = self._retrieve_stage(service_provider, arena, sessid, session_data)
        d.addCallback(self._retrieve_stage_done, op_run_defer, service_provider,
                          sessid, session_data)
        # retrieve stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    @threaded
    def _retrieve_stage(self, service_provider, arena, sessid, session_data):
        log.msg("_retrieve_stage")
        database_srv = service_provider.get('database')

        asdb = database_srv.dbpool().arena_segment.dbhandle()
        with database_srv.transaction() as txn:
            return bdb_helpers.get_all(asdb, arena, txn)

    def _retrieve_stage_done(self, res, op_run_defer, service_provider,
                                 sessid, session_data):
        log.msg("_retrieve_stage_done")
        op_run_defer.callback(res)


# vim:sts=4:ts=4:sw=4:expandtab:
