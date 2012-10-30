# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib import bdb_helpers
from lib.twisted_helpers import threaded


__all__ = ['GetArenasOp']


class GetArenasOp(SessionOperation, OperationHelpersMixin):
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
        self._check_access(service_provider, sessid, session_data, None)

    def _prepare_stage_done(self, _, op_run_defer, service_provider,
                                sessid, session_data):
        log.msg("_prepare_stage_done")
        # setup lock stage
        d = self._lock_stage(service_provider, self.GLOBAL_RESOURCE, sessid)
        d.addCallback(self._lock_stage_done, op_run_defer, service_provider,
                          sessid, session_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _lock_stage_done(self, _, op_run_defer, service_provider,
                             sessid, session_data):
        log.msg("_lock_stage_done")
        d = self._retrieve_stage(service_provider, sessid, session_data)
        d.addCallback(self._retrieve_stage_done, op_run_defer, service_provider,
                          sessid, session_data)
        # apply stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    @threaded
    def _retrieve_stage(self, service_provider, sessid, session_data):
        database_srv = service_provider.get('database')

        adb = database_srv.dbpool().arena.dbhandle()
        with database_srv.transaction() as txn:
            return bdb_helpers.keys(adb, txn)

    def _retrieve_stage_done(self, res, op_run_defer, service_provider, sessid, session_data):
        log.msg("_retrieve_stage_done")
        op_run_defer.callback(res)

    def _has_access(self, service_provider, sessid, session_data, action):
        return self.is_admin(session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
