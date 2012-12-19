# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.operation import OperationError
from lib.common import split
from lib import bdb_helpers
from lib.twisted_helpers import threaded


__all__ = ['GetRecordsOp']


class GetRecordsOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self.zone = self.required_data_by_key(kwargs, 'zone', str)

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        log.msg("_run_in_session")

        op_run_defer = defer.Deferred()

        # check access here and retrieve zone data
        d = self._prepare_stage(service_provider, sessid, session_data)
        d.addCallback(self._prepare_stage_done, op_run_defer,
                          service_provider, sessid, session_data)
        # prepare stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

        return op_run_defer

    @threaded
    def _prepare_stage(self, service_provider, sessid, session_data):
        log.msg("_prepare_stage")

        # also checks existance of zone
        self._check_access(service_provider, sessid, session_data, None)

        database_srv = service_provider.get('database')
        zone_data = self.get_zone_data(database_srv, self.zone)
        if not zone_data is None:
            if self.is_admin(session_data):
                arena = zone_data['arena']
            else:
                arena = session_data['arena']
            segment = zone_data['segment']
        else:
            # This is internal error, because access checking should give 'access denied'
            #   if zone is not exists.
            assert 0, "Unable to get arena and segment of zone '{}', should not get this".format(
                          self.zone)

        return (arena, segment)

    def _prepare_stage_done(self, zone_data, op_run_defer, service_provider,
                                sessid, session_data):
        log.msg("_prepare_stage_done")

        lock_srv = service_provider.get('lock')

        # setup lock stage
        arena, segment = zone_data
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, self.zone])
        d = self._lock_stage(service_provider, resource, sessid)
        d.addCallback(self._lock_stage_done, op_run_defer, service_provider,
                          sessid, session_data, zone_data)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _lock_stage_done(self, _, op_run_defer, service_provider,
                             sessid, session_data, zone_data):
        log.msg("_lock_stage_done")
        d = self._retrieve_stage(service_provider, zone_data, sessid, session_data)
        d.addCallback(self._retrieve_stage_done, op_run_defer, service_provider,
                          sessid, session_data)
        # retrieve stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    @threaded
    def _retrieve_stage(self, service_provider, zone_data, sessid, session_data):
        log.msg("_retrieve_stage")
        database_srv = service_provider.get('database')

        res = []
        ddb = database_srv.dbpool().dns_data.dbhandle()
        zddb = database_srv.dbpool().zone_dns_data.dbhandle()

        with database_srv.transaction() as txn:
            zdkeys = bdb_helpers.get_all(zddb, self.zone, txn)
            for zdkey in zdkeys:
                recs = bdb_helpers.get_all(ddb, zdkey, txn)
                for rec in recs:
                    rec_spec = self.make_rec_spec(zdkey, rec)
                    if not rec_spec is None:
                        res.append(rec_spec)

        return res

    def _retrieve_stage_done(self, res, op_run_defer, service_provider,
                                 sessid, session_data):
        log.msg("_retrieve_stage_done")
        op_run_defer.callback(res)

    def _has_access(self, service_provider, sessid, session_data, _):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, self.zone, session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
