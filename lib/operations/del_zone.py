# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone
from lib.common import reorder
from lib.twisted_helpers import threaded


__all__ = ['DelZoneOp']


class DelZoneOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        log.msg("_run_in_session")

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

        # parameters validation also goes here
        do_action = DelZone(**self._kwargs)

        # check zone is accessable under this session
        self._check_access(service_provider, sessid, session_data, do_action)

        # retrieve zone arena and segment needed for undo action and lock
        zone_data = self.get_zone_data(database_srv, do_action.zone)
        if not zone_data is None:
            arena = zone_data['arena']
            segment = zone_data['segment']
        else:
            arena = segment = ""

        undo_action = AddZone(arena=arena,
                              segment=segment,
                              zone=do_action.zone)

        return (do_action, undo_action, arena, segment)

    def _prepare_stage_done(self, data, op_run_defer, service_provider,
                                sessid, session_data):
        lock_srv = service_provider.get('lock')

        do_action, undo_action, arena, segment = data
        actions = (do_action, undo_action)

        # setup lock stage
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, do_action.zone])
        d = self._lock_stage(service_provider, resource, sessid)
        d.addCallback(self._lock_stage_done, op_run_defer, service_provider,
                          sessid, session_data, actions)
        # lock stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    def _lock_stage_done(self, _, op_run_defer, service_provider,
                             sessid, session_data, actions):
        log.msg("_lock_stage_done")
        d = self._apply_stage(service_provider, sessid, session_data,
                                  actions[0], actions[1])
        d.addCallback(self._apply_stage_done, op_run_defer, service_provider,
                          sessid, session_data)
        # apply stage failure causes entire operation failure
        d.addErrback(op_run_defer.errback)

    @threaded
    def _apply_stage(self, service_provider, sessid, session_data,
                         do_action, undo_action):
        log.msg("_apply_stage")
        session_srv = service_provider.get('session')
        session_srv.apply_action(sessid, do_action, undo_action)

    def _apply_stage_done(self, _, op_run_defer, service_provider, sessid, session_data):
        log.msg("_apply_stage_done")
        # action applied - finalize operation run
        op_run_defer.callback(None)

    def _has_access(self, service_provider, sessid, session_data, action):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, action.zone, session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
