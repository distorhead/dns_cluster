# -*- coding: utf-8 -*-

from twisted.internet import reactor, threads, defer
from twisted.python import log

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment
from lib.twisted_helpers import threaded


__all__ = ['DelSegmentOp']


class DelSegmentOp(SessionOperation, OperationHelpersMixin):
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
        # retrieve arena needed for action construction
        if self.is_admin(session_data):
            self.required_data_by_key(self._kwargs, 'arena', str)
        else:
            self._kwargs['arena'] = session_data['arena']

        # parameters validation also goes here
        do_action = DelSegment(**self._kwargs)
        undo_action = AddSegment(arena=do_action.arena,
                                 segment=do_action.segment)

        return (do_action, undo_action)

    def _prepare_stage_done(self, actions, op_run_defer, service_provider,
                                sessid, session_data):
        lock_srv = service_provider.get('lock')

        # setup lock stage
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     actions[0].arena,
                                                     actions[0].segment])
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


# vim:sts=4:ts=4:sw=4:expandtab:
