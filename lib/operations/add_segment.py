# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment


__all__ = ['AddSegmentOp']


class AddSegmentOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # retrieve arena needed for action construction
        if self.is_admin(session_data):
            self.required_data_by_key(self._kwargs, 'arena', str)
        else:
            self._kwargs['arena'] = session_data['arena']

        # parameters validation also goes here
        do_action = AddSegment(**self._kwargs)
        undo_action = DelSegment(arena=do_action.arena, segment=do_action.segment)

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     do_action.arena])
        self._acquire_lock(service_provider, resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action)


# vim:sts=4:ts=4:sw=4:expandtab:
