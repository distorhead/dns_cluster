# -*- coding: utf-8 -*-

from lib.operation import OperationError
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone


__all__ = ['AddZoneOp']


class AddZoneOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs
        self._records = kwargs.get('initial_records', [])

        # only for validation
        for rec_spec in self._records:
            self.required_data_by_key(rec_spec, 'type', str)

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # get arena needed for action construction
        if self.is_admin(session_data):
            self.required_data_by_key(self._kwargs, 'arena', str)
        else:
            self._kwargs['arena'] = session_data['arena']

        # construct actions, validation of parameters also goes here
        do_action = AddZone(**self._kwargs)
        undo_action = DelZone(arena=do_action.arena,
                              segment=do_action.segment,
                              zone=do_action.zone)

        # construct and lock resource
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     do_action.arena,
                                                     do_action.segment])
        self._acquire_lock(service_provider, resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action)

        for rec_spec in self._records:
            rec_type = rec_spec['type']
            act_do = self.make_add_record(rec_type, rec_spec)
            act_undo = self.make_del_record(rec_type, rec_spec)
            session_srv.apply_action(sessid, act_do, act_undo)


# vim:sts=4:ts=4:sw=4:expandtab:
