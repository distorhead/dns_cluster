# -*- coding: utf-8 -*-

from lib.operation import OperationError
from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone


__all__ = ["AddZoneOp"]


class AddZoneOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs
        self._records = kwargs.get('initial_records', [])

        # only for validation
        for rec_spec in self._records:
            self.required_data_by_key(rec_spec, 'type', str)

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
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
        resource = lock_srv.RESOURCE_DELIMITER.join(
                       [do_action.arena, do_action.segment])
        lock_srv.acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

        for rec_spec in self._records:
            print "rec_spec:", rec_spec
            rec_type = rec_spec['type']
            act_do = self.make_add_record(rec_type, rec_spec)
            act_undo = self.make_del_record(rec_type, rec_spec)
            session_srv.apply_action(sessid, act_do, act_undo, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
