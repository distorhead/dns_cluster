# -*- coding: utf-8 -*-

from lib.operation import OperationError
from lib.operations.session_operation import SessionOperation
from lib.operations.record_operation import RecordOperation
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone


__all__ = ["AddZoneOp"]


class AddZoneOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._action = AddZone(**kwargs)
        self._records = kwargs.get('initial_records', [])

        # only for validation
        for rec_spec in self._records:
            self.required_data_by_key(rec_spec, 'type', str)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        resource = lock_srv.RESOURCE_DELIMITER.join(
                       [self._action.arena, self._action.segment])
        lock_srv.acquire(resource, sessid)

        with database_srv.transaction() as txn:
            undo_action = DelZone(arena=self._action.arena,
                                  segment=self._action.segment,
                                  zone=self._action.zone)
            session_srv.apply_action(sessid, self._action, undo_action, txn=txn)

            record_actions = []
            for rec_spec in self._records:
                rec_type = rec_spec.get['type']
                act_do = self.make_add_record(rec_type, rec_spec)
                act_undo = self.make_del_record(rec_type, rec_spec)
                session_srv.apply_action(sessid, act_do, act_undo, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
