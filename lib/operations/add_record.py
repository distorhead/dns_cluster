# -*- coding: utf-8 -*-

from lib.operations.record_operation import RecordOperation
from lib.operations.session_operation import SessionOperation
from lib.operation import OperationError


__all__ = ["AddRecordOp"]


class AddRecordOp(SessionOperation, RecordOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        rec_spec = self.required_data_by_key(kwargs, 'rec_spec', dict)
        self._rec_type = self.required_data_by_key(rec_spec, 'type', str)
        self._action = self.make_add_record(self._rec_type, rec_spec)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        with database_srv.transaction() as txn:
            as_pair = self.arena_segment_by_zone(database_srv, self._action.zone, txn)
            if not as_pair is None:
                arena, segment = as_pair
                resource = lock_srv.RESOURCE_DELIMITER.join(
                               [arena, segment, self._action.zone])

                lock_srv.acquire(resource, sessid)
                undo_action = self.add_to_del_record(database_srv,
                                                     self._rec_type,
                                                     self._action, txn)
                session_srv.apply_action(sessid, self._action,
                                         undo_action, txn=txn)
            else:
                raise OperationError("Unable to locate zone '{}'".format(
                                        self._action.zone))


# vim:sts=4:ts=4:sw=4:expandtab:
