# -*- coding: utf-8 -*-

from lib.operations.operation_helpers import OperationHelpersMixin
from lib.operations.session_operation import SessionOperation
from lib.operation import OperationError


__all__ = ["DelRecordOp"]


class DelRecordOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._rec_spec = self.required_data_by_key(kwargs, 'rec_spec', dict)
        self.required_data_by_key(self._rec_spec, 'type', str)

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # record specification validation also goes here
        do_action = self.make_del_record(self._rec_spec['type'], self._rec_spec)
        undo_action = self.del_to_add_record(database_srv, self._rec_spec['type'],
                                             do_action, txn)

        # retrieve arena and segment of zone needed for lock
        arena, segment = self.arena_segment_by_zone(database_srv, do_action.zone, txn)
        resource = lock_srv.RESOURCE_DELIMITER.join([arena, segment, do_action.zone])
        lock_srv.acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, action.zone, session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
