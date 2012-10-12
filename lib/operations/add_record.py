# -*- coding: utf-8 -*-

from lib.operations.operation_helpers import OperationHelpersMixin
from lib.operations.session_operation import SessionOperation
from lib.operation import OperationError


__all__ = ['AddRecordOp']


class AddRecordOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._rec_spec = self.required_data_by_key(kwargs, 'rec_spec', dict)

        # only for validation
        self.required_data_by_key(self._rec_spec, 'type', str)

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # validation of rec_spec also goes here
        do_action = self.make_add_record(self._rec_spec['type'], self._rec_spec)
        undo_action = self.add_to_del_record(database_srv, self._rec_spec['type'],
                                             do_action, txn)

        self._check_access(service_provider, sessid, session_data, do_action, txn)

        # retrieve zone arena and segment needed for lock
        zone_data = self.get_zone_data(database_srv, do_action.zone, txn)
        if not zone_data is None:
            arena = zone_data['arena']
            segment = zone_data['segment']
        else:
            arena = segment = ""

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, do_action.zone])
        lock_srv.try_acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, action.zone, session_data, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
