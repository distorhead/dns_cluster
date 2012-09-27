# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone
from lib.common import reorder


__all__ = ["DelZoneOp"]


class DelZoneOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # parameters validation also goes here
        do_action = DelZone(**self._kwargs)

        # check zone is accessable under this session
        self._check_access(service_provider, sessid, session_data, do_action, txn)

        # retrieve zone arena and segment needed for undo action and lock
        arena, segment = self.arena_segment_by_zone(database_srv, do_action.zone, txn)
        undo_action = AddZone(arena=arena,
                              segment=segment,
                              zone=do_action.zone)

        # construct lock resource
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, do_action.zone])
        lock_srv.try_acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, action.zone, session_data, txn)


# vim:sts=4:ts=4:sw=4:expandtab: