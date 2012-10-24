# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone
from lib.common import reorder


__all__ = ['DelZoneOp']


class DelZoneOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

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

        # construct lock resource
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, do_action.zone])
        self._acquire_lock(service_provider, resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action)

    def _has_access(self, service_provider, sessid, session_data, action):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, action.zone, session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
