# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_arena import AddArena
from lib.actions.del_arena import DelArena


__all__ = ['AddArenaOp']


class AddArenaOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        # validation of arguments also goes here
        do_action = AddArena(**self._kwargs)
        undo_action = DelArena(arena=do_action.arena)

        self._check_access(service_provider, sessid, session_data, do_action)

        self._acquire_lock(service_provider, self.GLOBAL_RESOURCE, sessid)

        session_srv.apply_action(sessid, do_action, undo_action)

    def _has_access(self, service_provider, sessid, session_data, action):
        return self.is_admin(session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
