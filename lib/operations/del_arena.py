# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.add_arena import AddArena
from lib.actions.del_arena import DelArena


__all__ = ['DelArenaOp']


class DelArenaOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')
        database_srv = service_provider.get('database')

        # validation of arguments also goes here
        do_action = DelArena(**self._kwargs)

        # retrieve arena key from database needed for undo action
        arena_data = self.get_arena_data(database_srv, do_action.arena, txn)
        key = arena_data['key']

        undo_action = AddArena(arena=do_action.arena, key=key)

        self._check_access(service_provider, sessid, session_data, do_action, txn)

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     do_action.arena])
        lock_srv.try_acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        return self.is_admin(session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
