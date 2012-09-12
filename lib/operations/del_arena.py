# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_arena import AddArena
from lib.actions.del_arena import DelArena


__all__ = ["DelArenaOp"]


class DelArenaOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._action = DelArena(**kwargs)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire(self._action.arena, sessid)
        with database_srv.transaction() as txn:
            undo_action = AddArena(arena=self._action.arena)
            session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
