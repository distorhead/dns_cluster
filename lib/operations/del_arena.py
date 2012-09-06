# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_arena import AddArena
from lib.actions.del_arena import DelArena


__all__ = ["DelArenaOp"]


class DelArenaOp(SessionOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        SessionOperation.__init__(self, database_srv, session_srv, **kwargs)
        self._action = DelArena(**kwargs)

    def _run_in_session(self, sessid):
        undo_action = AddArena(arena=self._action.arena)
        self.session_srv.apply_action(sessid, self._action, undo_action)


# vim:sts=4:ts=4:sw=4:expandtab:
