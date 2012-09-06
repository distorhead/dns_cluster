# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone


__all__ = ["AddZoneOp"]


class AddZoneOp(SessionOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        SessionOperation.__init__(self, database_srv, session_srv, **kwargs)
        self._action = AddZone(**kwargs)

    def _run_in_session(self, sessid):
        undo_action = DelZone(arena=self._action.arena,
                              segment=self._action.segment,
                              zone=self._action.zone)
        self.session_srv.apply_action(sessid, self._action, undo_action)


# vim:sts=4:ts=4:sw=4:expandtab:
