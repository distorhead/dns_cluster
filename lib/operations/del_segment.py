# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment


__all__ = ["DelSegmentOp"]


class DelSegmentOp(SessionOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        SessionOperation.__init__(self, database_srv, session_srv, **kwargs)
        self._action = DelSegment(**kwargs)

    def _run_in_session(self, sessid):
        undo_action = AddSegment(arena=self._action.arena,
                                 segment=self._action.segment)
        self.session_srv.apply_action(sessid, self._action, undo_action)


# vim:sts=4:ts=4:sw=4:expandtab:
