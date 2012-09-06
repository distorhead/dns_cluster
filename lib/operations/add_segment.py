# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment


__all__ = ["AddSegmentOp"]


class AddSegmentOp(SessionOperation):
    def __init__(self, session_srv, **kwargs):
        SessionOperation.__init__(self, session_srv, **kwargs)
        self._action = AddSegment(**kwargs)

    def _run_in_session(self, sessid):
        undo_action = DelSegment(arena=self._action.arena,
                                 segment=self._action.segment)
        self.session_srv.apply_action(sessid, self._action, undo_action)


# vim:sts=4:ts=4:sw=4:expandtab:
