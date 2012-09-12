# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment


__all__ = ["AddSegmentOp"]


class AddSegmentOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._action = AddSegment(**kwargs)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        lock_srv.acquire(self._action.arena, sessid)
        with database_srv.transaction() as txn:
            undo_action = DelSegment(arena=self._action.arena,
                                     segment=self._action.segment)
            session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
