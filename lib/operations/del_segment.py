# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_segment import AddSegment
from lib.actions.del_segment import DelSegment


__all__ = ["DelSegmentOp"]


class DelSegmentOp(SessionOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._action = DelSegment(**kwargs)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        resource = lock_srv.RESOURCE_DELIMITER.join(
                       [self._action.arena, self._action.segment])
        lock_srv.acquire(resource, sessid)

        with database_srv.transaction() as txn:
            undo_action = AddSegment(arena=self._action.arena,
                                     segment=self._action.segment)
            session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
