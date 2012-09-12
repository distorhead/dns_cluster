# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.record_operation import RecordOperation
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone
from lib.common import reorder


__all__ = ["DelZoneOp"]


class DelZoneOp(SessionOperation, RecordOperation):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._action = DelZone(**kwargs)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        with database_srv.transaction() as txn:
            as_pair = self.arena_segment_by_zone(database_srv, self._action.zone, txn)
            if not as_pair is None:
                arena, segment = as_pair
                resource = lock_srv.RESOURCE_DELIMITER.join(
                               [arena, segment, self._action.zone])

                lock_srv.acquire(resource, sessid)
                undo_action = AddZone(arena=arena,
                                      segment=segment,
                                      zone=self._action.zone)
                session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
