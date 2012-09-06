# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone
from lib.common import reorder


__all__ = ["DelZoneOp"]


class DelZoneOp(SessionOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        SessionOperation.__init__(self, database_srv, session_srv, **kwargs)
        self._action = DelZone(**kwargs)

    def _run_in_session(self, sessid):
        rzone = reorder(self._action.zone)
        with self.database_srv.transaction() as txn:
            zdb = self.database_srv.dbpool().dns_zone.dbhandle()
            if zdb.exists(rzone, txn):
                arena_segment = zdb.get(rzone, None, txn)
                as_list = arena_segment.split(' ', 1)
                if len(as_list) == 2:
                    arena, segment = as_list
                else:
                    arena = arena_segment
                    segment = ""
            else:
                arena = segment = ""

            undo_action = AddZone(arena=arena,
                                  segment=segment,
                                  zone=self._action.zone)
            self.session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
