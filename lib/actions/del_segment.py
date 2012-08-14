# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


@Action.register_action
class DelSegment(Action, Dbstate):
    ERROR_MSG_TEMPLATE = ("unable to delete segment '{segment}' "
                          "[arena:'{arena}']: {reason}")

    def __init__(self, **kwargs):
        super(DelSegment, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)

    def _current_dbstate(self, database, txn):
        return self.get_arena(self.arena, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        szdb = database.dbpool().segment_zone.open()

        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg("arena doesn't exist"))

        if szdb.exists(self.arena + ' ' + self.segment, txn):
            raise ActionError(self._make_error_msg("segment contains zones"))

        if bdb_helpers.pair_exists(asdb, self.arena, self.segment, txn):
            bdb_helpers.delete_pair(asdb, self.arena, self.segment, txn)
        else:
            raise ActionError(self._make_error_msg("segment doesn't exist"))

        adb.close()
        asdb.close()
        szdb.close()

        self.del_segment(self.arena, self.segment, database, txn)
        self.update_arena(self.arena, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena,
                    segment=self.segment,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
