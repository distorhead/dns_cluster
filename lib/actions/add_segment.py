# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


@Action.register_action
class AddSegment(Action, Dbstate):
    ERROR_MSG_TEMPLATE = ("unable to add segment '{segment}' "
                          "[arena:'{arena}']: {reason}")

    def __init__(self, **kwargs):
        super(AddSegment, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)

    def _current_dbstate(self, database, txn):
        return self.get_arena(self.arena, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()

        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg("arena doesn't exist"))

        segments = bdb_helpers.get_all(asdb, self.arena, txn)
        if not self.segment in segments:
            asdb.put(self.arena, self.segment, txn)
        else:
            raise ActionError(self._make_error_msg("segment already exists"))

        adb.close()
        asdb.close()

        self.update_segment(self.arena, self.segment, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena,
                    segment=self.segment,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
