# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError


@Action.register_action
class AddSegment(Action):
    ERROR_MSG_TEMPLATE = ("unable to {action} segment '{segment}' "
                          "[arena:'{arena}']: {reason}")

    def __init__(self, **kwargs):
        super(AddSegment, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)

    def _apply_do(self, txn, database):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()

        action = "add"

        self._check_arena(adb, txn, action)

        segments = bdb_helpers.get_all(asdb, self.arena, txn)
        if not self.segment in segments:
            asdb.put(self.arena, self.segment, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "segment already exists"))

        adb.close()
        asdb.close()

    def _apply_undo(self, txn, database):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        szdb = database.dbpool().segment_zone.open()

        action= "delete"

        self._check_arena(adb, txn, action)

        if szdb.exists(self.arena + ' ' + self.segment, txn):
            raise ActionError(self._make_error_msg(action,
                              "segment contains zones"))

        if bdb_helpers.pair_exists(asdb, self.arena, self.segment, txn):
            bdb_helpers.delete_pair(asdb, self.arena, self.segment, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "segment doesn't exist"))

        adb.close()
        asdb.close()
        szdb.close()

    def _check_arena(self, adb, txn, action):
        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg(action,
                              "arena doesn't exist"))

    def _make_error_msg(self, action, reason):
        return self.ERROR_MSG_TEMPLATE.format( arena=self.arena,
                    segment=self.segment,
                    action=action,
                    reason=reason
                )


def add_action(**kwargs):
    kwargs["state"] = Action.State.DO
    return AddSegment(**kwargs)

def del_action(**kwargs):
    kwargs["state"] = Action.State.UNDO
    return AddSegment(**kwargs)


# vim:sts=4:ts=4:sw=4:expandtab:
