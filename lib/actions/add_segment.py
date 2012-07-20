# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError


@Action.register_action
class AddSegment(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("segment_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]), str(data["segment_name"]), data["state"])

    ERROR_MSG_TEMPLATE = ("unable to {action} segment '{segment}' "
                          "[arena:'{arena}']: {reason}")

    def __init__(self, arena_name, segment_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name
        self.segment_name = segment_name

    def _apply_do(self, txn):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()

        action = "add"

        self._check_arena(adb, txn, action)

        segments = bdb_helpers.get_all(asdb, self.arena_name, txn)
        if not self.segment_name in segments:
            asdb.put(self.arena_name, self.segment_name, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "segment already exists"))

        adb.close()
        asdb.close()

    def _apply_undo(self, txn):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()
        szdb = database.context().dbpool().segment_zone.open()

        action= "delete"

        self._check_arena(adb, txn, action)

        if szdb.exists(self.arena_name + ' ' + self.segment_name, txn):
            raise ActionError(self._make_error_msg(action,
                              "segment contains zones"))

        if bdb_helpers.pair_exists(asdb, self.arena_name, self.segment_name, txn):
            bdb_helpers.delete_pair(asdb, self.arena_name, self.segment_name, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "segment doesn't exist"))

        adb.close()
        asdb.close()
        szdb.close()

    def _check_arena(self, adb, txn, action):
        if not adb.exists(self.arena_name, txn):
            raise ActionError(self._make_error_msg(action,
                              "arena doesn't exist"))

    def _make_error_msg(self, action, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena_name,
                    segment=self.segment_name,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
