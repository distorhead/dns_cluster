# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError


@Action.register_action
class AddArena(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]), data["state"])

    def __init__(self, arena_name, state=None):
        super(AddArena, self).__init__(state)
        self.arena_name = arena_name

    def _apply_do(self, txn):
        adb = database.context().dbpool().arena.open()
        if not adb.exists(self.arena_name, txn):
            adb.put(self.arena_name, '', txn)
        else:
            raise ActionError("unable to add arena '{0}': "
                              "arena already exists".format(
                                            self.arena_name))
        adb.close()

    def _apply_undo(self, txn):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()

        if asdb.exists(self.arena_name, txn):
            raise ActionError("unable to delete arena '{0}': "
                              "arena contains segments".format(
                                            self.arena_name))

        if adb.exists(self.arena_name, txn):
            adb.delete(self.arena_name, txn)
        else:
            raise ActionError("unable to delete arena '{0}': "
                              "arena doesn't exist".format(
                                            self.arena_name))

        adb.close()
        asdb.close()


# vim:sts=4:ts=4:sw=4:expandtab:
