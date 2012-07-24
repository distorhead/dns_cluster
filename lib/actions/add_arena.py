# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError


@Action.register_action
class AddArena(Action):
    def __init__(self, **kwargs):
        super(AddArena, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)

    def _apply_do(self, txn, database):
        adb = database.dbpool().arena.open()
        if not adb.exists(self.arena, txn):
            adb.put(self.arena, '', txn)
        else:
            raise ActionError("unable to add arena '{0}': "
                              "arena already exists".format(
                                            self.arena))
        adb.close()

    def _apply_undo(self, txn, database):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()

        if asdb.exists(self.arena, txn):
            raise ActionError("unable to delete arena '{0}': "
                              "arena contains segments".format(
                                            self.arena))

        if adb.exists(self.arena, txn):
            adb.delete(self.arena, txn)
        else:
            raise ActionError("unable to delete arena '{0}': "
                              "arena doesn't exist".format(
                                            self.arena))

        adb.close()
        asdb.close()


# vim:sts=4:ts=4:sw=4:expandtab:
