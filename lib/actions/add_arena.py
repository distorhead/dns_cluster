# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


@Action.register_action
class AddArena(Action, Dbstate):
    def __init__(self, **kwargs):
        super(AddArena, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.dbhandle()

        if not adb.exists(self.arena, txn):
            adb.put(self.arena, '', txn)
        else:
            raise ActionError("unable to add arena '{0}': "
                              "arena already exists".format(
                                            self.arena))

        self.update_arena(self.arena, database, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
