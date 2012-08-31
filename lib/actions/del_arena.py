# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


__all__ = ["DelArena"]


@Action.register_action
class DelArena(Action, Dbstate):
    def __init__(self, **kwargs):
        super(DelArena, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)

    def _current_dbstate(self, database, txn):
        return self.get_global(database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.dbhandle()
        asdb = database.dbpool().arena_segment.dbhandle()

        if asdb.exists(self.arena, txn):
            raise ActionError("unable to delete arena {}: "
                              "arena contains segments".format(self.desc()))

        if adb.exists(self.arena, txn):
            adb.delete(self.arena, txn)
        else:
            raise ActionError("unable to delete arena {}: "
                              "arena doesn't exist".format(self.desc()))

        self.del_arena(self.arena, database, txn)
        self.update_global(database, txn)

    def desc(self):
        return "{{arena='{}'}}".format(self.arena)


# vim:sts=4:ts=4:sw=4:expandtab:
