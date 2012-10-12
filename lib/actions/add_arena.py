# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


__all__ = ['AddArena']


@Action.register_action
class AddArena(Action, Dbstate):
    def __init__(self, **kwargs):
        super(AddArena, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, 'arena', str)
        self.key = self.required_data_by_key(kwargs, 'key', str)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.dbhandle()
        aadb = database.dbpool().arena_auth.dbhandle()

        if not adb.exists(self.arena, txn):
            adb.put(self.arena, '', txn)
            aadb.put(self.arena, self.key, txn)
        else:
            raise ActionError("unable to add arena {}: "
                              "arena already exists".format(self.desc()))

        self.update_arena(self.arena, database, txn)

    def desc(self):
        return "{{arena='{}'}}".format(self.arena)


# vim:sts=4:ts=4:sw=4:expandtab:
