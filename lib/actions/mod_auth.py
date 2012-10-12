# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError


__all__ = ['ModAuth']


@Action.register_action
class ModAuth(Action):
    def __init__(self, **kwargs):
        super(ModAuth, self).__init__(**kwargs)
        self.target = self.required_data_by_key(kwargs, 'target', str)
        self.key = self.required_data_by_key(kwargs, 'key', str)

    def _do_apply(self, database, txn):
        aadb = database.dbpool().arena_auth.dbhandle()
        aadb.put(self.target, self.key, txn)

    def desc(self):
        return "{{target='{}'}}".format(self.target)


# vim:sts=4:ts=4:sw=4:expandtab:
