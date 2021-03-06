# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate


__all__ = ['DelSegment']


@Action.register_action
class DelSegment(Action, Dbstate):
    ERROR_MSG_TEMPLATE = "unable to delete segment {}: {reason}"

    def __init__(self, **kwargs):
        super(DelSegment, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, 'arena', str)
        self.segment = self.required_data_by_key(kwargs, 'segment', str)

    def _current_dbstate(self, database, txn):
        return self.get_arena(self.arena, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.dbhandle()
        asdb = database.dbpool().arena_segment.dbhandle()
        szdb = database.dbpool().segment_zone.dbhandle()

        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg("arena doesn't exist"))

        if szdb.exists(self.arena + ' ' + self.segment, txn):
            raise ActionError(self._make_error_msg("segment contains zones"))

        if bdb_helpers.pair_exists(asdb, self.arena, self.segment, txn):
            bdb_helpers.delete_pair(asdb, self.arena, self.segment, txn)
        else:
            raise ActionError(self._make_error_msg("segment doesn't exist"))

        self.del_segment(self.arena, self.segment, database, txn)
        self.update_arena(self.arena, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(self.desc(), reason=reason)

    def desc(self):
        return "{{arena='{}', segment='{}'}}".format(self.arena, self.segment)


# vim:sts=4:ts=4:sw=4:expandtab:
