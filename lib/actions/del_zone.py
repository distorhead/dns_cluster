# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate
from lib.common import reorder


__all__ = ['DelZone']


@Action.register_action
class DelZone(Action, Dbstate):
    ERROR_MSG_TEMPLATE = "unable to delete zone {}: {reason}"

    def __init__(self, **kwargs):
        super(DelZone, self).__init__(**kwargs)
        self.zone = self.required_data_by_key(kwargs, 'zone', str)

    def _current_dbstate(self, database, txn):
        zdb = database.dbpool().dns_zone.dbhandle()
        arena_segment = zdb.get(reorder(self.zone), None, txn)
        if arena_segment:
            as_list = arena_segment.split(' ', 1)
            if len(as_list) == 2:
                arena, segment = as_list
                return self.get_segment(arena, segment, database, txn)
            else:
                return None
        else:
            return None

    def _do_apply(self, database, txn):
        zdb = database.dbpool().dns_zone.dbhandle()
        zddb = database.dbpool().dns_data.dbhandle()
        xdb = database.dbpool().dns_xfr.dbhandle()
        szdb = database.dbpool().segment_zone.dbhandle()

        zone_rname = reorder(self.zone)
        if not zdb.exists(zone_rname, txn):
            raise ActionError(self._make_error_msg("zone doesn't exits"))

        if zddb.exists(self.zone, txn) or xdb.exists(self.zone, txn):
            raise ActionError(self._make_error_msg("zone contains records"))

        arena_segment = zdb.get(zone_rname, None, txn)
        bdb_helpers.delete_pair(szdb, arena_segment, self.zone, txn)
        zdb.delete(zone_rname, txn)

        self.del_zone(self.zone, database, txn)

        as_list = arena_segment.split(' ', 1)
        if len(as_list) == 2:
            arena, segment = as_list
            self.update_segment(arena, segment, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(self.desc(), reason=reason)

    def desc(self):
        return "{{zone='{}'}}".format(self.zone)


# vim:sts=4:ts=4:sw=4:expandtab:
