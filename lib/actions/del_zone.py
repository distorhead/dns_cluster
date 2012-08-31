# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate
from lib.common import reorder


__all__ = ["DelZone"]


@Action.register_action
class DelZone(Action, Dbstate):
    ERROR_MSG_TEMPLATE = "unable to delete zone {}: {reason}"

    def __init__(self, **kwargs):
        super(DelZone, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _current_dbstate(self, database, txn):
        return self.get_segment(self.arena, self.segment, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.dbhandle()
        asdb = database.dbpool().arena_segment.dbhandle()
        zdb = database.dbpool().dns_zone.dbhandle()
        xdb = database.dbpool().dns_xfr.dbhandle()
        zddb = database.dbpool().zone_dns_data.dbhandle()
        szdb = database.dbpool().segment_zone.dbhandle()

        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg("arena doesn't exist"))

        segments = bdb_helpers.get_all(asdb, self.arena, txn)
        if not self.segment in segments:
            raise ActionError(self._make_error_msg("segment doesn't exist"))

        if zddb.exists(self.zone, txn) or xdb.exists(self.zone, txn):
            raise ActionError(self._make_error_msg("zone contains records"))

        zone_rname = reorder(self.zone)

        if zdb.exists(zone_rname, txn):
            zdb.delete(zone_rname, txn)
        else:
            raise ActionError(self._make_error_msg("zone doesn't exist"))

        bdb_helpers.delete_pair(szdb, self.arena + ' ' + self.segment,
                                self.zone, txn)

        self.del_zone(self.zone, database, txn)
        self.update_segment(self.arena, self.segment, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(self.desc(), reason=reason)

    def desc(self):
        return "{{arena='{}', segment='{}', zone='{}'}}".format(
                self.arena, self.segment, self.zone)


# vim:sts=4:ts=4:sw=4:expandtab:
