# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate
from lib.common import reorder


@Action.register_action
class DelZone(Action, Dbstate):
    ERROR_MSG_TEMPLATE = ("unable to delete zone '{zone}' "
                          "[arena:'{arena}', segment:'{segment}']: {reason}")

    def __init__(self, **kwargs):
        super(DelZone, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _current_dbstate(self, database, txn):
        return self.get_segment(self.arena, self.segment, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        zdb = database.dbpool().dns_zone.open()
        xdb = database.dbpool().dns_xfr.open()
        zddb = database.dbpool().zone_dns_data.open()
        szdb = database.dbpool().segment_zone.open()

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

        adb.close()
        asdb.close()
        zdb.close()
        xdb.close()
        zddb.close()
        szdb.close()

        self.del_zone(self.zone, database, txn)
        self.update_segment(self.arena, self.segment, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena,
                    segment=self.segment,
                    zone=self.zone,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
