# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate
from lib.common import reorder


@Action.register_action
class AddZone(Action, Dbstate):
    ERROR_MSG_TEMPLATE = ("unable to add zone '{zone}' "
                          "[arena:'{arena}', segment:'{segment}']: {reason}")

    def __init__(self, **kwargs):
        super(AddZone, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _current_dbstate(self, database, txn):
        return self.get_segment(self.arena, self.segment, database, txn)

    def _do_apply(self, database, txn):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        szdb = database.dbpool().segment_zone.open()
        zdb = database.dbpool().dns_zone.open()

        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg("arena doesn't exist"))

        segments = bdb_helpers.get_all(asdb, self.arena, txn)
        if not self.segment in segments:
            raise ActionError(self._make_error_msg("segment doesn't exist"))

        zone_rname = reorder(self.zone)
        sz_key = self.arena + ' ' + self.segment

        if not zdb.exists(zone_rname, txn):
            zdb.put(zone_rname, sz_key, txn)
        else:
            arena_segm = zdb.get(zone_rname, txn)
            raise ActionError(self._make_error_msg("zone already exists in '{0}'".format(
                              arena_segm)))

        zones = bdb_helpers.get_all(szdb, sz_key, txn)
        if not self.zone in zones:
            szdb.put(sz_key, self.zone, txn)

        adb.close()
        asdb.close()
        szdb.close()
        zdb.close()

        self.update_zone(self.zone, database, txn)

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena,
                    segment=self.segment,
                    zone=self.zone,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
