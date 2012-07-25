# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.common import reorder


@Action.register_action
class AddZone(Action):
    ERROR_MSG_TEMPLATE = ("unable to {action} zone '{zone}' "
                          "[arena:'{arena}', segment:'{segment}']: {reason}")

    def __init__(self, **kwargs):
        super(AddZone, self).__init__(**kwargs)
        self.arena = self.required_data_by_key(kwargs, "arena", str)
        self.segment = self.required_data_by_key(kwargs, "segment", str)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _apply_do(self, txn, database):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        szdb = database.dbpool().segment_zone.open()
        zdb = database.dbpool().dns_zone.open()

        action = "add"

        self._check_arena(adb, txn, action)
        self._check_segment(asdb, txn, action)

        zone_rname = reorder(self.zone)
        sz_key = self.arena + ' ' + self.segment

        if not zdb.exists(zone_rname, txn):
            zdb.put(zone_rname, sz_key, txn)
        else:
            arena_segm = zdb.get(zone_rname, txn)
            raise ActionError(self._make_error_msg(action,
                              "zone already exists in '{0}'".
                              format(arena_segm)))

        zones = bdb_helpers.get_all(szdb, sz_key, txn)
        if not zone_rname in zones:
            szdb.put(sz_key, zone_rname, txn)

        adb.close()
        asdb.close()
        szdb.close()
        zdb.close()

    def _apply_undo(self, txn, database):
        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        zdb = database.dbpool().dns_zone.open()
        xdb = database.dbpool().dns_xfr.open()
        zddb = database.dbpool().zone_dns_data.open()
        szdb = database.dbpool().segment_zone.open()

        action = "delete"

        self._check_arena(adb, txn, action)
        self._check_segment(asdb, txn, action)

        if zddb.exists(self.zone, txn) or xdb.exists(self.zone, txn):
            raise ActionError(self._make_error_msg(action,
                              "zone contains records"))

        zone_rname = reorder(self.zone)

        if zdb.exists(zone_rname, txn):
            zdb.delete(zone_rname, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "zone doesn't exist"))

        bdb_helpers.delete_pair(szdb, self.arena + ' ' + self.segment,
                                zone_rname, txn)

        adb.close()
        asdb.close()
        zdb.close()
        xdb.close()
        zddb.close()
        szdb.close()

    def _check_arena(self, adb, txn, action):
        if not adb.exists(self.arena, txn):
            raise ActionError(self._make_error_msg(action,
                              "arena doesn't exist"))

    def _check_segment(self, asdb, txn, action):
        segments = bdb_helpers.get_all(asdb, self.arena, txn)
        if not self.segment in segments:
            raise ActionError(self._make_error_msg(action,
                              "segment doesn't exist"))

    def _make_error_msg(self, action, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena,
                    segment=self.segment,
                    zone=self.zone,
                    action=action,
                    reason=reason
                )


def add_action(**kwargs):
    kwargs["state"] = Action.State.DO
    return AddZone(**kwargs)

def del_action(**kwargs):
    kwargs["state"] = Action.State.UNDO
    return AddZone(**kwargs)


# vim:sts=4:ts=4:sw=4:expandtab:
