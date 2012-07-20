# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.common import reorder


@Action.register_action
class AddZone(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("segment_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("zone_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]),
                   str(data["segment_name"]),
                   str(data["zone_name"]),
                   data["state"])

    ERROR_MSG_TEMPLATE = ("unable to {action} zone '{zone}' "
                          "[arena:'{arena}', segment:'{segment}']: {reason}")

    def __init__(self, arena_name, segment_name, zone_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name
        self.segment_name = segment_name
        self.zone_name = zone_name

    def _apply_do(self, txn):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()
        szdb = database.context().dbpool().segment_zone.open()
        zdb = database.context().dbpool().dns_zone.open()

        action = "add"

        self._check_arena(adb, txn, action)
        self._check_segment(asdb, txn, action)

        zone_rname = reorder(self.zone_name)
        sz_key = self.arena_name + ' ' + self.segment_name

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

    def _apply_undo(self, txn):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()
        zdb = database.context().dbpool().dns_zone.open()
        xdb = database.context().dbpool().dns_xfr.open()
        zddb = database.context().dbpool().zone_dns_data.open()
        szdb = database.context().dbpool().segment_zone.open()

        action = "delete"

        self._check_arena(adb, txn, action)
        self._check_segment(asdb, txn, action)

        if zddb.exists(self.zone_name, txn) or xdb.exists(self.zone_name, txn):
            raise ActionError(self._make_error_msg(action,
                              "zone contains records"))

        zone_rname = reorder(self.zone_name)

        if zdb.exists(zone_rname, txn):
            zdb.delete(zone_rname, txn)
        else:
            raise ActionError(self._make_error_msg(action,
                              "zone doesn't exist"))

        bdb_helpers.delete_pair(szdb, self.arena_name + ' ' + self.segment_name,
                                zone_rname, txn)

        adb.close()
        asdb.close()
        zdb.close()
        xdb.close()
        zddb.close()
        szdb.close()

    def _check_arena(self, adb, txn, action):
        if not adb.exists(self.arena_name, txn):
            raise ActionError(self._make_error_msg(action,
                              "arena doesn't exist"))

    def _check_segment(self, asdb, txn, action):
        segments = bdb_helpers.get_all(asdb, self.arena_name, txn)
        if not self.segment_name in segments:
            raise ActionError(self._make_error_msg(action,
                              "segment doesn't exist"))

    def _make_error_msg(self, action, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    arena=self.arena_name,
                    segment=self.segment_name,
                    zone=self.zone_name,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
