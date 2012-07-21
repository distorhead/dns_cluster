# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.common import reorder, required_kwarg


@Action.register_action
class AddRecord_A(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("zone_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: zone_name required")

        if not data.has_key("host"):
            raise ActionError("unable to construct action: "
                              "wrong action data: host required")

        if not data.has_key("ip"):
            raise ActionError("unable to construct action: "
                              "wrong action data: ip required")

        if not data.has_key("ttl"):
            raise ActionError("unable to construct action: "
                              "wrong action data: ttl required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(data["state"],
                   zone_name=str(data["zone_name"]),
                   host=str(data["host"]),
                   ip=str(data["ip"]),
                   ttl=int(data["ttl"]))

    ERROR_MSG_TEMPLATE = ("unable to {action} A record {arec} "
                          "[zone:'{zone}']: {reason}")

    TTL_DEFAULT = 100

    def __init__(self, state=None, **kwargs):
        super(self.__class__, self).__init__(state)

        self.zone_name = required_kwarg(kwargs, "zone_name", ActionError)
        self.host = required_kwarg(kwargs, "host", ActionError)
        self.ip = required_kwarg(kwargs, "ip", ActionError)
        self.ttl = kwargs.get("ttl", self.TTL_DEFAULT)

    def _apply_do(self, txn):
        zdb = database.context().dbpool().dns_zone.open()
        ddb = database.context().dbpool().dns_data.open()
        xdb = database.context().dbpool().dns_xfr.open()

        action = "add"
        zone_rname = reorder(self.zone_name)

        self._check_zone(zdb, zone_rname, txn, action)

        hosts = bdb_helpers.get_all(xdb, self.zone_name, txn)
        if not self.host in hosts:
            xdb.put(self.zone_name, self.host, txn)

        dkey = self.zone_name + ' ' + self.host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = rec.split(' ')
            if rlist[3] == 'A':
                ip = rlist[4]
                if ip == self.ip:
                    raise ActionError(self._make_error_msg(action,
                                      "record with the same ip exists"))

        seq = database.context().dbpool().sequence.sequence("dns_data")
        newid = seq.get()
        akey = self.zone_name + " " + self.host
        arec = " ".join([str(newid), "@", str(self.ttl), "A", self.ip])
        ddb.put(akey, arec, txn)

        ddb.close()
        zdb.close()
        xdb.close()

    def _apply_undo(self, txn):
        zdb = database.context().dbpool().dns_zone.open()
        ddb = database.context().dbpool().dns_data.open()
        xdb = database.context().dbpool().dns_xfr.open()

        action = "delete"
        zone_rname = reorder(self.zone_name)

        self._check_zone(zdb, zone_rname, txn, action)
        hosts = bdb_helpers.get_all(xdb, self.zone_name, txn)
        if self.host in hosts:
            bdb_helpers.delete_pair(xdb, self.zone_name, self.host, txn)

        found = False
        dkey = self.zone_name + ' ' + self.host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = rec.split(' ')
            if rlist[3] == 'A':
                ip = rlist[4]
                if ip == self.ip:
                    found = True
                    bdb_helpers.delete_pair(ddb, dkey, rec, txn)

        if not found:
            raise ActionError(self._make_error_msg(action,
                              "record doesn't exist"))

        ddb.close()
        zdb.close()
        xdb.close()

    def _check_zone(self, zdb, zone_rname, txn, action):
        if not zdb.exists(zone_rname, txn):
            raise ActionError(self._make_error_msg(action,
                              "zone doesn't exist"))

    def _make_error_msg(self, action, reason):
        arec = "{{host='{0}', ttl='{1}', ip='{2}'}}".format(
                self.host, self.ttl, self.ip)
        return self.ERROR_MSG_TEMPLATE.format(
                    arec=arec,
                    zone=self.zone_name,
                    action=action,
                    reason=reason
                )

# vim:sts=4:ts=4:sw=4:expandtab:
