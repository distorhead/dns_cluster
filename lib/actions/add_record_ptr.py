# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.common import required_kwarg
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_PTR(AddRecord):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("zone"):
            raise ActionError("unable to construct action: "
                              "wrong action data: zone required")

        if not data.has_key("host"):
            raise ActionError("unable to construct action: "
                              "wrong action data: host required")

        if not data.has_key("domain"):
            raise ActionError("unable to construct action: "
                              "wrong action data: domain required")

        if not data.has_key("ttl"):
            raise ActionError("unable to construct action: "
                              "wrong action data: ttl required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(data["state"],
                   zone=str(data["zone"]),
                   host=str(data["host"]),
                   ip=str(data["ip"]),
                   ttl=int(data["ttl"]))

    ERROR_MSG_TEMPLATE = "unable to {action} PTR record {arec}: {reason}"

    def __init__(self, state=None, **kwargs):
        super(AddRecord_PTR, self).__init__(state, **kwargs)

        self.host = required_kwarg(kwargs, "host", ActionError)
        self.domain = required_kwarg(kwargs, "domain", ActionError)

    def _apply_do(self, txn):
        rec_data = "PTR " + self.domain
        self._create_rec(txn, self.host, rec_data, True)

    def _apply_undo(self, txn):
        self._delete_rec(txn, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "PTR" and rlist[4] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        arec = "{{zone='{0}', host='{1}', ttl='{2}', domain='{3}'}}".format(
                self.zone, self.host, self.ttl, self.domain)
        return self.ERROR_MSG_TEMPLATE.format(
                    arec=arec,
                    zone=self.zone,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
