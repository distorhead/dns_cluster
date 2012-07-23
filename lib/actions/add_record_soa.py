# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_SOA(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} SOA record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_SOA, self).__init__(**kwargs)
        self.primary_ns = self.required_data_by_key(kwargs, "primary_ns", str)
        self.resp_person = self.required_data_by_key(kwargs, "resp_person", str)
        self.serial = self.required_data_by_key(kwargs, "serial", int)
        self.refresh = self.required_data_by_key(kwargs, "refresh", int)
        self.retry = self.required_data_by_key(kwargs, "retry", int)
        self.expire = self.required_data_by_key(kwargs, "expire", int)
        self.minimum = self.required_data_by_key(kwargs, "minimum", int)

    def _apply_do(self, txn):
        rec_data = " ".join([str(token) for token in
                                ["SOA", self.primary_ns, self.resp_person,
                                 self.serial, self.refresh, self.retry,
                                 self.expire, self.minimum]])
        self._create_rec(txn, "@", rec_data, False)

    def _apply_undo(self, txn):
        self._delete_rec(txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "SOA":
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        arec = ("{{zone='{0}', primary_ns='{1}', resp_person='{2}', "
                "serial='{3}', refresh='{4}', retry='{5}', expire='{6}', "
                "minimum='{7}', ttl='{8}'}}".format(self.zone, self.primary_ns,
                self.resp_person, self.serial, self.refresh, self.retry, self.expire,
                self.minimum, self.ttl))
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
