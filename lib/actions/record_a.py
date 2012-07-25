# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import AddRecord


@Action.register_action
class AddRecord_A(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} A record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_A, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, "host", str)
        self.ip = self.required_data_by_key(kwargs, "ip", str)

    def _apply_do(self, txn, database):
        rec_data = "A " + self.ip
        self._create_rec(txn, database, self.host, rec_data, True)

    def _apply_undo(self, txn, database):
        self._delete_rec(txn, database, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "A" and rlist[4] == self.ip:
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', host='{1}', ip='{2}', ttl='{3}'}}".format(
                self.zone, self.host, self.ip, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


def add_action(**kwargs):
    kwargs["state"] = Action.State.DO
    return AddRecord_A(**kwargs)

def del_action(**kwargs):
    kwargs["state"] = Action.State.UNDO
    return AddRecord_A(**kwargs)


# vim:sts=4:ts=4:sw=4:expandtab:
