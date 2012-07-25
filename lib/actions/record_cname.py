# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import AddRecord


@Action.register_action
class AddRecord_CNAME(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} CNAME record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_CNAME, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, "host", str)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _apply_do(self, txn, database):
        rec_data = "CNAME " + self.domain
        self._create_rec(txn, database, self.host, rec_data, True)

    def _apply_undo(self, txn, database):
        self._delete_rec(txn, database, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "CNAME" and rlist[4] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', host='{1}', domain='{2}', ttl='{3}'}}".format(
                self.zone, self.host, self.domain, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


def add_action(**kwargs):
    kwargs["state"] = Action.State.DO
    return AddRecord_CNAME(**kwargs)

def del_action(**kwargs):
    kwargs["state"] = Action.State.UNDO
    return AddRecord_CNAME(**kwargs)


# vim:sts=4:ts=4:sw=4:expandtab:
