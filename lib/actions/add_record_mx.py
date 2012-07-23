# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_MX(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} MX record {rec}: {reason}"
    PRIORITY_DEFAULT = 20

    def __init__(self, **kwargs):
        super(AddRecord_MX, self).__init__(**kwargs)
        self.domain = self.required_data_by_key(kwargs, "domain", str)
        self.priority = self.optional_data_by_key(kwargs, "priority", int,
                                                  self.PRIORITY_DEFAULT)

    def _apply_do(self, txn):
        rec_data = " ".join([str(token) for token in
                            ["MX", self.priority, self.domain]])
        self._create_rec(txn, "@", rec_data, False)

    def _apply_undo(self, txn):
        self._delete_rec(txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "MX" and rlist[5] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', domain='{1}', priority='{2}', ttl='{3}'}}".format(
                self.zone, self.domain, self.priority, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
