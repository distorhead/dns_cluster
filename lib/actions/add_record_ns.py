# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_NS(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} NS record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_NS, self).__init__(**kwargs)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _apply_do(self, txn, database):
        rec_data = "NS " + self.domain
        self._create_rec(txn, database, "@", rec_data, False)

    def _apply_undo(self, txn, database):
        self._delete_rec(txn, database, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "NS" and rlist[4] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', domain='{1}', ttl='{2}'}}".format(
                self.zone, self.domain, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
