# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_DNAME(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} DNAME record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_DNAME, self).__init__(**kwargs)
        self.zone_dst = self.required_data_by_key(kwargs, "zone_dst", str)

    def _apply_do(self, txn):
        rec_data = "DNAME " + self.zone_dst
        self._create_rec(txn, "@", rec_data, False)

    def _apply_undo(self, txn):
        self._delete_rec(txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] in ("SOA", "DNAME"):
            return True
        else:
            return False

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', zone_dst='{1}', ttl='{2}'}}".format(
                self.zone, self.zone_dst, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
