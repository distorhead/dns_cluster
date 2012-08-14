# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_MX(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete MX record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_MX, self).__init__(**kwargs)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "MX" and rlist[5] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = "{{zone='{0}', domain='{1}'}}".format(
                self.zone, self.domain)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
