# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_A(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete A record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_A, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, "host", str)
        self.ip = self.required_data_by_key(kwargs, "ip", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "A" and rlist[4] == self.ip:
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = "{{zone='{0}', host='{1}', ip='{2}'}}".format(
                self.zone, self.host, self.ip)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
