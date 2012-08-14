# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_CNAME(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete CNAME record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_CNAME, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, "host", str)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "CNAME" and rlist[4] == self.domain:
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = "{{zone='{0}', host='{1}', domain='{2}'}}".format(
                self.zone, self.host, self.domain)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
