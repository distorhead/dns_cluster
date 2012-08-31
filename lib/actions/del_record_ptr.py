# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ["DelRecord_PTR"]


@Action.register_action
class DelRecord_PTR(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete record {}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_PTR, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, "host", str)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, self.host, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "PTR" and rlist[4] == self.domain:
            return True
        else:
            return False

    def desc(self):
        return "{{type='PTR', zone='{}', host='{}', domain='{}'}}".format(
                self.zone, self.host, self.domain)


# vim:sts=4:ts=4:sw=4:expandtab:
