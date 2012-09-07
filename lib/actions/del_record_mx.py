# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ["DelRecord_MX"]


@Action.register_action
class DelRecord_MX(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete record {}: {reason}"

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

    def desc(self):
        return "{{type='MX', zone='{}', domain='{}'}}".format(
                self.zone, self.domain)


# vim:sts=4:ts=4:sw=4:expandtab:
