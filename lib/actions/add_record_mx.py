# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ["AddRecord_MX"]


@Action.register_action
class AddRecord_MX(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add MX record {}: {reason}"
    PRIORITY_DEFAULT = 20

    def __init__(self, **kwargs):
        super(AddRecord_MX, self).__init__(**kwargs)
        self.domain = self.required_data_by_key(kwargs, "domain", str)
        self.priority = self.optional_data_by_key(kwargs, "priority", int,
                                                  self.PRIORITY_DEFAULT)
        self.ttl = self.optional_data_by_key(kwargs, "ttl", int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = " ".join([str(token) for token in
                            ["MX", self.priority, self.domain]])
        self._create_rec(database, txn, "@", self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "MX" and rlist[5] == self.domain:
            return True
        else:
            return False

    def desc(self):
        rec = "{{type='MX', zone='{}', domain='{}', priority='{}', ttl='{}'}}".format(
                self.zone, self.domain, self.priority, self.ttl)


# vim:sts=4:ts=4:sw=4:expandtab:
