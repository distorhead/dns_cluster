# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ["AddRecord_NS"]


@Action.register_action
class AddRecord_NS(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add record {}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_NS, self).__init__(**kwargs)
        self.domain = self.required_data_by_key(kwargs, "domain", str)
        self.ttl = self.optional_data_by_key(kwargs, "ttl", int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = "NS " + self.domain
        self._create_rec(database, txn, "@", self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "NS" and rlist[4] == self.domain:
            return True
        else:
            return False

    def desc(self):
        return "{{type='NS', zone='{}', domain='{}', ttl='{}'}}".format(
                self.zone, self.domain, self.ttl)


# vim:sts=4:ts=4:sw=4:expandtab:
