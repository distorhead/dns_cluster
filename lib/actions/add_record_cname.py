# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['AddRecord_CNAME']


@Action.register_action
class AddRecord_CNAME(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add record {}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_CNAME, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, 'host', str)
        self.domain = self.required_data_by_key(kwargs, 'domain', str)
        self.ttl = self.optional_data_by_key(kwargs, 'ttl', int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = "CNAME " + self.domain
        self._create_rec(database, txn, self.host, self.ttl, rec_data, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "CNAME" and rlist[4] == self.domain:
            return True
        else:
            return False

    def desc(self):
        return "{{type='CNAME', zone='{}', host='{}', domain='{}', ttl='{}'}}".format(
                self.zone, self.host, self.domain, self.ttl)


# vim:sts=4:ts=4:sw=4:expandtab:
