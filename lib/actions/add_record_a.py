# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['AddRecord_A']


@Action.register_action
class AddRecord_A(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add record {}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_A, self).__init__(**kwargs)
        self.host = self.required_data_by_key(kwargs, 'host', str)
        self.ip = self.required_data_by_key(kwargs, 'ip', str)
        self.ttl = self.optional_data_by_key(kwargs, 'ttl', int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = "A " + self.ip
        self._create_rec(database, txn, self.host, self.ttl, rec_data, True)

    def _is_record_equal(self, rlist):
        if rlist[3] == "A" and rlist[4] == self.ip:
            return True
        else:
            return False

    def desc(self):
        return "{{type='A', zone='{}', host='{}', ip='{}', ttl='{}'}}".format(
                self.zone, self.host, self.ip, self.ttl)


# vim:sts=4:ts=4:sw=4:expandtab:
