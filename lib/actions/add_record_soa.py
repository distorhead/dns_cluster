# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['AddRecord_SOA']


@Action.register_action
class AddRecord_SOA(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add record {}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_SOA, self).__init__(**kwargs)
        self.primary_ns = self.required_data_by_key(kwargs, 'primary_ns', str)
        self.resp_person = self.required_data_by_key(kwargs, 'resp_person', str)
        self.serial = self.required_data_by_key(kwargs, 'serial', int)
        self.refresh = self.required_data_by_key(kwargs, 'refresh', int)
        self.retry = self.required_data_by_key(kwargs, 'retry', int)
        self.expire = self.required_data_by_key(kwargs, 'expire', int)
        self.minimum = self.required_data_by_key(kwargs, 'minimum', int)
        self.ttl = self.optional_data_by_key(kwargs, 'ttl', int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = " ".join([str(token) for token in
                                ["SOA", self.primary_ns, self.resp_person,
                                 self.serial, self.refresh, self.retry,
                                 self.expire, self.minimum]])
        self._create_rec(database, txn, "@", self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "SOA":
            return True
        else:
            return False

    def desc(self):
        return ("{{type='SOA', zone='{}', primary_ns='{}', resp_person='{}', "
                "serial='{}', refresh='{}', retry='{}', expire='{}', "
                "minimum='{}', ttl='{}'}}".format(self.zone, self.primary_ns,
                self.resp_person, self.serial, self.refresh, self.retry, self.expire,
                self.minimum, self.ttl))


# vim:sts=4:ts=4:sw=4:expandtab:
