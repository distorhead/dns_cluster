# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['AddRecord_TXT']


@Action.register_action
class AddRecord_TXT(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add record {}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_TXT, self).__init__(**kwargs)
        self.text = self.required_data_by_key(kwargs, 'text', str)
        self.ttl = self.optional_data_by_key(kwargs, 'ttl', int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = "TXT " + self._format(self.text)
        self._create_rec(database, txn, "@", self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "TXT" and rlist[4] == self._format(self.text):
            return True
        else:
            return False

    def _format(self, text):
        return '"' + text + '"'

    def desc(self):
        return "{{type='TXT', zone='{0}', text='{1}', ttl='{2}'}}".format(
                self.zone, self.text, self.ttl)


# vim:sts=4:ts=4:sw=4:expandtab:
