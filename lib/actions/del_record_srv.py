# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['DelRecord_SRV']


@Action.register_action
class DelRecord_SRV(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete record {}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_SRV, self).__init__(**kwargs)
        self.service = self.required_data_by_key(kwargs, 'service', str)
        self.port = self.required_data_by_key(kwargs, 'port', int)
        self.domain = self.required_data_by_key(kwargs, 'domain', str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, self.service, False)

    def _is_record_equal(self, rlist):
        if (rlist[3] == "SRV" and
            rlist[6] == str(self.port) and
            rlist[7] == self.domain):
            return True
        else:
            return False

    def desc(self):
        return ("{{type='SRV', zone='{}', service='{}', port='{}', "
                "domain='{}'}}".format(self.zone, self.service, self.port,
                 self.domain))


# vim:sts=4:ts=4:sw=4:expandtab:
