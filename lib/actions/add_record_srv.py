# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class AddRecord_SRV(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add SRV record {rec}: {reason}"
    PRIORITY_DEFAULT = 10
    WEIGHT_DEFAULT = 1

    def __init__(self, **kwargs):
        super(AddRecord_SRV, self).__init__(**kwargs)
        self.priority = self.optional_data_by_key(kwargs, "priority", int,
                                                  self.PRIORITY_DEFAULT)
        self.weight = self.optional_data_by_key(kwargs, "weight", int,
                                                self.WEIGHT_DEFAULT)

        self.service = self.required_data_by_key(kwargs, "service", str)
        self.port = self.required_data_by_key(kwargs, "port", int)
        self.domain = self.required_data_by_key(kwargs, "domain", str)
        self.ttl = self.optional_data_by_key(kwargs, "ttl", int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = " ".join([str(token) for token in
                            ["SRV", self.priority, self.weight, 
                             self.port, self.domain]])
        self._create_rec(database, txn, self.service, self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if (rlist[3] == "SRV" and
            rlist[6] == str(self.port) and
            rlist[7] == self.domain):
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = ("{{zone='{0}', service='{1}', priority='{2}', weight='{3}', "
               "port='{4}', domain='{5}', ttl='{6}'}}".format(
               self.zone, self.service, self.priority, self.weight,
               self.port, self.domain, self.ttl))
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
