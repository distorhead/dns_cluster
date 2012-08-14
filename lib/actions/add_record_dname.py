# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class AddRecord_DNAME(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to add DNAME record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_DNAME, self).__init__(**kwargs)
        self.zone_dst = self.required_data_by_key(kwargs, "zone_dst", str)
        self.ttl = self.optional_data_by_key(kwargs, "ttl", int, self.TTL_DEFAULT)

    def _do_apply(self, database, txn):
        rec_data = "DNAME " + self.zone_dst
        self._create_rec(database, txn, "@", self.ttl, rec_data, False)

    def _is_record_equal(self, rlist):
        if rlist[3] in ("SOA", "DNAME"):
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = "{{zone='{0}', zone_dst='{1}', ttl='{2}'}}".format(
                self.zone, self.zone_dst, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
