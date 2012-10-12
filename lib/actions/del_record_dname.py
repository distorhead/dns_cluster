# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ['DelRecord_DNAME']


@Action.register_action
class DelRecord_DNAME(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete record {}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_DNAME, self).__init__(**kwargs)
        self.zone_dst = self.required_data_by_key(kwargs, 'zone_dst', str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] in ("SOA", "DNAME"):
            return True
        else:
            return False

    def desc(self):
        return "{{type='DNAME', zone='{}', zone_dst='{}'}}".format(
                self.zone, self.zone_dst)


# vim:sts=4:ts=4:sw=4:expandtab:
