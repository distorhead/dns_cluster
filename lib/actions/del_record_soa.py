# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_SOA(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete SOA record for zone '{zone}': {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_SOA, self).__init__(**kwargs)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "SOA":
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        return self.ERROR_MSG_TEMPLATE.format(
                    zone=self.zone,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
