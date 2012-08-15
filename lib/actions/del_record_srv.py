# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_SRV(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete SRV record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_SRV, self).__init__(**kwargs)
        self.service = self.required_data_by_key(kwargs, "service", str)
        self.port = self.required_data_by_key(kwargs, "port", int)
        self.domain = self.required_data_by_key(kwargs, "domain", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, self.service, False)

    def _is_record_equal(self, rlist):
        if (rlist[3] == "SRV" and
            rlist[6] == str(self.port) and
            rlist[7] == self.domain):
            return True
        else:
            return False

    def _make_error_msg(self, reason):
        rec = ("{{zone='{0}', service='{1}', port='{2}', domain='{3}'}}".format(
               self.zone, self.service, self.port, self.domain))
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
