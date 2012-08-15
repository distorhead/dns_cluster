# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


@Action.register_action
class DelRecord_TXT(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete TXT record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(DelRecord_TXT, self).__init__(**kwargs)
        self.text = self.required_data_by_key(kwargs, "text", str)

    def _do_apply(self, database, txn):
        self._delete_rec(database, txn, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "TXT" and rlist[4] == self._format(self.text):
            return True
        else:
            return False

    def _format(self, text):
        return '"' + text + '"'

    def _make_error_msg(self, reason):
        rec = "{{zone='{0}', text='{1}'}}".format(
                self.zone, self.text)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
