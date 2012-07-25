# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.add_record import AddRecord


@Action.register_action
class AddRecord_TXT(AddRecord):
    ERROR_MSG_TEMPLATE = "unable to {action} TXT record {rec}: {reason}"

    def __init__(self, **kwargs):
        super(AddRecord_TXT, self).__init__(**kwargs)
        self.text = self.required_data_by_key(kwargs, "text", str)

    def _apply_do(self, txn, database):
        rec_data = "TXT " + self._format(self.text)
        self._create_rec(txn, database, "@", rec_data, False)

    def _apply_undo(self, txn, database):
        self._delete_rec(txn, database, "@", False)

    def _is_record_equal(self, rlist):
        if rlist[3] == "TXT" and rlist[4] == self._format(self.text):
            return True
        else:
            return False

    def _format(self, text):
        return '"' + text + '"'

    def _make_error_msg(self, action, reason):
        rec = "{{zone='{0}', text='{1}', ttl='{2}'}}".format(
                self.zone, self.text, self.ttl)
        return self.ERROR_MSG_TEMPLATE.format(
                    rec=rec,
                    action=action,
                    reason=reason
                )


# vim:sts=4:ts=4:sw=4:expandtab:
