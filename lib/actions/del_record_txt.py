# -*- coding: utf-8 -*-

from lib.action import Action, ActionError
from lib.actions.record import RecordAction


__all__ = ["DelRecord_TXT"]


@Action.register_action
class DelRecord_TXT(RecordAction):
    ERROR_MSG_TEMPLATE = "unable to delete record {}: {reason}"

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

    def desc(self):
        return "{{type='TXT', zone='{}', text='{}'}}".format(
                self.zone, self.text)


# vim:sts=4:ts=4:sw=4:expandtab:
