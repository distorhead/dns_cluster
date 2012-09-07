# -*- coding: utf-8 -*-

from lib.operations.record_operation import RecordOperation
from lib.operations.session_operation import SessionOperation


__all__ = ["DelRecordOp"]


class DelRecordOp(SessionOperation, RecordOperation):
    def __init__(self, database, session, **kwargs):
        SessionOperation.__init__(self, database, session, **kwargs)
        rec_spec = self.required_data_by_key(kwargs, "rec_spec", dict)
        self._rec_type = self.required_data_by_key(rec_spec, "type", str)
        self._action = self.make_del_record(self._rec_type, rec_spec)

    def _run_in_session(self, sessid):
        with self.database_srv.transaction() as txn:
            undo_action = self.del_to_add_record(self.database_srv,
                                                 self._rec_type,
                                                 self._action, txn)
            self.session_srv.apply_action(sessid, self._action, undo_action, txn=txn)


# vim:sts=4:ts=4:sw=4:expandtab:
