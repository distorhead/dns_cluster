# -*- coding: utf-8 -*-

from lib.operation import OperationError
from lib.operations.session_operation import SessionOperation
from lib.operations.record_operation import RecordOperation
from lib.actions.add_zone import AddZone
from lib.actions.del_zone import DelZone


__all__ = ["AddZoneOp"]


class AddZoneOp(SessionOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        SessionOperation.__init__(self, database_srv, session_srv, **kwargs)
        self._action = AddZone(**kwargs)
        self._records = kwargs.get("initial_records", [])

    def _run_in_session(self, sessid):
        undo_action = DelZone(arena=self._action.arena,
                              segment=self._action.segment,
                              zone=self._action.zone)
        self.session_srv.apply_action(sessid, self._action, undo_action)

        record_actions = []
        for rec_spec in self._records:
            rec_type = rec_spec.get('type', None)
            if rec_type is None:
                raise OperationError("Record type doesn't specified")

            act_cls = RecordOperation.ACTION_BY_REC_TYPE.get(rec_type.lower(), None)
            if act_cls is None:
                raise OperationError("Unknown record type '{}'".format(rec_type))

            assert len(act_cls) == 2, "Wrong record type => actions map"

            rec_spec['zone'] = self._action.zone
            act_do = act_cls[0](**rec_spec)
            act_undo = act_cls[1](**rec_spec)
            self.session_srv.apply_action(sessid, act_do, act_undo)


# vim:sts=4:ts=4:sw=4:expandtab:
