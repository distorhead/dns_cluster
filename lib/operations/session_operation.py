# -*- coding: utf-8 -*-

from lib.operation import Operation, OperationError


class SessionOperation(Operation):
    """
    Base class for operations that always operates in session.
    Subclasses should implement _run_in_session method.
    """

    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.optional_data_by_key(kwargs, "sessid", int, None)

    def _do_run(self, database, session):
        if self.sessid is None:
            with session.session() as sessid:
                self._run_in_session(database, session, sessid)
        else:
            self._run_in_session(database, session, self.sessid)

    def _run_in_session(self, database, session, sessid):
        assert 0, "SessionOperation _run_in_session method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
