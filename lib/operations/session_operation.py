# -*- coding: utf-8 -*-

from lib.operation import Operation, OperationError


class SessionOperation(Operation):
    """
    Base class for operations that always operates in session.
    Subclasses should implement _run_in_session method.
    """

    def __init__(self, session, **kwargs):
        Operation.__init__(self, **kwargs)
        self._session = session
        self.sessid = self.optional_data_by_key(kwargs, "sessid", int, None)

    def __enter__(self):
        self.sessid = self._session.start_session()
        if self.sessid is None:
            raise OperationError("unable to start session")

        return self.sessid

    def __exit__(self, type, value, traceback):
        if type is None:
            self._session.commit_session(self.sessid)
        else:
            self._session.rollback_session(self.sessid)

    def _do_run(self):
        if self.sessid is None:
            with self as sessid:
                self._run_in_session(sessid)
        else:
            self._run_in_session(self.sessid)

    def _run_in_session(self, sessid):
        assert 0, "SessionOperation _run_in_session method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
