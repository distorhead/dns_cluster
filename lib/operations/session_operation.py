# -*- coding: utf-8 -*-

from lib.operation import Operation, OperationError


class SessionOperation(Operation):
    """
    Base class for operations that always operates in session.
    Subclasses must implement _run_in_session method.
    Subclasses may reimplement methods:
        - _do_lock
        - _do_unlock
        - _has_access
    """

    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.optional_data_by_key(kwargs, 'sessid', int, None)
        if self.sessid is None:
            if not kwargs.has_key('arena'):
                raise OperationError("Unable to construct operation: "
                                     "wrong operation data: "
                                     "either 'sessid' or 'arena' field required")
            self.arena = kwargs['arena']

    def _do_run(self, service_provider, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        with database_srv.transaction() as txn:
            if self.sessid is None:
                with session_srv.session(self.arena, txn=txn) as sessid:
                    session_data = session_srv.get_session_data(sessid, txn=txn)

                    try:
                        return self._run_in_session(
                                   service_provider,
                                   sessid,
                                   session_data,
                                   txn,
                                   **kwargs)
                    finally:
                        print 'releasing lock'
                        lock_srv.release_session(sessid, txn=txn)
                        print 'locks released'
            else:
                d = session_srv.unset_watchdog(self.sessid, txn=txn)
                session_data = session_srv.get_session_data(self.sessid, txn=txn)
                res = self._run_in_session(
                          service_provider,
                          self.sessid,
                          session_data,
                          txn,
                          **kwargs)
                session_srv.set_watchdog(self.sessid, deferred=d, txn=txn)
                return res

            return res

    def _check_access(self, service_provider, sessid, session_data, action, txn):
        if not self._has_access(service_provider, sessid, session_data, action, txn):
            raise OperationError("Access denied")

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        """
        Access checker hook. Reimplement in subclass
            if operation needs special access checking.
        """
        return True

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        assert 0, "SessionOperation _run_in_session method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
