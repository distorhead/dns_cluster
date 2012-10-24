# -*- coding: utf-8 -*-

from lib.operation import Operation, OperationError
from lib.operations.operation_helpers import OperationHelpersMixin


class SessionOperation(Operation, OperationHelpersMixin):
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
            self.auth_arena = kwargs.get('auth_arena', None)
            self.auth_key = kwargs.get('auth_key', None)
            if self.auth_arena is None:
                raise OperationError("Unable to construct operation: "
                                     "wrong operation data: "
                                     "either 'sessid' or 'auth_arena' field required")
            elif self.auth_key is None:
                raise OperationError("Unable to construct operation: "
                                     "wrong operataion data: "
                                     "'auth_key' required")

    def _do_run(self, service_provider, **kwargs):
        database_srv = service_provider.get('database')
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        if self.sessid is None:
            self.check_authenticate(database_srv, self.auth_arena, self.auth_key)
            with session_srv.session(self.auth_arena) as sessid:
                session_data = session_srv.get_session_data(sessid)

                try:
                    return self._run_in_session(
                               service_provider,
                               sessid,
                               session_data,
                               **kwargs)
                finally:
                    lock_srv.release_session(sessid)
        else:
            d = session_srv.unset_watchdog(self.sessid)
            session_data = session_srv.get_session_data(self.sessid)

            res = self._run_in_session(
                      service_provider,
                      self.sessid,
                      session_data,
                      **kwargs)

            session_srv.set_watchdog(self.sessid, deferred=d)
            return res

        return res

    def _acquire_lock(self, service_provider, resource, sessid):
        lock_srv = service_provider.get('lock')
        session_srv = service_provider.get('session')

        if not lock_srv.acquire(resource, sessid):
            if not self.sessid is None:
                # this session is started by the user
                lock_srv.release_session(self.sessid)
                session_srv.rollback_session(self.sessid)
            else:
                pass # this is auto session and it will be rolled back on exception

            raise OperationError("unable to acquire lock for resource '{}': "
                                 "session '{}' rolled back".format(resource, sessid))

    def _check_access(self, service_provider, sessid, session_data, action):
        if not self._has_access(service_provider, sessid, session_data, action):
            raise OperationError("access denied")

    def _has_access(self, service_provider, sessid, session_data, action):
        """
        Access checker hook. Reimplement in subclass
            if operation needs special access checking.
        """
        return True

    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        assert 0, "SessionOperation _run_in_session method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
