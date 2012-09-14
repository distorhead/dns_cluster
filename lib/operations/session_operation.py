# -*- coding: utf-8 -*-

from lib.operation import Operation, OperationError


class SessionOperation(Operation):
    """
    Base class for operations that always operates in session.
    Subclasses must implement _run_in_session method.
    Subclasses may reimplement _do_lock and _do_unlock methods if needed.
    """

    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.optional_data_by_key(kwargs, 'sessid', int, None)

    def _do_run(self, service_provider, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        if self.sessid is None:
            with session_srv.session() as sessid:
                try:
                    res = self._run_in_session(service_provider, sessid, **kwargs)
                except:
                    raise
                finally:
                    lock_srv.release_session(sessid)

                return res
        else:
            if not session_srv.is_valid_session(self.sessid):
                raise OperationError("Session '{}' is not valid".format(self.sessid))

            d = session_srv.unset_watchdog(self.sessid)
            res = self._run_in_session(service_provider, self.sessid, **kwargs)
            session_srv.set_watchdog(self.sessid, deferred=d)
            return res

    def _run_in_session(self, service_provider, sessid, **kwargs):
        assert 0, "SessionOperation _run_in_session method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
