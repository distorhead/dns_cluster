# -*- coding: utf-8 -*-

import threading

from twisted.internet import reactor, defer
from twisted.python import log

from lib.operation import Operation, OperationError
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.twisted_helpers import asynchronous2, threaded


class SessionOperation(Operation, OperationHelpersMixin):
    """
    Base class for operations that always operates in session.
    Subclasses must implement _run_in_session method.
    Subclasses may reimplement methods:
        - _do_lock
        - _do_unlock
        - _has_access
    """

    LOCK_MAX_ATTEMPTS = 2
    # seconds
    LOCK_REPEAT_PERIOD = 10.0

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

    @asynchronous2
    def _do_run(self, op_result_defer, service_provider, **kwargs):
        """
        op_result_defer -- deferred that should be called on the
            finish of operation with the operation result.
        Any failure exceptions in any operation stages results in calling
            op_result_defer.errback.
        """

        log.msg("_do_run")

        if self.sessid is None:
            d = self._check_authenticate(service_provider)
            d.addCallback(self._check_authenticate_done, op_result_defer,
                              service_provider, **kwargs)
            d.addErrback(op_result_defer.errback)
        else:
            d = self._get_session_data(self.sessid, service_provider)
            d.addCallback(self._get_session_data_done, self.sessid, op_result_defer,
                              service_provider, **kwargs)
            d.addErrback(op_result_defer.errback)


    @threaded
    def _check_authenticate(self, service_provider):
        log.msg("_check_authenticate")
        database_srv = service_provider.get('database')
        # method raises exception on authentication failure
        # method transactional
        return self.check_authenticate(database_srv, self.auth_arena, self.auth_key)

    def _check_authenticate_done(self, _, op_result_defer, service_provider, **kwargs):
        log.msg("_check_authenticate_done")
        d = self._begin_session(service_provider)
        d.addCallback(self._begin_session_done, op_result_defer,
                          service_provider, **kwargs)
        d.addErrback(op_result_defer.errback)


    @threaded
    def _begin_session(self, service_provider):
        log.msg("_begin_session")
        session_srv = service_provider.get('session')
        return session_srv.begin_session(self.auth_arena)

    @threaded
    def _get_session_data(self, sessid, service_provider):
        log.msg("_get_session_data")
        session_srv = service_provider.get('session')
        return session_srv.get_session_data(sessid)

    @threaded
    def _commit_session(self, sessid, service_provider):
        log.msg("_commit_session")
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        session_srv.commit_session(sessid)
        lock_srv.release_session(sessid)

    @threaded
    def _rollback_session(self, sessid, service_provider):
        log.msg("_rollback_session")
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        session_srv.rollback_session(sessid)
        lock_srv.release_session(sessid)

    def _begin_session_done(self, sessid, op_result_defer, service_provider, **kwargs):
        log.msg("_begin_session_done => {}".format(sessid))
        d = self._get_session_data(sessid, service_provider)
        d.addCallback(self._get_session_data_done, sessid, op_result_defer,
                          service_provider, **kwargs)

        # rollback temporary session and continue error chain
        d.addErrback(lambda x: (self._rollback_session(sessid, service_provider), x)[1])

        d.addErrback(op_result_defer.errback)

    def _get_session_data_done(self, session_data, sessid, op_result_defer,
                                   service_provider, **kwargs):
        log.msg("_get_session_data_done")
        d = self._run_in_session(service_provider, sessid, session_data, **kwargs)
        d.addCallback(self._run_in_session_done, session_data, sessid,
                          op_result_defer, service_provider)

        if self.sessid is None:
            d.addErrback(lambda x: (self._rollback_session(sessid, service_provider), x)[1])

        d.addErrback(op_result_defer.errback)

    def _run_in_session_done(self, op_result, session_data, sessid,
                                 op_result_defer, service_provider):
        log.msg("_run_in_session_done")
        if self.sessid is None:
            d = self._commit_session(sessid, service_provider)
            d.addCallback(lambda _: op_result_defer.callback(op_result))
            d.addErrback(lambda x: (self._rollback_session(sessid, service_provider), x)[1])
            d.addErrback(op_result_defer.errback)
        else:
            op_result_defer.callback(op_result)


    def _run_in_session(self, service_provider, sessid, session_data, **kwargs):
        """
        This method will be called in the reactor thread.
        Method should return deferred fired up in the end of operation with result.
        """

        assert 0, "{}._run_in_session method is not implemented".format(
                      self.__class__.__name__)

    def _lock_stage(self, service_provider, resource, sessid):
        """
        May be called by derived class from reactor thread to acquire lock.
        Returns deferred fired up in reactor thread on successful acquire.
        Raises exception if unable to lock.
        """

        log.msg("_lock_stage")

        lock_srv = service_provider.get('lock')

        lock_acqure_defer = defer.Deferred()
        self._start_acquire_lock_request(lock_srv, resource, sessid, lock_acqure_defer)
        return lock_acqure_defer

    def _start_acquire_lock_request(self, lock_srv, resource, sessid,
                                        lock_acqure_defer, attempt=0):
        d = self._try_acqure_lock(lock_srv, resource, sessid,
                                      lock_acqure_defer, attempt)
        # try acquire failure causes acquire failure
        d.addErrback(lock_acqure_defer.errback)

    @threaded
    def _try_acqure_lock(self, lock_srv, resource, sessid, lock_acqure_defer, attempt):
        """
        Check lock may be acquired and acquire.
        """
        log.msg("_try_acqure_lock")

        if lock_srv.try_acquire(resource, sessid):
            log.msg("_try_acqure_lock => lock acquired")
            lock_srv.unmark_acquire_wait(resource, sessid)

            reactor.callFromThread(lock_acqure_defer.callback, None)

        elif attempt >= self.LOCK_MAX_ATTEMPTS:
            log.msg("_try_acqure_lock => max attempts")
            lock_srv.unmark_acquire_wait(resource, sessid)

            if not self.sessid is None:
                # this session is started by the user
                lock_srv.release_session(self.sessid)
                session_srv.rollback_session(self.sessid)
            else:
                # this is auto session and it will be
                #   rolled back on the following exception
                pass

            raise OperationError("unable to acquire lock for resource '{}': "
                                 "session '{}' rolled back".format(resource, sessid))

        else:
            log.msg("_try_acqure_lock => call later")
            lock_srv.mark_acquire_wait(resource, sessid)

            callID = reactor.callLater(self.LOCK_REPEAT_PERIOD,
                                       self._start_acquire_lock_request,
                                       lock_srv,
                                       resource,
                                       sessid,
                                       lock_acqure_defer,
                                       attempt + 1)


    def _check_access(self, service_provider, sessid, session_data, action):
        if not self._has_access(service_provider, sessid, session_data, action):
            raise OperationError("access denied")

    def _has_access(self, service_provider, sessid, session_data, action):
        """
        Access checker hook. Reimplement in subclass
            if operation needs special access checking.
        """
        return True


# vim:sts=4:ts=4:sw=4:expandtab:
