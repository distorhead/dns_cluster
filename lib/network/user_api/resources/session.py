# -*- coding: utf-8 -*-

from twisted.web import server, resource
from twisted.python import log
from lib.network.user_api.resources.operation_resource import *


class SessionResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_POST(self, request):
        cmd = self.required_field(request.args, 'cmd')

        if cmd[0] == 'begin':
            log.msg("Session begin request")
            operation = self._make_session_begin(request)
            d = self.run_operation(operation, request)
            d.addCallback(self._session_begin_done, request)
            d.addErrback(self.operation_failure, request)
            d.addErrback(self.unknown_failure, request)
        elif cmd[0] == 'commit':
            log.msg("Session commit request")
            operation = self._make_session_commit(request)
            d = self.run_operation(operation, request)
            d.addCallback(self._session_commit_done, request)
            d.addErrback(self.operation_failure, request)
            d.addErrback(self.unknown_failure, request)
        elif cmd[0] == 'rollback':
            log.msg("Session rollback request")
            operation = self._make_session_rollback(request)
            d = self.run_operation(operation, request)
            d.addCallback(self._session_rollback_done, request)
            d.addErrback(self.operation_failure, request)
            d.addErrback(self.unknown_failure, request)
        elif cmd[0] == 'keepalive':
            log.msg("Session keepalive request")
            operation = self._make_session_keepalive(request)
            d = self.run_operation(operation, request)
            d.addCallback(self._session_keepalive_done, request)
            d.addErrback(self.operation_failure, request)
            d.addErrback(self.unknown_failure, request)
        else:
            raise RequestError("Unknown cmd '{}'".format(cmd))

        return server.NOT_DONE_YET


    def _make_session_begin(self, request):
        return SessionBeginOp()

    def _session_begin_done(self, res, request):
        log.msg("Session begin done:", res)
        resp = {"sessid": res}
        self.response(request, 200, resp)


    def _make_session_commit(self, request):
        sessid = self.required_field(request.args, 'sessid')[0]
        return SessionCommitOp(sessid=sessid)

    def _session_commit_done(self, _, request):
        log.msg("Session commit done")
        self.response(request, 200)


    def _make_session_rollback(self, request):
        sessid = self.required_field(request.args, 'sessid')[0]
        return SessionRollbackOp(sessid=sessid)

    def _session_rollback_done(self, _, request):
        log.msg("Session rollback done")
        self.response(request, 200)


    def _make_session_keepalive(self, request):
        sessid = self.required_field(request.args, 'sessid')[0]
        return SessionKeepaliveOp(sessid=sessid)

    def _session_keepalive_done(self, _, request):
        log.msg("Session keepalive done")
        self.response(request, 200)


# vim:sts=4:ts=4:sw=4:expandtab:
