# -*- coding: utf-8 -*-

from lib.network.user_api.resources.operation_resource import *


__all__ = ['CommitSessionResource']


class CommitSessionResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'sessid')
        operation = SessionCommitOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._commit_session_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _commit_session_done(self, _, request):
        log.msg("Session commit done")
        self.response(request, 200)


# vim:sts=4:ts=4:sw=4:expandtab:
