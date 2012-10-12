# -*- coding: utf-8 -*-

from lib.network.user_api.resources.operation_resource import *


__all__ = ['BeginSessionResource']


class BeginSessionResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'auth_arena', 'auth_key')
        operation = SessionBeginOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._begin_session_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _begin_session_done(self, res, request):
        log.msg("Session begin done:", res)
        self.response(request, 200, {'status': 200, 'sessid': res})


# vim:sts=4:ts=4:sw=4:expandtab:
