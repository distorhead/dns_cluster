# -*- coding: utf-8 -*-

from lib.network.user_api.resources.operation_resource import *


__all__ = ['AddSegmentResource']


class AddSegmentResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena',
                                          'auth_key', 'arena', 'segment')
        operation = AddSegmentOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._add_segment_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _add_segment_done(self, _, request):
        log.msg("Adding segment done")
        self.response(request, 200, {'status': 200})


# vim:sts=4:ts=4:sw=4:expandtab:
