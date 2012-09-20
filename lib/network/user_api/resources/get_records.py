# -*- coding: utf-8 -*-

from lib.network.user_api.resources.operation_resource import *


__all__ = ['GetRecordsResource']


class GetRecordsResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        print "render_GET"
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena')
        kwargs.update(self.optional_fields(request.args, 'zone'))

        operation = GetRecordsOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._get_records_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _get_records_done(self, res, request):
        log.msg("Getting records done:", res)
        self.response(request, 200, res)


# vim:sts=4:ts=4:sw=4:expandtab:
