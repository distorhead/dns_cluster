# -*- coding: utf-8 -*-

from lib.network.user_api.resources.operation_resource import *


__all__ = ['GetZonesResource']


class GetZonesResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena',
                                          'auth_key' 'arena', 'segment')
        operation = GetZonesOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._get_zones_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _get_zones_done(self, res, request):
        log.msg("Getting all zones done:", res)
        self.response(request, 200, {'status': 200, 'data': res})


# vim:sts=4:ts=4:sw=4:expandtab:
