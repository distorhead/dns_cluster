# -*- coding: utf-8 -*-

import yaml

from lib.network.user_api.resources.operation_resource import *


__all__ = ['DelZoneResource']


class DelZoneResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena', 'arena')
        kwargs.update(self.optional_fields(request.args, 'segment', 'zone'))
        operation = DelZoneOp(**kwargs)

        d.delCallback(self._del_zone_done, request)
        d.delErrback(self.operation_failure, request)
        d.delErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _del_zone_done(self, _, request):
        log.msg("Deleting zone done")
        self.response(request, 200)


# vim:sts=4:ts=4:sw=4:expandtab:
