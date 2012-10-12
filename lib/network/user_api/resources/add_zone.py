# -*- coding: utf-8 -*-

import yaml

from lib.network.user_api.resources.operation_resource import *


__all__ = ['AddZoneResource']


class AddZoneResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena',
                                          'auth_key', 'arena', 'segment', 'zone')
        return self.add_zone_finalize(kwargs, request)

    @request_handler
    def render_PUT(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena',
                                          'auth_key', 'arena', 'segment', 'zone')

        req = self.parse_content(request.content.read())

        if isinstance(req, list):
            kwargs['initial_records'] = req

        return self.add_zone_finalize(kwargs, request)

    def add_zone_finalize(self, op_kwargs, request):
        operation = AddZoneOp(**op_kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._add_zone_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _add_zone_done(self, _, request):
        log.msg("Adding zone done")
        self.response(request, 200, {'status': 200})


# vim:sts=4:ts=4:sw=4:expandtab:
