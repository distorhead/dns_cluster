# -*- coding: utf-8 -*-

import yaml

from lib.network.user_api.resources.operation_resource import *


__all__ = ['AddRecordResource']


class AddRecordResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        print "render_GET"
        return self._add_record_handler(request)

    @request_handler
    def render_POST(self, request):
        print "render_POST"
        return self._add_record_handler(request)

    def _add_record_handler(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena')

        rec_spec = self.required_fields(request.args, 'zone', 'type')
        rec_spec.update(self.optional_fields(request.args,
                                             'ttl', 'host', 'ip',
                                             'domain', 'zone_dst', 'priority',
                                             'primary_ns', 'resp_person', 'serial',
                                             'refresh', 'retry', 'expire',
                                             'minimum', 'weight', 'service',
                                             'port', 'text'))

        kwargs['rec_spec'] = rec_spec
        operation = AddRecordOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._add_record_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _add_record_done(self, _, request):
        log.msg("Adding record done")
        self.response(request, 200)


# vim:sts=4:ts=4:sw=4:expandtab:
