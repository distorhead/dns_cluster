# -*- coding: utf-8 -*-

import yaml

from lib.network.user_api.resources.operation_resource import *


__all__ = ['DelRecordResource']


class DelRecordResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        return self._del_record_handler(request)

    @request_handler
    def render_POST(self, request):
        return self._del_record_handler(request)

    def _del_record_handler(self, request):
        kwargs = self.optional_fields(request.args, 'sessid', 'auth_arena', 'auth_key')

        rec_spec = self.optional_fields(request.args, 'zone', 'type')
        rec_spec.update(self.optional_fields(request.args,
                                             'host', 'ip', 'domain', 'zone_dst',
                                             'primary_ns', 'resp_person', 'serial',
                                             'refresh', 'retry', 'expire',
                                             'minimum', 'service', 'port', 'text'))

        kwargs['rec_spec'] = rec_spec
        operation = DelRecordOp(**kwargs)

        d = self.run_operation(operation, request)
        d.addCallback(self._del_record_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    def _del_record_done(self, _, request):
        log.msg("Deleting record done")
        self.response(request, 200, {'status': 200})


# vim:sts=4:ts=4:sw=4:expandtab:
