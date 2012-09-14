# -*- coding: utf-8 -*-

from twisted.web import server, resource
from twisted.python import log
from lib.network.user_api.resources.operation_resource import *


class ArenasResource(OperationResource):
    isLeaf = True

    @request_handler
    def render_GET(self, request):
        sessid = request.args.get('sessid', None)

        if not sessid is None:
            operation = GetArenasOp(sessid=sessid[0])
        else:
            operation = GetArenasOp()

        d = self.run_operation(operation, request)
        d.addCallback(self._get_arenas_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    @request_handler
    def render_PUT(self, request):
        req = self.parse_content(request.content.read())
        print "Content:\n", req

        sessid = request.args.get('sessid', None)
        arena = self.required_field(req, 'arena')

        if not sessid is None:
            operation = AddArenaOp(arena=arena, sessid=sessid[0])
        else:
            operation = AddArenaOp(arena=arena)

        d = self.run_operation(operation, request)
        d.addCallback(self._add_arena_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET

    @request_handler
    def render_DELETE(self, request):
        sessid = request.args.get('sessid', None)

        if len(request.postpath) != 1:
            raise RequestError("Wrong resource locator '{}': "
                               "/arenas/<arena> required".format(request.path))
        arena = request.postpath[0]

        if not sessid is None:
            operation = DelArenaOp(arena=arena, sessid=sessid[0])
        else:
            operation = DelArenaOp(arena=arena)

        d = self.run_operation(operation, request)
        d.addCallback(self._del_arena_done, request)
        d.addErrback(self.operation_failure, request)
        d.addErrback(self.unknown_failure, request)

        return server.NOT_DONE_YET


    def _get_arenas_done(self, res, request):
        log.msg("Getting arenas done:", res)
        self.response(request, 200, res)

    def _add_arena_done(self, _, request):
        log.msg("Adding arena done")
        self.response(request, 200)

    def _del_arena_done(self, _, request):
        log.msg("Deleting arena done")
        self.response(request, 200)


# vim:sts=4:ts=4:sw=4:expandtab:
