# -*- coding: utf-8 -*-

from twisted.application import strports
from twisted.web import server, resource


class SimpleResource(resource.Resource):
    isLeaf = True
    def render_GET(self, request):
        return "<html>Hello, world!</html>"


class UserApiApp(object):
    def make_service(self):
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                         interface='127.0.0.1', port=2100)
        factory = server.Site(SimpleResource())
        twisted_service = strports.service(endpoint_spec, factory)
        return twisted_service


# vim:sts=4:ts=4:sw=4:expandtab:
