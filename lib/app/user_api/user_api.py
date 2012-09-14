# -*- coding: utf-8 -*-

from lib import database
from lib import session
from lib import lock
from lib.network.user_api.resources.session import SessionResource
from lib.network.user_api.resources.arenas import ArenasResource

from twisted.application import strports
from twisted.web import server, resource


class UserApiApp(object):
    def __init__(self, interface, port, sp):
        self._interface = interface
        self._port = port
        self._sp = sp

    def make_service(self):
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                         interface=self._interface, port=self._port)

        root = resource.Resource()
        root.putChild('session', SessionResource(self._sp))
        root.putChild('arenas', ArenasResource(self._sp))
        factory = server.Site(root)

        twisted_service = strports.service(endpoint_spec, factory)
        return twisted_service


# vim:sts=4:ts=4:sw=4:expandtab:
