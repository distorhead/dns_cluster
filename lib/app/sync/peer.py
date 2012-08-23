# -*- coding: utf-8 -*-

from lib.network.sync.sync import SyncFactory
from twisted.internet import reactor, endpoints


class Peer(object):
    def __init__(self, name, host, port):
        self.name = name
        self.host = host
        self.port = port

        endpoint_spec = "tcp:host={host}:port={port}".format(
                                host=self.host, port=self.port)
        self.endpoint = endpoints.clientFromString(reactor, endpoint_spec)
        self.connection = None

    def _on_connect(self, connection):
        self.connection = connection
        self.connection.peer = self

    def connect(self, actions_handler):
        d = self.endpoint.connect(SyncFactory(actions_handler))
        d.addCallback(self._on_connect)
        return d


# vim:sts=4:ts=4:sw=4:expandtab:
