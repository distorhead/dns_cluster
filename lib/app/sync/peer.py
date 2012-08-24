# -*- coding: utf-8 -*-

from lib.network.sync.sync import SyncFactory
from twisted.internet import reactor, endpoints, defer


#TODO: set self.connection to None when disconnected
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
        self.connection.connectionLost = self._on_disconnect

    def _on_disconnect(self, _):
        self.connection = None

    def connect(self, actions_handler):
        """
        Connect to the peer. Method always returns deferred,
          that fired up upon connection establishmentnnection
          established.
        """
        if self.connection is None:
            d = self.endpoint.connect(SyncFactory(actions_handler))
            d.addCallback(self._on_connect)
            return d
        else:
            return defer.succeed(self)


# vim:sts=4:ts=4:sw=4:expandtab:
