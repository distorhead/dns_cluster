# -*- coding: utf-8 -*-

from lib.network.sync.protocol import SyncClientFactory
from twisted.internet import reactor, endpoints, defer


class Peer(object):

    class Connection(object):
        def __init__(self, connection, service):
            self.connection = connection
            self.service = service

    def __init__(self, name, host, port):
        """
        Create new peer by name 'name'.
        'host' and 'port' used for establishing client connection.
        """

        self.name = name
        self.host = host
        self.port = port
        self.position = None

        endpoint_spec = "tcp:host={host}:port={port}".format(
                                host=self.host, port=self.port)
        self.endpoint = endpoints.clientFromString(reactor, endpoint_spec)
        self.client = None
        self.server = None

    def setup_client_connection(self, connection)
        c = self.Connection(connection, connection.service)
        self.client = c
        self.client.connection.connectionLost = self._on_client_disconnect
        self.client.service.peer = self

    def setup_server_connection(self, connection):
        c = self.Connection(connection, connection.service)
        self.server = c
        self.server.connection.connectionLost = self._on_server_disconnect
        self.server.service.peer = self

    def _on_client_disconnect(self, _):
        self.client = None

    def _on_server_disconnect(self, _):
        self.server = None

    def connect(self):
        """
        Establish client connection to the peer.
        Method always returns deferred, that fired up when connection established.
        """

        if self.client is None:
            d = self.endpoint.connect(SyncClientFactory())
            d.addCallback(self.setup_client_connection)
            return d
        else:
            return defer.succeed(self)


# vim:sts=4:ts=4:sw=4:expandtab:
