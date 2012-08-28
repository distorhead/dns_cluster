# -*- coding: utf-8 -*-

from twisted.internet import reactor, endpoints, defer
from twisted.python import log
from lib.network.sync.protocol import SyncClientFactory, SyncServerFactory


class Peer(object):
    class Connection(object):
        def __init__(self, connection, service):
            self.connection = connection
            self.service = service

    @staticmethod
    def listen(interface, port, connectionMade):
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                        interface=interface, port=port)
        ep = endpoints.serverFromString(reactor, endpoint_spec)
        f = SyncServerFactory()
        ep.listen(f)
        f.connectionMade = connectionMade

    def __init__(self, name, **kwargs):
        """
        Create new peer with name 'name'. 'host' and 'port' 
          used for establishing client connection.
        """

        self.name = name
        self.client_host = kwargs.get('client_host', None)
        self.client_port = kwargs.get('client_port', None)
        self.client = None
        self.server = None

    def setup_client_connection(self, connection):
        c = self.Connection(connection, connection.service)
        self.client = c
        self.client.connection.connectionLost = self._on_client_disconnect
        self.client.service.peer = self
        return connection

    def setup_server_connection(self, connection):
        c = self.Connection(connection, connection.service)
        self.server = c
        self.server.connection.connectionLost = self._on_server_disconnect
        self.server.service.peer = self
        return connection

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
            endpoint_spec = "tcp:host={host}:port={port}".format(
                             host=self.client_host, port=self.client_port)
            ep = endpoints.clientFromString(reactor, endpoint_spec)
            d = ep.connect(SyncClientFactory())
            d.addCallback(self.setup_client_connection)
            return d
        else:
            return defer.succeed(self.client.connection)


# vim:sts=4:ts=4:sw=4:expandtab:
