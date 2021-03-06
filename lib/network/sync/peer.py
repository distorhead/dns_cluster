# -*- coding: utf-8 -*-

from twisted.internet import reactor, endpoints, defer
from twisted.application import strports
from twisted.python import log
from lib.network.sync.protocol import SyncClientFactory, SyncServerFactory


class Peer(object):
    class Connection(object):
        def __init__(self, connection, service):
            self.connection = connection
            self.service = service

    @staticmethod
    def make_service(endpoint_data, connectionMade):
        if endpoint_data['transport-encrypt']:
            endpoint_spec = ("ssl:interface={interface}:port={port}:"
                             "privateKey={pkey}:certKey={cert}".format(
                                interface=endpoint_data['interface'],
                                port=endpoint_data['port'],
                                pkey=endpoint_data['private-key'],
                                cert=endpoint_data['cert']))
        else:
            endpoint_spec = "tcp:interface={interface}:port={port}".format(
                             interface=endpoint_data['interface'],
                             port=endpoint_data['port'])

        f = SyncServerFactory()
        f.connectionMade = connectionMade
        twisted_service = strports.service(endpoint_spec, f)
        return twisted_service

    def __init__(self, name, key, **kwargs):
        """
        Create new peer with name 'name'. 'host' and 'port' 
          used for establishing client connection.
        """

        self.name = name
        self.key = key
        self.client_host = kwargs.get('client_host', None)
        self.client_port = kwargs.get('client_port', None)
        self.client_transport_encrypt = kwargs.get('client_transport_encrypt', False)
        self.client_auth_schema = kwargs.get('client_auth_schema', "chap")
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
        log.msg("Client connection with peer '{}' lost".format(self.name))
        self.client = None

    def _on_server_disconnect(self, _):
        log.msg("Server connection with peer '{}' lost".format(self.name))
        self.server = None

    def connect(self):
        """
        Establish client connection to the peer.
        Method always returns deferred, that fired up when connection established.
        """

        if self.client is None:
            if self.client_transport_encrypt:
                endpoint_spec = "ssl:host={host}:port={port}".format(
                                    host=self.client_host, port=self.client_port)
            else:
                endpoint_spec = "tcp:host={host}:port={port}".format(
                                    host=self.client_host, port=self.client_port)

            ep = endpoints.clientFromString(reactor, endpoint_spec)
            d = ep.connect(SyncClientFactory())
            d.addCallback(self.setup_client_connection)
            return d
        else:
            return defer.succeed(self.client.connection)


# vim:sts=4:ts=4:sw=4:expandtab:
