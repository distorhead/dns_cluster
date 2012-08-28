# -*- coding: utf-8 -*-

import yaml

from zope.interface import implements
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from lib.action import Action, ActionError
from lib.network.sync.sync import IProtocol, IService
from lib.network.sync.service import SyncClient, SyncServer
from lib.common import is_callable


class YamlMsgProtocol(LineReceiver):
    implements(IProtocol)

    def __init__(self):
        self.service = None
        self._received_lines = []

    def _parse_msg(self, msg_dump):
        try:
            return yaml.load(msg_dump)
        except yaml.YAMLError, exc:
            log.msg("Unable to parse message: bad yaml:", exc)
            return None

    def _emit_msg(self, msg):
        try:
            return yaml.dump(msg)
        except yaml.YAMLError, exc:
            log.err("Unable to make yaml dump from message:", exc)

    def _handle_message(self, msg):
        if not self.service is None:
            self.service.handle_message(msg)

    def _handle_message_str(self, msg_dump):
        msg_data = self._parse_msg(msg_dump)

        if isinstance(msg_data, dict):
            log.msg("Raw message:", repr(msg_dump))
            self._handle_message(msg_data)

    def set_service(self, service):
        self.service = IService(service)

    def lineReceived(self, line):
        if line == '':
            msg = "\n".join(self._received_lines)
            self._received_lines = []
            self._handle_message_str(msg)
        else:
            self._received_lines.append(line)

    def send_message(self, msg):
        msg_dump = self._emit_msg(msg)
        if not msg_dump is None:
            resp = msg_dump + "\r\n\r\n"
            self.transport.write(resp)
        else:
            log.err("Unable to send message '{0}'".format(repr(msg)))


class SyncFactory(Factory):
    PROTOCOLS = {
        "yaml": YamlMsgProtocol
        #TODO: binary
    }

    def __init__(self, **kwargs):
        protocol = kwargs.get("protocol", "yaml")
        self.protocol = self.PROTOCOLS.get(protocol, YamlMsgProtocol)

    def _build_protocol(self, service):
        p = self.protocol()
        p.set_service(service)
        service.set_protocol(p)
        return p


class SyncClientFactory(SyncFactory):
    def buildProtocol(self, addr):
        return self._build_protocol(SyncClient())


class SyncServerFactory(SyncFactory):
    def __init__(self, **kwargs):
        SyncFactory.__init__(self, **kwargs)
        self.connectionMade = kwargs.get("connectionMade", None)

    def buildProtocol(self, addr):
        p = self._build_protocol(SyncServer())

        # Black Magic
        old_connectionMade = p.connectionMade
        def connectionMade():
            if is_callable(self.connectionMade):
                self.connectionMade(p)
            old_connectionMade()
        p.connectionMade = connectionMade

        return p


# vim:sts=4:ts=4:sw=4:expandtab:
