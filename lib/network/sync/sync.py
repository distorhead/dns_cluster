# -*- coding: utf-8 -*-

import yaml

from twisted.internet import reactor, threads
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.python.threadpool import ThreadPool
from twisted.python import log
from lib.action import Action, ActionError


class SyncProtocolError(Exception): pass


class SyncProtocol(object):
    class ProtocolStatus:
        OK = 0
        LOST_POSITION = 1

    class State:
        NORMAL = 0
        PULL_REQ_SENT = 1
        WAIT = 2

    def __init__(self, actions_handler, **kwargs):
        self._actions_handler = actions_handler
        self._actions_relay = []
        self._actions_handler_run = False
        self._state = self.State.NORMAL
        self._state_watchdog_timeout = kwargs.get("state_watchdog_timeout", 360.0)
        self._state_watchdog_timer = None

    def _allowed_state(self, *allowed_states):
        return self._state in allowed_states

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)

    def _reset_state(self):
        log.msg("Resetting connection state on watchdog timer")
        self._state = self.State.NORMAL

    def _setup_action_handler(self, actions):
        d = self._actions_handler(actions)
        d.addCallback(self._on_actions_handled)

    def _setup_state_watchdog_timer(self):
        if (self._state_watchdog_timer is None or 
            not self._state_watchdog_timer.active()):
            self._state_watchdog_timer = reactor.callLater(
                    self._state_watchdog_timeout,
                    self._reset_state)

    def _reset_state_watchdog_timer(self):
        self._cancel_state_watchdog_timer()
        self._state_watchdog_timer = reactor.callLater(
                self._state_watchdog_timeout,
                self._reset_state)

    def _cancel_state_watchdog_timer(self):
        if not self._state_watchdog_timer is None:
            if self._state_watchdog_timer.active():
                self._state_watchdog_timer.cancel()
            self._state_watchdog_timer = None

    def _on_actions_handled(self, _):
        if self._actions_relay:
            self._setup_action_handler(self._actions_relay)
            self._actions_relay = []
        else:
            self._actions_handler_run = False

    def _handle_cmd_actions(self, msg):
        if self._allowed_state(self.State.PULL_REQ_SENT, self.State.WAIT):
            if not msg.has_key("status"):
                return

            if msg["status"] == self.ProtocolStatus.OK:
                for act_desc in msg.get("data", []):
                    if act_desc.has_key("action") and act_desc.has_key("position"):
                        self._actions_relay.append(act_desc)

                if not self._actions_handler_run:
                    self._setup_action_handler(self._actions_relay)
                    self._actions_relay = []
                    self._actions_handler_run = True

                self._state = self.State.NORMAL

            elif msg["status"] == self.ProtocolStatus.LOST_POSITION:
                log.msg("Unable to synchronize with peer '{0}': "
                        "peer lost requested position".format(self.peer.name))
                self._state = self.State.NORMAL

    def _handle_cmd_wait(self, msg):
        if self._allowed_state(self.State.PULL_REQ_SENT,
                               self.State.NORMAL,
                               self.State.WAIT):
            log.msg("Waiting action from peer '{0}'".format(self.peer.name))
            self._state = self.State.WAIT
            self._reset_state_watchdog_timer()

    def handle_message(self, msg):
        log.msg("Received message:", msg)
        try:
            if not msg.has_key("cmd"):
                return

            cmd = msg["cmd"]
            if cmd == "actions":
                self._handle_cmd_actions(msg)

            elif cmd == "wait":
                self._handle_cmd_wait(msg)

            else:
                log.msg("Unknown cmd '{0}', unable to handle".format(cmd))

        except Exception, exc:
            log.err("Unable to handle message", exc)

    def pull_request(self, position):
        if self._allowed_state(self.State.NORMAL):
            log.msg("Requesting pull from peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": "pull_request",
                "position": position
            }
            self.send_message(msg)
            self._state = self.State.PULL_REQ_SENT
            self._reset_state_watchdog_timer()

    def send_message(self, msg):
        assert 0, "Send message method is not implemented"


class YamlSyncProtocol(LineReceiver, SyncProtocol):
    def __init__(self, actions_handler):
        SyncProtocol.__init__(self, actions_handler)
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

    def _handle_message_str(self, msg_dump):
        msg_data = self._parse_msg(msg_dump)

        if isinstance(msg_data, dict):
            log.msg("Raw message:", repr(msg_dump))
            self.handle_message(msg_data)

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
        "yaml": YamlSyncProtocol
        #TODO: binary
    }

    def __init__(self, actions_handler=None, **kwargs):
        self._actions_handler = actions_handler
        protocol = kwargs.get("protocol", "yaml")
        self._protocol = self.PROTOCOLS.get(protocol, YamlSyncProtocol)

    def buildProtocol(self, addr):
        return self._protocol(self._actions_handler)


# vim:sts=4:ts=4:sw=4:expandtab:
