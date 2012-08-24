# -*- coding: utf-8 -*-

import yaml

from twisted.internet import reactor, threads
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.python.threadpool import ThreadPool
from twisted.python import log
from lib.action import Action, ActionError
from lib.common import is_callable


#class SyncProtocolError(Exception): pass


class Protocol:
    class Status:
        OK = 0
        LOST_POSITION = 1

    class Cmd:
       PULL_REQUEST = "pull_request" 
       ACTIONS = "actions"
       WAIT = "wait"
       #TODO: HEARTBEAT cmd


class SyncService(object):
    def __init__(self, message_sender=None, **kwargs):
        self.message_sender = message_sender
        self.peer = None
        self._state = None

    def _handle_cmd_pull_request(self, msg): pass
    def _handle_cmd_actions(self, msg): pass
    def _handle_cmd_wait(self, msg): pass

    def _allowed_state(self, *allowed_states):
        return self._state in allowed_states

    def send_message(self, msg):
        if is_callable(self.message_sender):
            self.message_sender(msg)

    def handle_message(self, msg):
        log.msg("Received message:", msg)
        try:
            if not msg.has_key("cmd"):
                return

            cmd = msg["cmd"]
            if cmd == Protocol.Cmd.PULL_REQUEST:
                self._handle_cmd_pull_request(msg)

            elif cmd == Protocol.Cmd.ACTIONS:
                self._handle_cmd_actions(msg)

            elif cmd == Protocol.Cmd.WAIT:
                self._handle_cmd_wait(msg)

            else:
                log.msg("Unknown cmd '{0}', unable to handle".format(cmd))

        except Exception, exc:
            log.err("Unable to handle message")
            log.err(exc)


class SyncClient(SyncService):
    class State:
        NORMAL = 0
        PULL_REQ_SENT = 1
        WAIT = 2

    def __init__(self, actions_handler=None, message_sender=None, **kwargs):
        SyncService.__init__(self, message_sender, **kwargs)
        self.actions_handler = actions_handler
        self._actions_relay = []
        self._actions_handler_run = False
        self._state = self.State.NORMAL
        self._state_watchdog_timeout = kwargs.get("state_watchdog_timeout", 360.0)
        self._state_watchdog_timer = None

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)

    def _reset_state(self):
        log.msg("Resetting connection state on watchdog timer")
        self._state = self.State.NORMAL

    def _setup_action_handler(self, actions):
        if is_callable(self.actions_handler):
            d = self.actions_handler(actions, self.peer)
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

            if msg["status"] == Protocol.Status.OK:
                for act_desc in msg.get("data", []):
                    if act_desc.has_key("action") and act_desc.has_key("position"):
                        self._actions_relay.append(act_desc)

                if not self._actions_handler_run:
                    self._setup_action_handler(self._actions_relay)
                    self._actions_relay = []
                    self._actions_handler_run = True

                self._state = self.State.NORMAL

            elif msg["status"] == Protocol.Status.LOST_POSITION:
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

    def pull_request(self, position):
        if self._allowed_state(self.State.NORMAL):
            log.msg("Requesting pull from peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.PULL_REQUEST,
                "position": position
            }
            self.send_message(msg)
            self._state = self.State.PULL_REQ_SENT
            self._reset_state_watchdog_timer()


class SyncServer(SyncService):
    class State:
        NORMAL = 0
        WAIT_SENT = 1

    def __init__(self, pull_handler=None, message_sender=None, **kwargs):
        SyncService.__init__(self, message_sender, **kwargs)
        self.pull_handler = pull_handler
        self.message_sender = message_sender
        self._state = self.State.NORMAL

    def _setup_pull_handler(self, position):
        if is_callable(self.pull_handler):
            d = self.pull_handler(position)
            d.addCallback(self.push_actions)

    def _handle_cmd_pull_request(self, msg):
        if self._allowed_state(self.State.NORMAL):
            if not msg.has_key("position"):
                return

            self._setup_pull_handler(msg["position"])

    def push_actions(self, actions):
        #TODO
        pass

    def do_wait(self):
        #TODO
        pass


class YamlMsgProtocol(LineReceiver):
    def __init__(self, message_handler=None):
        self.message_handler = message_handler
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
        if is_callable(self.message_handler):
            self.message_handler(msg)

    def _handle_message_str(self, msg_dump):
        msg_data = self._parse_msg(msg_dump)

        if isinstance(msg_data, dict):
            log.msg("Raw message:", repr(msg_dump))
            self._handle_message(msg_data)

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
        self._protocol = self.PROTOCOLS.get(protocol, YamlMsgProtocol)


class SyncClientFactory(SyncFactory):
    def __init__(self, actions_handler, **kwargs):
        SyncFactory.__init__(self, **kwargs)
        self._actions_handler = actions_handler

    def buildProtocol(self, addr):
        log.msg("ADDR:", repr(addr))
        service = SyncClient()
        service.actions_handler = self._actions_handler

        p = self._protocol()
        p.message_handler = service.handle_message
        p.service = service
        service.message_sender = p.send_message

        return p


# vim:sts=4:ts=4:sw=4:expandtab:
