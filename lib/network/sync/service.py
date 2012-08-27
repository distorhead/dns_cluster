# -*- coding: utf-8 -*-

from zope.interface import implements
from lib.network.sync.sync import IService, IProtocol
from lib.event_storage import EventStorage
from lib.common import is_callable


class Protocol:
    class Status:
        OK = 0
        NO_POSITION = 1
        FORBIDDEN = 2
        IDENTIFIED = 3

    class Cmd:
        IDENT = "ident"
        PULL_REQUEST = "pull_request" 
        ACTIONS = "actions"
        WAIT = "wait"
        POSITION = "position"
        #TODO: HEARTBEAT cmd


class SyncService(object):
    implements(IService)

    def __init__(self, **kwargs):
        self.protocol = None
        self.peer = None
        self._state = None
        self._state_watchdog_timeout = kwargs.get("state_watchdog_timeout", 30.0)
        self._state_watchdog_timer = None
        #self._reset_state_watchdog_timer()

    #def _reset_state_watchdog_timer(self):
        #self._cancel_state_watchdog_timer()
        #self._state_watchdog_timer = reactor.callLater(
                #self._state_watchdog_timeout,
                #self._reset_state)

    #def _cancel_state_watchdog_timer(self):
        #if not self._state_watchdog_timer is None:
            #if self._state_watchdog_timer.active():
                #self._state_watchdog_timer.cancel()
            #self._state_watchdog_timer = None

    #def _reset_state(self):
        #log.msg("Resetting connection state on watchdog timer ({0})".format(self))
        #self._state = self.State.NORMAL

    def _handle_cmd(self, cmd, msg):
        assert 0, "Message handler doesn't implemented"

    def _allowed_state(self, *allowed_states):
        return self._state in allowed_states

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)

    def set_protocol(self, protocol):
        self.protocol = IProtocol(protocol)

    def send_message(self, msg):
        if not self.protocol is None:
            self.protocol.send_message(msg)

    def hangup(self):
        if not self.protocol is None:
            self.protocol.hangup()

    def handle_message(self, msg):
        log.msg("Received message:", msg)
        try:
            if not msg.has_key("cmd"):
                return

            self._handle_cmd(msg["cmd"], msg)

        except Exception, exc:
            log.err("Unable to handle message")
            log.err(exc)


class SyncClient(SyncService):
    class State:
        CONNECTED = 0
        IDENT_SENT = 1
        IDENTIFIED = 2
        PULL_REQ_SENT = 3
        WAIT = 4

    def __init__(self, **kwargs):
        SyncService.__init__(self, **kwargs)
        self._state = self.State.CONNECTED
        self._es = EventStorage('actions_received')
        self.peer = None

    def _handle_cmd_actions(self, msg):
        if self._allowed_state(self.State.PULL_REQ_SENT, self.State.WAIT):
            if not msg.has_key("status"):
                return

            if msg["status"] == Protocol.Status.OK:
                valid_actions = []
                for act_desc in msg.get("data", []):
                    if act_desc.has_key("action") and act_desc.has_key("position"):
                        valid_actions.append(act_desc)

                d = self._es.retrieve_event('actions_received')
                if not d is None:
                    d.callback(valid_actions)

                self._state = self.State.IDENTIFIED

            elif msg["status"] == Protocol.Status.NO_POSITION:
                log.msg("Unable to synchronize with peer '{0}': "
                        "peer doesn't have requested position".format(self.peer.name))

                d = self._es.retrieve_event('actions_received')
                if not d is None:
                    fail = SyncClientError("No such position on peer '{0}'".format(
                                           self.peer.name))
                    d.errback(fail)

                self._state = self.State.IDENTIFIED

    def _handle_cmd_wait(self, msg):
        if self._allowed_state(self.State.PULL_REQ_SENT,
                               self.State.WAIT):
            log.msg("Waiting action from peer '{0}'".format(self.peer.name))
            self._state = self.State.WAIT

    def _handle_cmd(self, cmd, msg):
        if cmd == Protocol.Cmd.ACTIONS:
            self._handle_cmd_actions(msg)
        elif cmd == Protocol.Cmd.WAIT:
            self._handle_cmd_wait(msg)
        else:
            log.msg("Unknown cmd '{0}', unable to handle".format(cmd))

    def register_event(self, event):
        self._es.register_event(event)

    def send_pull_request(self, position):
        if self._allowed_state(self.State.IDENTIFIED):
            log.msg("Requesting pull from peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.PULL_REQUEST,
                "position": position
            }
            self.send_message(msg)
            self._state = self.State.PULL_REQ_SENT

    def send_ident(self):
        if self._allowed_state(self.State.CONNECTED, self.State.IDENT_SENT):
            log.msg("Sending identification info from peer '{0}'".format(
                        self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.IDENT,
                "name": self.peer.name
            }
            self.send_message(msg)
            self._state = self.State.IDENT_SENT

    def send_position(self, position):
        if self._allowed_state(self.State.IDENTIFIED):
            log.msg("Sending position info from peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.POSITION,
                "position": position
            }
            self.send_message(msg)


class SyncServer(SyncService):
    class State:
        CONNECTED = 0
        IDENT_RECEIVED = 1
        IDENTIFIED = 2
        PULL_REQ_RECEIVED = 3
        WAIT_SENT = 4
        ACTIONS_SENT = 5

    def __init__(self, **kwargs):
        SyncService.__init__(self, **kwargs)
        self._state = self.State.CONNECTED
        self._es = EventStorage('pull_request',
                                'position_update',
                                'ident_request')
        self.peer = None
        #TODO: watchdog timer to reset states

    def _handle_cmd_ident(self, msg):
        if self._allowed_state(self.State.CONNECTED):
            if not msg.has_key("name"):
                return

            d = self._es.retrieve_event('ident_request')
            if not d is None:
                d.callback(msg["name"])

            self._state = IDENT_RECEIVED

    def _handle_cmd_pull_request(self, msg):
        if self._allowed_state(self.State.IDENTIFIED):
            if not msg.has_key("position"):
                return

            d = self._es.retrieve_event('pull_request')
            if not d is None:
                d.callback(msg["position"])

            self._state = self.State.PULL_REQ_RECEIVED

    def _handle_cmd_position(self, msg):
        if self._allowed_state(self.State.ACTIONS_SENT):
            if not msg.has_key("position"):
                return

            d = self._es.retrieve_event('position_update')
            if not d is None:
                d.callback(msg["position"])

            self._state = self.State.IDENTIFIED

    def _handle_cmd(self, cmd, msg):
        if cmd == Protocol.Cmd.IDENT:
            self._handle_cmd_ident(msg)
        if cmd == Protocol.Cmd.PULL_REQUEST:
            self._handle_cmd_pull_request(msg)
        elif cmd == Protocol.Cmd.POSITION:
            self._handle_cmd_position(msg)
        else:
            log.msg("Unknown cmd '{0}', unable to handle".format(cmd))

    def register_event(self, event):
        self._es.register_event(event)

    def send_no_position(self):
        if self._allowed_state(self.State.PULL_REQ_RECEIVED):
            log.msg("Sending no position to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.ACTIONS,
                "status": Protocol.Status.NO_POSITION
            }
            self.send_message(msg)
            self._state = self.State.IDENTIFIED

    def send_actions(self, actions):
        if self._allowed_state(self.State.PULL_REQ_RECEIVED, self.State.WAIT_SENT):
            log.msg("Sending actions to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.ACTIONS,
                "status": Protocol.Status.OK,
                "data": actions
            }
            self.send_message(msg)
            self._state = self.State.ACTIONS_SENT

    def send_wait(self):
        if self._allowed_state(self.State.PULL_REQ_RECEIVED):
            log.msg("Sending wait to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.WAIT,
            }
            self.send_message(msg)
            self._state = self.State.WAIT_SENT

    def send_ident_success(self):
        if self._allowed_state(self.State.IDENT_RECEIVED):
            log.msg("Sending identification success to peer '{0}'".format(
                    self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.IDENT,
                "status": Protocol.Status.IDENTIFIED
            }
            self.send_message(msg)
            self._state = self.State.IDENTIFIED

    def send_ident_fail(self):
        if self._allowed_state(self.State.IDENT_RECEIVED):
            log.msg("Sending identification failure to peer '{0}'".format(
                    self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.IDENT,
                "status": Protocol.Status.FORBIDDEN
            }
            self.send_message(msg)
            self._state = self.State.CONNECTED


# vim:sts=4:ts=4:sw=4:expandtab:
