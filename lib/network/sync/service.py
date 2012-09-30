# -*- coding: utf-8 -*-

from twisted.python import log
from zope.interface import implements

from lib.network.sync.sync import IService, IProtocol
from lib.event_storage import EventStorage
from lib.enum import Enum
from lib.glossary import Glossary


class Protocol:
    Status = Glossary(OK = 0,
                      NO_POSITION = 1,
                      FORBIDDEN = 2)

    AuthSchema = Glossary(PAP = "pap",
                          CHAP = "chap")

    Cmd = Glossary(AUTH_REQUEST = "auth_request",
                   AUTH_RESPONSE = "auth_response",
                   AUTH_CHALLENGE = "auth_challenge",
                   PULL_REQUEST = "pull_request",
                   ACTIONS = "actions",
                   WAIT = "wait",
                   PING = "ping")


class SyncService(object):
    implements(IService)

    def __init__(self, **kwargs):
        self.protocol = None
        self.peer = None
        self._state = None

    def _handle_cmd(self, cmd, msg):
        if cmd in Protocol.Cmd:
            handler_name = "_handle_cmd_{}".format(cmd)
            if hasattr(self, handler_name):
                handler = getattr(self, handler_name)
                handler(msg)
            else:
                log.msg("Cmd '{}' is not allowed".format(cmd))
        else:
            log.msg("Unknown cmd '{}', ignoring".format(cmd))

    def _allowed_state(self, *allowed_states):
        return self._state in allowed_states

    def set_protocol(self, protocol):
        self.protocol = IProtocol(protocol)

    def remote_addr(self):
        if not self.protocol is None:
            return self.protocol.transport.getPeer()
        else:
            assert 0, "protocol attribute is not set"

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
    State = Enum('CONNECTED',
                 'AUTH_REQUEST_SENT',
                 'AUTH_CHALLENGE_RECEIVED',
                 'AUTH_CHALLENGE_SENT',
                 'OPERATIONAL',
                 'PULL_REQUEST_SENT',
                 'WAIT')

    def __init__(self, **kwargs):
        SyncService.__init__(self, **kwargs)
        self._state = self.State.CONNECTED
        self._es = EventStorage('actions_received',
                                'auth_challenge',
                                'auth_response')
        self.peer = None

    def _handle_cmd_auth_response(self, msg):
        if self._allowed_state(self.State.AUTH_REQUEST_SENT,
                               self.State.AUTH_CHALLENGE_SENT):
            if not msg.has_key("status"):
                return

            if msg["status"] == Protocol.Status.OK:
                log.msg("Authentication response - OK")
                self._state = self.State.OPERATIONAL

                d = self._es.retrieve_event('auth_response')
                if not d is None:
                    d.callback(True)

            elif msg["status"] == Protocol.Status.FORBIDDEN:
                log.msg("Authentication response - FORBIDDEN")
                self._state = self.State.CONNECTED

                d = self._es.retrieve_event('auth_response')
                if not d is None:
                    d.callback(False)

    def _handle_cmd_auth_challenge(self, msg):
        if self._allowed_state(self.State.AUTH_REQUEST_SENT):
            if not msg.has_key("challenge"):
                return

            log.msg("Authentication challenge received")

            self._state = self.State.AUTH_CHALLENGE_RECEIVED

            d = self._es.retrieve_event('auth_challenge')
            if not d is None
                d.callback(msg["challenge"])

    def _handle_cmd_actions(self, msg):
        if self._allowed_state(self.State.PULL_REQUEST_SENT, self.State.WAIT):
            if not msg.has_key("status"):
                return

            if msg["status"] == Protocol.Status.OK:
                valid_actions = []
                for act_desc in msg.get("data", []):
                    if act_desc.has_key("action") and act_desc.has_key("position"):
                        valid_actions.append(act_desc)

                self._state = self.State.OPERATIONAL
                d = self._es.retrieve_event('actions_received')
                if not d is None:
                    d.callback(valid_actions)

            elif msg["status"] == Protocol.Status.NO_POSITION:
                self._state = self.State.OPERATIONAL
                d = self._es.retrieve_event('actions_received')
                if not d is None:
                    d.callback(None)

    def _handle_cmd_wait(self, msg):
        if self._allowed_state(self.State.PULL_REQUEST_SENT,
                               self.State.WAIT):
            log.msg("Waiting action from peer '{0}'".format(self.peer.name))
            self._state = self.State.WAIT


    def register_event(self, event):
        return self._es.register_event(event)

    def send_pull_request(self, position):
        if self._allowed_state(self.State.OPERATIONAL):
            log.msg("Requesting pull from peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.PULL_REQUEST,
                "position": position
            }
            self.send_message(msg)
            self._state = self.State.PULL_REQUEST_SENT

    def send_auth_pap(self, name, pswd):
        if self._allowed_state(self.State.CONNECTED):
            log.msg("Sending authentication info to peer '{}'".format(
                        self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.AUTH_REQUEST,
                "auth_schema": Protocol.AuthSchema.PAP
                "name": name,
                "pswd": pswd
            }
            self.send_message(msg)
            self._state = self.State.AUTH_REQUEST_SENT

    def send_auth_chap(self, name):
        if self._allowed_state(self.State.CONNECTED):
            log.msg("Sending authentication info to peer '{}'".format(
                        self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.AUTH_REQUEST,
                "auth_schema": Protocol.AuthSchema.CHAP,
                "name": name
            }
            self.send_message(msg)
            self._state = self.State.AUTH_REQUEST_SENT

    def send_auth_chap_challenge(self, challenge):
        if self._allowed_state(self.State.AUTH_CHALLENGE_RECEIVED):
            log.msg("Sending authentication challenge to peer '{}'".format(
                        self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.AUTH_CHALLENGE,
                "challenge": challenge
            }
            self.send_message(msg)
            self._state = self.State.AUTH_CHALLENGE_SENT

    def is_authenticated(self):
        return self._state == self.State.OPERATIONAL


class SyncServer(SyncService):
    State = Enum('CONNECTED',
                 'AUTH_REQUEST_RECEIVED',
                 'AUTH_CHALLENGE_SENT',
                 'AUTH_CHALLENGE_RECEIVED',
                 'OPERATIONAL',
                 'PULL_REQUEST_RECEIVED',
                 'WAIT_SENT')

    def __init__(self, **kwargs):
        SyncService.__init__(self, **kwargs)

        self._state = self.State.CONNECTED
        self._es = EventStorage('pull_request',
                                'auth_request_pap',
                                'auth_request_chap',
                                'auth_challenge')
        self.peer = None

    def _handle_cmd_auth_request(self, msg):
        if self._allowed_state(self.State.CONNECTED):
            if not msg.has_key("auth_schema") or not msg.has_key("name"):
                return

            if msg["auth_schema"] == Protocol.AuthSchema.PAP:
                if not msg.has_key("pswd"):
                    return

                self._state = self.State.AUTH_REQUEST_RECEIVED
                d = self._es.retrieve_event('auth_request_pap')
                if not d is None:
                    d.callback(msg["name"], msg["pswd"])

            elif msg["auth_schema"] == Protocol.AuthSchema.CHAP:
                self._state = self.State.AUTH_REQUEST_RECEIVED
                d = self._es.retrieve_event('auth_request_chap')
                if not d is None:
                    d.callback(msg["name"])

            else:
                log.msg("Unknown auth schema '{}'".format(msg["auth_schema"]))

    def _handle_cmd_auth_challenge(self, msg):
        if self._allowed_state(self.State.AUTH_CHALLENGE_SENT):
            if not msg.has_key("challenge"):
                return

            self._state = self.State.AUTH_CHALLENGE_RECEIVED
            d = self._es.retrieve_event('auth_challenge')
            if not d is None:
                d.callback(msg["challenge"])

    def _handle_cmd_pull_request(self, msg):
        if self._allowed_state(self.State.OPERATIONAL):
            if not msg.has_key("position"):
                return

            try:
                pos = int(msg["position"])
            except:
                log.err("Wrong position '{}' in pull request from peer '{}'".format(
                         self.peer.name))
                return

            self._state = self.State.PULL_REQUEST_RECEIVED
            d = self._es.retrieve_event('pull_request')
            if not d is None:
                d.callback(pos)


    def register_event(self, event):
        return self._es.register_event(event)

    def send_no_position(self):
        if self._allowed_state(self.State.PULL_REQUEST_RECEIVED):
            log.msg("Sending no position to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.ACTIONS,
                "status": Protocol.Status.NO_POSITION
            }
            self.send_message(msg)
            self._state = self.State.OPERATIONAL

    def send_actions(self, actions):
        if self._allowed_state(self.State.PULL_REQUEST_RECEIVED, self.State.WAIT_SENT):
            log.msg("Sending actions to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.ACTIONS,
                "status": Protocol.Status.OK,
                "data": actions
            }
            self.send_message(msg)
            self._state = self.State.OPERATIONAL

    def send_wait(self):
        if self._allowed_state(self.State.PULL_REQUEST_RECEIVED):
            log.msg("Sending wait to peer '{0}'".format(self.peer.name))
            msg = {
                "cmd": Protocol.Cmd.WAIT,
            }
            self.send_message(msg)
            self._state = self.State.WAIT_SENT

    def send_auth_success(self):
        if self._allowed_state(self.State.AUTH_REQUEST_RECEIVED,
                               self.State.AUTH_CHALLENGE_RECEIVED):
            ra = self.remote_addr()
            log.msg("Sending auth success to '{}:{}'".format(
                        ra.host, ra.port))
            msg = {
                "cmd": Protocol.Cmd.AUTH_RESPONSE,
                "status": Protocol.Status.OK
            }
            self.send_message(msg)
            self._state = self.State.OPERATIONAL

    def send_auth_challenge(self, challenge):
        if self._allowed_state(self.State.AUTH_REQUEST_RECEIVED):
            ra = self.remote_addr()
            log.msg("Sending auth challenge to '{}:{}'".format(
                        ra.host, ra.port))
            msg = {
                "cmd": Protocol.Cmd.AUTH_CHALLENGE,
                "challenge": challenge
            }
            self.send_message(msg)
            self._state = self.State.AUTH_CHALLENGE_SENT

    def send_auth_fail(self):
        if self._allowed_state(self.State.AUTH_REQUEST_RECEIVED,
                               self.State.AUTH_CHALLENGE_RECEIVED):
            ra = self.remote_addr()
            log.msg("Sending auth failure to '{}:{}'".format(
                        ra.host, ra.port))
            msg = {
                "cmd": Protocol.Cmd.AUTH_RESPONSE,
                "status": Protocol.Status.FORBIDDEN
            }
            self.send_message(msg)
            self._state = self.State.CONNECTED


# vim:sts=4:ts=4:sw=4:expandtab:
