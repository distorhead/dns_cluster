# -*- coding: utf-8 -*-

import yaml

from twisted.internet import reactor, threads
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.python.threadpool import ThreadPool
from twisted.python import log


# Define Protocol and Factory here, nothing more.
# The rest will be defined in /daemons/syncd.tac.

class SyncProtocol(object):
    OK = 0
    UP_TO_DATE = 1
    LACK_POSITION = 2

    def __init__(self, action_journal):
        self._action_journal = action_journal

    def _errback(self, failure):
        log.err("an error occured:")
        log.err(failure)

    def _make_response(self, pos):
        log.msg("make response")
        msg = {"cmd": "pull_response"}

        cur_pos = self._action_journal.get_position()
        if cur_pos <= pos:
            msg["status"] = self.UP_TO_DATE

        elif not self._action_journal.position_exists(pos + 1):
            msg["status"] = self.LACK_POSITION

        else:
            actions = self._action_journal.get_since_position(pos)
            msg["status"] = self.OK
            msg["server_position"] = cur_pos
            msg["data"] = []
            for pos_act in actions:
                action_data = {
                    "position": pos_act[0],
                    "action": pos_act[1]
                }
                msg["data"].append(action_data)

        return msg

    def _handle_pull_request(self, msg):
        pos = msg["position"]
        d = threads.deferToThread(self._make_response, pos)
        d.addCallback(self.send_message)
        d.addErrback(self._errback)

    def _handle_pull_response(self, msg):
        #TODO
        pass

    def handle_message(self, msg):
        cmd = msg.get("cmd", None)
        log.msg("received cmd: ", cmd)
        if cmd == "pull_request":
            self._handle_pull_request(msg)
        elif cmd == "pull_response":
            self._handle_pull_response(msg)
        else:
            log.msg("unknown cmd '{0}', unable to handle".format(cmd))

    def send_message(self, msg):
        assert 0, "Send message method is not implemented"


class YamlSyncProtocol(LineReceiver, SyncProtocol):
    def __init__(self, action_journal):
        SyncProtocol.__init__(self, action_journal)
        self._received_lines = []

    def _parse_msg(self, msg_dump):
        try:
            return yaml.load(msg_dump)
        except yaml.YAMLError, exc:
            log.msg("unable to parse message: bad yaml: " + str(exc))
            return None

    def _emit_msg(self, msg):
        try:
            return yaml.dump(msg)
        except yaml.YAMLError, exc:
            log.err("unable to make yaml dump from message: ", exc)

    def _handle_message_str(self, msg_dump):
        msg_data = self._parse_msg(msg_dump)

        if isinstance(msg_data, dict):
            log.msg("raw message: ", str(msg_data))
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
            log.err("unable to send message '{0}'".format(repr(msg)))


class SyncFactory(Factory):
    PROTOCOLS = {
        "yaml": YamlSyncProtocol
        #TODO: binary
    }

    def __init__(self, action_journal, **kwargs):
        self._action_journal = action_journal
        protocol = kwargs.get("protocol", "yaml")
        self._protocol = self.PROTOCOLS.get(protocol, YamlSyncProtocol)

    def buildProtocol(self, addr):
        return self._protocol(self._action_journal);


# vim:sts=4:ts=4:sw=4:expandtab:
