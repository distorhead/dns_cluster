# -*- coding: utf-8 -*-

import yaml

from twisted.internet import reactor, threads
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.python.threadpool import ThreadPool
from twisted.python import log
from lib.action import Action, ActionError


# Define Protocol and Factory here, nothing more.
# The rest will be defined in /daemons/syncd.tac.

class SyncProtocolError(Exception): pass

class SyncProtocol(object):
    OK = 0
    UP_TO_DATE = 1
    LACK_POSITION = 2

    def __init__(self, action_journal, database):
        self._action_journal = action_journal
        self._database = database

    def _errback(self, failure):
        log.err("An error occured:")
        log.err(failure)

    def _make_response(self, pos):
        log.msg("Make response")
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

    def _do_apply(self, actions):
        for act_msg in actions:
            if isinstance(act_msg, dict):
                act_dump = act_msg.get("action", None)
                if act_dump is None:
                    raise SyncProtocolError("Bad action specification: no action field")

                pos = act_msg.get("position", None)
                if pos is None:
                    raise SyncProtocolError("Bad action specification: no position field")

                act = Action.unserialize(act_dump)
                with self._database.transaction() as txn:
                    log.msg("Applying action:", act.name())
                    act.apply(self._database, txn)
                    log.msg("Writing to journal")
                    self._action_journal.record_action(act, txn, pos)

                log.msg("Applying done")

            else:
                raise SyncProtocolError("Bad action specification")

    def _do_pull(self):
        msg = {"cmd": "pull_request"}
        msg["position"] = self._action_journal.get_position()
        return msg

    def _handle_pull_request(self, msg):
        pos = msg["position"]
        d = threads.deferToThread(self._make_response, pos)
        d.addCallback(self.send_message)
        d.addErrback(self._errback)

    def _handle_pull_response(self, msg):
        log.msg("Handling pull response")

        status = msg["status"]
        if status == self.UP_TO_DATE:
            log.msg("Server up to date")

        elif status == self.LACK_POSITION:
            log.err("Unable to sync, remote server doesn't have necessary positions")

        elif status == self.OK:
            actions = msg["data"]
            d = threads.deferToThread(self._do_apply, actions)
            d.addErrback(self._errback)

        else:
            log.msg("Unknown status '{0}', unable to handle".format(status))

    def handle_message(self, msg):
        try:
            cmd = msg["cmd"]
            if cmd == "pull_request":
                self._handle_pull_request(msg)

            elif cmd == "pull_response":
                self._handle_pull_response(msg)

            else:
                log.msg("Unknown cmd '{0}', unable to handle".format(cmd))

        except KeyError, key:
            log.msg("Bad message: no key", key)

        except Exception, exc:
            log.err("Unable to handle message")

    def send_message(self, msg):
        assert 0, "Send message method is not implemented"

    def pull(self):
        log.msg("Making pull")
        d = threads.deferToThread(self._do_pull)
        d.addCallback(self.send_message)
        d.addErrback(self._errback)


class YamlSyncProtocol(LineReceiver, SyncProtocol):
    def __init__(self, action_journal, database):
        SyncProtocol.__init__(self, action_journal, database)
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

    def __init__(self, action_journal, database, **kwargs):
        self._action_journal = action_journal
        self._database = database

        protocol = kwargs.get("protocol", "yaml")
        self._protocol = self.PROTOCOLS.get(protocol, YamlSyncProtocol)

    def buildProtocol(self, addr):
        return self._protocol(self._action_journal, self._database)


# vim:sts=4:ts=4:sw=4:expandtab:
