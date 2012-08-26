# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.network.sync.peer import Peer
from lib.network.sync.protocol import SyncServerFactory
from twisted.internet import threads, reactor, task, endpoints
from twisted.python import log


class SyncApp(object):
    PUSH_BATCH_SIZE = 5

    DATABASES = {
        "peer": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    def __init__(self, interface, port, peers, db, action_journal, **kwargs):
        self._interface = interface
        self._port = port
        self._database = db
        self._action_journal = action_journal
        self._pull_period = kwargs.get("pull_period", 10.0)
        self._active_peers = []
        self._peers = {}
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())

        pdb = self._dbpool.peer.dbhandle()
        with self._database.transaction() as txn:
            for pname in peers:
                pdesc = peers[pname]
                peer = Peer(pname, pdesc["host"], pdesc["port"])
                self._peers[peer.name] = peer

                if not pdb.exists(pname, txn):
                    # add new peer entry
                    pdb.put(pname, "-1", txn)

        l = task.LoopingCall(self.pull)
        l.start(self._pull_period)
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                        interface=self._interface, port=self._port)
        self.server_endpoint = endpoints.serverFromString(reactor, endpoint_spec)

    def listen(self):
        self.server_endpoint.listen(SyncServerFactory(self.retrieve_actions))

    def database_updated(self):
        log.msg("Updating active peers:", self._active_peers)
        while self._active_peers:
            peer = self._active_peers.pop(0)
            d = threads.deferToThread(self._get_peer_update, peer)
            d.addCallback(self._got_peer_update, peer)
            d.addErrback(self._errback, "Error while getting update for peer "
                                        "'{0}'".format(peer.host))

    def pull(self):
        """
        Initiate pull requests to all known cluster servers.
        Method do not blocks.
        """

        for pname in self._peers:
            peer = self._peers[pname]
            d = peer.connect(self.apply_actions)
            d.addCallback(self._peer_connected, peer)
            d.addErrback(self._errback,
                         "Error while connecting to peer '{0}'".format(peer.name))

    def retrieve_actions(self, position, peer):
        """
        Get actions from specified position.
        Method do not blocks.
        """

        d = threads.deferToThread(self._do_retrieve_actions, position, peer)
        d.addCallback(self._actions_retrieved, position, peer)
        d.addErrback(self._errback, "Error while retrieving actions for peer "
                                    "'{0}' from position {1}".format(
                                    peer.host, position))

    def apply_actions(self, actions, peer):
        """
        Apply actions from specified peer.
        Method returns deferred activated when actions applied.
        """

        d = threads.deferToThread(self._do_apply_actions, actions, peer)
        d.addErrback(self._actions_apply_failure)
        d.addErrback(self._errback, "Error while handling actions from peer "
                                    "'{0}'".format(peer.name))
        return d


    def _do_apply_actions(self, actions, peer):
        log.msg("Applying actions from peer '{0}'".format(peer.name))
        pdb = self._dbpool.peer.dbhandle()
        for act_desc in actions:
            act_dump = act_desc["action"]
            act = Action.unserialize(act_dump)
            pos = act_desc["position"]
            with self._database.transaction() as txn:
                act.apply(self._database, txn)
                self._action_journal.record_action(act, txn)
                pdb.put(peer.name, str(pos), txn)

    def _actions_apply_failure(self, failure):
        failure.trap(ActionError)
        #TODO: real failure handler
        log.msg("Unable to apply action", failure)

    def _get_peer_update(self, peer):
        """
        Blocking call to DB, should be called from thread pool.
        """

        with self._database.transaction() as txn:
            cur_pos = self._action_journal.get_position(txn)
            actions = self._action_journal.get_since_position(peer.position,
                      self.PUSH_BATCH_SIZE, txn)
            return (cur_pos, actions)

    def _got_peer_update(self, res, peer):
        cur_pos, actions = res

        if len(actions) == 0:
            log.msg("No updates for peer '{0}'".format(peer.host))
            self._active_peers.append(peer)
        elif actions[-1]["position"] == cur_pos:
            log.msg("Got updates for peer '{0}', peer stay active".format(
                        peer.host))
            peer.service.send_actions(actions)
            peer.service.send_wait()
            peer.position = cur_pos
            self._active_peers.append(peer)
        else:
            log.msg("Got updates for peer '{0}'".format(peer.host))
            peer.service.send_actions(actions)
            peer.position = actions[-1]["position"]

    def _do_retrieve_actions(self, position, peer):
        log.msg("Retrieving actions for peer '{0}' from position {1}".format(
                peer.host, position))

        with self._database.transaction() as txn:
            if not self._action_journal.position_exists(position, txn):
                return None
            else:
                cur_pos = self._action_journal.get_position(txn)
                actions = self._action_journal.get_since_position(
                          position, self.PUSH_BATCH_SIZE, txn)
                return (cur_pos, actions)

    def _actions_retrieved(self, res, position, peer):
        log.msg("Actions retrieved")
        if res is None:
            peer.service.send_no_position()
        else:
            cur_pos, actions = res

            actions_len = len(actions)
            if actions_len != 0:
                peer.service.send_actions(actions)
                peer.position = actions[-1]["position"]
            if actions_len == 0 or cur_pos == actions[-1]["position"]:
                peer.service.send_wait()
                peer.position = cur_pos
                self._active_peers.append(peer)

    def _peer_connected(self, connection, peer):
        d = threads.deferToThread(self._get_peer_position, peer)
        d.addCallback(self._got_peer_position, peer)
        d.addErrback(self._errback,
                     "Error while getting peer's '{0}' position".format(peer.name))

    def _get_peer_position(self, peer):
        """
        Blocking call to DB, should be called from thread pool.
        """

        pdb = self._dbpool.peer.dbhandle()
        return int(pdb.get(peer.name, None))

    def _got_peer_position(self, pos, peer):
        if not pos is None:
            peer.service.send_pull_request(pos)
        else:
            peer.service.send_pull_request(-1)

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)


# vim:sts=4:ts=4:sw=4:expandtab:
