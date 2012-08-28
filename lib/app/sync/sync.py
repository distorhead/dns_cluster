# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.network.sync.peer import Peer
from lib.network.sync.protocol import SyncServerFactory
from twisted.internet import threads, reactor, task, endpoints
from twisted.python import log


class SyncApp(object):
    ACTIONS_BATCH_SIZE = 5

    DATABASES = {
        "peer": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    def __init__(self, name, interface, port, peers, db, action_journal, **kwargs):
        self._name = name
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
                peer = Peer(pname, client_host=pdesc["host"],
                                   client_port=pdesc["port"])
                self._peers[peer.name] = {
                    "peer": peer,
                    "pull_in_progress": False
                }

                if not pdb.exists(pname, txn):
                    # add new peer entry
                    pdb.put(pname, "0", txn)

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)



    def start_pull(self):
        l = task.LoopingCall(self.pull)
        l.start(self._pull_period)

    def make_service(self):
        return Peer.make_service(self._interface, self._port, self._client_connected)

    def _client_connected(self, conn):
        log.msg("New client connected")
        service = conn.service
        d = service.register_event('ident_request')
        d.addCallback(self._client_ident_request, service)

    def _client_ident_request(self, name, service):
        if not self._peers.has_key(name):
            # unknown peer name
            service.send_ident_fail()
            d = service.register_event('ident_request')
            d.addCallback(self._client_ident_request, service)

        else:
            peer = self._peers[name]["peer"]
            peer.setup_server_connection(service.protocol)
            service.send_ident_success()
            d = service.register_event('pull_request')
            d.addCallback(self._client_pull_request, peer)


    def _client_pull_request(self, position, peer):
        d = threads.deferToThread(self._do_retrieve_actions, position, peer)
        d.addCallback(self._actions_retrieved, position, peer)
        d.addErrback(self._errback, "Error while retrieving actions for peer "
                                    "'{0}' from position {1}".format(
                                    peer.name, position))

    def _do_retrieve_actions(self, position, peer):
        log.msg("Retrieving actions for peer '{0}' from position {1}".format(
                peer.name, position))

        with self._database.transaction() as txn:
            cur_pos = self._action_journal.get_position(txn)
            if cur_pos == position:
                return (cur_pos, [])
            elif not self._action_journal.position_exists(position + 1, txn):
                return None
            else:
                actions = self._action_journal.get_since_position(
                                    position,
                                    self.ACTIONS_BATCH_SIZE,
                                    txn
                                )
                return (cur_pos, actions)

    def _actions_retrieved(self, res, position, peer):
        if res is None:
            log.msg("No such position '{0}' needed for peer '{1}'".format(position,
                    peer.name))
            peer.server.service.send_no_position()
            d = peer.server.service.register_event('pull_request')
            d.addCallback(self._client_pull_request, peer)
        else:
            cur_pos, actions = res

            actions_len = len(actions)
            if actions_len != 0:
                log.msg("Actions for peer '{0}' retrieved".format(peer.name))
                peer.server.service.send_actions(actions)
                d = peer.server.service.register_event('pull_request')
                d.addCallback(self._client_pull_request, peer)

            if actions_len == 0 or cur_pos == actions[-1]["position"]:
                log.msg("Adding active peer '{0}' with position '{1}'".format(
                        peer.name, cur_pos))
                peer.server.service.send_wait()
                self._active_peers.append((peer, cur_pos))



    def pull(self):
        """
        Initiate pull requests to all known cluster peers.
        Method do not blocks.
        """

        for _, pdesc in self._peers.iteritems():
            peer = pdesc["peer"]
            if not pdesc["pull_in_progress"]:
                d = peer.connect()
                d.addCallback(self._peer_connected, peer)
                
                def errback(failure):
                    pdesc["pull_in_progress"] = False
                    raise failure

                d.addErrback(errback)
                d.addErrback(self._errback,
                             "Error while connecting to peer '{0}'".format(peer.name))
                pdesc["pull_in_progress"] = True

    def _peer_connected(self, connection, peer):
        log.msg("Connected to the peer '{0}'".format(peer.name))

        if not peer.client.service.is_identified():
            peer.client.service.send_ident(self._name)
            d = peer.client.service.register_event('ident_response')
            d.addCallback(self._on_ident_response, peer)
        else:
            self._do_pull_request(peer)

    def _on_ident_response(self, identified, peer):
        if identified:
            self._do_pull_request(peer)
        else:
            log.err("Unable to pull from peer '{0}': identification failed".format(
                        peer.name))
            self._peers[peer.name]["pull_in_progress"] = False

    def _do_pull_request(self, peer):
        d = threads.deferToThread(self._get_peer_position, peer)
        d.addCallback(self._got_peer_position, peer)
        d.addErrback(self._errback,
                     "Error while getting peer's '{0}' position".format(peer.name))

    def _get_peer_position(self, peer):
        """
        Blocking call to DB, should be called from thread pool.
        """

        pdb = self._dbpool.peer.dbhandle()
        pos = pdb.get(peer.name, None)
        if not pos is None:
            pos = int(pos)
        else:
            pos = 0
        return pos

    def _got_peer_position(self, pos, peer):
        log.msg("Got server position on peer '{0}': {1}".format(peer.name, pos))
        peer.client.service.send_pull_request(pos)
        d = peer.client.service.register_event('actions_received')
        d.addCallback(self._on_actions_received, peer)

    def _on_actions_received(self, actions, peer):
        """
        Apply actions from specified peer.
        Method do not blocks.
        """

        d = threads.deferToThread(self._do_apply_actions, actions, peer)
        d.addCallback(self._actions_applied, peer)
        d.addErrback(self._actions_apply_failure, peer)
        d.addErrback(self._errback, "Error while applying actions from peer "
                                    "'{0}'".format(peer.name))

    def _do_apply_actions(self, actions, peer):
        log.msg("Applying actions from peer '{0}'".format(peer.name))
        pdb = self._dbpool.peer.dbhandle()
        for act_desc in actions:
            act_dump = act_desc["action"]
            act = Action.unserialize(act_dump)
            pos = act_desc["position"]
            with self._database.transaction() as txn:
                act.apply(self._database, txn)
                pdb.put(peer.name, str(pos), txn)

    def _actions_applied(self, _, peer):
        self._do_pull_request(peer)

    def _actions_apply_failure(self, failure, peer):
        self._peers[peer.name]["pull_in_progress"] = False
        failure.trap(ActionError)
        #TODO: real failure handler
        log.msg("Unable to apply action", failure)



    def database_updated(self):
        log.msg("Updating active peers:", self._active_peers)
        while self._active_peers:
            peer, pos = self._active_peers.pop(0)
            d = threads.deferToThread(self._get_peer_update, peer, pos)
            d.addCallback(self._got_peer_update, peer, pos)
            d.addErrback(self._errback, "Error while getting update for peer "
                                        "'{0}'".format(peer.name))

    def _get_peer_update(self, peer, position):
        """
        Blocking call to DB, should be called from thread pool.
        """

        with self._database.transaction() as txn:
            cur_pos = self._action_journal.get_position(txn)
            actions = self._action_journal.get_since_position(position,
                      self.ACTIONS_BATCH_SIZE, txn)
            return (cur_pos, actions)

    def _got_peer_update(self, res, peer, position):
        cur_pos, actions = res

        if len(actions) == 0:
            log.msg("No updates for peer '{0}'".format(peer.name))
            self._active_peers.append((peer, position))
        else:
            log.msg("Got updates for peer '{0}'".format(peer.name))
            peer.server.service.send_actions(actions)
            d = peer.server.service.register_event('pull_request')
            d.addCallback(self._client_pull_request, peer)


# vim:sts=4:ts=4:sw=4:expandtab:
