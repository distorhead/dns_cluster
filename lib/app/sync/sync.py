# -*- coding: utf-8 -*-

from lib import database
from lib.action import Action, ActionError
from lib.app.sync.peer import Peer
from twisted.internet import threads, reactor, task
from twisted.python import log


class SyncApp(object):
    DATABASES = {
        "peer": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    def __init__(self, peers, db, action_journal, **kwargs):
        self._database = db
        self._action_journal = action_journal
        self._pull_period = kwargs.get("pull_period", 10.0)
        self._active_peers = {} #TODO
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

    def apply_actions(self, actions, peer):
        """
        Apply actions from specified peer.
        Method returns deferred activated when actions applied.
        """

        d = threads.deferToThread(self._do_apply_actions, actions, peer)
        d.addErrback(self._actions_apply_failure)
        d.addErrback(self._errback, "Error while handling actions")
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


    def pull(self):
        """
        Initiate pull requests to all known cluster servers.
        Method do not blocks.
        """

        for pname in self._peers:
            peer = self._peers[pname]
            d = peer.connect(lambda x: self.apply_actions(x, peer))
            d.addCallback(self._peer_connected, peer)
            d.addErrback(self._errback,
                         "Error while connecting to peer '{0}'".format(peer.name))

    def _peer_connected(self, connection, peer):
        d = threads.deferToThread(self._get_peer_position, peer)
        d.addCallback(self._got_peer_position, peer)
        d.addErrback(self._errback,
                     "Error while getting peer's '{0}' position".format(peer.name))

    def _get_peer_position(self, peer):
        """
        Blocking call to DB, should be called from thread pool
        """

        pdb = self._dbpool.peer.dbhandle()
        return int(pdb.get(peer.name, None))

    def _got_peer_position(self, pos, peer):
        if not pos is None:
            peer.connection.pull_request(pos)
        else:
            peer.connection.pull_request(-1)

    def _errback(self, failure, desc):
        log.err(desc)
        log.err(failure)


# vim:sts=4:ts=4:sw=4:expandtab:
