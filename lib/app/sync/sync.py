# -*- coding: utf-8 -*-

import random
import hashlib

from lib import database
from lib.action import Action, ActionError
from lib.network.sync.peer import Peer
from lib.network.sync.protocol import SyncServerFactory
from lib.common import retrieve_key

from twisted.internet import threads, reactor, task, endpoints
from twisted.python import log


class SyncAppError(Exception): pass


class SyncApp(object):
    ACTIONS_BATCH_SIZE = 50

    DATABASES = {
        "peer": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    @classmethod
    def cfg_failure(cls, msg):
        raise SyncAppError("Configuration failure: {}".format(msg))

    @classmethod
    def required_key(cls, cfg, key, msg):
        return retrieve_key(cfg, key, failure_func=cls.cfg_failure, failure_msg=msg)

    def __init__(self, cfg, db, action_journal, **kwargs):
        srv = self.required_key(cfg, 'server', "server config section required")
        self._name = self.required_key(srv, 'name', "server name required")

        transport_encrypt = cfg.get('transport-encrypt', True)

        # initialize server endpoint data
        self._endpoint_data = {
            'transport-encrypt': transport_encrypt,
            'interface': self.required_key(srv, 'interface',
                                           "server interface required"),
            'port': self.required_key(srv, 'port', "server port required")
        }

        if transport_encrypt:
            self._endpoint_data['private-key'] = self.required_key(srv, 'private-key',
                    "server private-key field required "
                    "for transport encryption mode")
            self._endpoint_data['cert'] = self.required_key(srv, 'cert',
                    "server cert field required "
                    "for transport encryption mode")

        self._database = db
        self._action_journal = action_journal
        self._pull_period = kwargs.get('pull_period', 10.0)
        self._active_peers = []
        self._peers = {}
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())

        peers = self.required_key(cfg, 'peers', {})

        # application just started -- block operations allowed now
        pdb = self._dbpool.peer.dbhandle()
        with self._database.transaction() as txn:
            for pname in peers:
                pdesc = peers[pname]
                peerdata = {
                    'client_host': self.required_key(pdesc, 'host',
                            "host required for peer '{}'".format(pname)),

                    'client_port': self.required_key(pdesc, 'port',
                            "port required for peer '{}'".format(pname)),

                    'client_auth_schema': pdesc.get('auth', "chap")
                }

                if peerdata['client_auth_schema'] not in ("pap", "chap"):
                    raise self.cfg_failure("unknown value '{}' for auth option "
                                           "for peer '{}': "
                                           "allowed 'pap' or 'chap'".format(
                                           peerdata['client_auth_schema'],
                                           pname))

                if transport_encrypt:
                    peerdata['client_transport_encrypt'] = True

                pkey = self.required_key(pdesc, 'key', "key required for peer "
                                         "'{}'".format(pname))

                peer = Peer(pname, pkey, **peerdata)
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


    def _setup_client_auth_request_pap_handler(self, service):
        d = service.register_event('auth_request_pap')
        d.addCallback(self._client_auth_request_pap_handler, service)

    def _setup_client_auth_request_chap_handler(self, service):
        d = service.register_event('auth_request_chap')
        d.addCallback(self._client_auth_request_chap_handler, service)

    def _setup_client_pull_request_handler(self, peer):
        d = peer.server.service.register_event('pull_request')
        d.addCallback(self._client_pull_request_handler, peer)

    def _setup_client_auth_challenge_handler(self, ch_res, service, peer):
        d = service.register_event('auth_challenge')
        d.addCallback(self._client_auth_challenge_handler, ch_res, service, peer)


    def _setup_server_auth_response_handler(self, peer):
        d = peer.client.service.register_event('auth_response')
        d.addCallback(self._server_auth_response_handler, peer)

    def _setup_server_auth_challenge_handler(self, peer):
        d = peer.client.service.register_event('auth_challenge')
        d.addCallback(self._server_auth_challenge_handler, peer)


    def _calc_challenge_res(self, challenge, pkey):
        return hashlib.md5(str(challenge) + str(pkey)).hexdigest()


    def start_pull(self):
        l = task.LoopingCall(self.pull)
        l.start(self._pull_period)

    def make_service(self, cfg):
        if cfg.has_key('accept-auth'):
            if cfg['accept-auth'] == "pap":
                self._pap_auth = True
                self._chap_auth = False
            elif cfg['accept-auth'] == "chap":
                self._chap_auth = True
                self._pap_auth = False
            elif cfg['accept-auth'] == "any":
                self._pap_auth = True
                self._chap_auth = True
            else:
                raise self.cfg_failure("unknown value '{}' for accept-auth option: "
                                       "allowed 'pap', 'chap', 'any'".format(
                                            cfg['accept-auth']))
        else:
            self._chap_auth = True
            self._pap_auth = False

        return Peer.make_service(self._endpoint_data, self._client_connected)


    def _client_connected(self, conn):
        log.msg("New client connected")

        # new client connected, retrieve client-related service
        #   (each client has own service object)
        service = conn.service

        if self._pap_auth:
            self._setup_client_auth_request_pap_handler(service)

        if self._chap_auth:
            self._setup_client_auth_request_chap_handler(service)

    def _client_auth_request_pap_handler(self, res, service):
        name, pswd = res
        if not self._peers.has_key(name):
            # unknown peer name
            service.send_auth_fail()
            self._setup_client_auth_request_pap_handler(service)

        else:
            peer = self._peers[name]['peer']
            if peer.key == pswd:
                peer.setup_server_connection(service.protocol)
                service.send_auth_success()
                self._setup_client_pull_request_handler(peer)

            else:
                service.send_auth_fail()
                self._setup_client_auth_request_pap_handler(service)

    def _client_auth_request_chap_handler(self, name, service):
        if not self._peers.has_key(name):
            service.send_auth_fail()
            self._setup_client_auth_request_chap_handler(service)

        else:
            peer = self._peers[name]['peer']
            
            ch_len = random.randint(10, 100)
            challenge = ""
            while ch_len:
                challenge += chr(random.randint(0, 255))
                ch_len -= 1

            ch_res = self._calc_challenge_res(challenge, peer.key)

            service.send_auth_challenge(challenge)
            self._setup_client_auth_challenge_handler(ch_res, service, peer)

    def _client_auth_challenge_handler(self, client_ch_res, ch_res, service, peer):
        # Method should link service (from which auth request received)
        #   with desired peer.
        if client_ch_res != ch_res:
            # failed attempt
            service.send_auth_fail()
            self._setup_client_auth_request_chap_handler(service)
        else:
            # link service with peer
            peer.setup_server_connection(service.protocol)
            service.send_auth_success()
            self._setup_client_pull_request_handler(peer)


    def _client_pull_request_handler(self, position, peer):
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
        if peer.server is None:
            return

        if res is None:
            log.msg("No such position '{0}' needed for peer '{1}'".format(position,
                    peer.name))
            peer.server.service.send_no_position()
            self._setup_client_pull_request_handler(peer)

        else:
            cur_pos, actions = res

            actions_len = len(actions)
            if actions_len != 0:
                log.msg("Actions for peer '{0}' retrieved".format(peer.name))
                peer.server.service.send_actions(actions)
                self._setup_client_pull_request_handler(peer)
            else:
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

            #TODO:
            # Possibly it's better to add event 'connection_lost'
            #   to peer class
            if peer.client is None:
                # connection lost, reset pull state
                pdesc["pull_in_progress"] = False

            if not pdesc["pull_in_progress"]:
                d = peer.connect()
                d.addCallback(self._peer_connected, peer)

                def errback(failure, pdesc):
                    log.msg("Unsetting pull in progress state for peer '{0}'".format(
                                pdesc["peer"].name))
                    pdesc["pull_in_progress"] = False
                    raise failure

                d.addErrback(errback, pdesc)
                d.addErrback(self._errback,
                             "Error while connecting to peer '{0}'".format(peer.name))
                pdesc["pull_in_progress"] = True

    def _peer_connected(self, connection, peer):
        log.msg("Connected to the peer '{0}'".format(peer.name))

        if peer.client.service.is_authenticated():
            self._do_pull_request(peer)

        else:
            if peer.client_auth_schema == "pap":
                peer.client.service.send_auth_pap(self._name, peer.key)
                self._setup_server_auth_response_handler(peer)
            elif peer.client_auth_schema == "chap":
                peer.client.service.send_auth_chap(self._name)
                self._setup_server_auth_challenge_handler(peer)

    def _server_auth_challenge_handler(self, challenge, peer):
        ch_res = self._calc_challenge_res(challenge, peer.key)
        peer.client.service.send_auth_challenge(ch_res)
        self._setup_server_auth_response_handler(peer)

    def _server_auth_response_handler(self, authenticated, peer):
        if authenticated:
            self._do_pull_request(peer)
        else:
            log.err("Unable to pull from peer '{}': authentication failed".format(
                        peer.name))
            self._peers[peer.name]["pull_in_progress"] = False

    def _do_pull_request(self, peer):
        if peer.client is None:
            return

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
        try:
            pos = int(pos)
        except:
            pos = 0
        return pos

    def _got_peer_position(self, pos, peer):
        if peer.client is None:
            return

        log.msg("Got server position on peer '{0}': {1}".format(peer.name, pos))
        peer.client.service.send_pull_request(pos)
        d = peer.client.service.register_event('actions_received')
        d.addCallback(self._on_actions_received, peer, pos)

    def _on_actions_received(self, actions, peer, pos):
        """
        Apply actions from specified peer.
        Method do not blocks.
        """

        if actions is None:
            log.msg("No position {0} on peer '{1}'".format(pos, peer.name))
            self._peers[peer.name]["pull_in_progress"] = False
        else:
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
            self._setup_client_pull_request_handler(peer)


# vim:sts=4:ts=4:sw=4:expandtab:
