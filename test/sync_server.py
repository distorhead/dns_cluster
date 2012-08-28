import signal

from context import *
from actions import *

from lib.network.sync.service import *
from lib.network.sync.protocol import *
from lib.network.sync.peer import *
from lib.app.sync.sync import SyncApp
from twisted.internet import reactor, endpoints

log.startLogging(sys.stdout)
peers = [ Peer('foo') ]


def pull_request(pos, s):
    log.msg("Pull request for position:", pos)
    s.send_wait()
    actions = [
        {"action": "?\0\0\0\x03data\0!\0\0\0\x02arena\0\b\0\0\0myarena\0\ndbstate\0\0\x02name\0\
    \t\0\0\0AddArena\0\0",
         "position": 0}
    ]
    s.send_actions(actions)


def ident_request(name, s):
    log.msg("Identification request received!")
    global peers
    for p in peers:
        if p.name == name:
            log.msg("Peer found!")
            s.send_ident_success()
            
            p.setup_server_connection(s.protocol)

            d = s.register_event('pull_request')
            d.addCallback(pull_request, s)
        else:
            log.msg("Peer not found!")
            s.send_ident_fail()
            d = s.register_event('ident_request')
            d.addCallback(ident_request, s)

def on_connect(conn):
    log.msg("Connected!", conn)
    s = conn.service
    d = s.register_event('ident_request')
    d.addCallback(ident_request, s)


d = Peer.listen('127.0.0.1', 1234)
d.addCallback(on_connect)

reactor.run()
