import signal

from context import *
from actions import *

from lib.network.sync.service import *
from lib.network.sync.protocol import *
from lib.network.sync.peer import *
from lib.app.sync.sync import SyncApp
from twisted.internet import reactor


log.startLogging(sys.stdout)
p = Peer('foo', client_host='127.0.0.1', client_port=4321)

def on_error(fail):
    log.err(fail)


def actions_received(res, service):
    if res is None:
        log.msg('No position')
    else:
        log.msg("Actions:", res)


def ident_response_received_cb(res, service):
    if res:
        log.msg('Identification passed! service =', service)
        service.send_pull_request(0)
        d = service.register_event('actions')
        d.addCallback(actions_received, service)
    else:
        log.msg('Identification failed!')


def on_connect(conn):
    log.msg('on_connect {')
    conn.service.send_ident()
    d = conn.service.register_event('ident_response')
    d.addCallback(ident_response_received_cb, conn.service)
    log.msg('} on_connect')

d = p.connect()
d.addCallback(on_connect)
d.addErrback(on_error)

reactor.run()
