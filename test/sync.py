import signal

from context import *
from actions import *

from lib.network.sync.service import *
from lib.network.sync.protocol import *
from lib.network.sync.peer import *
from lib.app.sync.sync import SyncApp
from twisted.internet import reactor


log.startLogging(sys.stdout)


sa = SyncApp(cfg["server"]["interface"],
             cfg["server"]["port"],
             cfg["peers"], database, a_journal)
pdb = sa._dbpool.peer.dbhandle()


def sighandler(signum, _):
    print 'sighandler({0}, {1}) {{'.format(signum, _)
    sa.database_updated()
    print '}} sighandler({0}, {1})'.format(signum, _)

signal.signal(signal.SIGUSR2, sighandler)


def s():
    global sa
    global reactor
    sa.listen()
    reactor.run()
