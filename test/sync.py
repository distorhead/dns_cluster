import signal

from context import *
from actions import *

from lib.app.sync.sync import SyncApp
from twisted.internet import reactor


sa = SyncApp(cfg["server"]["interface"],
             cfg["server"]["port"],
             cfg["peers"], database, a_journal)
pdb = sa._dbpool.peer.dbhandle()
log.startLogging(sys.stdout)


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
