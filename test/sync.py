from context import *
from actions import *

from lib.app.sync.sync import SyncApp
from twisted.internet import reactor


sa = SyncApp(cfg["peers"], database, a_journal)
pdb = sa._dbpool.peer.dbhandle()


log.startLogging(sys.stdout)
reactor.callLater(1.0, sa.pull)
