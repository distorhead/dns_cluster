import signal

from context import *
from actions import *

from lib.network.sync.service import *
from lib.network.sync.protocol import *
from lib.network.sync.peer import *
from lib.app.sync.sync import SyncApp
from twisted.internet import reactor


_dbpool = lib.database.DatabasePool(SyncApp.DATABASES,
                                    database.dbenv(),
                                    database.dbfile())
pdb = _dbpool.peer.dbhandle()
