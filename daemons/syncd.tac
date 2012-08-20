# Required steps:
# 1. Create twisted.application.service.Application object.
# 2. Assign this object to application top-level variable.
# 3. Create target service from factory and endpoint string spec.
# 4. Assign this object as parent to the target service

import lib.action

from twisted.application import service, strports
from lib.network import sync
from lib.service import ServiceProvider
from lib.actions import *


cfg = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    }
}

sp = ServiceProvider(init_srv=True, cfg=cfg)

application = service.Application("Dns cluster sync daemon")
service = strports.service("tcp:1234", sync.SyncFactory(sp.get("action_journal"),
                                                        sp.get("database")))
service.setServiceParent(application)
