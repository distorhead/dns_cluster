# Required steps:
# 1. Create twisted.application.service.Application object.
# 2. Assign this object to application top-level variable.
# 3. Create target service from factory and endpoint string spec.
# 4. Assign this object as parent to the target service

import lib.action

from twisted.application import service, strports
from twisted.internet import task, reactor
from twisted.internet import endpoints
from lib.network import sync
from lib.service import ServiceProvider
from lib.actions import *


cfg = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    }
}


def setup_pull_client(cfg, sp):
    def connect():
        ep = endpoints.clientFromString(reactor, "tcp:host=localhost:port=4321")
        d = ep.connect(sync.SyncFactory(sp.get("action_journal"),
                                        sp.get("database")))
        d.addCallback(sync.SyncProtocol.pull)

    l = task.LoopingCall(connect)
    l.start(60.0)


def setup_pull_server(cfg, sp):
    global service

    application = service.Application("Dns cluster sync daemon")
    #TODO: take endpoint spec from cfg
    service = strports.service("tcp:1234", sync.SyncFactory(sp.get("action_journal"),
                                                            sp.get("database")))
    service.setServiceParent(application)
    return (application, service)


sp = ServiceProvider(init_srv=True, cfg=cfg)
setup_pull_client(cfg, sp)
application, service = setup_pull_server(cfg, sp)

