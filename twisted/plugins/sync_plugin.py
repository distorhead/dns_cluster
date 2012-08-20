import yaml
import os

from zope.interface import implements
from twisted.internet import task, reactor, endpoints
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import strports

from lib.network import sync
from lib.service import ServiceProvider
from lib.actions import *


CONFIG_DEFAULT = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    },

    "server": {
        "interface": "localhost",
        "port": 1234
    },

    "sync": {
        "host": "localhost",
        "port": 1234
    }
}


class Options(usage.Options):
    optParameters = [
        ["interface", "i", None, "The host name or IP to listen on."],
        ["port", "p", None, "The port number to listen on."],
        ["sync_host", None, None, "Sync server host"],
        ["sync_port", None, None, "Sync server port"],
        ["config", "c", "/etc/dns_cluster.yaml", "Path to the configuration file."]
    ]


class SyncServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "syncd"
    description = "Dns cluster synchronization daemon"
    options = Options

    def _read_cfg(self, cfg_path):
        try:
            f = open(cfg_path, 'r')
            cfg = yaml.load(f)
            f.close()
            return cfg
        except:
            return {}

    def _setup_pull_client(self, options, cfg, sp):
        if not options["sync_host"] is None:
            host = options["sync_host"]
        else:
            host = cfg["sync"]["host"]

        if not options["sync_port"] is None:
            port = options["sync_port"]
        else:
            port = cfg["sync"]["port"]

        def connect():
            endpoint_spec = "tcp:host={host}:port={port}".format(host=host, port=port)
            ep = endpoints.clientFromString(reactor, endpoint_spec)
            d = ep.connect(sync.SyncFactory(sp.get("action_journal"),
                                            sp.get("database")))
            d.addCallback(sync.SyncProtocol.pull)

        l = task.LoopingCall(connect)
        l.start(60.0)

    def makeService(self, options):
        cfg = CONFIG_DEFAULT
        new_cfg = self._read_cfg(options["config"])
        cfg.update(new_cfg)

        if not options["interface"] is None:
            interface = options["interface"]
        else:
            interface = cfg["server"]["interface"]

        if not options["port"] is None:
            port = options["port"]
        else:
            port = cfg["server"]["port"]

        sp = ServiceProvider(init_srv=True, cfg=cfg)
        endpoint_spec = "tcp:{port}:interface={interface}".format(
                        port=port, interface=interface)
        service = strports.service(endpoint_spec, sync.SyncFactory(sp.get("action_journal"),
                                                                   sp.get("database")))

        self._setup_pull_client(options, cfg, sp)

        return service


sync_service_maker = SyncServiceMaker()
