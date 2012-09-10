# -*- coding: utf-8 -*-

import yaml
import signal

from zope.interface import implements
from twisted.internet import reactor
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from lib.app.sync.sync import SyncApp
from lib.service import ServiceProvider
from lib.actions import *


CONFIG_DEFAULT = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    },

    "server": {
        "name": "sync",
        "interface": "localhost",
        "port": 1234
    },

    "peers": {}
}


class Options(usage.Options):
    optParameters = [
        ["name", "n", None, "Server identificator."],
        ["interface", "i", None, "The host name or IP to listen on."],
        ["port", "p", None, "The port number to listen on."],
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

    def _sighandler(self, signum, _):
        self._sa.database_updated()

    def makeService(self, options):
        cfg = CONFIG_DEFAULT
        new_cfg = self._read_cfg(options["config"])
        cfg.update(new_cfg)

        if not options["name"] is None:
            name = options["name"]
        else:
            name = cfg["server"]["name"]

        if not options["interface"] is None:
            interface = options["interface"]
        else:
            interface = cfg["server"]["interface"]

        if not options["port"] is None:
            port = options["port"]
        else:
            port = cfg["server"]["port"]

        peers = cfg["peers"]

        sp = ServiceProvider(init_srv=True, cfg=cfg)
        self._sa = SyncApp(name, interface, port, peers,
                           sp.get("database"),
                           sp.get("action_journal"))

        signal.signal(signal.SIGUSR2, self._sighandler)
        self._sa.start_pull()
        return self._sa.make_service()


sync_service_maker = SyncServiceMaker()


# vim:sts=4:ts=4:sw=4:expandtab:
