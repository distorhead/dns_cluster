# -*- coding: utf-8 -*-

import yaml

from zope.interface import implements
from twisted.internet import reactor
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from lib.service import ServiceProvider
from lib.app.user_api.user_api import UserApiApp


CONFIG_DEFAULT = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    },

    "interface": "localhost",
    "port": 2100,
    "syncd_pid_path": "twistd.pid"
}


class Options(usage.Options):
    optParameters = [
        ["interface", "i", None, "The host name or IP to listen on."],
        ["port", "p", None, "The port number to listen on."],
        ["config", "c", "/etc/dns_cluster/user_apid.yaml",
            "Path to the configuration file."],
        ["syncd_pid_path", "s", None,
            "Path to the file containing pid of the syncd daemon."]
    ]


class UserApiServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "user_apid"
    description = "Dns cluster user api daemon"
    options = Options

    def _read_cfg(self, cfg_path):
        try:
            f = open(cfg_path, 'r')
            cfg = yaml.load(f)
            f.close()

            if cfg is None:
                return {}
            else:
                return cfg
        except:
            return {}

    def makeService(self, options):
        cfg = CONFIG_DEFAULT
        new_cfg = self._read_cfg(options["config"])
        cfg.update(new_cfg)

        if not options["interface"] is None:
            interface = options["interface"]
        else:
            interface = cfg["interface"]

        if not options["port"] is None:
            port = options["port"]
        else:
            port = cfg["port"]

        if not options["syncd_pid_path"] is None:
            syncd_pid_path = options["syncd_pid_path"]
        else:
            syncd_pid_path = cfg["syncd_pid_path"]

        sp = ServiceProvider(init_srv=True, cfg=cfg)
        self._app = UserApiApp(interface, port, syncd_pid_path, sp)
        return self._app.make_service()


user_api_service_maker = UserApiServiceMaker()


# vim:sts=4:ts=4:sw=4:expandtab:
