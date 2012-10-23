# -*- coding: utf-8 -*-

import yaml

from zope.interface import implements
from twisted.internet import reactor
from twisted.python import usage, log
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
    "syncd_pid_path": "twistd.pid",
    "transport-encrypt": False
}


class Options(usage.Options):
    optParameters = [
        ["interface", "i", None, "The host name or IP to listen on."],
        ["port", "p", None, "The port number to listen on."],
        ["config", "c", "/etc/dns_cluster/user_apid.yaml",
            "Path to the configuration file."],
        ["syncd_pid_path", "s", None,
            "Path to the file containing pid of the syncd daemon."],
        ["transport-encrypt", "e", None, "Enable/disable transport encryption [yes (default)/no]"],
        ["private-key", "k", None, "Private key file for transport encryption"],
        ["cert", "s", None, "Certificate file for transport encryption"],
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
        try:
            cfg = CONFIG_DEFAULT
            new_cfg = self._read_cfg(options['config'])
            cfg.update(new_cfg)

            if not options['interface'] is None:
                cfg['interface'] = options['interface']

            if not options['port'] is None:
                cfg['port'] = options['port']

            if not options['syncd_pid_path'] is None:
                cfg['syncd_pid_path'] = options['syncd_pid_path']

            if not options['private-key'] is None:
                cfg['private-key'] = options['private-key']

            if not options['cert'] is None:
                cfg['cert'] = options['cert']

            if not options['transport-encrypt'] is None:
                val = options['transport-encrypt']
                if val == "yes":
                    cfg['transport-encrypt'] = True
                elif val == "no":
                    cfg['transport-encrypt'] = False
                else:
                    raise Exception("unknown value for transport-encrypt: "
                                    "'{}'".format(val));

            sp = ServiceProvider(init_srv=True, cfg=cfg)
            self._app = UserApiApp(cfg, sp)
            return self._app.make_service()

        except Exception, e:
            log.err(str(e))
            exit(1)


user_api_service_maker = UserApiServiceMaker()


# vim:sts=4:ts=4:sw=4:expandtab:
