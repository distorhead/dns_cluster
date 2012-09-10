# -*- coding: utf-8 -*-

from zope.interface import implements
from twisted.internet import reactor
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from lib.app.user_api.user_api import UserApiApp


class Options(usage.Options):
    optParameters = [
        ["interface", "i", None, "The host name or IP to listen on."],
        ["port", "p", None, "The port number to listen on."],
        ["config", "c", "/etc/dns_cluster/user_apid.yaml",
            "Path to the configuration file."]
    ]


class UserApiServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "user_apid"
    description = "Dns cluster user api daemon"
    options = Options

    def makeService(self, options):
        self._sa = UserApiApp()
        return self._sa.make_service()


user_api_service_maker = UserApiServiceMaker()


# vim:sts=4:ts=4:sw=4:expandtab:
