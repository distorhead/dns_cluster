# -*- coding: utf-8 -*-

from lib import database
from lib import session
from lib import lock
from lib.network.user_api.resources import *

from twisted.application import strports
from twisted.web import server, resource


class UserApiApp(object):
    def __init__(self, interface, port, sp):
        self._interface = interface
        self._port = port
        self._sp = sp

    def make_service(self):
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                         interface=self._interface, port=self._port)

        root = resource.Resource()
        root.putChild('get_arenas', GetArenasResource(self._sp))
        root.putChild('add_arena', AddArenaResource(self._sp))
        root.putChild('del_arena', DelArenaResource(self._sp))
        root.putChild('get_segments', GetSegmentsResource(self._sp))
        root.putChild('add_segment', AddSegmentResource(self._sp))
        root.putChild('del_segment', DelSegmentResource(self._sp))
        root.putChild('get_zones', GetZonesResource(self._sp))
        root.putChild('add_zone', AddZoneResource(self._sp))
        root.putChild('del_zone', DelZoneResource(self._sp))
        root.putChild('get_records', GetRecordsResource(self._sp))
        root.putChild('add_record', AddRecordResource(self._sp))
        root.putChild('del_record', DelRecordResource(self._sp))
        root.putChild('begin_session', BeginSessionResource(self._sp))
        root.putChild('commit_session', CommitSessionResource(self._sp))
        root.putChild('rollback_session', RollbackSessionResource(self._sp))
        root.putChild('keepalive_session', KeepaliveSessionResource(self._sp))
        factory = server.Site(root)

        twisted_service = strports.service(endpoint_spec, factory)
        return twisted_service


# vim:sts=4:ts=4:sw=4:expandtab:
