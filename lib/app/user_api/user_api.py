# -*- coding: utf-8 -*-

import os
import signal

from lib import database
from lib import session
from lib import lock
from lib.network.user_api.resources import *
from lib.operations import *
from lib.common import retrieve_key

from twisted.application import strports
from twisted.web import server, resource
from twisted.python import log


class UserApiAppError(Exception): pass


class UserApiApp(object):
    @classmethod
    def cfg_failure(cls, msg):
        raise UserApiAppError("Configuration failure: {}".format(msg))

    @classmethod
    def required_key(cls, cfg, key, msg):
        return retrieve_key(cfg, key, failure_func=cls.cfg_failure, failure_msg=msg)

    def __init__(self, cfg, sp):
        self._interface = self.required_key(cfg, 'interface', "interface required")
        self._port = self.required_key(cfg, 'port', "port required")
        self._syncd_pid_path = self.required_key(cfg, 'syncd_pid_path', 
                                                     "syncd_pid_path required")
        self._sp = sp
        self._syncd_pid_last_mtime = None
        self._syncd_pid = None

    def make_service(self):
        endpoint_spec = "tcp:interface={interface}:port={port}".format(
                         interface=self._interface, port=self._port)

        root = resource.Resource()
        root.putChild('get_arenas', GetArenasResource(self._sp))
        root.putChild('get_segments', GetSegmentsResource(self._sp))
        root.putChild('get_zones', GetZonesResource(self._sp))
        root.putChild('get_records', GetRecordsResource(self._sp))
        root.putChild('begin_session', BeginSessionResource(self._sp))
        root.putChild('rollback_session', RollbackSessionResource(self._sp))
        root.putChild('keepalive_session', KeepaliveSessionResource(self._sp))

        self.add_database_change_resource(
            root, 'add_arena', AddArenaResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'del_arena', DelArenaResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'add_segment', AddSegmentResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'del_segment', DelSegmentResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'add_zone', AddZoneResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'del_zone', DelZoneResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'add_record', AddRecordResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'del_record', DelRecordResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'commit_session', CommitSessionResource(self._sp)
        )
        self.add_database_change_resource(
            root, 'mod_auth', ModAuthResource(self._sp)
        )

        factory = server.Site(root)
        twisted_service = strports.service(endpoint_spec, factory)
        return twisted_service

    def add_database_change_resource(self, root, resource_path, resource):
        root.putChild(resource_path, resource)
        d = resource.register_event('operation_done')
        d.addCallback(self.on_operation_done, resource)

    def on_operation_done(self, operation, resource):
        # magic hack
        if operation.sessid is None or operation.__class__ == SessionCommitOp:
            self.database_updated()

        d = resource.register_event('operation_done')
        d.addCallback(self.on_operation_done, resource)

    def database_updated(self):
        try:
            if os.path.exists(self._syncd_pid_path):
                new_mtime = os.stat(self._syncd_pid_path).st_mtime
                if (self._syncd_pid is None or
                    new_mtime > self._syncd_pid_last_mtime):

                    log.msg("Reading new pid from '{}'".format(self._syncd_pid_path))
                    self._syncd_pid = int(open(self._syncd_pid_path, 'r').read())
                    self._syncd_pid_last_mtime = new_mtime

            if not self._syncd_pid is None:
                log.msg("Database updated: sending signal to '{}'".format(
                            self._syncd_pid))
                os.kill(self._syncd_pid, signal.SIGUSR2)

        except:
            log.err("Unable to send database updated signal to syncd")


# vim:sts=4:ts=4:sw=4:expandtab:
