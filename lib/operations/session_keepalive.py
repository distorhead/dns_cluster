# -*- coding: utf-8 -*-

from lib.operation import Operation
from lib.twisted_helpers import threaded


__all__ = ['SessionKeepaliveOp']


class SessionKeepaliveOp(Operation):
    def __init__(self, **kwargs):
        Operation.__init__(self, **kwargs)
        self.sessid = self.required_data_by_key(kwargs, 'sessid', int)

    @threaded
    def _do_run(self, service_provider, **kwargs):
        session_srv = service_provider.get('session')
        session_srv.keepalive_session(self.sessid)


# vim:sts=4:ts=4:sw=4:expandtab:
