# -*- coding: utf-8 -*-

from lib.operation import Operation


__all__ = ["SessionBeginOp"]


class SessionBeginOp(Operation):
    def _do_run(self, service_provider, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')

        sessid = session_srv.begin_session(watchdog=True)
        d = session_srv.set_watchdog(sessid)
        d.addCallback(lambda x: lock_srv.release_session(sessid))
        return sessid


# vim:sts=4:ts=4:sw=4:expandtab:
