# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.actions.mod_auth import ModAuth


__all__ = ['ModAuthOp']


class ModAuthOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self._kwargs = kwargs

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        session_srv = service_provider.get('session')
        lock_srv = service_provider.get('lock')
        database_srv = service_provider.get('database')

        # validation of arguments also goes here
        do_action = ModAuth(**self._kwargs)

        self._check_access(service_provider, sessid, session_data, do_action, txn)

        # retrieve old key from database needed for undo action
        auth_data = self.get_auth_data(database_srv, do_action.target, txn)
        if not auth_data is None:
            key = auth_data['key']
        else:
            # here we know that arena doesn't exists
            #   or in inconsistent state,
            #   so reset key is the choise
            key = ""

        undo_action = ModAuth(target=do_action.target, key=key)

        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE,
                                                     do_action.target])
        lock_srv.try_acquire(resource, sessid)

        session_srv.apply_action(sessid, do_action, undo_action, txn=txn)

    def _has_access(self, service_provider, sessid, session_data, action, txn):
        return self.is_admin(session_data)


# vim:sts=4:ts=4:sw=4:expandtab:
