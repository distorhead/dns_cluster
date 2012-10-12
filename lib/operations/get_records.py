# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.operation import OperationError
from lib.common import split
from lib import bdb_helpers


__all__ = ['GetRecordsOp']


class GetRecordsOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self.zone = self.required_data_by_key(kwargs, 'zone', str)

    def _run_in_session(self, service_provider, sessid, session_data, txn, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        self.check_zone_exists(database_srv, self.zone, txn)

        self._check_access(service_provider, sessid, session_data, None, txn)

        # retrieve arena and segment needed for lock resource
        zone_data = self.get_zone_data(database_srv, self.zone, txn)
        if not zone_data is None:
            arena = zone_data['arena']
            segment = zone_data['segment']
        else:
            arena = segment = ""

        # construct and lock resource
        resource = lock_srv.RESOURCE_DELIMITER.join([self.GLOBAL_RESOURCE, arena,
                                                     segment, self.zone])
        lock_srv.try_acquire(resource, sessid)

        res = []
        ddb = database_srv.dbpool().dns_data.dbhandle()
        zddb = database_srv.dbpool().zone_dns_data.dbhandle()

        zdkeys = bdb_helpers.get_all(zddb, self.zone, txn)
        for zdkey in zdkeys:
            recs = bdb_helpers.get_all(ddb, zdkey, txn)
            for rec in recs:
                rec_spec = self.make_rec_spec(zdkey, rec)
                if not rec_spec is None:
                    res.append(rec_spec)

        return res

    def _has_access(self, service_provider, sessid, session_data, _, txn):
        database_srv = service_provider.get('database')
        return self.has_access_to_zone(database_srv, self.zone, session_data, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
