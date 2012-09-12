# -*- coding: utf-8 -*-

from lib.operations.session_operation import SessionOperation
from lib.operations.operation_helpers import OperationHelpersMixin
from lib.operation import OperationError
from lib.common import split
from lib import bdb_helpers


__all__ = ["GetRecordsOp"]


class GetRecordsOp(SessionOperation, OperationHelpersMixin):
    def __init__(self, **kwargs):
        SessionOperation.__init__(self, **kwargs)
        self.zone = self.required_data_by_key(kwargs, 'zone', str)

    def _run_in_session(self, service_provider, sessid, **kwargs):
        database_srv = service_provider.get('database')
        lock_srv = service_provider.get('lock')

        res = []
        with database_srv.transaction() as txn:
            as_pair = self.arena_segment_by_zone(database_srv, self.zone, txn)
            if not as_pair is None:
                arena, segment = as_pair
                resource = lock_srv.RESOURCE_DELIMITER.join(
                               [arena, segment, self.zone])
                lock_srv.acquire(resource, sessid)

                ddb = database_srv.dbpool().dns_data.dbhandle()
                zddb = database_srv.dbpool().zone_dns_data.dbhandle()

                zdkeys = bdb_helpers.get_all(zddb, self.zone, txn)
                for zdkey in zdkeys:
                    recs = bdb_helpers.get_all(ddb, zdkey, txn)
                    for rec in recs:
                        rec_spec = self.make_rec_spec(zdkey, rec)
                        if not rec_spec is None:
                            res.append(rec_spec)

            else:
                raise OperationError("Unable to locate zone '{}'".format(
                                        self._action.zone))

        return res


# vim:sts=4:ts=4:sw=4:expandtab:
