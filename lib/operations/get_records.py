# -*- coding: utf-8 -*-

from lib.operation import Operation
from lib.operations.record_operation import RecordOperation
from lib.common import split
from lib import bdb_helpers


__all__ = ["GetRecordsOp"]


class GetRecordsOp(Operation, RecordOperation):
    def __init__(self, database_srv, session_srv, **kwargs):
        Operation.__init__(self, database_srv, session_srv, **kwargs)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _do_run(self):
        res = []
        with self.database_srv.transaction() as txn:
            ddb = self.database_srv.dbpool().dns_data.dbhandle()
            zddb = self.database_srv.dbpool().zone_dns_data.dbhandle()

            zdkeys = bdb_helpers.get_all(zddb, self.zone, txn)
            for zdkey in zdkeys:
                recs = bdb_helpers.get_all(ddb, zdkey, txn)
                for rec in recs:
                    rec_spec = self.make_rec_spec(zdkey, rec)
                    if not rec_spec is None:
                        res.append(rec_spec)

        return res


# vim:sts=4:ts=4:sw=4:expandtab:
