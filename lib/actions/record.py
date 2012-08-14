# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.dbstate import Dbstate
from lib.common import reorder, split


class RecordAction(Action, Dbstate):
    """
    Class implements common for record actions part,
      by providing _create_rec and  _delete_rec methods.
    Subclasses should implement following methods:
      * _make_error_msg -- error message constructor
      * _is_record_equal -- record equality checker
    """

    TTL_DEFAULT = 100

    def __init__(self, **kwargs):
        super(RecordAction, self).__init__(**kwargs)
        self.zone = self.required_data_by_key(kwargs, "zone", str)

    def _current_dbstate(self, database, txn):
        return self.get_zone(self.zone, database, txn)

    def _create_rec(self, database, txn, host, ttl, rec_data, add_host=False):
        zdb = database.dbpool().dns_zone.open()
        self._check_zone(zdb, txn)
        zdb.close()

        if add_host:
            xdb = database.dbpool().dns_xfr.open()
            self._add_host(xdb, txn, host)
            xdb.close()

        ddb = database.dbpool().dns_data.open()
        zddb = database.dbpool().zone_dns_data.open()

        dkey = self.zone + ' ' + host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = split(rec)
            if self._is_record_equal(rlist):
                raise ActionError(self._make_error_msg("record already exist"))

        seq = database.dbpool().sequence.sequence("dns_data")
        newid = seq.get(1, txn)
        seq.close()

        raw_rec = " ".join([str(newid), "@", str(ttl), rec_data])
        ddb.put(dkey, raw_rec, txn)
        zddb.put(self.zone, dkey, txn)

        ddb.close()
        zddb.close()
        self.update_zone(self.zone, database, txn)

    def _delete_rec(self, database, txn, host, del_host=False):
        zdb = database.dbpool().dns_zone.open()
        self._check_zone(zdb, txn)
        zdb.close()

        if del_host:
            xdb = database.dbpool().dns_xfr.open()
            self._del_host(xdb, txn, host)
            xdb.close()

        ddb = database.dbpool().dns_data.open()
        zddb = database.dbpool().zone_dns_data.open()

        found = False
        dkey = self.zone + ' ' + host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = split(rec)
            if self._is_record_equal(rlist):
                found = True
                bdb_helpers.delete_pair(ddb, dkey, rec, txn)

        if not found:
            raise ActionError(self._make_error_msg("record doesn't exist"))

        bdb_helpers.delete_pair(zddb, self.zone, dkey, txn)

        ddb.close()
        zddb.close()
        self.update_zone(self.zone, database, txn)

    def _check_zone(self, zdb, txn):
        zone_rname = reorder(self.zone)
        if not zdb.exists(zone_rname, txn):
            raise ActionError(self._make_error_msg("zone doesn't exist"))

    def _add_host(self, xdb, txn, host):
        hosts = bdb_helpers.get_all(xdb, self.zone, txn)
        if not host in hosts:
            xdb.put(self.zone, host, txn)

    def _del_host(self, xdb, txn, host):
        hosts = bdb_helpers.get_all(xdb, self.zone, txn)
        if host in hosts:
            bdb_helpers.delete_pair(xdb, self.zone, host, txn)

    def _make_error_msg(self, reason):
        assert 0, "Error message constructor is not implemented"

    def _is_record_equal(self, rlist):
        assert 0, "Record equal checker is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
