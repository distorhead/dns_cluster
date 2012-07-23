# -*- coding: utf-8 -*-

from lib import database
from lib import bdb_helpers
from lib.action import Action, ActionError
from lib.common import reorder, split


class AddRecord(Action):
    """
    Class implements common for record actions part,
      by providing _create_rec and  _delete_rec methods.
    Subclasses should implement following methods:
      * _make_error_msg -- error message constructor
      * _is_record_equal -- record equality checker
    """

    TTL_DEFAULT = 100

    def __init__(self, **kwargs):
        super(AddRecord, self).__init__(**kwargs)

        self.zone = self.required_data_by_key(kwargs, "zone", str)
        self.ttl = self.optional_data_by_key(kwargs, "ttl", int, self.TTL_DEFAULT)

    def _rec_list(self, record):
        return [item for item in self._SPLIT_REGEX.finditer(record)]

    def _create_rec(self, txn, host, rec_data, add_host=False):
        action = "add"

        zdb = database.context().dbpool().dns_zone.open()
        self._check_zone(zdb, txn, action)
        zdb.close()

        if add_host:
            xdb = database.context().dbpool().dns_xfr.open()
            self._add_host(xdb, txn, host)
            xdb.close()

        ddb = database.context().dbpool().dns_data.open()

        dkey = self.zone + ' ' + host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = split(rec)
            if self._is_record_equal(rlist):
                raise ActionError(self._make_error_msg(action,
                                  "record already exist"))

        seq = database.context().dbpool().sequence.sequence("dns_data")
        newid = seq.get(1, txn)
        seq.close()

        raw_rec = " ".join([str(newid), "@", str(self.ttl), rec_data])
        ddb.put(dkey, raw_rec, txn)

        ddb.close()

    def _delete_rec(self, txn, host, del_host=False):
        action = "delete"

        zdb = database.context().dbpool().dns_zone.open()
        self._check_zone(zdb, txn, action)
        zdb.close()

        if del_host:
            xdb = database.context().dbpool().dns_xfr.open()
            self._del_host(xdb, txn, host)
            xdb.close()

        ddb = database.context().dbpool().dns_data.open()

        found = False
        dkey = self.zone + ' ' + host
        for rec in bdb_helpers.get_all(ddb, dkey, txn):
            rlist = split(rec)
            if self._is_record_equal(rlist):
                found = True
                bdb_helpers.delete_pair(ddb, dkey, rec, txn)

        if not found:
            raise ActionError(self._make_error_msg(action,
                              "record doesn't exist"))

        ddb.close()

    def _check_zone(self, zdb, txn, action):
        zone_rname = reorder(self.zone)
        if not zdb.exists(zone_rname, txn):
            raise ActionError(self._make_error_msg(action,
                              "zone doesn't exist"))

    def _add_host(self, xdb, txn, host):
        hosts = bdb_helpers.get_all(xdb, self.zone, txn)
        if not host in hosts:
            xdb.put(self.zone, host, txn)

    def _del_host(self, xdb, txn, host):
        hosts = bdb_helpers.get_all(xdb, self.zone, txn)
        if host in hosts:
            bdb_helpers.delete_pair(xdb, self.zone, host, txn)

    def _make_error_msg(self, action, reason):
        assert 0, "Error message constructor is not implemented"

    def _is_record_equal(self, rlist):
        assert 0, "Record equal checker is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
