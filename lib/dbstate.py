# -*- coding: utf-8 -*-

import hashlib

from twisted.python import log

from lib.bdb_helpers import get_all, keys
from lib.common import reorder


class Dbstate(object):
    DELIMITER = "_"
    GLOBAL_STATE = "global"
    ARENA_STATE_PREFIX = "a"
    SEGMENT_STATE_PREFIX = "s"
    ZONE_STATE_PREFIX = "z"

    def _make_key(self, resource_list):
        return self.DELIMITER.join(resource_list)

    def _global_key(self):
        return self.GLOBAL_STATE

    def _arena_key(self, arena):
        return self._make_key([self.ARENA_STATE_PREFIX, arena])

    def _segment_key(self, arena, segment):
        return self._make_key([self.SEGMENT_STATE_PREFIX, arena, segment])

    def _zone_key(self, zone):
        return self._make_key([self.ZONE_STATE_PREFIX, zone])

    def _get_state(self, key, database, txn=None):
        sdb = database.dbpool().dbstate.open()
        res = sdb.get(key, None, txn)
        sdb.close()
        return res

    def _make_state(self, data):
        try:
            return hashlib.md5(data).digest()
        except:
            log.err("Unable to make hash of '{0}'".format(data))
            return ""

    def get_global(self, database, txn=None):
        gkey = self._global_key()
        return self._get_state(gkey, database, txn)

    def get_arena(self, arena, database, txn=None):
        akey = self._arena_key(arena)
        return self._get_state(akey, database, txn)

    def get_segment(self, arena, segment, database, txn=None):
        skey = self._segment_key(arena, segment)
        return self._get_state(skey, database, txn)

    def get_zone(self, zone, database, txn=None):
        zkey = self._zone_key(zone)
        return self._get_state(zkey, database, txn)

    """
    Change global state
    """
    def update_global(self, database, txn=None):
        adb = database.dbpool().arena.open()
        sdb = database.dbpool().dbstate.open()

        astate_list = []
        for arena in keys(adb, txn):
            akey = self._arena_key(arena)

            astate = sdb.get(akey, None, txn)
            if astate is None:
                astate = self.update_arena(arena, database, txn, cascade=False)
            astate_list.append(astate)

        gstate = self._make_state(str(astate_list))
        sdb.put(self._global_key(), gstate, txn)

        adb.close()
        sdb.close()
        return gstate

    """
    Change arena state -> change global state
    """
    def update_arena(self, arena, database, txn=None, **kwargs):
        cascade = kwargs.get("cascade", True)

        asdb = database.dbpool().arena_segment.open()
        sdb = database.dbpool().dbstate.open()

        sstate_list = []
        for segment in get_all(asdb, arena, txn):
            skey = self._segment_key(arena, segment)
            sstate = sdb.get(skey, None, txn)
            if sstate is None:
                sstate = self.update_segment(arena, segment, database, txn,
                                             cascade=False)
            sstate_list.append(sstate)

        astate = self._make_state(str(sstate_list))
        sdb.put(self._arena_key(arena), astate, txn)

        asdb.close()
        sdb.close()

        if cascade:
            self.update_global(database, txn)

        return astate

    """
    Change segment state -> change arena state
                         -> change global state
    """
    def update_segment(self, arena, segment, database, txn=None, **kwargs):
        cascade = kwargs.get("cascade", True)

        szdb = database.dbpool().segment_zone.open()
        sdb = database.dbpool().dbstate.open()

        zstate_list = []
        szkey = arena + ' ' + segment
        for zone in get_all(szdb, szkey, txn):
            zkey = self._zone_key(zone)
            zstate = sdb.get(zkey, None, txn)
            if zstate is None:
                zstate = self.update_zone(zone, database, txn, cascade=False)
            zstate_list.append(zstate)

        sstate = self._make_state(str(zstate_list))
        sdb.put(self._segment_key(arena, segment), sstate, txn)

        szdb.close()
        sdb.close()

        if cascade:
            self.update_arena(arena, database, txn, cascade=True)

        return sstate

    """
    Change zone state -> change segment state
                      -> change arena state
                      -> change global state
    """
    def update_zone(self, zone, database, txn=None, **kwargs):
        cascade = kwargs.get("cascade", True)

        zddb = database.dbpool().zone_dns_data.open()
        ddb = database.dbpool().dns_data.open()
        cdb = database.dbpool().dns_client.open()
        xdb = database.dbpool().dns_xfr.open()
        sdb = database.dbpool().dbstate.open()

        zone_data = []

        for dkey in get_all(zddb, zone, txn):
            dns_data = get_all(ddb, dkey, txn)
            zone_data.append(dns_data)

        dns_client = get_all(cdb, zone, txn)
        zone_data.append(dns_client)

        dns_xfr = get_all(xdb, zone, txn)
        zone_data.append(dns_xfr)

        zstate = self._make_state(str(zone_data))
        sdb.put(self._zone_key(zone), zstate, txn)

        zddb.close()
        ddb.close()
        cdb.close()
        xdb.close()
        sdb.close()

        if cascade:
            zdb = database.dbpool().dns_zone.open()

            zone_rname = reorder(zone)
            arena_segment = zdb.get(zone_rname, None, txn)
            if not arena_segment is None:
                aslist = arena_segment.split(' ', 1)
                if len(aslist) == 2:
                    self.update_segment(aslist[0], aslist[1], database, txn,
                                        cascade=True)

            zdb.close()

        return zstate


# vim:sts=4:ts=4:sw=4:expandtab:
