# -*- coding: utf-8 -*-

import hashlib

from twisted.python import log

from lib.bdb_helpers import get_all, keys, delete, pair_exists
from lib.common import reorder


class Dbstate(object):
    """
    Class used to manage database data state hierarchy.
    This class is collection of methods and constants and
      supposed to be mixed into another class by inheritance.
    """

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

    def _get_state(self, key, database, txn):
        sdb = database.dbpool().dbstate.open()
        res = sdb.get(key, None, txn)
        sdb.close()
        return res

    def _make_state(self, data):
        try:
            return hashlib.md5(data).hexdigest()
        except:
            log.err("Unable to make hash of '{0}'".format(data))
            return ""

    def _del_state(self, key, database, txn):
        sdb = database.dbpool().dbstate.open()
        delete(sdb, key, txn)
        sdb.close()

    def get_global(self, database, txn=None):
        """
        Retrieve global state.
        """

        gkey = self._global_key()
        return self._get_state(gkey, database, txn)

    def get_arena(self, arena, database, txn=None):
        """
        Retrieve arena state.
        """

        akey = self._arena_key(arena)
        return self._get_state(akey, database, txn)

    def get_segment(self, arena, segment, database, txn=None):
        """
        Retrieve segment state.
        """

        skey = self._segment_key(arena, segment)
        return self._get_state(skey, database, txn)

    def get_zone(self, zone, database, txn=None):
        """
        Retrieve zone state.
        """

        zkey = self._zone_key(zone)
        return self._get_state(zkey, database, txn)

    def del_global(self, database, txn=None):
        self._del_state(self._global_key(), database, txn)

    def del_arena(self, arena, database, txn=None):
        self._del_state(self._arena_key(arena), database, txn)

    def del_segment(self, arena, segment, database, txn=None):
        self._del_state(self._segment_key(arena, segment), database, txn)

    def del_zone(self, zone, database, txn=None):
        self._del_state(self._zone_key(zone), database, txn)

    def update_global(self, database, txn=None):
        """
        Update global state.
        """

        adb = database.dbpool().arena.open()
        sdb = database.dbpool().dbstate.open()

        astate_acc = set()
        for arena in keys(adb, txn):
            akey = self._arena_key(arena)

            astate = sdb.get(akey, None, txn)
            if astate is None:
                astate = self.update_arena(arena, database, txn, cascade=False)
            astate_acc.add(astate)

        gstate = self._make_state(str(astate_acc))
        sdb.put(self._global_key(), gstate, txn)

        adb.close()
        sdb.close()

        return gstate

    def update_arena(self, arena, database, txn=None, **kwargs):
        """
        Update arena state.
        Keyword argument cascade=True (default),
          causes global state update.
        """

        cascade = kwargs.get("cascade", True)

        adb = database.dbpool().arena.open()
        asdb = database.dbpool().arena_segment.open()
        sdb = database.dbpool().dbstate.open()

        sstate_acc = set()
        for segment in get_all(asdb, arena, txn):
            skey = self._segment_key(arena, segment)
            sstate = sdb.get(skey, None, txn)
            if sstate is None:
                sstate = self.update_segment(arena, segment, database, txn,
                                             cascade=False)
            sstate_acc.add(sstate)

        if adb.exists(arena, txn):
            astate = self._make_state(str(sstate_acc))
            sdb.put(self._arena_key(arena), astate, txn)
        else:
            self.del_arena(arena, database, txn)
            astate = None

        if cascade:
            self.update_global(database, txn)

        adb.close()
        asdb.close()
        sdb.close()

        return astate

    def update_segment(self, arena, segment, database, txn=None, **kwargs):
        """
        Update segment state.
        Keyword argument cascade=True (default),
          causes arena state update with cascade=True.
        """

        cascade = kwargs.get("cascade", True)

        asdb = database.dbpool().arena_segment.open()
        szdb = database.dbpool().segment_zone.open()
        sdb = database.dbpool().dbstate.open()

        zstate_acc = set()
        szkey = arena + ' ' + segment
        for zone in get_all(szdb, szkey, txn):
            zkey = self._zone_key(zone)
            zstate = sdb.get(zkey, None, txn)
            if zstate is None:
                zstate = self.update_zone(zone, database, txn, cascade=False)
            zstate_acc.add(zstate)

        if pair_exists(asdb, arena, segment, txn):
            sstate = self._make_state(str(zstate_acc))
            sdb.put(self._segment_key(arena, segment), sstate, txn)
        else:
            self.del_segment(arena, segment, database, txn)
            sstate = None

        if cascade:
            self.update_arena(arena, database, txn, cascade=True)

        asdb.close()
        szdb.close()
        sdb.close()

        return sstate

    def update_zone(self, zone, database, txn=None, **kwargs):
        """
        Update zone state.
        Keyword argument cascade=True (default),
          causes segment state update with cascade=True.
        """

        cascade = kwargs.get("cascade", True)

        zdb = database.dbpool().dns_zone.open()
        zddb = database.dbpool().zone_dns_data.open()
        ddb = database.dbpool().dns_data.open()
        cdb = database.dbpool().dns_client.open()
        xdb = database.dbpool().dns_xfr.open()
        sdb = database.dbpool().dbstate.open()

        zone_acc = set()

        for dkey in get_all(zddb, zone, txn):
            dns_data = str(set([rec_data[rec_data.find(' '):] 
                                for rec_data in get_all(ddb, dkey, txn)]))
            zone_acc.add(dns_data)

        dns_client = str(set(get_all(cdb, zone, txn)))
        zone_acc.add(dns_client)

        dns_xfr = str(set(get_all(xdb, zone, txn)))
        zone_acc.add(dns_xfr)

        zone_rname = reorder(zone)

        if zdb.exists(zone_rname, txn):
            zstate = self._make_state(str(zone_acc))
            sdb.put(self._zone_key(zone), zstate, txn)
        else:
            self.del_zone(zone, database, txn)
            zstate = None

        if cascade:
            arena_segment = zdb.get(zone_rname, None, txn)
            if not arena_segment is None:
                aslist = arena_segment.split(' ', 1)
                if len(aslist) == 2:
                    self.update_segment(aslist[0], aslist[1], database, txn,
                                        cascade=True)

        zdb.close()
        zddb.close()
        ddb.close()
        cdb.close()
        xdb.close()
        sdb.close()

        return zstate


# vim:sts=4:ts=4:sw=4:expandtab:
