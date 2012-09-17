# -*- coding: utf-8 -*-

from lib.actions import *
from lib.actions.record import RecordAction
from lib.common import split, reorder
from lib import bdb_helpers


class OperationHelpersMixin(object):
    def _retrieve_record(self, database_srv, zone, host, pred, txn):
        ddb = database_srv.dbpool().dns_data.dbhandle()
        recs = bdb_helpers.get_all(ddb, zone + ' ' + host, txn)
        for rec in recs:
            rec_list = split(rec)
            if pred(rec_list):
                return rec_list

        return []


    def add_to_del_a(self, database_srv, act, txn):
        return DelRecord_A(zone=act.zone, host=act.host, ip=act.ip)

    def del_to_add_a(self, database_srv, act, txn):
        zone = act.zone
        host = act.host
        ip = act.ip
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'A' and rec_list[4] == ip
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, host, pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_A(zone=zone, host=host, ip=ip, ttl=ttl)

    def make_spec_a(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, host = zd_list
            return {
                'type': 'A',
                'zone': zone,
                'host': host,
                'ip': rec_list[4],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_cname(self, database_srv, act, txn):
        return DelRecord_CNAME(zone=act.zone, host=act.host, domain=act.domain)

    def del_to_add_cname(self, database_srv, act, txn):
        zone = act.zone
        host = act.host
        domain = act.domain
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'CNAME' and rec_list[4] == domain
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, host, pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_CNAME(zone=zone, host=host, domain=domain, ttl=ttl)

    def make_spec_cname(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, host = zd_list
            return {
                'type': 'CNAME',
                'zone': zone,
                'host': host,
                'domain': rec_list[4],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_dname(self, database_srv, act, txn):
        return DelRecord_DNAME(zone=act.zone, zone_dst=act.zone_dst)

    def del_to_add_dname(self, database_srv, act, txn):
        zone = act.zone
        zone_dst = act.zone_dst
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'DNAME' and rec_list[4] == zone_dst
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, '@', pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_DNAME(zone=zone, zone_dst=zone_dst, ttl=ttl)

    def make_spec_dname(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, _ = zd_list
            return {
                'type': 'DNAME',
                'zone': zone,
                'zone_dst': rec_list[4],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_mx(self, database_srv, act, txn):
        return DelRecord_MX(zone=act.zone, domain=act.domain)

    def del_to_add_mx(self, database_srv, act, txn):
        zone = act.zone
        domain = act.domain
        priority = AddRecord_MX.PRIORITY_DEFAULT
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 6:
                return rec_list[3] == 'MX' and rec_list[5] == domain
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, '@', pred, txn)
        if rec_list:
            priority = rec_list[4]
            ttl = rec_list[2]

        return AddRecord_MX(zone=zone, domain=domain, priority=priority, ttl=ttl)

    def make_spec_mx(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, _ = zd_list
            return {
                'type': 'MX',
                'zone': zone,
                'priority': int(rec_list[4]),
                'domain': rec_list[5],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_ns(self, database_srv, act, txn):
        return DelRecord_NS(zone=act.zone, domain=act.domain)

    def del_to_add_ns(self, database_srv, act, txn):
        zone = act.zone
        domain = act.domain
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'NS' and rec_list[4] == domain
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, '@', pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_NS(zone=zone, domain=domain, ttl=ttl)

    def make_spec_ns(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, _ = zd_list
            return {
                'type': 'NS',
                'zone': zone,
                'domain': rec_list[4],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_ptr(self, database_srv, act, txn):
        return DelRecord_PTR(zone=act.zone, host=act.host, domain=act.domain)

    def del_to_add_ptr(self, database_srv, act, txn):
        zone = act.zone
        host = act.host
        domain = act.domain
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'PTR' and rec_list[4] == domain
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, host, pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_PTR(zone=zone, host=host, domain=domain, ttl=ttl)

    def make_spec_ptr(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, host = zd_list
            return {
                'type': 'PTR',
                'zone': zone,
                'host': host,
                'domain': rec_list[4],
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_soa(self, database_srv, act, txn):
        return DelRecord_SOA(zone=act.zone)

    def del_to_add_soa(self, database_srv, act, txn):
        zone = act.zone
        primary_ns = None
        resp_person = None
        serial = None
        refresh = None
        retry = None
        expire = None
        minimum = None
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 11:
                return rec_list[3] == 'SOA'
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, '@', pred, txn)
        if rec_list:
            ttl = rec_list[2]
            (primary_ns, resp_person, serial,
                refresh, retry, expire, minimum) = rec_list[4:]

        return AddRecord_SOA(zone=zone,
                             primary_ns=primary_ns,
                             resp_person=resp_person,
                             serial=serial,
                             refresh=refresh,
                             retry=retry,
                             expire=expire,
                             minimum=minimum)

    def make_spec_soa(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, _ = zd_list
            return {
                'type': 'SOA',
                'zone': zone,
                'primary_ns': rec_list[4],
                'resp_person': rec_list[5],
                'serial': int(rec_list[6]),
                'refresh': int(rec_list[7]),
                'retry': int(rec_list[8]),
                'expire': int(rec_list[9]),
                'minimum': int(rec_list[10]),
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_srv(self, database_srv, act, txn):
        return DelRecord_SRV(zone=act.zone,
                             service=act.service,
                             port=act.port,
                             domain=act.domain)

    def del_to_add_srv(self, database_srv, act, txn):
        zone = act.zone
        service = act.service
        port = act.port
        domain = act.domain
        priority = AddRecord_SRV.PRIORITY_DEFAULT
        weight = AddRecord_SRV.WEIGHT_DEFAULT
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 8:
                return (rec_list[3] == 'SRV' and
                        rec_list[6] == str(port) and
                        rec_list[7] == domain)
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, service, pred, txn)
        if rec_list:
            priority = rec_list[4]
            weight = rec_list[5]
            ttl = rec_list[2]

        return AddRecord_SRV(zone=zone,
                             service=service,
                             port=port,
                             domain=domain,
                             priority=priority,
                             weight=weight,
                             ttl=ttl)

    def make_spec_srv(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, service = zd_list
            return {
                'type': 'SRV',
                'zone': zone,
                'service': service,
                'port': int(rec_list[6]),
                'domain': rec_list[7],
                'priority': int(rec_list[4]),
                'weight': int(rec_list[5]),
                'ttl': int(rec_list[2])
            }

        except:
            return None


    def add_to_del_txt(self, database_srv, act, txn):
        return DelRecord_TXT(zone=act.zone, text=act.text)

    def del_to_add_txt(self, database_srv, act, txn):
        zone = act.zone
        text = act.text
        ttl = RecordAction.TTL_DEFAULT

        def pred(rec_list):
            if len(rec_list) == 5:
                return rec_list[3] == 'TXT' and rec_list[4] == '"' + text + '"'
            else:
                return False

        rec_list = self._retrieve_record(database_srv, zone, '@', pred, txn)
        if rec_list:
            ttl = rec_list[2]

        return AddRecord_TXT(zone=zone, text=text, ttl=ttl)

    def make_spec_txt(self, zone_data_key, rec_list):
        try:
            zd_list = zone_data_key.split(' ', 1)
            zone, _ = zd_list
            return {
                'type': 'TXT',
                'zone': zone,
                'text': rec_list[4][1:-1],
                'ttl': int(rec_list[2])
            }

        except:
            return None

    ACTION_BY_REC_TYPE = {
        'a': {
            'add': AddRecord_A,
            'del': DelRecord_A,
            'add_to_del': add_to_del_a,
            'del_to_add': del_to_add_a,
            'make_spec': make_spec_a
        },
        'cname': {
            'add': AddRecord_CNAME,
            'del': DelRecord_CNAME,
            'add_to_del': add_to_del_cname,
            'del_to_add': del_to_add_cname,
            'make_spec': make_spec_cname
        },
        'dname': {
            'add': AddRecord_DNAME,
            'del': DelRecord_DNAME,
            'add_to_del': add_to_del_dname,
            'del_to_add': del_to_add_dname,
            'make_spec': make_spec_dname
        },
        'mx': {
            'add': AddRecord_MX,
            'del': DelRecord_MX,
            'add_to_del': add_to_del_mx,
            'del_to_add': del_to_add_mx,
            'make_spec': make_spec_mx
        },
        'ns': {
            'add': AddRecord_NS,
            'del': DelRecord_NS,
            'add_to_del': add_to_del_ns,
            'del_to_add': del_to_add_ns,
            'make_spec': make_spec_ns
        },
        'ptr': {
            'add': AddRecord_PTR,
            'del': DelRecord_PTR,
            'add_to_del': add_to_del_ptr,
            'del_to_add': del_to_add_ptr,
            'make_spec': make_spec_ptr
        },
        'soa': {
            'add': AddRecord_SOA,
            'del': DelRecord_SOA,
            'add_to_del': add_to_del_soa,
            'del_to_add': del_to_add_soa,
            'make_spec': make_spec_soa
        },
        'srv': {
            'add': AddRecord_SRV,
            'del': DelRecord_SRV,
            'add_to_del': add_to_del_srv,
            'del_to_add': del_to_add_srv,
            'make_spec': make_spec_srv
        },
        'txt': {
            'add': AddRecord_TXT,
            'del': DelRecord_TXT,
            'add_to_del': add_to_del_txt,
            'del_to_add': del_to_add_txt,
            'make_spec': make_spec_txt
        }
    }

    def _get_rec_map(self, rec_type, raise_error=True):
        rec_type = rec_type.lower()
        if not self.ACTION_BY_REC_TYPE.has_key(rec_type):
            if raise_error:
                raise OperationError("Unknown record type '{}'".format(
                                     rec_type))
            else:
                return None
        else:
            return self.ACTION_BY_REC_TYPE[rec_type]

    def make_add_record(self, rec_type, spec):
        rec_map = self._get_rec_map(rec_type)
        return rec_map['add'](**spec)

    def make_del_record(self, rec_type, spec):
        rec_map = self._get_rec_map(rec_type)
        return rec_map['del'](**spec)

    def add_to_del_record(self, database_srv, rec_type, act, txn):
        rec_map = self._get_rec_map(rec_type)
        converter = rec_map['add_to_del']
        return converter(self, database_srv, act, txn)

    def del_to_add_record(self, database_srv, rec_type, act, txn):
        rec_map = self._get_rec_map(rec_type)
        converter = rec_map['del_to_add']
        return converter(self, database_srv, act, txn)

    def make_rec_spec(self, zone_data_key, rec):
        rec_list = split(rec)
        if len(rec_list) > 3:
            rec_map = self._get_rec_map(rec_list[3])
            if not rec_map is None:
                spec_maker = rec_map['make_spec']
                return spec_maker(self, zone_data_key, rec_list)
            else:
                return None
        else:
            return None

    def arena_segment_by_zone(self, database_srv, zone, txn=None):
        zdb = database_srv.dbpool().dns_zone.dbhandle()
        arena_segment = zdb.get(reorder(zone), None, txn)
        if not arena_segment is None:
            as_list = arena_segment.split(' ', 1)
            if len(as_list) == 2:
                return (as_list[0], as_list[1])

        return None

    def is_zone_in_arena(self, database_srv, zone, arena, txn=None):
        zdb = database_srv.dbpool().dns_zone.dbhandle()
        as_pair = self.arena_segment_by_zone(database_srv, zone, txn)
        if not as_pair is None:
            zarena, _ = as_pair
            if arena == zarena:
                return True

        return False

    def is_admin(self, session_data):
        print 'is_admin called'
        return session_data['arena'] == '__admin__'

    def has_access_to_zone(self, database_srv, zone, session_data, txn=None):
        arena = session_data['arena']

        if self.is_admin(session_data):
            return True
        else:
            return self.is_zone_in_arena(database_srv, zone, arena, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
