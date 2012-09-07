# -*- coding: utf-8 -*-

from lib.actions import *
from lib.actions.record import RecordAction
from lib.common import split
from lib import bdb_helpers


class RecordOperationError(Exception): pass


class RecordOperation(object):
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

    ACTION_BY_REC_TYPE = {
        'a': {
            'add': AddRecord_A,
            'del': DelRecord_A,
            'add_to_del': add_to_del_a,
            'del_to_add': del_to_add_a
        },
        'cname': {
            'add': AddRecord_CNAME,
            'del': DelRecord_CNAME,
            'add_to_del': add_to_del_cname,
            'del_to_add': del_to_add_cname
        },
        'dname': {
            'add': AddRecord_DNAME,
            'del': DelRecord_DNAME,
            'add_to_del': add_to_del_dname,
            'del_to_add': del_to_add_dname
        },
        'mx': {
            'add': AddRecord_MX,
            'del': DelRecord_MX,
            'add_to_del': add_to_del_mx,
            'del_to_add': del_to_add_mx
        },
        'ns': {
            'add': AddRecord_NS,
            'del': DelRecord_NS,
            'add_to_del': add_to_del_ns,
            'del_to_add': del_to_add_ns
        },
        'ptr': {
            'add': AddRecord_PTR,
            'del': DelRecord_PTR,
            'add_to_del': add_to_del_ptr,
            'del_to_add': del_to_add_ptr
        },
        'soa': {
            'add': AddRecord_SOA,
            'del': DelRecord_SOA,
            'add_to_del': add_to_del_soa,
            'del_to_add': del_to_add_soa
        },
        'srv': {
            'add': AddRecord_SRV,
            'del': DelRecord_SRV,
            'add_to_del': add_to_del_srv,
            'del_to_add': del_to_add_srv
        },
        'txt': {
            'add': AddRecord_TXT,
            'del': DelRecord_TXT,
            'add_to_del': add_to_del_txt,
            'del_to_add': del_to_add_txt
        }
    }

    def _get_rec_map(self, rec_type):
        rec_type = rec_type.lower()
        if not self.ACTION_BY_REC_TYPE.has_key(rec_type):
            raise RecordOperationError("Unknown record type '{}'".format(rec_type))
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


# vim:sts=4:ts=4:sw=4:expandtab:
