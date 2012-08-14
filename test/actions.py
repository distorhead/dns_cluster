from context import *
from dbstate import *

from lib.actions import *


def apply(act):
    with database.transaction() as txn:
        act.apply(database, txn)


add_actions = []
del_actions = []

def add_action(act):
    add_actions.append(act)
    return act

def del_action(act):
    del_actions.append(act)
    return act

def apply_add():
    for act in add_actions:
        apply(act)

def apply_del():
    for act in reversed(del_actions):
        apply(act)


a_add = add_action(add_arena.AddArena(arena='myarena'))
s_add = add_action(add_segment.AddSegment(arena='myarena', segment='mysegment'))
z_add = add_action(add_zone.AddZone(arena='myarena', segment='mysegment', zone='myzone'))
rcname_add = add_action(add_record_cname.AddRecord_CNAME(zone='myzone', host='go',
                        domain='www.yandex.ru.', ttl=101))
ra_add = add_action(add_record_a.AddRecord_A(zone='myzone', host='fuuu', ip='1.2.3.4', ttl=10))
z2_add = add_action(add_zone.AddZone(arena='myarena', segment='mysegment', zone='myzone2'))
rdname_add = add_action(add_record_dname.AddRecord_DNAME(zone='myzone2', zone_dst="myzone."))
rns_add = add_action(add_record_ns.AddRecord_NS(zone='myzone', domain="ns.myzone."))
rns2_add = add_action(add_record_ns.AddRecord_NS(zone='myzone', domain="ns2.myzone."))
rmx_add = add_action(add_record_mx.AddRecord_MX(zone='myzone', domain="mail.myzone."))
z3_add = add_action(add_zone.AddZone(arena='myarena', segment='mysegment',
                                     zone='3.2.1.in-addr.arpa'))
rptr_add = add_action(add_record_ptr.AddRecord_PTR(zone='3.2.1.in-addr.arpa', host='4',
                                                   domain='fuuu.myzone.', ttl=107))

a_del = del_action(del_arena.DelArena(arena='myarena'))
s_del = del_action(del_segment.DelSegment(arena='myarena', segment='mysegment'))
z_del = del_action(del_zone.DelZone(arena='myarena', segment='mysegment', zone='myzone'))
rcname_del = del_action(del_record_cname.DelRecord_CNAME(zone='myzone', host='go',
                        domain='www.yandex.ru.'))
ra_del = del_action(del_record_a.DelRecord_A(zone='myzone', host='fuuu', ip='1.2.3.4'))
z2_del = del_action(del_zone.DelZone(arena='myarena', segment='mysegment', zone='myzone2'))
rdname_del = del_action(del_record_dname.DelRecord_DNAME(zone='myzone2', zone_dst="myzone."))
rns_del = del_action(del_record_ns.DelRecord_NS(zone='myzone', domain="ns.myzone."))
rns2_del = del_action(del_record_ns.DelRecord_NS(zone='myzone', domain="ns2.myzone."))
rmx_del = del_action(del_record_mx.DelRecord_MX(zone='myzone', domain="mail.myzone."))
z3_del = del_action(del_zone.DelZone(arena='myarena', segment='mysegment',
                                     zone='3.2.1.in-addr.arpa'))
rptr_del = del_action(del_record_ptr.DelRecord_PTR(zone='3.2.1.in-addr.arpa', host='4',
                                                   domain='fuuu.myzone.'))
