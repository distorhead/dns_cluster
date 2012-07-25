from context import *
from lib import action
from lib.actions import *


action_journal = sp.get("action_journal")
jadb = action_journal.dbpool().action.open()


a = arena.add_action(arena='myarena1')
s = segment.add_action(arena='myarena1', segment='mysegm1')

z1 = zone.add_action(arena='myarena1', segment='mysegm1',
             zone='myzone1')

z1s = record_soa.add_action(zone='myzone1', primary_ns='myns.myzone1.',
                    resp_person='root.myzone1.', serial=2, refresh=2800,
                    retry=7200, expire=604800, minimum=86400)

ra = record_a.add_action(zone='myzone1', host='somedomain',
                 ip='192.168.100.200', ttl=300)

rc = record_cname.add_action(zone='myzone1', host='link',
                     domain='somedomain.myzone1.', ttl=300)

z2 = zone.add_action(arena='myarena1', segment='mysegm1', zone='1.in-addr.arpa')
rp = record_ptr.add_action(zone='1.in-addr.arpa', host='4.3.2',
                   domain='foo.myzone.', ttl=300)


z3 = zone.add_action(arena='myarena1', segment='mysegm1',
             zone='link')
rd = record_dname.add_action(zone='link', zone_dst='fffuuuuu.')

ns = record_ns.add_action(zone='myzone1', domain='myns.myzone1.')
mx = record_mx.add_action(zone='myzone1', domain='mail.myzone1.', priority=50)
srv = record_srv.add_action(zone='myzone1', service='_sip._tcp', port=123, domain='ololo.myzone1.')
txt = record_txt.add_action(zone='myzone1', text='This is the first txt record of this zone.')


all = [a, s, z1, z1s, ra, rc, z2, rp, z3, rd, ns, mx, srv, txt]

def apply(act):
    with database.transaction() as txn:
        act.apply(txn, database)
        action_journal.record_action(act, txn)

def apply_all():
    for act in all:
        apply(act)

def invert_all():
    global all
    old = [item for item in reversed(all)]
    i = 0
    for act in old:
        act.invert()
        all[i] = act
        i += 1

