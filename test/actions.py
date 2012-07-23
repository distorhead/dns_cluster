from context import *
from lib import action
from lib.actions.add_arena import AddArena
from lib.actions.add_segment import AddSegment
from lib.actions.add_zone import AddZone
from lib.actions.add_record_a import AddRecord_A
from lib.actions.add_record_ptr import AddRecord_PTR
from lib.actions.add_record_cname import AddRecord_CNAME
from lib.actions.add_record_dname import AddRecord_DNAME
from lib.actions.add_record_soa import AddRecord_SOA
from lib.actions.add_record_ns import AddRecord_NS
from lib.actions.add_record_mx import AddRecord_MX
from lib.actions.add_record_srv import AddRecord_SRV
from lib.actions.add_record_txt import AddRecord_TXT


jadb = action.journal().dbpool().action.open()


a = AddArena(arena='myarena1')
s = AddSegment(arena='myarena1', segment='mysegm1')

z1 = AddZone(arena='myarena1', segment='mysegm1',
             zone='myzone1')

z1s = AddRecord_SOA(zone='myzone1', primary_ns='myns.myzone1.',
                    resp_person='root.myzone1.', serial=2, refresh=2800,
                    retry=7200, expire=604800, minimum=86400)

ra = AddRecord_A(zone='myzone1', host='somedomain',
                 ip='192.168.100.200', ttl=300)

rc = AddRecord_CNAME(zone='myzone1', host='link',
                     domain='somedomain.myzone1.', ttl=300)

z2 = AddZone(arena='myarena1', segment='mysegm1', zone='1.in-addr.arpa')
rp = AddRecord_PTR(zone='1.in-addr.arpa', host='4.3.2',
                   domain='foo.myzone.', ttl=300)


z3 = AddZone(arena='myarena1', segment='mysegm1',
             zone='link')
rd = AddRecord_DNAME(zone='link', zone_dst='fffuuuuu.')

ns = AddRecord_NS(zone='myzone1', domain='myns.myzone1.')
mx = AddRecord_MX(zone='myzone1', domain='mail.myzone1.', priority=50)
srv = AddRecord_SRV(zone='myzone1', service='_sip._tcp', port=123, domain='ololo.myzone1.')
txt = AddRecord_TXT(zone='myzone1', text='This is the first txt record of this zone. Nothing more.')


all = [a, s, z1, z1s, ra, rc, z2, rp, z3, rd, ns, mx, srv, txt]

def apply(act):
    with database.context().transaction() as txn:
        act.apply(txn)
        action.journal().record_action(act, txn)

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

