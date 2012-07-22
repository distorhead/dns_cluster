from context import *
from lib.actions.add_arena import AddArena
from lib.actions.add_segment import AddSegment
from lib.actions.add_zone import AddZone
from lib.actions.add_record_a import AddRecord_A
from lib.actions.add_record_ptr import AddRecord_PTR

import fixture


a = AddArena('myarena1')
s = AddSegment('myarena1', 'mysegm1')

z1 = AddZone('myarena1', 'mysegm1', 'myzone1')
ra = AddRecord_A(zone='myzone1', host='somedomain',
                 ip='192.168.100.200', ttl=300)

z2 = AddZone('myarena1', 'mysegm1', '1.in-addr.arpa')
rp = AddRecord_PTR(zone='1.in-addr.arpa', host='4.3.2',
                   domain='foo.myzone.', ttl=300)


def apply_all():
    with database.context().transaction() as txn:
        a.apply(txn)

    with database.context().transaction() as txn:
        s.apply(txn)

    with database.context().transaction() as txn:
        z1.apply(txn)

    with database.context().transaction() as txn:
        z2.apply(txn)

    with database.context().transaction() as txn:
        ra.apply(txn)

    with database.context().transaction() as txn:
        rp.apply(txn)
