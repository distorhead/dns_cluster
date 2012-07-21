from context import *
from lib.actions.add_arena import AddArena
from lib.actions.add_segment import AddSegment
from lib.actions.add_zone import AddZone
from lib.actions.add_record_a import AddRecord_A

import fixture


a = AddArena('myarena1')
s = AddSegment('myarena1', 'mysegm1')
z = AddZone('myarena1', 'mysegm1', 'myzone1')
ra = AddRecord_A(zone_name='myzone1', host='somedomain',
                 ip='192.168.100.200', ttl=300)
