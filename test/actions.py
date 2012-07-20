from context import *
from lib.actions.add_arena import AddArena
from lib.actions.add_segment import AddSegment
from lib.actions.add_zone import AddZone

a = AddArena('myarena')
s = AddSegment('myarena', 'mysegm')
z = AddZone('myarena', 'mysegm', 'myzone')
