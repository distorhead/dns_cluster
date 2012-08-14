from context import *
from dbstate import *

from lib.actions import *


def apply(act):
    with database.transaction() as txn:
        act.apply(database, txn)



a_add = add_arena.AddArena(arena='myarena')
a_del = del_arena.DelArena(arena='myarena')
s_add = add_segment.AddSegment(arena='myarena', segment='mysegment')
s_del = del_segment.DelSegment(arena='myarena', segment='mysegment')

