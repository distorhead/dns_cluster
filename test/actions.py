from context import *
from dbstate import *

from lib.actions import *


def apply(act):
    with database.transaction() as txn:
        act.apply(database, txn)


add_actions = []
del_actions = []
all_actions = []

def add_action(act):
    all_actions.append(act)
    add_actions.append(act)
    return act

def del_action(act):
    all_actions.append(act)
    del_actions.append(act)
    return act

def apply_add():
    for act in add_actions:
        apply(act)

def apply_del():
    for act in reversed(del_actions):
        apply(act)


a_add = add_action(add_arena.AddArena(arena='myarena'))
a_del = del_action(del_arena.DelArena(arena='myarena'))
s_add = add_action(add_segment.AddSegment(arena='myarena', segment='mysegment'))
s_del = del_action(del_segment.DelSegment(arena='myarena', segment='mysegment'))
z_add = add_action(add_zone.AddZone(arena='myarena', segment='mysegment', zone='myzone'))
z_del = del_action(del_zone.DelZone(arena='myarena', segment='mysegment', zone='myzone'))

