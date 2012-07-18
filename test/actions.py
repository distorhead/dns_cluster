from lock import *
from lib.action import *
from lib.actions import *

arena_act = add_arena.AddArena('myarena')


def cb(res):
    print '-- SUCCESS:', res


def eb(failure):
    print '-- ERROR:', failure


def apply(act, sessid):
    d = act.apply(sessid)
    d.addCallback(cb)
    d.addErrback(eb)

