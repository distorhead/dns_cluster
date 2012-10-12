# -*- coding: utf-8 -*-

import sys
import getopt
import os
import shutil

from twisted.internet import reactor


# Initiate adding services
import lib.database
import lib.action
import lib.lock
import lib.session

from lib.bdb_helpers import *
from lib.service import ServiceProvider
from lib.actions import *
from lib.operations import *
from lib.action import Action
from lib.dbstate import Dbstate
from lib.app.sync.sync import SyncApp


# Define some helper functions

def purge_db(dbenv_homedir):
    shutil.rmtree(dbenv_homedir)
    os.mkdir(dbenv_homedir)
    open(dbenv_homedir + "/.holder", 'w')

def apply(act):
    with database.transaction() as txn:
        act.apply(database, txn)
        action_journal.record_action(act, txn)

def get_hash(**kwargs):
    glbl = kwargs.get('global', None)
    arena = kwargs.get('arena', None)
    segment = kwargs.get('segment', None)
    zone = kwargs.get('zone', None)

    res = []
    with database.transaction() as txn:
            if not glbl is None:
                res["global"] = o.get_global(database, txn)

            if not arena is None:
                res["arena"] = o.get_arena(arena, database, txn)
                if not segment is None:
                    res["segment"] = o.get_segment(arena, segment, database, txn)

            if not zone is None:
                res["zone"] = o.get_zone(zone, database, txn)

    return res

def cb(res):
    print "Callback:", res


add_actions = []
del_actions = []

def append_add_action(act):
    add_actions.append(act)
    return act

def append_del_action(act):
    del_actions.append(act)
    return act

def apply_add():
    for act in add_actions:
        apply(act)

def apply_del():
    for act in reversed(del_actions):
        apply(act)


# Read command line options and setup config

cfg = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    },

    "server": {
        "name": "sync",
        "interface": "localhost",
        "port": 1234
    },

    "peers": {}
}

args = sys.argv[1:]
optlist, _ = getopt.getopt(args, "pc:")
options = dict(optlist)

if options.has_key("-c"):
    cfg_path = options["-c"]
    top_mod = __import__(cfg_path)

    for mod in cfg_path.split('.')[1:]:
        top_mod = getattr(top_mod, mod)

    cfg = top_mod.cfg


if options.has_key("-p"):
    purge_db(cfg["database"]["dbenv_homedir"])

##################################

sp = ServiceProvider(init_srv=True, cfg=cfg)
database = sp.get("database")
action_journal = sp.get("action_journal")
lock = sp.get("lock")
session = sp.get("session")


_sync_app_dbpool = lib.database.DatabasePool(SyncApp.DATABASES,
                                             database.dbenv(),
                                             database.dbfile())


adb  = database.dbpool().arena.dbhandle()
aadb  = database.dbpool().arena_auth.dbhandle()
asdb = database.dbpool().arena_segment.dbhandle()
szdb = database.dbpool().segment_zone.dbhandle()
zddb = database.dbpool().zone_dns_data.dbhandle()
zdb  = database.dbpool().dns_zone.dbhandle()
ddb  = database.dbpool().dns_data.dbhandle()
xdb  = database.dbpool().dns_xfr.dbhandle()
cdb  = database.dbpool().dns_client.dbhandle()
jdb  = action_journal.dbpool().action.dbhandle()
sdb  = database.dbpool().dbstate.dbhandle()
pdb = _sync_app_dbpool.peer.dbhandle()
ldb = lock.dbpool().lock.dbhandle()
lhdb = lock.dbpool().lock_hier.dbhandle()
sldb = lock.dbpool().session_lock.dbhandle()
s_sdb = session.dbpool().session.dbhandle()
s_jdb = session.dbpool().action_journal.dbhandle()
s_sadb = session.dbpool().session_action.dbhandle()

dbstate = Dbstate()




a_add = append_add_action(AddArena(arena='myarena', key='qwerty'))
s_add = append_add_action(AddSegment(arena='myarena', segment='mysegment'))
z_add = append_add_action(AddZone(arena='myarena', segment='mysegment', zone='myzone'))
rcname_add = append_add_action(AddRecord_CNAME(zone='myzone', host='go',
                        domain='www.yandex.ru.', ttl=101))
ra_add = append_add_action(AddRecord_A(zone='myzone', host='fuuu', ip='1.2.3.4', 
                                       ttl=10))
z2_add = append_add_action(AddZone(arena='myarena', segment='mysegment', 
                                   zone='myzone2'))
rdname_add = append_add_action(AddRecord_DNAME(zone='myzone2', zone_dst="myzone."))
rns_add = append_add_action(AddRecord_NS(zone='myzone', domain="ns.myzone."))
rns2_add = append_add_action(AddRecord_NS(zone='myzone', domain="ns2.myzone."))
rmx_add = append_add_action(AddRecord_MX(zone='myzone', domain="mail.myzone."))
z3_add = append_add_action(AddZone(arena='myarena', segment='mysegment',
                                     zone='3.2.1.in-addr.arpa'))
rptr_add = append_add_action(AddRecord_PTR(zone='3.2.1.in-addr.arpa', host='4',
                                                   domain='fuuu.myzone.', ttl=107))
rsoa_add = append_add_action(AddRecord_SOA(zone='myzone2', primary_ns='ns.myzone.',
                                             resp_person='sdf', serial=10, refresh=30,
                                             retry=21, expire=21, minimum=12, ttl=123))
rsrv_add = append_add_action(AddRecord_SRV(zone='myzone', service='_httpd._tcp',
                                                   port=8080, domain='web.myzone.'))
rtxt_add = append_add_action(AddRecord_TXT(zone='myzone', text='!dlroW ,olleH'))

a_del = append_del_action(DelArena(arena='myarena'))
s_del = append_del_action(DelSegment(arena='myarena', segment='mysegment'))
z_del = append_del_action(DelZone(arena='myarena', segment='mysegment', zone='myzone'))
rcname_del = append_del_action(DelRecord_CNAME(zone='myzone', host='go',
                        domain='www.yandex.ru.'))
ra_del = append_del_action(DelRecord_A(zone='myzone', host='fuuu', ip='1.2.3.4'))
z2_del = append_del_action(DelZone(arena='myarena', segment='mysegment', 
                                   zone='myzone2'))
rdname_del = append_del_action(DelRecord_DNAME(zone='myzone2', zone_dst="myzone."))
rns_del = append_del_action(DelRecord_NS(zone='myzone', domain="ns.myzone."))
rns2_del = append_del_action(DelRecord_NS(zone='myzone', domain="ns2.myzone."))
rmx_del = append_del_action(DelRecord_MX(zone='myzone', domain="mail.myzone."))
z3_del = append_del_action(DelZone(arena='myarena', segment='mysegment',
                                     zone='3.2.1.in-addr.arpa'))
rptr_del = append_del_action(DelRecord_PTR(zone='3.2.1.in-addr.arpa', host='4',
                                                   domain='fuuu.myzone.'))
rsoa_del = append_del_action(DelRecord_SOA(zone='myzone2'))
rsrv_del = append_del_action(DelRecord_SRV(zone='myzone', service='_httpd._tcp',
                                                   port=8080, domain='web.myzone.'))
rtxt_del = append_del_action(DelRecord_TXT(zone='myzone', text='!dlroW ,olleH'))


# vim:sts=4:ts=4:sw=4:expandtab:
