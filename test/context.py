import sys
import getopt
import os
import shutil

cfg = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    }
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
    shutil.rmtree(cfg["database"]["dbenv_homedir"])
    os.mkdir(cfg["database"]["dbenv_homedir"])


from lib.bdb_helpers import print_db, get_all
from lib.service import ServiceProvider

import lib.database
import lib.action
import lib.lock


sp = ServiceProvider(init_srv=True, cfg=cfg)
database = sp.get("database")

adb  = database.dbpool().arena.dbhandle()
asdb = database.dbpool().arena_segment.dbhandle()
szdb = database.dbpool().segment_zone.dbhandle()
zddb = database.dbpool().zone_dns_data.dbhandle()
zdb  = database.dbpool().dns_zone.dbhandle()
ddb  = database.dbpool().dns_data.dbhandle()
xdb  = database.dbpool().dns_xfr.dbhandle()
cdb  = database.dbpool().dns_client.dbhandle()

