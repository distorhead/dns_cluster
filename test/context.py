from lib.bdb_helpers import print_db, get_all
from lib.service import ServiceProvider

import lib.database
import lib.action
import lib.lock


cfg = {
    "database": {
        "dbenv_homedir": "/var/lib/bind",
        "dbfile": "dlz.db"
    }
}

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

