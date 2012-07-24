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

adb  = database.dbpool().arena.open()
asdb = database.dbpool().arena_segment.open()
szdb = database.dbpool().segment_zone.open()
zddb = database.dbpool().zone_dns_data.open()
zdb  = database.dbpool().dns_zone.open()
ddb  = database.dbpool().dns_data.open()
xdb  = database.dbpool().dns_xfr.open()
cdb  = database.dbpool().dns_client.open()

