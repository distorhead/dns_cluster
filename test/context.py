from lib.bdb_helpers import print_db, get_all
from lib import database

database.context(dbenv_homedir="/var/lib/bind", dbfile='dlz.db')

adb  = database.context().dbpool().arena.open()
asdb = database.context().dbpool().arena_segment.open()
szdb = database.context().dbpool().segment_zone.open()
zddb = database.context().dbpool().zone_dns_data.open()
zdb  = database.context().dbpool().dns_zone.open()
ddb  = database.context().dbpool().dns_data.open()
xdb  = database.context().dbpool().dns_xfr.open()
cdb  = database.context().dbpool().dns_client.open()


