from lib.bdb_helpers import print_db, get_all
from lib.database import *

context(dbenv_homedir="/var/lib/bind", dbfile='dlz.db')

adb  = context().dbpool().arena.open()
asdb = context().dbpool().arena_segment.open()
szdb = context().dbpool().segment_zone.open()
zddb = context().dbpool().zone_dns_data.open()
zdb  = context().dbpool().dns_zone.open()
ddb  = context().dbpool().dns_data.open()
xdb  = context().dbpool().dns_xfr.open()
cdb  = context().dbpool().dns_client.open()


