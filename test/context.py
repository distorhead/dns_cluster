from lib import database
from lib.bdb_helpers import print_db, get_all

database.context(env_homedir="/var/lib/bind", dbfile='dlz.db')

adb = database.context().pool().arena.open()
asdb = database.context().pool().arena_segment.open()
szdb = database.context().pool().segment_zone.open()
zddb = database.context().pool().zone_dns_data.open()
zdb = database.context().pool().dns_zone.open()
ddb = database.context().pool().dns_data.open()
xdb = database.context().pool().dns_xfr.open()
cdb = database.context().pool().dns_client.open()


