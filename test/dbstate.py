from context import *
from fixture import *
from lib.dbstate import *

sdb = database.dbpool().dbstate.open()
o = Dbstate()


def get_hash(**kwargs):
    arena = kwargs.get('arena', 'myarena')
    segment = kwargs.get('segment', 'mysegm1')
    zone = kwargs.get('zone', 'myzone')

    g, a, s, z = (None, None, None, None)
    with database.transaction() as txn:
            g = o.get_global(database, txn)
            a = o.get_arena('myarena', database, txn)
            s = o.get_segment('myarena', 'mysegm1', database, txn)
            z = o.get_zone('myzone', database, txn)

    return (g, a, s, z)

