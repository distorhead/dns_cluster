# -*- coding: utf-8 -*-


from bsddb3 import db as bdb


def get_all(dbh, key):
    res = []
    val = dbh.get(key)
    c = dbh.cursor()
    kv = c.get(key, val, bdb.DB_GET_BOTH)
    while kv:
        res.append(kv[1])
        kv = c.get('', '', bdb.DB_NEXT_DUP)

    return res


def delete_pair(dbenvh, dbh, key, val):
    dbtxnh = dbenvh.txn_begin()
    c = dbh.cursor(dbtxnh)

    res = c.get(key, val, bdb.DB_GET_BOTH)
    if res:
        c.delete()

    c.close()
    dbtxnh.commit()

    return res


def print_db(dbh, txn=None):
    c = dbh.cursor(txn)
    kv = c.first()
    while kv:
        print "'{0}' -> '{1}'".format(kv[0], kv[1])
        kv = c.next()



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
