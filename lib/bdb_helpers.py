# -*- coding: utf-8 -*-


from bsddb3 import db


def get_all(dbh, key):
    res = []
    kv = dbh.get(key)
    c = dbh.cursor()
    while kv:
        res.append(kv[1])
        kv = c.get('', '', db.DB_NEXT_DUP)

    return res


def delete_pair(dbenvh, dbh, key, val):
    dbtxnh = dbenvh.txn_begin()
    c = dbh.cursor(dbtxnh)

    res = c.get(key, val, db.DB_GET_BOTH)
    if res:
        c.delete()

    c.close()
    dbtxnh.commit()

    return res


def print_db(dbh):
    c = dbh.cursor()
    kv = c.first()
    while kv:
        print "'{0}' -> '{1}'".format(kv[0], kv[1])
        kv = c.next()



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
