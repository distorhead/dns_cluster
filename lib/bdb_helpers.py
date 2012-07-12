# -*- coding: utf-8 -*-

import database

from bsddb3 import db as bdb


def get_all(db, key, txn=None):
    res = []
    if db.exists(key, txn):
        val = db.get(key, txn)
        c = db.cursor(txn)

        kv = c.get(key, val, bdb.DB_GET_BOTH)
        while kv:
            res.append(kv[1])
            kv = c.get('', '', bdb.DB_NEXT_DUP)

        c.close()

    return res


def delete_pair(db, key, val, txn):
    c = db.cursor(txn)

    res = c.get(key, val, bdb.DB_GET_BOTH)
    if res:
        c.delete()

    c.close()
    return res


def delete(dbh, key, txn=None):
    try:
        dbh.delete(key, txn)
    except bdb.DBNotFoundError:
        pass


def print_db(dbh, txn=None):
    c = dbh.cursor(txn)
    kv = c.first()
    while kv:
        print "'{0}' -> '{1}'".format(kv[0], kv[1])
        kv = c.next()
    c.close()



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
