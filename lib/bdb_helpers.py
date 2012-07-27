# -*- coding: utf-8 -*-

import database

from bsddb3 import db as bdb


def get_all(db, key, txn=None):
    res = []

    val = db.get(key, None, txn)
    if not val is None:
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


def pair_exists(db, key, val, txn):
    c = db.cursor(txn)
    return not c.get(key, val, bdb.DB_GET_BOTH) is None


def for_each(dbh, func, txn=None):
    c = dbh.cursor(txn)
    kv = c.first()
    while kv:
        func(kv)
        kv = c.next()

    c.close()


def keys(dbh, txn=None):
    res = []

    def append(kv):
        res.append(kv[0])

    for_each(dbh, append, txn)
    
    return res


def keys_values(dbh, txn=None):
    res = []

    def append(kv):
        res.append((kv[0], kv[1]))

    for_each(dbh, append, txn)
    
    return res


def print_db(dbh, txn=None):
    def printer(kv):
        print "'{0}' -> '{1}'".format(kv[0], kv[1])
    for_each(dbh, printer, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
