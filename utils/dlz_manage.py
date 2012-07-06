#!/usr/bin/env python

import atexit
from bsddb3 import db


config = {
    "dbfile": "dlz.db",

    "env_homedir": "/var/lib/bind/",
    "env_flags": db.DB_CREATE | db.DB_THREAD | db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_INIT_LOG | db.DB_INIT_TXN,

    "data_dbname": "dns_data",
    "data_type": db.DB_HASH,
    "data_flags": db.DB_DUP | db.DB_DUPSORT,

    "zone_dbname": "dns_zone",
    "zone_type": db.DB_BTREE,
    "zone_flags": 0,

    "xfr_dbname": "dns_xfr",
    "xfr_type": db.DB_BTREE,
    "xfr_flags": db.DB_DUP | db.DB_DUPSORT,

    "client_dbname": "dns_client",
    "client_type": db.DB_BTREE,
    "client_flags": db.DB_DUP | db.DB_DUPSORT,
}


_cleanup_resourses = []


def _cleanup():
    while _cleanup_resourses:
        res = _cleanup_resourses.pop()
        print "closing", res
        res.close()


def open_env():
    dbenvh = db.DBEnv()
    dbenvh.open(config["env_homedir"], config["env_flags"])
    _cleanup_resourses.append(dbenvh)
    return dbenvh


def open_db(dbenv, dbfile, dbname, dbtype, dbflags, create, truncate):
    dbh = db.DB(dbenv)
    dbtxnh = dbenv.txn_begin()

    open_flags = 0
    if create:
        open_flags |= db.DB_CREATE
    if truncate:
        open_flags |= db.DB_TRUNCATE

    dbh.set_flags(dbflags)
    dbh.open(dbfile, dbname, dbtype, open_flags, 0660, dbtxnh)
    dbtxnh.commit()

    _cleanup_resourses.append(dbh)
    return dbh


def get_all(dbh, key):
    res = []
    kv = dbh.get(key)
    c = dbh.cursor()
    while kv:
        res.append(kv[1])
        kv = c.get('', '', db.DB_NEXT_DUP)

    return res


def delete(dbenvh, dbh, key, val):
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


def open_data_db(dbenv, create=True, truncate=False):
    return open_db(dbenv, config["dbfile"], config["data_dbname"], config["data_type"],
                   config["data_flags"], create, truncate)


def open_zone_db(dbenv, create=True, truncate=False):
    return open_db(dbenv, config["dbfile"], config["zone_dbname"], config["zone_type"],
                   config["zone_flags"], create, truncate)


def open_xfr_db(dbenv, create=True, truncate=False):
    return open_db(dbenv, config["dbfile"], config["xfr_dbname"], config["xfr_type"],
                   config["xfr_flags"], create, truncate)


def open_client_db(dbenv, create=True, truncate=False):
    return open_db(dbenv, config["dbfile"], config["client_dbname"], config["client_type"],
                   config["client_flags"], create, truncate)


########################
atexit.register(_cleanup)
dbenvh = open_env()
ddb = open_data_db(dbenvh)
cdb = open_client_db(dbenvh)
xdb = open_xfr_db(dbenvh)
zdb = open_zone_db(dbenvh)

