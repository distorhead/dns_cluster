# -*- coding: utf-8 -*-

import atexit

from bsddb3 import db as bdb
from twisted.python import log

from common import singleton


class TransactionError(Exception): pass


class Transaction(object):
    """
    Bdb transaction wrapper with context manager support.
    """

    def __init__(self, dbenv):
        self._dbenv = dbenv
        self._used = False

    def _check_valid(self):
        if self._used:
            raise TransactionError("Transaction object must not be used repeatedly")

    def __enter__(self):
        return self.start()

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        else:
            log.err("Aborting transaction")
            self.rollback()

    def get(self):
        self._check_valid()
        if hasattr(self, "_txn"):
            return self._txn
        else:
            raise TransactionError("Transaction is not started")

    def start(self):
        self._check_valid()
        self._txn = self._dbenv.txn_begin()
        return self._txn

    def commit(self):
        self._check_valid()
        self._txn.commit()
        self._used = True

    def rollback(self):
        self._check_valid()
        self._txn.abort()
        self._used = True


class Database(object):
    """
    Class used to store database name, options and flags
    needed to open bdb database handle.
    """

    DBFILE_PERMISSIONS = 0660
    SEQUENCE_KEY = "_seq"
    SEQUENCE_FLAGS = bdb.DB_CREATE | bdb.DB_THREAD

    def __init__(self, dbenv, file, name, type, flags, open_flags):
        self._dbenv = dbenv
        self._file = file
        self._name = name
        self._type = type
        self._flags = flags
        self._open_flags = open_flags

    def open(self, txn=None):
        """
        Open database with stored options and flags
        in specified transaction (may be useful for opening many
        databases in one transaction) or in implicitly created one.
        """

        # Transactional database may be opened only in transaction
        #   so create own if not given as arg
        is_tmp_txn = False
        try:
            db = bdb.DB(self._dbenv)
            if txn is None:
                txn = self._dbenv.txn_begin()
                is_tmp_txn = True

            db.set_flags(self._flags)
            db.open(self._file, self._name, self._type,
                     self._open_flags, self.DBFILE_PERMISSIONS, txn)

            if is_tmp_txn:
                txn.commit()

            return db
        except bdb.DBError, e:
            log.err("Unable to open database '{0}.{1}'".format(
                                                        self._file,
                                                        self._name))

            if is_tmp_txn:
                txn.abort()
            raise

    @classmethod
    def sequence(cls, db, txn=None, initial=None):
        dbseq = bdb.DBSequence(db)

        if not initial is None:
            dbseq.initial_value(initial)

        dbseq.open(cls.SEQUENCE_KEY, txn, cls.SEQUENCE_FLAGS)
        return dbseq


class DatabasePool(object):
    def __init__(self, databases_spec, dbenv, dbfile):
        for dbname in databases_spec:
            dbdesc = Database(dbenv, dbfile, dbname,
                              databases_spec[dbname]["type"],
                              databases_spec[dbname]["flags"],
                              databases_spec[dbname]["open_flags"])
            setattr(self, dbname, dbdesc)

            if databases_spec[dbname].get("autoincrement", False):
                db = dbdesc.open()
                Database.sequence(db, None, 0)
                db.close()


@singleton
class context:
    DATABASES = {
        "arena": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "arena_segment": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "segment_zone": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "segment_zone_inactive": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "zone_dns_data": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "zone_dns_data_inactive": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_data": {
            "type": bdb.DB_HASH,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_data_inactive": {
            "type": bdb.DB_HASH,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_zone": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "dns_zone_inactive": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "dns_xfr": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_xfr_inactive": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_client": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_client_inactive": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        }
    }

    ENV_FLAGS_DEFAULT = (bdb.DB_CREATE | bdb.DB_THREAD |
                         bdb.DB_INIT_MPOOL | bdb.DB_INIT_LOCK |
                         bdb.DB_INIT_LOG | bdb.DB_INIT_TXN)

    def __init__(self, *args, **kwargs):
        self._dbenv_flags = kwargs.get("dbenv_flags", self.ENV_FLAGS_DEFAULT)
        self._dbenv_homedir = kwargs.get("dbenv_homedir", None)
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._dbenv_homedir is None
        assert not self._dbfile is None
   
        self._dbenv = bdb.DBEnv()
        self._dbenv.open(self._dbenv_homedir, self._dbenv_flags)

        self._dbpool = DatabasePool(self.DATABASES, self._dbenv, self._dbfile)

        atexit.register(self._terminate)

    def _terminate(self):
        #TODO: print -> twisted log
        print 'terminating context'
        log.msg("Terminating database context")
        self._dbenv.close()

    def dbenv(self):
        return self._dbenv

    def dbenv_homedir(self):
        return self._dbenv_homedir

    def dbfile(self):
        return self._dbfile

    def dbpool(self):
        return self._dbpool

    def transaction(self):
        return Transaction(self.dbenv())



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
