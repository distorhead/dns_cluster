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

    def _check_used(self):
        if self._used:
            raise TransactionError("Transaction object must not be used repeatedly")

    def _check_started(self):
        if not hasattr(self, "_txn"):
            raise TransactionError("Transaction is not started")

    def __enter__(self):
        return self.start()

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        else:
            log.err("Aborting transaction")
            self.rollback()

    def get(self):
        self._check_used()
        self._check_started()
        return self._txn

    def start(self):
        self._check_used()
        self._txn = self._dbenv.txn_begin()
        return self._txn

    def commit(self):
        self._check_used()
        self._check_started()
        self._txn.commit()
        self._used = True

    def rollback(self):
        self._check_used()
        self._check_started()
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
        if txn is None:
            t = Transaction(self._dbenv)
            with t as txn:
                return self._do_open(txn)
        else:
            return self._do_open(txn)

    def _do_open(self, txn):
        db = bdb.DB(self._dbenv)
        db.set_flags(self._flags)
        db.open(self._file, self._name, self._type,
                 self._open_flags, self.DBFILE_PERMISSIONS, txn)

        return db

    def sequence(self, seqkey=None, **kwargs):
        txn = kwargs.get("txn", None)
        initial = kwargs.get("initial", None)
        dbhandle = kwargs.get("dbhandle", None)

        if seqkey is None:
            seqkey = self.SEQUENCE_KEY

        if dbhandle is None:
            dbhandle = self.open()

        dbseq = bdb.DBSequence(dbhandle)

        if not initial is None:
            dbseq.initial_value(initial)

        dbseq.open(seqkey, txn, self.SEQUENCE_FLAGS)
        return dbseq


class DatabasePool(object):
    """
    Class used to store specified database descriptors
    as object attributes.
    """

    def __init__(self, databases_spec, dbenv, dbfile):
        for dbname in databases_spec:
            dbdesc = Database(dbenv, dbfile, dbname,
                              databases_spec[dbname]["type"],
                              databases_spec[dbname]["flags"],
                              databases_spec[dbname]["open_flags"])
            setattr(self, dbname, dbdesc)

            seq_spec = databases_spec[dbname].get("seq_spec", {})
            for seq_name in seq_spec:
                dbdesc.sequence(seq_name, initial=seq_spec[seq_name])


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
        },
        "sequence": {
            "type": bdb.DB_HASH,
            "flags": 0,
            "open_flags": bdb.DB_CREATE,
            "seq_spec": {"dns_data": 1}
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


# vim:sts=4:ts=4:sw=4:expandtab:
