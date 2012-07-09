# -*- coding: utf-8 -*-

import atexit

from bsddb3 import db as bdb
from twisted.python import log

from common import singleton


class Database(object):
    """
    Class used to store database name, options and flags
    needed to open bdb database handle.
    """

    DBFILE_PERMISSIONS = 0660

    def __init__(self, dbenv, file, name, type, flags, open_flags):
        self._dbenv = dbenv
        self._file = file
        self._name = name
        self._type = type
        self._flags = flags
        self._open_flags = open_flags

    def open(self):
        dbh = bdb.DB(self._dbenv)
        dbtxnh = self._dbenv.txn_begin()
        dbh.set_flags(self._flags)
        dbh.open(self._file, self._name, self._type, self._open_flags,
                 self.DBFILE_PERMISSIONS, dbtxnh)
        dbtxnh.commit()

        return dbh


@singleton
class context:
    DLZ_DATABASES = {
        "dns_data": {
            "type": bdb.DB_HASH,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_zone": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        },
        "dns_xfr": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        },
        "dns_client": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP|bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE
        }
    }

    ENV_FLAGS_DEFAULT = (bdb.DB_CREATE | bdb.DB_THREAD |
                         bdb.DB_INIT_MPOOL | bdb.DB_INIT_LOCK |
                         bdb.DB_INIT_LOG | bdb.DB_INIT_TXN)

    def __init__(self, *args, **kwargs):
        self._env_flags = kwargs.get("env_flags", self.ENV_FLAGS_DEFAULT)
        self._env_homedir = kwargs.get("env_homedir", None)
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._env_homedir is None
        assert not self._dbfile is None
   
        self._dbenv = bdb.DBEnv()
        self._dbenv.open(self._env_homedir, self._env_flags)

        for dbname in self.DLZ_DATABASES:
            db = Database(self._dbenv, self._dbfile, dbname,
                          self.DLZ_DATABASES[dbname]["type"],
                          self.DLZ_DATABASES[dbname]["flags"],
                          self.DLZ_DATABASES[dbname]["open_flags"])
            setattr(self, dbname, db)

        atexit.register(self._terminate)

    def _terminate(self):
        #TODO: remove print
        print 'terminating context'
        self._dbenv.close()

    def dbenv(self):
        return self._dbenv



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
