# -*- coding: utf-8 -*-

import database


class LockError(Exception): pass


class Lock(object):
    """
    Class used to manage resorces locks.
    Resorce locks stored into database and
    may be shared between many processes.
    """

    RESOURCE_DELIMITER = "::"

    DATABASES = {
        "lock": {
            "type": bdb.DB_HASH,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        }
    }

    def __init__(self, resource_spec, sessid):
        class Pool: pass
        self._dbpool = DatabasePool(self.DATABASES,
                                    database.context().dbenv(),
                                    database.context().dbfile())


# vim:sts=4:ts=4:sw=4:expandtab:
