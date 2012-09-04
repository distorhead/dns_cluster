# -*- coding: utf-8 -*-

from twisted.python import log

from lib import database
from lib import bdb_helpers
from lib.service import ServiceProvider


@ServiceProvider.register("lock", deps=["database"])
class manager(object):
    """
    Class used to manage resorces locks.
    Resorce locks stored into database and
    may be shared between many processes.
    """

    RESOURCE_DELIMITER = "::"
    RECORD_DELIMITER = " "

    TIMEOUT_DEFAULT = 30.0
    WAIT_INTERVAL_SECONDS_DEFAULT = 2.0
    MAX_ATTEMPTS_DEFAULT = 10

    DATABASES = {
        "lock": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        },
        "lock_hier": {
            "type": database.bdb.DB_BTREE,
            "flags": database.bdb.DB_DUP|database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    class Lock(object):
        def __init__(self, resource, sessid):
            self.resource = resource
            self.sessid = sessid

    def __init__(self, sp, *args, **kwargs):
        self._database = sp.get("database")
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())

    def dbpool(self):
        return self._dbpool

    def acquire(self, resource, sessid):
        if self._is_valid_resource(resource):
            l = self.Lock(resource, sessid)
            return self._do_acquire(l)
        else:
            return False

    def release(self, resource):
        return self._do_release(resource)


    def _is_valid_resource(self, resource):
        return len(resource) > 0

    def _do_acquire(self, l):
        """
        Method uses DB blocking API calls and should be called from separate thread.
        Returns True if lock is acquired, False otherwise.
        """

        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        with self._database.transaction() as txn:
            if ldb.exists(l.resource, txn, database.bdb.DB_RMW):
                # lock already exists
                rec = ldb.get(l.resource, None, txn, database.bdb.DB_RMW)
                rec_list = rec.split(self.RECORD_DELIMITER)

                rec_sessid = int(rec_list[0])
                if l.sessid == rec_sessid:
                    # updating existed lock counter
                    rec_count = int(rec_list[1])
                    rec_count += 1
                    rec_list[1] = str(rec_count)
                    ldb.put(l.resource, self.RECORD_DELIMITER.join(rec_list), txn)
                    return True
                else:
                    # lock in other session
                    return False

            else:
                # lock doesn't exist, checking for common and special locks
                resource_list = l.resource.split(self.RESOURCE_DELIMITER)
                parent_res = ""
                for res in resource_list[:-1]:
                    if parent_res:
                        res = parent_res + self.RESOURCE_DELIMITER + res;
                    parent_res = res

                    if ldb.exists(res, txn, database.bdb.DB_RMW):
                        # common lock exists
                        rec = ldb.get(res, None, txn, database.bdb.DB_RMW)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = int(rec_list[0])
                        if l.sessid == rec_sessid:
                            # common lock belongs to this session, acquiring
                            self._insert_lock(l, txn)
                            return True
                        else:
                            # common lock belongs to other session, delay repeat call
                            return False

                # check for special lock
                ldb_keys = bdb_helpers.get_all(lhdb, l.resource, txn)
                for ldb_key in ldb_keys:
                    if ldb.exists(ldb_key, txn, database.bdb.DB_RMW):
                        rec = ldb.get(ldb_key, None, txn, database.bdb.DB_RMW)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = int(rec_list[0])
                        if l.sessid != rec_sessid:
                            # special lock in other session exists,
                            #   delay repeat call
                            return False

                # special locks don't exist or all locks
                #   from this session, acquiring
                self._insert_lock(l, txn)
                return True

    def _insert_lock(self, l, txn):
        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        rec = str(l.sessid) + self.RECORD_DELIMITER + "1"
        ldb.put(l.resource, rec, txn)

        def inserter(res):
            lhdb.put(res, l.resource, txn)

        resource_list = l.resource.split(self.RESOURCE_DELIMITER)
        self._for_each_resource(resource_list[:-1], inserter)

    def _for_each_resource(self, resource_list, func):
        parent_res = ""
        for res in resource_list:
            if parent_res:
                res = parent_res + self.RESOURCE_DELIMITER + res;
            parent_res = res
            func(res)

    def _do_release(self, resource):
        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        def do_delete():
            def deleter(res):
                bdb_helpers.delete_pair(lhdb, res, resource, txn)

            resource_list = resource.split(self.RESOURCE_DELIMITER)
            self._for_each_resource(resource_list[:-1], deleter)

            bdb_helpers.delete(ldb, resource, txn)

        sessid = None
        with self._database.transaction() as txn:
            if ldb.exists(resource, txn, database.bdb.DB_RMW):
                rec = ldb.get(resource, None, txn, database.bdb.DB_RMW)

                rec_list = rec.split(self.RECORD_DELIMITER)

                if len(rec_list) == 2:
                    sessid = int(rec_list[0])
                    count = int(rec_list[1])
                    if count > 1:
                        rec_list[1] = str(count - 1)
                        rec = self.RECORD_DELIMITER.join(rec_list)
                        ldb.put(resource, rec, txn)
                    else:
                        do_delete()
                else:
                    do_delete()

        return (resource, sessid)


# vim:sts=4:ts=4:sw=4:expandtab:
