# -*- coding: utf-8 -*-

import time

from twisted.python import log

from lib import database
from lib import bdb_helpers
from lib.service import ServiceProvider


class LockError(Exception): pass


@ServiceProvider.register("lock", deps=["database"])
class manager(object):
    """
    Class used to manage resorces locks.
    Resorce locks stored into database and
    may be shared between many processes.
    """

    RESOURCE_DELIMITER = "::"
    RECORD_DELIMITER = " "

    REPEAT_PERIOD_SECONDS_DEFAULT = 30.0
    MAX_ATTEMPTS_DEFAULT = 2

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
        },
        "session_lock": {
            "type": database.bdb.DB_BTREE,
            "flags": database.bdb.DB_DUP|database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE
        },
        "lock_wait": {
            "type": database.bdb.DB_HASH,
            "flags": database.bdb.DB_DUP|database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    class Lock(object):
        def __init__(self, resource, sessid):
            self.resource = resource
            self.sessid = sessid

    def __init__(self, sp, *args, **kwargs):
        self._database = sp.get('database')
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())

    def dbpool(self):
        return self._dbpool

    def acquire(self, resource, sessid, **kwargs):
        """
        Blocking method to acquire a lock.
        Returns True if acquire successful.
        Returns False if acquire cannot be done.
        Method internally open's transaction when trying to acquire lock.
        """

        repeat_period = kwargs.get('repeat_period', self.REPEAT_PERIOD_SECONDS_DEFAULT)
        max_attempts = kwargs.get('max_attempts', self.MAX_ATTEMPTS_DEFAULT)

        ldb = self.dbpool().lock.dbhandle()
        lwdb = self.dbpool().lock_wait.dbhandle()
        attempt = 1
        while attempt <= max_attempts:
            try:
                self.try_acquire(resource, sessid)
                bdb_helpers.delete(lwdb, str(sessid), None)
                return True
            except LockError:
                with self._database.transaction() as txn:
                    next_resource = resource
                    while not next_resource is None:
                        holder_info = ldb.get(next_resource, None, txn)
                        if holder_info is None:
                            # this is inconsistency of database
                            return False
                        else:
                            holder_info_list = holder_info.split(self.RECORD_DELIMITER, 1)
                            if len(holder_info_list) != 2:
                                # this is inconsistency of database
                                return False
                            else:
                                holder_sessid = int(holder_info_list[0])
                                if holder_sessid == sessid:
                                    # deadlock ring detected
                                    log.msg("deadlock ring detected when trying "
                                            "to acquire lock for "
                                            "resource '{}' in session '{}'".format(
                                                resource, sessid))
                                    return False

                                next_resource = lwdb.get(str(holder_sessid), None, txn)

                    if not lwdb.exists(str(sessid), txn):
                        lwdb.put(str(sessid), resource, txn)

                time.sleep(repeat_period)
                attempt += 1

        bdb_helpers.delete(lwdb, str(sessid), None)
        return False

    @database.transactional(database_srv_attr='_database')
    def try_acquire(self, resource, sessid, **kwargs):
        if self._is_valid_resource(resource):
            txn = kwargs['txn']
            l = self.Lock(resource, sessid)
            if not self._do_acquire(l, txn):
                self._lock_failure(resource, sessid)
        else:
            self._resource_not_valid_failure(resource)

    @database.transactional(database_srv_attr='_database')
    def release(self, resource, **kwargs):
        txn = kwargs['txn']
        purge = kwargs.get('purge', False)
        return self._do_release(resource, purge, txn)

    @database.transactional(database_srv_attr='_database')
    def release_session(self, sessid, **kwargs):
        txn = kwargs['txn']
        return self._do_release_session(sessid, txn)


    def _is_valid_resource(self, resource):
        return len(resource) > 0

    def _do_acquire(self, l, txn=None):
        """
        Method uses DB blocking API calls and should be called from separate thread.
        Returns True if lock is acquired, False otherwise.
        """

        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

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
        sldb = self.dbpool().session_lock.dbhandle()

        sessid_str = str(l.sessid)
        rec = sessid_str + self.RECORD_DELIMITER + "1"
        ldb.put(l.resource, rec, txn)
        sldb.put(sessid_str, l.resource, txn)

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

    def _do_release(self, resource, purge=False, txn=None):
        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        def do_delete():
            def deleter(res):
                bdb_helpers.delete_pair(lhdb, res, resource, txn)

            resource_list = resource.split(self.RESOURCE_DELIMITER)
            self._for_each_resource(resource_list[:-1], deleter)

            bdb_helpers.delete(ldb, resource, txn)

        sessid = None
        if ldb.exists(resource, txn, database.bdb.DB_RMW):
            rec = ldb.get(resource, None, txn, database.bdb.DB_RMW)

            rec_list = rec.split(self.RECORD_DELIMITER)

            if len(rec_list) == 2:
                sessid = int(rec_list[0])
                count = int(rec_list[1])
                if not purge and count > 1:
                    rec_list[1] = str(count - 1)
                    rec = self.RECORD_DELIMITER.join(rec_list)
                    ldb.put(resource, rec, txn)
                else:
                    do_delete()
            else:
                do_delete()

        return (resource, sessid)

    def _do_release_session(self, sessid, txn=None):
        sldb = self.dbpool().session_lock.dbhandle()
        sessid_str = str(sessid)
        resources = bdb_helpers.get_all(sldb, sessid_str, txn)
        for res in resources:
            self._do_release(res, True, txn)

        bdb_helpers.delete(sldb, sessid_str, txn)

    def _lock_failure(self, resource, sessid):
        raise LockError("Unable to acquire lock for resource "
                        "'{}' in session '{}'".format(resource, sessid))

    def _resource_not_valid_failure(self, resource):
        raise LockError("Bad resource '{}'".format(resource))


# vim:sts=4:ts=4:sw=4:expandtab:
