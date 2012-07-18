# -*- coding: utf-8 -*-

from twisted.python import log
from twisted.internet import reactor, defer

from lib import database
from lib import bdb_helpers
from lib.common import singleton


class LockError(Exception): pass


class Lock(object):
    """
    Class provides wrapper over lock manager with object 
    interface and context manager support.
    """

    def __init__(self, resource, sessid, timeout):
        self._resource = resource
        self._sessid = sessid
        self._timeout = timeout
        self._used = False
        self._locked = False

    def _check_used(self):
        if self._used:
            raise LockError("Lock object must not be used repeatedly")

    def __enter__(self):
        return self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()

    def acquire(self):
        self._check_used()
        if self._locked:
            raise LockError("Lock is already acquired")

        res = manager().acquire(self._resource,
                self._sessid, self._timeout)
        self._locked = True
        return res

    def release(self):
        self._check_used()
        if not self._locked:
            raise LockError("Lock is not acquired")

        manager().release(self._resource)
        self._used = True

    def is_locked(self):
        if not self._used:
            return self._locked
        else:
            return False

    def is_used(self):
        return self._used

    def resource(self):
        return self._resource

    def sessid(self):
        return self._sessid


@singleton
class manager(object):
    """
    Class used to manage resorces locks.
    Resorce locks stored into database and
    may be shared between many processes.
    """

    RESOURCE_DELIMITER = "::"
    RECORD_DELIMITER = " "
    WAIT_INTERVAL_SECONDS_DEFAULT = 2
    WAIT_COUNTER_MAX = 10

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

    def __init__(self):
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             database.context().dbenv(),
                                             database.context().dbfile())

    def lock(self, resource, sessid, timeout=None):
        return Lock(resource, sessid, timeout)

    def acquire(self, resource, sessid, timeout=None):
        if timeout is None:
            timeout = self.WAIT_INTERVAL_SECONDS_DEFAULT

        return self._do_acquire(resource, sessid, timeout, None, 1)

    def release(self, resource):
        ldb = self.dbpool().lock.open()
        lhdb = self.dbpool().lock_hier.open()

        with database.context().transaction() as txn:
            def deleter(res):
                print "deleting '{0} -> {1}'".format(res, resource)
                bdb_helpers.delete_pair(lhdb, res, resource, txn)

            resource_list = resource.split(self.RESOURCE_DELIMITER)
            self._for_each_resource(resource_list[:-1], deleter)

            bdb_helpers.delete(ldb, resource, txn)

        ldb.close()
        lhdb.close()

    def dbpool(self):
        return self._dbpool


    def _do_acquire(self, resource, sessid, timeout, deferred, count):
        ldb = self.dbpool().lock.open()
        lhdb = self.dbpool().lock_hier.open()
        sessid = int(sessid)

        def finalize():
            ldb.close()
            lhdb.close()

        with database.context().transaction() as txn:
            if ldb.exists(resource, txn):
                # lock already exists
                print "lock already exists"
                rec = ldb.get(resource, txn)
                rec_list = rec.split(self.RECORD_DELIMITER)

                rec_sessid = self._to_int(rec_list[0])
                if sessid == rec_sessid:
                    # updating existed lock counter
                    print "updating existed lock counter"
                    rec_count = self._to_int(rec_list[1])
                    rec_count += 1
                    rec_list[1] = str(rec_count)
                    ldb.put(resource, self.RECORD_DELIMITER.join(rec_list), txn)
                    finalize()
                    return self._finalize_acquire(resource, sessid, deferred)
                else:
                    # lock in other session, delay repeat call
                    print "lock in other session, delay repeat call"
                    finalize()
                    return self._defer_acquire(resource, sessid, timeout,
                                               deferred, count)
            else:
                # lock doesn't exist, checking for common locks
                print "lock doesn't exist, checking for common locks"
                resource_list = resource.split(self.RESOURCE_DELIMITER)
                parent_res = ""
                for res in resource_list[:-1]:
                    if parent_res:
                        res = parent_res + self.RESOURCE_DELIMITER + res;
                    parent_res = res
                    print 'checking', res

                    if ldb.exists(res, txn):
                        rec = ldb.get(res, txn)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = self._to_int(rec_list[0])
                        if sessid == rec_sessid:
                            # common lock in this session exists, acquiring
                            print "common lock in this session exists, acquiring"
                            self._insert_lock(resource, sessid, ldb, lhdb, txn)
                            finalize()
                            return self._finalize_acquire(resource, sessid, deferred)
                        else:
                            # common lock in other session exists,
                            #   delay repeat call
                            print("common lock in other session exists, "
                                  "delay repeat call")
                            finalize()
                            return self._defer_acquire(resource, sessid, timeout,
                                                       deferred, count)

                # common locks don't exist, checking special locks
                #   for this resource
                print("common locks don't exist, checking special locks "
                      "for this resource")
                ldb_keys = bdb_helpers.get_all(lhdb, resource, txn)
                for ldb_key in ldb_keys:
                    if ldb.exists(ldb_key, txn):
                        rec = ldb.get(ldb_key, txn)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = self._to_int(rec_list[0])
                        if sessid != rec_sessid:
                            # special lock in other session exists,
                            #   delay repeat call
                            print("special lock in other session exists, "
                                  "delay repeat call")
                            finalize()
                            return self._defer_acquire(resource, sessid, timeout,
                                                       deferred, count)

                # special locks don't exist or all locks
                #   from this session, acquiring
                print("special locks don't exist "
                      "or all from this session, acquiring")
                self._insert_lock(resource, sessid, ldb, lhdb, txn)
                finalize()
                return self._finalize_acquire(resource, sessid, deferred)

    def _finalize_acquire(self, resource, sessid, deferred):
        if deferred is None:
            return defer.succeed((resource, sessid))
        else:
            deferred.callback((resource, sessid))
            return None

    def _defer_acquire(self, resource, sessid, timeout, deferred, count):
        if deferred is None:
            deferred = defer.Deferred()
        elif count > self.WAIT_COUNTER_MAX:
            deferred.errback((resource, sessid))
            return

        reactor.callLater(timeout**count, self._do_acquire, resource,
                          sessid, timeout, deferred, count+1)
        return deferred

    def _insert_lock(self, resource, sessid, ldb, lhdb, txn):
        rec = str(sessid) + self.RECORD_DELIMITER + str(1)
        ldb.put(resource, rec, txn)

        def inserter(res):
            print 'inserting', res, 'into lock_hier database for resource', resource
            lhdb.put(res, resource, txn)

        resource_list = resource.split(self.RESOURCE_DELIMITER)
        self._for_each_resource(resource_list[:-1], inserter)

    def _for_each_resource(self, resource_list, func):
        parent_res = ""
        for res in resource_list:
            if parent_res:
                res = parent_res + self.RESOURCE_DELIMITER + res;
            parent_res = res
            func(res)

    def _to_int(self, val):
        try:
            return int(val)
        except ValueError as e:
            raise LockError(e)


# vim:sts=4:ts=4:sw=4:expandtab:
