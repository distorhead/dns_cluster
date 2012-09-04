# -*- coding: utf-8 -*-

from twisted.python import log
from twisted.internet import reactor, defer, threads


from lib import database
from lib import bdb_helpers
from lib.service import ServiceProvider


@ServiceProvider.register("locker", deps=["database"])
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
        def __init__(self, resource, sessid, timeout, wait_interval,
                     max_attempts, user_defer):
            self.resource = resource
            self.sessid = int(sessid)
            self.timeout = timeout
            self.wait_interval = wait_interval
            self.max_attempts = max_attempts
            self.user_defer = user_defer
            self.attempt = 1

    def __init__(self, sp, *args, **kwargs):
        self._database = sp.get("database")
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())

    def dbpool(self):
        return self._dbpool

    def acquire(self, resource, sessid, **kwargs):
        """
        Acquire lock for resource in given session.
        After timeout (in seconds) the lock will be released automatically.
        If lock cannot be acquired now, attempt will be repeated after
            wait_interval seconds. Maximum max_attempts allowed. To turn it off
            set max_attempts=0, it will try forever.
        All database related work will be in separate threads.
        Method returns deferred that will fire up when lock acquired.
        If lock is not acquired after specified number of attempts,
            then errback will be called.
        """

        timeout = kwargs.get("timeout", self.TIMEOUT_DEFAULT)
        wait_interval = kwargs.get("wait_interval", self.WAIT_INTERVAL_SECONDS_DEFAULT)
        max_attempts = kwargs.get("max_attempts", self.MAX_ATTEMPTS_DEFAULT)
        user_defer = defer.Deferred()

        l = self.Lock(resource, sessid, timeout, wait_interval,
                      max_attempts, user_defer)
        self._try_acquire(l)
        return user_defer

    def release(self, resource):
        d = threads.deferToThread(self._do_release, resource)

        def eb(failure):
            log.err("Unable to release lock for resource '{}'".format(resource))
            return failure

        d.addErrback(eb)
        return d


    def _try_acquire(self, l):
        d = threads.deferToThread(self._do_acquire, l)
        d.addCallback(self._on_acquire_result, l)

        def eb(failure):
            log.err("Unable to acquire lock for resource '{}' in session '{}'".format(
                     l.resource, l.sessid))
            l.user_defer.errback(failure)

        d.addErrback(eb)

    def _do_acquire(self, l):
        """
        Method uses DB blocking API calls and should be called from separate thread.
        Returns True if lock is acquired, False otherwise.
        """

        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        with self._database.transaction() as txn:
            if ldb.exists(l.resource, txn):
                # lock already exists
                print "lock already exists"
                rec = ldb.get(l.resource, txn)
                rec_list = rec.split(self.RECORD_DELIMITER)

                rec_sessid = int(rec_list[0])
                if l.sessid == rec_sessid:
                    # updating existed lock counter
                    print "updating existed lock counter"
                    rec_count = int(rec_list[1])
                    rec_count += 1
                    rec_list[1] = str(rec_count)
                    ldb.put(l.resource, self.RECORD_DELIMITER.join(rec_list), txn)
                    return True
                else:
                    # lock in other session, delay repeat call
                    print "lock in other session, delay repeat call"
                    return False

            else:
                # lock doesn't exist, checking for common and special locks
                print "lock doesn't exist, checking for common locks"
                resource_list = l.resource.split(self.RESOURCE_DELIMITER)
                parent_res = ""
                for res in resource_list[:-1]:
                    if parent_res:
                        res = parent_res + self.RESOURCE_DELIMITER + res;
                    parent_res = res
                    print "checking", res

                    if ldb.exists(res, txn):
                        # common lock exists
                        rec = ldb.get(res, txn)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = int(rec_list[0])
                        if l.sessid == rec_sessid:
                            # common lock belongs to this session, acquiring
                            print "common lock in this session exists, acquiring"
                            self._insert_lock(l, txn)
                            return True
                        else:
                            # common lock belongs to other session, delay repeat call
                            print("common lock in other session exists, "
                                  "delay repeat call")
                            return False

                # check for special lock
                print("common locks don't exist, checking special locks "
                      "for this resource")
                ldb_keys = bdb_helpers.get_all(lhdb, l.resource, txn)
                for ldb_key in ldb_keys:
                    if ldb.exists(ldb_key, txn):
                        rec = ldb.get(ldb_key, txn)
                        rec_list = rec.split(self.RECORD_DELIMITER)
                        rec_sessid = int(rec_list[0])
                        if l.sessid != rec_sessid:
                            # special lock in other session exists,
                            #   delay repeat call
                            print("special lock in other session exists, "
                                  "delay repeat call")
                            return False

                # special locks don't exist or all locks
                #   from this session, acquiring
                print("special locks don't exist "
                      "or all from this session, acquiring")
                self._insert_lock(l, txn)
                return True

    def _on_acquire_result(self, is_acquired, l):
        if is_acquired:
            reactor.callLater(l.timeout, self.release, l.resource)
            l.user_defer.callback((l.resource, l.sessid))
        elif l.attempt == l.max_attempts:
            # no errback, exceptions is for exceptional situations
            l.user_defer.callback(None)
        else:
            l.attempt += 1
            reactor.callLater(l.wait_interval, self._try_acquire, l)

    def _insert_lock(self, l, txn):
        ldb = self.dbpool().lock.dbhandle()
        lhdb = self.dbpool().lock_hier.dbhandle()

        rec = str(l.sessid) + self.RECORD_DELIMITER + "1"
        ldb.put(l.resource, rec, txn)

        def inserter(res):
            print 'inserting', res, 'into lock_hier database for resource', l.resource
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
                print "deleting '{0} -> {1}'".format(res, resource)
                bdb_helpers.delete_pair(lhdb, res, resource, txn)

            resource_list = resource.split(self.RESOURCE_DELIMITER)
            self._for_each_resource(resource_list[:-1], deleter)

            bdb_helpers.delete(ldb, resource, txn)

        sessid = None
        with self._database.transaction() as txn:
            if ldb.exists(resource, txn):
                rec = ldb.get(resource, txn)
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
