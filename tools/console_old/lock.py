from context import *
from twisted.internet import reactor


lock = sp.get("lock")

ldb = lock.dbpool().lock.open()
lhdb = lock.dbpool().lock_hier.open()


def _callback(res):
    print "DONE with result '{0}'".format(res)

def _errback(failure):
    print "FAILED with result '{0}'".format(failure)

def acquire(resource, sessid):
    d = lock.acquire(resource, sessid)
    d.addCallback(_callback)
    d.addErrback(_errback)

def release(resource):
    lock.release(resource)
