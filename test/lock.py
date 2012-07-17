from context import *
from lib.lock import context as lock_context
from twisted.internet import reactor

ldb = lock_context().dbpool().lock.open()
lhdb = lock_context().dbpool().lock_hier.open()


def _callback(res):
    print "DONE with result '{0}'".format(res)

def _errback(failure):
    print "FAILED with result '{0}'".format(failure)

def acquire(resource, sessid):
    d = lock_context().acquire(resource, sessid)
    d.addCallback(_callback)
    d.addErrback(_errback)

def release(resource):
    lock_context().release(resource)
