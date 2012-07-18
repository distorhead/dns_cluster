from context import *
from lib import lock
from twisted.internet import reactor

ldb = lock.manager().dbpool().lock.open()
lhdb = lock.manager().dbpool().lock_hier.open()


def _callback(res):
    print "DONE with result '{0}'".format(res)

def _errback(failure):
    print "FAILED with result '{0}'".format(failure)

def acquire(resource, sessid):
    d = lock.manager().acquire(resource, sessid)
    d.addCallback(_callback)
    d.addErrback(_errback)

def release(resource):
    lock.manager().release(resource)
