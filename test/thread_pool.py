import time
import signal

from twisted.internet import reactor, threads
from twisted.python.threadpool import ThreadPool
from threading import current_thread


tp = ThreadPool(minthreads=6, maxthreads=10, name="mypool")
tp.start()


def worker():
    print current_thread().ident


def cb(res):
    d = threads.deferToThreadPool(reactor, tp, worker)
    d.addCallback(cb)


cb(None)


def sig_handler(_, __):
    print 'sig handler'
    reactor.stop()
    tp.stop()
    exit(0)


signal.signal(signal.SIGINT, sig_handler)
reactor.run()
