import signal
import time
from twisted.internet import reactor, threads, task


def silly_blocking_bullshit():
    print 'silly_blocking_bullshit begin'
    time.sleep(10.0)
    print 'silly_blocking_bullshit end'
    return 'nothing'


def cb(res):
    print 'cb({0}) called'.format(res)


def sig_handler(signum, _):
    print 'sig_handler begin'
    d = threads.deferToThread(silly_blocking_bullshit)
    d.addCallback(cb)
    print 'sig_handler end'


def foo():
    print 'foo() called'


signal.signal(signal.SIGUSR2, sig_handler)
t = task.LoopingCall(foo)
t.start(0.5)

reactor.run()
