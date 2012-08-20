from twisted.internet import reactor, threads
from twisted.python.threadpool import ThreadPool


tp = ThreadPool()


def foo(arg=None, counter=5):
    print 'foo({0}) called'.format(arg)
    if counter == 1:
        return

    threads.deferToThread(foo, 1, counter - 1)
    threads.deferToThread(foo, 2, counter - 1)


threads.deferToThread(foo)

reactor.suggestThreadPoolSize(1)
reactor.run()
