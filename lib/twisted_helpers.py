# -*- coding: utf-8 -*-

import functools
from twisted.internet import threads, defer


def threaded(func):
    """
    Decorator used to make function run in separate thread
    (currently used for _do_run accross session operations).
    Function will return deferred object if decorated.
    """
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        d = threads.deferToThread(func, *args, **kwargs)
        return d

    return wrapper


def asynchronous(func):
    """
    Decorator used to change function interface as beeing asynchronous.
    Function signature should be compatible with: func(defer, *args, **kwargs).
    Function will return deferred object if decorated.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        d = defer.Deferred()
        func(d, *args, **kwargs)
        return d

    return wrapper


def asynchronous2(meth):
    """
    The same but for methods.
    """

    @functools.wraps(meth)
    def wrapper(self, *args, **kwargs):
        d = defer.Deferred()
        meth(self, d, *args, **kwargs)
        return d

    return wrapper


# vim:sts=4:ts=4:sw=4:expandtab:
