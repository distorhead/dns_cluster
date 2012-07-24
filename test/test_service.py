from lib.service import *


@ServiceProvider.register("ololo", deps=["base"])
class MyService1:
    def __init__(self, sp, *args, **kwargs):
        print 'MyService1.__init__, args="{0}", kwargs="{1}", sp="{2}"'.format(args, kwargs, sp)
        sp.get("base").foo()

    def foo(self):
        print 'MyService1.foo'


@ServiceProvider.register("base")
class MyService2:
    def __init__(self, *args, **kwargs):
        print 'MyService2.__init__, args="{0}", kwargs="{1}"'.format(args, kwargs)

    def foo(self):
        print 'MyService2.foo'


sp = ServiceProvider(with_srv=True)
