# -*- coding: utf-8 -*-

import unittest
import os


class TestError(Exception): pass


class Test1(unittest.TestCase):
    SERVERS = {
        "alpha": {
            "exec": "tests/syncd/alpha --logfile=alpha.log",
            "pyconfig": "tests.configs.syncd.alpha"
        },

        "beta": {
            "exec": "tests/syncd/beta --logfile=beta.log",
            "pyconfig": "tests.configs.syncd.beta"
        },

        "gamma": {
            "exec": "tests/syncd/gamma --logfile=gamma.log",
            "pyconfig": "tests.configs.syncd.gamma"
        }
    }

    def __init__(self, target_server, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        if target_server is None:
            servers = list(self.SERVERS)
            if len(servers) == 0:
                raise TestError("No test servers defined!")

            self.target = servers[0]
        elif target_server in self.SERVERS:
            self.target = target_server
        else:
            raise TestError("No such server defined '{0}'".format(target_server))

    def _get_pid(self, srv):
        with open("/run/" + srv + ".pid") as f:
            pid = f.read()
            return pid

    def setUp(self):
        for _, srv in self.SERVERS.iteritems():
            os.system(srv["exec"])

    def tearDown(self):
        for srv in self.SERVERS:
            pid = self._get_pid(srv)
            os.system("kill " + pid)

    def runTest(self):
        print 'RUNNING TEST on target:', self.target


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        target_server = sys.argv[1]
    else:
        target_server = None

    suite = unittest.TestSuite()
    suite.addTest(Test1(target_server))
    unittest.TextTestRunner(verbosity=2).run(suite)


# vim:sts=4:ts=4:sw=4:expandtab:
