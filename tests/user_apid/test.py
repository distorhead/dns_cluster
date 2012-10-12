# -*- coding: utf-8 -*-

import unittest
import urllib2
import os
import sys
import getopt
import random
import yaml
import subprocess
import time
import shutil

from lib.common import load_module


SCRIPT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestError(Exception): pass


class Test1(unittest.TestCase):
    server = {
        "pid_path": "/run/alpha_user_apid.pid",
        "url": "http://localhost:2100",
        "exec": "{}/alpha --logfile=user_api_alpha.log".format(SCRIPT_DIR_PATH),
        "pyconfig": "tests.configs.pyconf.alpha"
    }

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **{})
        self._run = kwargs.get('run', False)
        self._debug = kwargs.get('debug', False)
        self._purge = kwargs.get('purge', False)

        random.seed()

        self._alphabet = []
        for i in range(65, 90) + range(97, 122):
            self._alphabet.append(chr(i))
        self._alphabet += ['_']

    def log(self, *args):
        if self._debug:
            sys.stdout.write(args[0].format(*args[1:]) + '\n')

    def generate_str(self, min_len=1, max_len=50):
        res = ""
        len = random.randint(min_len, max_len)
        while len:
            res += random.choice(self._alphabet)
            len -= 1
        return res

    def request(self, path):
        url = self.server['url'] + '/' + path
        self.log("request: {}", url)
        resp = urllib2.urlopen(url)
        resp_msg = yaml.load(resp.read())
        if isinstance(resp_msg, dict) and resp_msg.has_key('error'):
            raise TestError("Request to '{}' returns: "
                            "'{}'".format(url, resp_msg['error']))

        return resp_msg

    def _load_pyconfig(self, path):
        self.log("loading path '{0}'", path)
        cfg_mod = load_module(path)
        return cfg_mod.cfg

    def _purge_db(self, dbenv_homedir):
        shutil.rmtree(dbenv_homedir)
        os.mkdir(dbenv_homedir)
        open(dbenv_homedir + "/.holder", 'w')

    def _run_server(self, srv_exec):
        try:
            self.log("Running server '{}'", srv_exec)
            subprocess.Popen([srv_exec],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
        except:
            pass

    def _kill_server(self, pid_path):
        try:
            pid = open(pid_path, 'r').read()
            self.log("Kill server '{}'", pid)
            subprocess.Popen(["kill " + pid],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
        except:
            pass

    def setUp(self):
        if self._run:
            self._kill_server(self.server['pid_path'])

        cfg = self._load_pyconfig(self.server['pyconfig'])
        if self._purge:
            self._purge_db(cfg['database']['dbenv_homedir'])

        if self._run:
            self._run_server(self.server['exec'])
            time.sleep(2)

    def tearDown(self):
        if self._run:
            self._kill_server(self.server['pid_path'])

    def test1(self):
        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?auth_arena=__all__"):
            arena_name = self.generate_str()
        self.request("add_arena?auth_arena=__all__&arena={}".format(arena_name))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena=__all__"))

        segment_name = self.generate_str()
        while segment_name in self.request("get_segments?auth_arena={}".format(arena_name)):
            segment_name = self.generate_str()
        self.request("add_segment?auth_arena={}&segment={}".format(arena_name, segment_name))
        self.assertTrue(segment_name in self.request("get_segments?auth_arena={}".format(arena_name)))

        self.assertTrue(self.request("get_segments?auth_arena={}".format(arena_name)) ==
                        self.request("get_segments?auth_arena=__all__&arena={}".format(arena_name)))

        zone_name = self.generate_str()
        while zone_name in self.request("get_zones?auth_arena=__all__"):
            zone_name = self.generate_str()
        self.request("add_zone?auth_arena={}&segment={}&zone={}".format(arena_name, segment_name, zone_name))
        zones = [spec['zone'] for spec in self.request("get_zones?auth_arena={}&segment={}".format(arena_name, segment_name))]
        self.assertTrue(zone_name in zones)

        # Add each record once: A, CNAME, DNAME, MX, NS, PTR, SOA, SRV, TXT
        
        # add A record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'A']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'A', 'host': host_name, 'ip': '1.1.1.1'}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&host={host}&ip={ip}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'ip': rspec['ip'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'A']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add CNAME record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'CNAME']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'CNAME', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'CNAME']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add DNAME record
        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'DNAME', 'zone_dst': self.generate_str()}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&zone_dst={zone_dst}".format(**rec))
        recs = [{'zone': rspec['zone'], 'zone_dst': rspec['zone_dst'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'DNAME']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add MX record
        while True:
            mx_hostname = self.generate_str() + '.' + zone_name
            mx_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'MX']
            if not mx_hostname in mx_hostnames:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'MX', 'domain': mx_hostname}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'MX']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add NS record
        while True:
            ns_hostname = self.generate_str() + '.' + zone_name
            ns_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'NS']
            if not ns_hostname in ns_hostnames:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'NS', 'domain': ns_hostname}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'NS']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add PTR record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'PTR']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'PTR', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'PTR']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add SOA record
        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'SOA', 'primary_ns': ns_hostname, 'resp_person': 'admin@admin.org', 'serial': 24, 'refresh': 12, 'retry': 11, 'expire': 21, 'minimum': 42}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&primary_ns={primary_ns}&resp_person={resp_person}&serial={serial}&refresh={refresh}&retry={retry}&expire={expire}&minimum={minimum}".format(**rec))
        recs = [{'zone': rspec['zone'], 'type': rspec['type'], 'primary_ns': rspec['primary_ns'], 'resp_person': rspec['resp_person'], 'serial': rspec['serial'], 'refresh': rspec['refresh'], 'retry': rspec['retry'], 'expire': rspec['expire'], 'minimum': rspec['minimum']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'SOA']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add SRV record
        while True:
            service = '_' + self.generate_str() + '._' + self.generate_str()
            services = [rspec['service'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'SRV']
            if not service in services:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'SRV', 'service': service, 'port': 10500, 'domain': self.generate_str() + '.' + zone_name}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&service={service}&port={port}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'service': rspec['service'], 'port': rspec['port'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'SRV']
        del rec['arena']
        self.assertTrue(rec in recs)

        # add TXT record
        while True:
            text = self.generate_str(1000, 10000)

            txts = [rspec['text'] for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'TXT']
            if not text in txts:
                break

        rec = {'arena': arena_name, 'zone': zone_name, 'type': 'TXT', 'text': text}
        self.request("add_record?auth_arena={arena}&zone={zone}&type={type}&text={text}".format(**rec))
        recs = [{'zone': rspec['zone'], 'text': rspec['text'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&zone={}".format(arena_name, zone_name)) if rspec['type'] == 'TXT']
        del rec['arena']
        self.assertTrue(rec in recs)

    def test2(self):
        sessid = self.request("begin_session?auth_arena=__all__")['sessid']

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        self.request("add_arena?sessid={}&arena={}".format(sessid, arena_name))
        self.assertTrue(arena_name in self.request("get_arenas?sessid={}".format(sessid)))

        segment_name = self.generate_str()
        while segment_name in self.request("get_segments?sessid={}&arena={}".format(sessid, arena_name)):
            segment_name = self.generate_str()
        self.request("add_segment?sessid={}&arena={}&segment={}".format(sessid, arena_name, segment_name))
        self.assertTrue(segment_name in self.request("get_segments?sessid={}&arena={}".format(sessid, arena_name)))

        zone_name = self.generate_str()
        while zone_name in self.request("get_zones?sessid={}".format(sessid)):
            zone_name = self.generate_str()
        self.request("add_zone?sessid={}&arena={}&segment={}&zone={}".format(sessid, arena_name, segment_name, zone_name))
        zones = [spec['zone'] for spec in self.request("get_zones?sessid={}&segment={}".format(sessid, segment_name))]
        self.assertTrue(zone_name in zones)

        # Add each record once: A, CNAME, DNAME, MX, NS, PTR, SOA, SRV, TXT
        
        # add A record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'A']
            if not host_name in hosts:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'A', 'host': host_name, 'ip': '1.1.1.1'}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&host={host}&ip={ip}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'ip': rspec['ip'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'A']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add CNAME record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'CNAME']
            if not host_name in hosts:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'CNAME', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'CNAME']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add DNAME record
        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'DNAME', 'zone_dst': self.generate_str()}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&zone_dst={zone_dst}".format(**rec))
        recs = [{'zone': rspec['zone'], 'zone_dst': rspec['zone_dst'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'DNAME']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add MX record
        while True:
            mx_hostname = self.generate_str() + '.' + zone_name
            mx_hostnames = [rspec['domain'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'MX']
            if not mx_hostname in mx_hostnames:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'MX', 'domain': mx_hostname}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'MX']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add NS record
        while True:
            ns_hostname = self.generate_str() + '.' + zone_name
            ns_hostnames = [rspec['domain'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'NS']
            if not ns_hostname in ns_hostnames:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'NS', 'domain': ns_hostname}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'NS']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add PTR record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'PTR']
            if not host_name in hosts:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'PTR', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'PTR']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add SOA record
        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'SOA', 'primary_ns': ns_hostname, 'resp_person': 'admin@admin.org', 'serial': 24, 'refresh': 12, 'retry': 11, 'expire': 21, 'minimum': 42}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&primary_ns={primary_ns}&resp_person={resp_person}&serial={serial}&refresh={refresh}&retry={retry}&expire={expire}&minimum={minimum}".format(**rec))
        recs = [{'zone': rspec['zone'], 'type': rspec['type'], 'primary_ns': rspec['primary_ns'], 'resp_person': rspec['resp_person'], 'serial': rspec['serial'], 'refresh': rspec['refresh'], 'retry': rspec['retry'], 'expire': rspec['expire'], 'minimum': rspec['minimum']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'SOA']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add SRV record
        while True:
            service = '_' + self.generate_str() + '._' + self.generate_str()
            services = [rspec['service'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'SRV']
            if not service in services:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'SRV', 'service': service, 'port': 10500, 'domain': self.generate_str() + '.' + zone_name}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&service={service}&port={port}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'service': rspec['service'], 'port': rspec['port'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'SRV']
        del rec['sessid']
        self.assertTrue(rec in recs)

        # add TXT record
        while True:
            text = self.generate_str(1000, 10000)

            txts = [rspec['text'] for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'TXT']
            if not text in txts:
                break

        rec = {'sessid': sessid, 'zone': zone_name, 'type': 'TXT', 'text': text}
        self.request("add_record?sessid={sessid}&zone={zone}&type={type}&text={text}".format(**rec))
        recs = [{'zone': rspec['zone'], 'text': rspec['text'], 'type': rspec['type']} for rspec in self.request("get_records?sessid={}&zone={}".format(sessid, zone_name)) if rspec['type'] == 'TXT']
        del rec['sessid']
        self.assertTrue(rec in recs)

        self.request("commit_session?sessid={}".format(sessid))

    def test3(self):
        sessid = self.request("begin_session?auth_arena=__all__")['sessid']

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        self.request("add_arena?sessid={}&arena={}".format(sessid, arena_name))
        self.assertTrue(arena_name in self.request("get_arenas?sessid={}".format(sessid)))

        self.request("rollback_session?sessid={}".format(sessid))
        self.assertTrue(not arena_name in self.request("get_arenas?auth_arena=__all__"))

    def test4(self):
        sessid = self.request("begin_session?auth_arena=__all__")['sessid']

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        self.request("add_arena?sessid={}&arena={}".format(sessid, arena_name))
        self.assertTrue(arena_name in self.request("get_arenas?sessid={}".format(sessid)))

        self.request("commit_session?sessid={}".format(sessid))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena=__all__"))

    def runTest(self):
        self.test1()
        self.test2()
        self.test3()
        self.test4()


if __name__ == '__main__':
    args = sys.argv[1:]
    optlist, _ = getopt.getopt(args, "drp")
    options = dict(optlist)

    if options.has_key('-d'):
        debug = True
    else:
        debug = False

    if options.has_key('-r'):
        run = True
    else:
        run = False

    if options.has_key('-p'):
        purge = True
    else:
        purge = False

    suite = unittest.TestSuite()
    suite.addTest(Test1(debug=debug, run=run, purge=purge))
    unittest.TextTestRunner(verbosity=2).run(suite)


# vim:sts=4:ts=4:sw=4:expandtab:
