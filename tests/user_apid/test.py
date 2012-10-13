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

import lib.database

from lib.common import load_module
from lib.service import ServiceProvider
from lib.defs import ADMIN_ARENA_NAME


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

    def request_raw_resp(self, path):
        url = self.server['url'] + '/' + path
        self.log("request: {}", url)
        resp = urllib2.urlopen(url)
        resp_raw = resp.read()
        resp_msg = yaml.load(resp_raw)

        if not isinstance(resp_msg, dict) or not resp_msg.has_key('status'):
            raise TestError("Request to '{}' returns bad yaml answer: '{}'".format(resp_raw))
        return resp_msg

    def request(self, path):
        resp_msg = self.request_raw_resp(path)

        if resp_msg['status'] != 200:
            if not resp_msg.has_key('error'):
                raise TestError("Answer is not contain 'error' field: '{}'".format(resp_msg))

            raise TestError("Non-OK status in response, error message: '{}'".format(resp_msg['error']))

        if resp_msg.has_key('data'):
            return resp_msg['data']
        elif resp_msg.has_key('sessid'):
            return resp_msg['sessid']
        else:
            return None

    def _load_pyconfig(self, path):
        self.log("loading path '{0}'", path)
        cfg_mod = load_module(path)
        return cfg_mod.cfg

    def _purge_db(self, dbenv_homedir):
        shutil.rmtree(dbenv_homedir)
        os.mkdir(dbenv_homedir)
        open(dbenv_homedir + "/.holder", 'w')

    def _setup_admin_pswd(self, cfg):
        sp = ServiceProvider(init_srv=True, cfg=cfg)
        database = sp.get('database')
        aadb = database.dbpool().arena_auth.dbhandle()
        aadb.put(ADMIN_ARENA_NAME, '')

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
            self._setup_admin_pswd(cfg)

        if self._run:
            self._run_server(self.server['exec'])
            time.sleep(2)

    def tearDown(self):
        if self._run:
            self._kill_server(self.server['pid_path'])

    def test1(self):
        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?auth_arena=__all__&auth_key=&arena={}&key={}".format(arena_name, arena_key))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="))

        segment_name = self.generate_str()
        while segment_name in self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)):
            segment_name = self.generate_str()
        self.request("add_segment?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))
        self.assertTrue(segment_name in self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)))

        self.assertTrue(self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)) ==
                        self.request("get_segments?auth_arena=__all__&auth_key=&arena={}".format(arena_name)))

        zone_name = self.generate_str()
        while zone_name in self.request("get_zones?auth_arena=__all__&auth_key="):
            zone_name = self.generate_str()
        self.request("add_zone?auth_arena={}&auth_key={}&segment={}&zone={}".format(arena_name, arena_key, segment_name, zone_name))
        zones = [spec['zone'] for spec in self.request("get_zones?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))]
        self.assertTrue(zone_name in zones)

        # Add each record once: A, CNAME, DNAME, MX, NS, PTR, SOA, SRV, TXT
        
        # add A record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'A']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'A', 'host': host_name, 'ip': '1.1.1.1'}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&ip={ip}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'ip': rspec['ip'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'A']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add CNAME record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'CNAME']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'CNAME', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'CNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add DNAME record
        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'DNAME', 'zone_dst': self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&zone_dst={zone_dst}".format(**rec))
        recs = [{'zone': rspec['zone'], 'zone_dst': rspec['zone_dst'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'DNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add MX record
        while True:
            mx_hostname = self.generate_str() + '.' + zone_name
            mx_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'MX']
            if not mx_hostname in mx_hostnames:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'MX', 'domain': mx_hostname}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'MX']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add NS record
        while True:
            ns_hostname = self.generate_str() + '.' + zone_name
            ns_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'NS']
            if not ns_hostname in ns_hostnames:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'NS', 'domain': ns_hostname}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'NS']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add PTR record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'PTR']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'PTR', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'PTR']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add SOA record
        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'SOA', 'primary_ns': ns_hostname, 'resp_person': 'admin@admin.org', 'serial': 24, 'refresh': 12, 'retry': 11, 'expire': 21, 'minimum': 42}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&primary_ns={primary_ns}&resp_person={resp_person}&serial={serial}&refresh={refresh}&retry={retry}&expire={expire}&minimum={minimum}".format(**rec))
        recs = [{'zone': rspec['zone'], 'type': rspec['type'], 'primary_ns': rspec['primary_ns'], 'resp_person': rspec['resp_person'], 'serial': rspec['serial'], 'refresh': rspec['refresh'], 'retry': rspec['retry'], 'expire': rspec['expire'], 'minimum': rspec['minimum']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SOA']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add SRV record
        while True:
            service = '_' + self.generate_str() + '._' + self.generate_str()
            services = [rspec['service'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SRV']
            if not service in services:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'SRV', 'service': service, 'port': 10500, 'domain': self.generate_str() + '.' + zone_name}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&service={service}&port={port}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'service': rspec['service'], 'port': rspec['port'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SRV']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        # add TXT record
        while True:
            text = self.generate_str(1000, 10000)

            txts = [rspec['text'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'TXT']
            if not text in txts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'TXT', 'text': text}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&text={text}".format(**rec))
        recs = [{'zone': rspec['zone'], 'text': rspec['text'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'TXT']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)


    def test2(self):
        sessid = self.request("begin_session?auth_arena=__all__&auth_key=")

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?sessid={}&arena={}&key={}".format(sessid, arena_name, arena_key))
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
        sessid = self.request("begin_session?auth_arena=__all__&auth_key=")

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?sessid={}&arena={}&key={}".format(sessid, arena_name, arena_key))
        self.assertTrue(arena_name in self.request("get_arenas?sessid={}".format(sessid)))

        self.request("rollback_session?sessid={}".format(sessid))
        self.assertTrue(not arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="))


    def test4(self):
        sessid = self.request("begin_session?auth_arena=__all__&auth_key=")

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?sessid={}".format(sessid)):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?sessid={}&arena={}&key={}".format(sessid, arena_name, arena_key))
        self.assertTrue(arena_name in self.request("get_arenas?sessid={}".format(sessid)))

        self.request("commit_session?sessid={}".format(sessid))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="))


    def test5(self):
        new_key = self.generate_str()
        self.request("mod_auth?auth_arena={}&auth_key=&target={}&key={}".format(ADMIN_ARENA_NAME, ADMIN_ARENA_NAME, new_key))

        resp = self.request_raw_resp("get_arenas?auth_arena={}&auth_key=badkey".format(ADMIN_ARENA_NAME))
        self.assertEqual(resp['status'], 400)

        self.request("mod_auth?auth_arena={}&auth_key={}&target={}&key=".format(ADMIN_ARENA_NAME, new_key, ADMIN_ARENA_NAME))

        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?auth_arena={}&auth_key=".format(ADMIN_ARENA_NAME)):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?auth_arena={}&auth_key=&arena={}&key={}".format(ADMIN_ARENA_NAME, arena_name, arena_key))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena={}&auth_key=".format(ADMIN_ARENA_NAME)))

        # change arena key in one way
        new_arena_key = self.generate_str()
        self.request("mod_auth?auth_arena={}&auth_key={}&target={}&key={}".format(arena_name, arena_key, arena_name, new_arena_key))
        resp = self.request_raw_resp("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key))
        self.assertEqual(resp['status'], 400)
        resp = self.request_raw_resp("get_segments?auth_arena={}&auth_key={}".format(arena_name, new_arena_key))
        self.assertEqual(resp['status'], 200)

        # restore arena key in second possible way
        self.request("mod_auth?auth_arena={}&auth_key={}&key={}".format(arena_name, new_arena_key, arena_key))
        resp = self.request_raw_resp("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key))
        self.assertEqual(resp['status'], 200)
        resp = self.request_raw_resp("get_segments?auth_arena={}&auth_key={}".format(arena_name, new_arena_key))
        self.assertEqual(resp['status'], 400)


    def test6(self):
        arena_name = self.generate_str()
        while arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="):
            arena_name = self.generate_str()
        arena_key = self.generate_str()
        self.request("add_arena?auth_arena=__all__&auth_key=&arena={}&key={}".format(arena_name, arena_key))
        self.assertTrue(arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="))

        segment_name = self.generate_str()
        while segment_name in self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)):
            segment_name = self.generate_str()
        self.request("add_segment?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))
        self.assertTrue(segment_name in self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)))

        self.assertTrue(self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)) ==
                        self.request("get_segments?auth_arena=__all__&auth_key=&arena={}".format(arena_name)))

        zone_name = self.generate_str()
        while zone_name in self.request("get_zones?auth_arena=__all__&auth_key="):
            zone_name = self.generate_str()
        self.request("add_zone?auth_arena={}&auth_key={}&segment={}&zone={}".format(arena_name, arena_key, segment_name, zone_name))
        zones = [spec['zone'] for spec in self.request("get_zones?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))]
        self.assertTrue(zone_name in zones)

        # Add and delete each record once: A, CNAME, DNAME, MX, NS, PTR, SOA, SRV, TXT
        
        # add and delete A record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'A']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'A', 'host': host_name, 'ip': '1.1.1.1'}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&ip={ip}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'ip': rspec['ip'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'A']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&ip={ip}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'ip': rspec['ip'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'A']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete CNAME record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'CNAME']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'CNAME', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'CNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'CNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete DNAME record
        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'DNAME', 'zone_dst': self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&zone_dst={zone_dst}".format(**rec))
        recs = [{'zone': rspec['zone'], 'zone_dst': rspec['zone_dst'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'DNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&zone_dst={zone_dst}".format(**rec))
        recs = [{'zone': rspec['zone'], 'zone_dst': rspec['zone_dst'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'DNAME']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete MX record
        while True:
            mx_hostname = self.generate_str() + '.' + zone_name
            mx_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'MX']
            if not mx_hostname in mx_hostnames:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'MX', 'domain': mx_hostname}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'MX']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'MX']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete NS record
        while True:
            ns_hostname = self.generate_str() + '.' + zone_name
            ns_hostnames = [rspec['domain'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'NS']
            if not ns_hostname in ns_hostnames:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'NS', 'domain': ns_hostname}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'NS']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'NS']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete PTR record
        while True:
            host_name = self.generate_str()
            hosts = [rspec['host'] for rspec in self.request("get_records?auth_arena={}&&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'PTR']
            if not host_name in hosts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'PTR', 'host': host_name, 'domain': self.generate_str() + '.' + self.generate_str()}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'PTR']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&host={host}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'host': rspec['host'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'PTR']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete SOA record
        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'SOA', 'primary_ns': ns_hostname, 'resp_person': 'admin@admin.org', 'serial': 24, 'refresh': 12, 'retry': 11, 'expire': 21, 'minimum': 42}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&primary_ns={primary_ns}&resp_person={resp_person}&serial={serial}&refresh={refresh}&retry={retry}&expire={expire}&minimum={minimum}".format(**rec))
        recs = [{'zone': rspec['zone'], 'type': rspec['type'], 'primary_ns': rspec['primary_ns'], 'resp_person': rspec['resp_person'], 'serial': rspec['serial'], 'refresh': rspec['refresh'], 'retry': rspec['retry'], 'expire': rspec['expire'], 'minimum': rspec['minimum']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SOA']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&primary_ns={primary_ns}&resp_person={resp_person}&serial={serial}&refresh={refresh}&retry={retry}&expire={expire}&minimum={minimum}".format(**rec))
        recs = [{'zone': rspec['zone'], 'type': rspec['type'], 'primary_ns': rspec['primary_ns'], 'resp_person': rspec['resp_person'], 'serial': rspec['serial'], 'refresh': rspec['refresh'], 'retry': rspec['retry'], 'expire': rspec['expire'], 'minimum': rspec['minimum']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SOA']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete SRV record
        while True:
            service = '_' + self.generate_str() + '._' + self.generate_str()
            services = [rspec['service'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SRV']
            if not service in services:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'SRV', 'service': service, 'port': 10500, 'domain': self.generate_str() + '.' + zone_name}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&service={service}&port={port}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'service': rspec['service'], 'port': rspec['port'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SRV']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&service={service}&port={port}&domain={domain}".format(**rec))
        recs = [{'zone': rspec['zone'], 'service': rspec['service'], 'port': rspec['port'], 'domain': rspec['domain'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'SRV']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # add and delete TXT record
        while True:
            text = self.generate_str(1000, 10000)

            txts = [rspec['text'] for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'TXT']
            if not text in txts:
                break

        rec = {'arena': arena_name, 'key': arena_key, 'zone': zone_name, 'type': 'TXT', 'text': text}
        self.request("add_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&text={text}".format(**rec))
        recs = [{'zone': rspec['zone'], 'text': rspec['text'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'TXT']
        del rec['arena']
        del rec['key']
        self.assertTrue(rec in recs)

        rec['arena'] = arena_name
        rec['key'] = arena_key
        self.request("del_record?auth_arena={arena}&auth_key={key}&zone={zone}&type={type}&text={text}".format(**rec))
        recs = [{'zone': rspec['zone'], 'text': rspec['text'], 'type': rspec['type']} for rspec in self.request("get_records?auth_arena={}&auth_key={}&zone={}".format(arena_name, arena_key, zone_name)) if rspec['type'] == 'TXT']
        del rec['arena']
        del rec['key']
        self.assertTrue(not rec in recs)


        # delete zone
        self.request("del_zone?auth_arena={}&auth_key={}&segment={}&zone={}".format(arena_name, arena_key, segment_name, zone_name))
        zones = [spec['zone'] for spec in self.request("get_zones?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))]
        self.assertTrue(not zone_name in zones)

        # delete segment
        self.request("del_segment?auth_arena={}&auth_key={}&segment={}".format(arena_name, arena_key, segment_name))
        self.assertTrue(not segment_name in self.request("get_segments?auth_arena={}&auth_key={}".format(arena_name, arena_key)))

        # delete arena
        self.request("del_arena?auth_arena=__all__&auth_key=&arena={}&key={}".format(arena_name, arena_key))
        self.assertTrue(not arena_name in self.request("get_arenas?auth_arena=__all__&auth_key="))

    def runTest(self):
        self.test1()
        self.test2()
        self.test3()
        self.test4()
        self.test5()
        self.test6()


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
