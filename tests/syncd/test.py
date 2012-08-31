# -*- coding: utf-8 -*-

import unittest
import os, sys, shutil
import random
import getopt
import signal
import time

import lib.database
import lib.action

from lib.common import load_module, split, reorder
from lib import bdb_helpers
from lib.service import ServiceProvider
from lib.app.sync.sync import SyncApp


act_mods = __import__("lib.actions", globals(), locals(), ['*'])
for act_mod_name in act_mods.__dict__['__all__']:
    act_mod = __import__("lib.actions." + act_mod_name, globals(), locals(), ['*'])
    act_name = act_mod.__dict__['__all__'][0]
    globals()[act_name] = getattr(act_mod, act_name)


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


class TestError(Exception): pass


class Test1(unittest.TestCase):
    servers = {
        "alpha": {
            "exec": "{}/alpha --logfile=alpha.log".format(SCRIPT_PATH),
            "pyconfig": "tests.configs.pyconf.alpha"
        },

        "beta": {
            "exec": "{}/beta --logfile=beta.log".format(SCRIPT_PATH),
            "pyconfig": "tests.configs.pyconf.beta"
        },

        "gamma": {
            "exec": "{}/gamma --logfile=gamma.log".format(SCRIPT_PATH),
            "pyconfig": "tests.configs.pyconf.gamma"
        }
    }

    class Environment(object):
        def __init__(self, cfg, name):
            self.name = name
            self.sp = ServiceProvider(init_srv=True, cfg=cfg)
            self.database = self.sp.get("database")
            self.action_journal = self.sp.get("action_journal")
            self.sa_dbpool = lib.database.DatabasePool(SyncApp.DATABASES,
                                                       self.database.dbenv(),
                                                       self.database.dbfile())

    def __init__(self, target_server, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **{})
        if target_server is None:
            servers = list(self.servers)
            if len(servers) == 0:
                raise TestError("No test servers defined!")

            self.target = servers[0]
        elif target_server in self.servers:
            self.target = target_server
        else:
            raise TestError("No such server defined '{0}'".format(target_server))

        self._run = kwargs.get("run", False)
        self._purge = kwargs.get("purge", False)
        self._debug = kwargs.get("debug", False)
        self.environments = {}
        random.seed()

        self._alphabet = []
        for i in range(65, 90) + range(97, 122):
            self._alphabet.append(chr(i))
        self._alphabet += ['_']

        self._record_map = {}
        self._record_map[AddRecord_A] = {
                "generate": self._generate_a_rec,
                "check_exists": self._check_a_rec_exists
        }
        self._record_map[DelRecord_A] = self._record_map[AddRecord_A]

        self._record_map[AddRecord_CNAME] = {
            "generate": self._generate_cname_rec,
            "check_exists": self._check_cname_rec_exists
        }
        self._record_map[DelRecord_CNAME] = self._record_map[AddRecord_CNAME]

        self._record_map[AddRecord_DNAME] = {
            "generate": self._generate_dname_rec,
            "check_exists": self._check_dname_rec_exists
        }
        self._record_map[DelRecord_DNAME] = self._record_map[AddRecord_DNAME]

        self._record_map[AddRecord_MX] = {
            "generate": self._generate_mx_rec,
            "check_exists": self._check_mx_rec_exists
        }
        self._record_map[DelRecord_MX] = self._record_map[AddRecord_MX]

        self._record_map[AddRecord_NS] = {
            "generate": self._generate_ns_rec,
            "check_exists": self._check_ns_rec_exists
        }
        self._record_map[DelRecord_NS] = self._record_map[AddRecord_NS]

        self._record_map[AddRecord_PTR] = {
            "generate": self._generate_ptr_rec,
            "check_exists": self._check_ptr_rec_exists
        }
        self._record_map[DelRecord_PTR] = self._record_map[AddRecord_PTR]

        self._record_map[AddRecord_SOA] = {
            "generate": self._generate_soa_rec,
            "check_exists": self._check_soa_rec_exists
        }
        self._record_map[DelRecord_SOA] = self._record_map[AddRecord_SOA]

        self._record_map[AddRecord_SRV] = {
            "generate": self._generate_srv_rec,
            "check_exists": self._check_srv_rec_exists
        }
        self._record_map[DelRecord_SRV] = self._record_map[AddRecord_SRV]

        self._record_map[AddRecord_TXT] = {
            "generate": self._generate_txt_rec,
            "check_exists": self._check_txt_rec_exists
        }
        self._record_map[DelRecord_TXT] = self._record_map[AddRecord_TXT]

        self._actions_map = {
            AddArena: DelArena,
            AddSegment: DelSegment,
            AddZone: DelZone,
            AddRecord_A: DelRecord_A,
            AddRecord_CNAME: DelRecord_CNAME,
            AddRecord_DNAME: DelRecord_DNAME,
            AddRecord_MX: DelRecord_MX,
            AddRecord_NS: DelRecord_NS,
            AddRecord_PTR: DelRecord_PTR,
            AddRecord_SOA: DelRecord_SOA,
            AddRecord_SRV: DelRecord_SRV,
            AddRecord_TXT: DelRecord_TXT
        }

    def log(self, *args):
        if self._debug:
            sys.stdout.write(args[0].format(*args[1:]) + '\n')

    def stdout(self, arg):
        if self._debug:
            sys.stdout.write(str(arg))
            sys.stdout.flush()

    def _get_pid(self, srv):
        path = "/run/" + srv + ".pid"
        if not os.path.exists(path):
            return None

        with open(path) as f:
            pid = f.read()
            return pid

    def _kill_server(self, srv):
        pid = self._get_pid(srv)
        if not pid is None:
            os.system("kill " + pid)

    def _database_updated(self, srv):
        pid = self._get_pid(srv)
        self.assertIsNot(pid, None)
        os.kill(int(pid), signal.SIGUSR2)

    def _purge_db(self, dbenv_homedir):
        shutil.rmtree(dbenv_homedir)
        os.mkdir(dbenv_homedir)
        open(dbenv_homedir + "/.holder", 'w')

    def _load_pyconfig(self, path):
        self.log("loading path '{0}'", path)
        cfg_mod = load_module(path)
        return cfg_mod.cfg

    def _wait(self, t):
        max_len = len(str(t))
        while t:
            self.stdout('\r' + ' ' * max_len + '\r')
            self.stdout(str(t))
            time.sleep(1)
            t -= 1
        self.stdout('\n')

    def setUp(self):
        for sname, srv in self.servers.iteritems():
            if self._run:
                self._kill_server(sname)

            cfg = self._load_pyconfig(srv["pyconfig"])
            if self._purge:
                self._purge_db(cfg["database"]["dbenv_homedir"])
            self.environments[sname] = self.Environment(cfg, sname)

            if self._run:
                os.system(srv["exec"])

    def tearDown(self):
        for srv in self.servers:
            self._kill_server(srv)


    def _invert_action(self, act):
        init = act.__dict__
        if init.has_key("dbstate"):
            del init["dbstate"]
        return self._actions_map[act.__class__](**act.__dict__)

    def _generate_str(self, min_len=1, max_len=50):
        res = ""
        len = random.randint(min_len, max_len)
        while len:
            res += random.choice(self._alphabet)
            len -= 1
        return res

    def _generate_domain(self, min_len=1, max_len=3):
        domain = self._generate_str()
        for i in range(0, random.randint(min_len - 1, max_len)):
            domain += '.' + self._generate_str()
        return domain

    def _generate_ip(self):
        ip = str(random.randint(0, 255))
        for i in range(3):
            ip += '.' + str(random.randint(0, 255))
        return ip

    def _generate_arena(self, env, txn):
        adb = env.database.dbpool().arena.dbhandle()
        name = ""
        while True:
            name = self._generate_str()
            if not adb.exists(name, txn):
                break
        return AddArena(arena=name)

    def _generate_segment(self, arena, env, txn):
        asdb = env.database.dbpool().arena_segment.dbhandle()
        name = ""
        while True:
            name = self._generate_str()
            if not name in bdb_helpers.get_all(asdb, arena, txn):
                break
        return AddSegment(arena=arena, segment=name)

    def _generate_zone(self, arena, segment, env, txn):
        zdb = env.database.dbpool().dns_zone.dbhandle()
        name = ""
        while True:
            name = self._generate_str()
            if not zdb.exists(reorder(name), txn):
                break
        return AddZone(arena=arena, segment=segment, zone=name)

    def _generate_ptr_zone(self, arena, segment, env, txn):
        zdb = env.database.dbpool().dns_zone.dbhandle()
        name = ""
        while True:
            res = ["in-addr.arpa"]
            for i in range(random.randint(1, 3)):
                res = [str(random.randint(0, 255))] + res
            name = '.'.join(res)

            if not zdb.exists(reorder(name), txn):
                break
        return AddZone(arena=arena, segment=segment, zone=name)

    def _generate_a_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        subdomain = ""
        while True:
            subdomain = self._generate_domain()
            if not ddb.exists(zone + ' ' + subdomain, txn):
                break

        ip = self._generate_ip()
        return AddRecord_A(zone=zone, host=subdomain, ip=ip)

    def _generate_cname_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        subdomain = ""
        while True:
            subdomain = self._generate_domain()
            if not ddb.exists(zone + ' ' + subdomain, txn):
                break

        target_domain = self._generate_domain()
        return AddRecord_CNAME(zone=zone, host=subdomain, domain=target_domain)

    def _generate_dname_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        if ddb.exists(zone + ' @', txn):
            return None

        target_zone = self._generate_domain()
        return AddRecord_DNAME(zone=zone, zone_dst=target_zone)

    def _generate_mx_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        target = self._generate_domain()
        return AddRecord_MX(zone=zone, domain=target)

    def _generate_ns_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        target = self._generate_domain()
        return AddRecord_NS(zone=zone, domain=target)

    def _generate_ptr_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        gen_len = 6 - len(zone.split('.'))
        host_res = ""
        while True:
            host = []
            for i in range(gen_len):
                host.append(str(random.randint(0, 255)))
            host_res = '.'.join(host)
            if not ddb.exists(zone + ' ' + host_res, txn):
                break
        domain = self._generate_domain()
        return AddRecord_PTR(zone=zone, host=host_res, domain=domain)

    def _generate_soa_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        for rec in bdb_helpers.get_all(ddb, zone + ' @', txn):
            data = split(rec)
            if data[3] == "SOA":
                return None

        primary_ns = self._generate_domain()
        resp_person = self._generate_str()
        serial = random.randint(10, 100)
        refresh = random.randint(10, 100)
        retry = random.randint(10, 100)
        expire = random.randint(10, 100)
        minimum = random.randint(10, 100)
        return AddRecord_SOA(zone=zone,
                             primary_ns=primary_ns,
                             resp_person=resp_person,
                             serial=serial,
                             refresh=refresh,
                             retry=retry,
                             expire=expire,
                             minimum=minimum)

    def _generate_srv_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        srv = ""
        while True:
            domain = self._generate_domain()
            srv = '.'.join(['_' + part for part in domain.split('.')])
            if not ddb.exists(zone + ' ' + srv, txn):
                break
        port = random.randint(1000, 65535)
        return AddRecord_SRV(zone=zone, service=srv, port=port, domain=domain)

    def _generate_txt_rec(self, zone, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        txt = ""
        for i in range(2, 50):
            txt += self._generate_str()
            txt += ' '
        return AddRecord_TXT(zone=zone, text=txt)

    def _generate_random_record(self, zone, env, txn):
        records = [AddRecord_A, AddRecord_CNAME, AddRecord_DNAME, AddRecord_MX,
                   AddRecord_NS, AddRecord_PTR, AddRecord_SOA, AddRecord_SRV,
                   AddRecord_TXT]
        generator = self._record_map[random.choice(records)]["generate"]
        return generator(zone, env, txn)


    def _check_arena_exists(self, act, env, txn):
        self.log("[{}] Check exists arena {}", env.name, act.desc())
        adb = env.database.dbpool().arena.dbhandle()
        return adb.exists(act.arena, txn)

    def _check_segment_exists(self, act, env, txn):
        self.log("[{}] Check exists segment {}", env.name, act.desc())
        asdb = env.database.dbpool().arena_segment.dbhandle()
        return act.segment in bdb_helpers.get_all(asdb, act.arena, txn)

    def _check_zone_exists(self, act, env, txn):
        self.log("[{}] Check exists zone {}", env.name, act.desc())
        zdb = env.database.dbpool().dns_zone.dbhandle()
        return zdb.exists(reorder(act.zone), txn)

    def _check_a_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' ' + act.host
        rec = ddb.get(key, None, txn)
        if not rec is None:
            data = split(rec)
            return data[3] == 'A' and data[4] == act.ip
        else:
            return False

    def _check_cname_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' ' + act.host
        rec = ddb.get(key, None, txn)
        if not rec is None:
            data = split(rec)
            return data[3] == 'CNAME' and data[4] == act.domain
        else:
            return False

    def _check_dname_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' @'
        for rec in bdb_helpers.get_all(ddb, key, txn):
            data = split(rec)
            if data[3] == 'DNAME' and data[4] == act.zone_dst:
                return True
        return False

    def _check_mx_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' @'
        for rec in bdb_helpers.get_all(ddb, key, txn):
            data = split(rec)
            if data[3] == 'MX' and data[5] == act.domain:
                return True
        return False

    def _check_ns_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' @'
        for rec in bdb_helpers.get_all(ddb, key, txn):
            data = split(rec)
            if data[3] == 'NS' and data[4] == act.domain:
                return True
        return False

    def _check_ptr_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' ' + act.host
        rec = ddb.get(key, None, txn)
        if not rec is None:
            data = split(rec)
            return data[3] == 'PTR' and data[4] == act.domain
        else:
            return False

    def _check_soa_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' @'
        for rec in bdb_helpers.get_all(ddb, key, txn):
            data = split(rec)
            if (data[3] == 'SOA' and
                data[4] == act.primary_ns and
                data[5] == act.resp_person and
                data[6] == str(act.serial) and
                data[7] == str(act.refresh) and
                data[8] == str(act.retry) and
                data[9] == str(act.expire) and
                data[10] == str(act.minimum)):
                return True
        return False

    def _check_srv_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' ' + act.service
        rec = ddb.get(key, None, txn)
        if not rec is None:
            data = split(rec)
            return (data[3] == 'SRV' and
                    data[6] == str(act.port) and
                    data[7] == act.domain)
        else:
            return False

    def _check_txt_rec_exists(self, act, env, txn):
        ddb = env.database.dbpool().dns_data.dbhandle()
        key = act.zone + ' @'
        for rec in bdb_helpers.get_all(ddb, key, txn):
            data = split(rec)
            if data[3] == 'TXT' and data[4] == '"' + act.text + '"':
                return True
        return False

    def _check_record_exists(self, act, env, txn):
        self.log("[{}] Check exists record {}", env.name, act.desc())
        checker = self._record_map[act.__class__]["check_exists"]
        return checker(act, env, txn)

    def _apply_action(self, act, env, txn):
        act.apply(env.database, txn)
        env.action_journal.record_action(act, txn)


    def _create_arenas(self, min, max, env, txn):
        arenas = []
        self.log("[{}] Generating arenas:", env.name)
        # generate arenas
        for i in range(random.randint(min, max)):
            a = self._generate_arena(env, txn)
            self.log("[{}] -- generated arena {}", env.name, a.desc())
            arenas.append(a)
            self._apply_action(a, env, txn)
        return arenas

    def _create_segments(self, arenas, min, max, env, txn):
        segments = []
        for arena in arenas:
            self.log("[{}] Generating segments for arena '{}':",
                     env.name, arena.arena)
            # generate segments
            for i in range(random.randint(min, max)):
                a = self._generate_segment(arena.arena, env, txn)
                self.log("[{}] -- generated segment {}", env.name, a.desc())
                segments.append(a)
                self._apply_action(a, env, txn)
        return segments

    def _create_zones(self, segments, min, max, ptr_min, ptr_max, env, txn):
        zones = []
        ptr_zones = []
        for segment in segments:
            self.log("[{}] Generating zones for segment {}", env.name, segment.desc())
            # generate usual zones
            for i in range(random.randint(min, max)):
                a = self._generate_zone(segment.arena, segment.segment, env, txn)
                self.log("[{}] -- generated zone {}", env.name, a.desc())
                zones.append(a)
                self._apply_action(a, env, txn)

            self.log("[{}] Generating ptr zones for segment {}",
                      env.name, segment.desc())
            # generate special ptr-zones
            for i in range(random.randint(ptr_min, ptr_max)):
                a = self._generate_ptr_zone(segment.arena, segment.segment,
                                            env, txn)
                self.log("[{}] -- generated ptr zone {}", env.name, a.desc())
                ptr_zones.append(a)
                self._apply_action(a, env, txn)
        return (zones, ptr_zones)

    def _create_records(self, zones, min, max, env, txn):
        records = []
        # generate records (this will not generate ptr records)
        for zone in zones:
            self.log("[{}] Generating records for zone {}:", env.name, zone.desc())
            for i in range(random.randint(min, max)):
                a = self._generate_random_record(zone.zone, env, txn)
                if not a is None:
                    self.log("[{}] -- generated record {}", env.name, a.desc())
                    records.append(a)
                    self._apply_action(a, env, txn)
        return records

    def _create_ptr_records(self, ptr_zones, min, max, env, txn):
        records = []
        # generate ptr records
        for pzone in ptr_zones:
            self.log("[{}] Generating ptr records for zone {}:",
                      env.name, pzone.desc())
            for i in range(random.randint(min, max)):
                a = self._generate_ptr_rec(pzone.zone, env, txn)
                self.log("[{}] -- generated ptr record {}", env.name, a.desc())
                records.append(a)
                self._apply_action(a, env, txn)
        return records

    def _check_data_exists(self, checker, arenas, segments, zones, records, env, txn):
        na = len(arenas + segments + zones + records)
        self.log("Checking environment '{}':", env.name)
        for arena in arenas:
            checker(self._check_arena_exists(arena, env, txn))

        for segment in segments:
            checker(self._check_segment_exists(segment, env, txn))

        for zone in zones:
            checker(self._check_zone_exists(zone, env, txn))

        for record in records:
            checker(self._check_record_exists(record, env, txn))

    def _check_position(self, actions_number, env, txn):
        pdb = env.sa_dbpool.peer.dbhandle()
        pos = pdb.get(self.target, None, txn)
        self.log("[{}] Checking position of '{}': {}", env.name, self.target, pos)
        self.assertTrue(pos == str(actions_number))


    def runTest(self):
        add_actions = []
        env = self.environments[self.target]
        # Create data set on target server
        with env.database.transaction() as txn:
            arenas = self._create_arenas(10, 20, env, txn)
            segments = self._create_segments(arenas, 1, 5, env, txn)
            zones, ptr_zones = self._create_zones(segments, 2, 5, 0, 2, env, txn)
            records = self._create_records(zones, 10, 20, env, txn)
            ptr_records = self._create_ptr_records(ptr_zones, 1, 10, env, txn)
            add_actions += (arenas + segments + zones + 
                            ptr_zones + records + ptr_records)

        # Check created data on target server
        with env.database.transaction() as txn:
            self._check_data_exists(self.assertTrue, arenas, segments,
                                    zones + ptr_zones, records + ptr_records,
                                    env, txn)

        actions_number = len(add_actions)

        self.log("Total actions: {}", actions_number)
        self.log("Sending database updated signal to target server '{}'", self.target)
        self._database_updated(self.target)

        self.log("Waiting before update on peers occurs")
        self._wait(160)

        # Check data added on remote servers
        for sname, env in self.environments.iteritems():
            if sname != self.target:
                with env.database.transaction() as txn:
                    self._check_data_exists(self.assertTrue, arenas,
                                            segments, zones + ptr_zones,
                                            records + ptr_records, env, txn)
                    self._check_position(actions_number, env, txn)

        del_actions = []
        del_arenas = []
        del_segments = []
        del_zones = []
        del_records = []
        for a in arenas:
            del_arenas.append(self._invert_action(a))
        for a in segments:
            del_segments.append(self._invert_action(a))
        for a in zones + ptr_zones:
            del_zones.append(self._invert_action(a))
        for a in records + ptr_records:
            del_records.append(self._invert_action(a))
        del_actions += del_arenas + del_segments + del_zones + del_records

        env = self.environments[self.target]
        with env.database.transaction() as txn:
            for a in reversed(del_actions):
                self.log("[{}] Applying delete action {}", env.name, a)
                self._apply_action(a, env, txn)

        # Check deleted data on target server
        with env.database.transaction() as txn:
            self._check_data_exists(self.assertFalse, del_arenas, del_segments,
                                    del_zones, del_records, env, txn)

        actions_number += len(del_actions)

        self.log("Total actions: {}", actions_number)
        self.log("Sending database updated signal to target server '{}'", self.target)
        self._database_updated(self.target)

        self.log("Waiting before update on peers occurs")
        self._wait(160)

        for sname, env in self.environments.iteritems():
            if sname != self.target:
                with env.database.transaction() as txn:
                    self._check_data_exists(self.assertFalse, del_arenas,
                                            del_segments, del_zones,
                                            del_records, env, txn)
                    self._check_position(actions_number, env, txn)


if __name__ == '__main__':
    args = sys.argv[1:]
    optlist, _ = getopt.getopt(args, "dprt:")
    options = dict(optlist)

    if options.has_key("-d"):
        debug = True
    else:
        debug = False

    if options.has_key("-p"):
        purge = True
    else:
        purge = False

    if options.has_key("-r"):
        run = True
    else:
        run = False

    target_server = options.get("-t", None)

    suite = unittest.TestSuite()
    suite.addTest(Test1(target_server, run=run, purge=purge, debug=debug))
    unittest.TextTestRunner(verbosity=2).run(suite)


# vim:sts=4:ts=4:sw=4:expandtab:
