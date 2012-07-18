from context import *
from lib.action import *
from lib.action import context as act_context


j_sdb = act_context().dbpool().session.open()
j_adb = act_context().dbpool().action.open()
j_sadb = act_context().dbpool().session_action.open()

adb.put('myarena', '')

asdb.put('myarena', 'mysegm1')
asdb.put('myarena', 'mysegm2')

szdb.put('myarena mysegm1', 'enozym')
szdb.put('myarena mysegm2', 'uuuuufff')

zdb.put('enozym', 'myarena mysegm1')
zdb.put('uuuuufff', 'myarena mysegm2')

xdb.put('myzone', 'backup')
xdb.put('myzone', 'bar')
xdb.put('myzone', 'foo')
xdb.put('myzone', 'mail')
xdb.put('myzone', 'ns')
xdb.put('myzone', 'ns2')
xdb.put('myzone', 'web')

ddb.put('myzone @', '12 @ 10 NS ns2.myzone.')
ddb.put('myzone @', '15 @ 10 TXT "Hello"')
ddb.put('myzone @', '2 @ 10 SOA ns2.myzone. root.myzone. 2 2800 7200 604800 86400')
ddb.put('myzone @', '3 @ 10 A 127.0.0.1')
ddb.put('myzone @', '4 @ 10 NS ns.myzone.')
ddb.put('myzone @', '8 @ 10 MX 40 mail.myzone.')
ddb.put('myzone @', '9 @ 10 MX 20 backup')
ddb.put('myzone backup', '11 @ 10 A 10.10.10.10')
ddb.put('myzone foo', '1 @ 10 A 172.27.33.126')
ddb.put('myzone web', '14 @ 10 A 127.0.0.1')
ddb.put('fffuuuuu @', '17 @ 10 DNAME myzone.')
ddb.put('fffuuuuu @', '18 @ 10 SOA ns2.fffuuuuu. root.fffuuuuu. 2 2800 7200 604800 86400')
ddb.put('myzone _httpd._tcp', '13 @ 10 SRV 20 0 8080 web.myzone.')
ddb.put('myzone bar', '6 @ 10 CNAME foo.myzone.')
ddb.put('myzone mail', '10 @ 10 A 9.9.9.9')
ddb.put('myzone ns2', '5 @ 10 A 8.8.8.8')
ddb.put('myzone ns', '7 @ 10 A 127.0.0.1')

zddb.put('myzone', 'myzone @')
zddb.put('myzone', 'myzone backup')
zddb.put('myzone', 'myzone foo')
zddb.put('myzone', 'myzone web')
zddb.put('myzone', 'myzone _httpd._tcp')
zddb.put('myzone', 'myzone bar')
zddb.put('myzone', 'myzone mail')
zddb.put('myzone', 'myzone ns2')
zddb.put('myzone', 'myzone ns')
zddb.put('fffuuuuu', 'fffuuuuu @')


#act_arena = AddArena('myarena')
#act_segm = AddSegment('myarena', 'ololosegm')
#act_zone = AddZone('myarena', 'mysegm1', 'ololozone')
