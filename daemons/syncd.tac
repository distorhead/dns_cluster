# Required steps:
# 1. Create twisted.application.service.Application object.
# 2. Assign this object to application top-level variable.
# 3. Create target service from factory and endpoint string spec.
# 4. Assign this object as parent to the target service

from twisted.application import service, strports
from lib.network import sync

application = service.Application("Dns cluster sync daemon")
service = strports.service("tcp:1234", sync.SyncServerFactory())
service.setServiceParent(application)
