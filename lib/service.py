# -*- coding: utf-8 -*-


class ServiceError(Exception): pass


class ServiceProvider(object):
    _registered_services = {}

    @classmethod
    def register(cls, srv_name, **kwargs):
        deps = kwargs.get("deps", [])
        force_replace = kwargs.get("force_replace", False)

        if cls._registered_services.has_key(srv_name) and not force_replace:
            raise ServiceError("Service '{0}' already registered".format(srv_name))

        deps = [dep for dep in deps if not dep == srv_name]

        def do_register(srv_cls):
            class ServiceHolder: pass

            srv = ServiceHolder()
            srv.name = srv_name
            srv.cls = srv_cls
            srv.deps = deps

            cls._registered_services[srv.name] = srv
            return srv_cls

        return do_register


    def __init__(self, **kwargs):
        self._instances = {}
        init_srv = kwargs.get("init_srv", False)
        cfg = kwargs.get("cfg", {})
        if init_srv:
            self.initialize(cfg)

    def _lookup_srv(self, srv_name):
        if not self._registered_services.has_key(srv_name):
            raise ServiceError("No such service registered: '{0}'".format(
                               srv_name))
        return self._registered_services[srv_name]

    def _lookup_instance(self, srv_name):
        srv = self._lookup_srv(srv_name)
        if not self._instances.has_key(srv):
            raise ServiceError("Service '{0}' doesn't initialized".format(srv_name))
        return self._instances[srv]

    def _get_srv_conf(self, srv_name, cfg):
        return cfg.get(srv_name, {})

    def _initialize_srv(self, srv, srv_cfg, cfg, count=1):
        if count > 999:
            raise ServiceError("There is a circular dependency between services")

        for dep in srv.deps:
            dsrv = self._lookup_srv(dep)
            dsrv_cfg = self._get_srv_conf(dep, cfg)
            self._initialize_srv(dsrv, dsrv_cfg, cfg, count+1)

        if not self._instances.has_key(srv):
            self._instances[srv] = srv.cls(self, **srv_cfg)

    def initialize(self, cfg={}):
        for (_, srv) in self._registered_services.items():
            srv_cfg = self._get_srv_conf(srv.name, cfg)
            self._initialize_srv(srv, srv_cfg, cfg)

    def get(self, srv_name):
        return self._lookup_instance(srv_name)


# vim:sts=4:ts=4:sw=4:expandtab:
