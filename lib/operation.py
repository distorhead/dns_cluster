# -*- coding: utf-8 -*-

from lib.common import required_key, required_type


class OperationError(Exception): pass


class Operation(object):
    @classmethod
    def construction_failure(cls, msg):
        raise OperationError("Unable to construct operation: " + str(msg))

    @classmethod
    def required_data_by_key(cls, operation_data, key, type=None):
        value = required_key(operation_data, key,
                             failure_func=cls.construction_failure,
                             failure_msg="wrong operation data: {} required".format(
                                          key))

        if not type is None:
            value = required_type(value, type,
                                  failure_func=cls.construction_failure,
                                  failure_msg="wrong operation data: bad value "
                                              "'{}'".format(value))

        return value

    @classmethod
    def optional_data_by_key(cls, operation_data, key, type, default):
        value = required_key(operation_data, key, default=default)
        return required_type(value, type, default=default)


    def __init__(self, **kwargs):
        self._used = False

    def run(self):
        if self._used:
            raise OperationError("Operation object must not be used repeatedly")
        else:
            self._used = True
            return self._do_run()

    def _do_run(self):
        assert 0, "Operation _do_run method is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
