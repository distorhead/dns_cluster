# -*- coding: utf-8 -*-

import functools

from lib.common import retrieve_key, cast_type


class OperationError(Exception): pass


class Operation(object):
    """
    Class represent a single user API operation.
    Subclasses must implement _do_run method, that performs actual operation.
    """

    GLOBAL_RESOURCE = '_global'

    @classmethod
    def construction_failure(cls, msg):
        raise OperationError("Unable to construct operation: " + str(msg))

    @classmethod
    def required_data_by_key(cls, operation_data, key, type=None):
        value = retrieve_key(operation_data, key,
                             failure_func=cls.construction_failure,
                             failure_msg="wrong operation data: {} required".format(
                                          key))

        if not type is None:
            value = cast_type(value, type,
                                  failure_func=cls.construction_failure,
                                  failure_msg="wrong operation data: bad value "
                                              "'{}'".format(value))

        return value

    @classmethod
    def optional_data_by_key(cls, operation_data, key, type, default):
        do_typecast = [True]
        def not_found(_):
            do_typecast[0] = False

        value = retrieve_key(operation_data, key,
                             failure_func=not_found,
                             default=default)

        if do_typecast[0]:
            value = cast_type(value, type, default=default)
        return value


    def __init__(self, **kwargs):
        self._used = False

    def run(self, service_provider, **kwargs):
        if self._used:
            raise OperationError("Operation object must not be used repeatedly")
        else:
            self._used = True
            return self._do_run(service_provider, **kwargs)

    def _do_run(self, service_provider, **kwargs):
        """
        Should return:
            - result of operation
            - deferred fired up with result when operation is done
                NOTE: deferred should be fired up in the reactor thread!
        """

        assert 0, "{}._do_run method is not implemented".format(
                      self.__class__.__name__)


# vim:sts=4:ts=4:sw=4:expandtab:
