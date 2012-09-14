# -*- coding: utf-8 -*-

import functools 
import yaml
import urllib

from twisted.web import server, resource
from twisted.internet import threads
from twisted.python import log

from lib.operations import *
from lib.operation import OperationError
from lib.action import ActionError
from lib.session import SessionError
from lib.lock import LockError


class RequestError(Exception): pass


class OperationResource(resource.Resource):
    """
    Base resource for interacting with operation API.
    """

    @classmethod
    def required_field(cls, d, field):
        if not d.has_key(field):
            raise RequestError("'{}' field required".format(field))
        return d[field]

    def __init__(self, sp):
        self._sp = sp

    def run_operation(self, operation, request):
        log.msg("Deferring operation run:", operation)
        d = threads.deferToThread(operation.run, self._sp)
        return d

    def operation_failure(self, failure, request):
        failure.trap(OperationError, ActionError, SessionError, LockError)
        log.msg("Operation failure:", failure)
        self.response(request, 400, {'error': failure.getErrorMessage()})

    def unknown_failure(self, failure, request):
        log.err("An error occured:")
        log.err(failure)
        self.response(request, 500)

    def response(self, request, code, answer=None):
        request.setResponseCode(code)
        request.setHeader('Content-Type', 'application/x-yaml')

        if not answer is None:
            resp = yaml.dump(answer)
            request.write(resp)

        request.finish()
        return server.NOT_DONE_YET

    def parse_content(self, content):
        return yaml.load(content)


def request_handler(func):
    @functools.wraps(func)
    def wrapper(self, request):
        try:
            log.msg("Request:", request.args)
            return func(self, request)

        except (RequestError, OperationError, ActionError, SessionError) as e:
            return self.response(request, 400, {"error": str(e)})

        except Exception, e:
            log.err("An error occured:")
            log.err(e)
            return self.response(request, 500)

    return wrapper


# vim:sts=4:ts=4:sw=4:expandtab:
