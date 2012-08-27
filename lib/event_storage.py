# -*- coding: utf-8 -*-

from twisted.internet.defer import Deferred


class EventError(Exception): pass

class EventStorage(object):
    """
    Class used to manage predefined string-based single-use events.
    """

    def __init__(self, *allowed_events):
        self._allowed_events = set(allowed_events)
        self._registered_events = {}

    def _check_allowed_event(self, event):
        if not event in self._allowed_events:
            raise EventError("Event '{0}' is not defined".format(event))

    def register_event(self, event):
        """
        Register event named by string.
        Method returns deferred fired up when event occurs.
        """

        self._check_allowed_event(event)
        if self._registered_events.has_key(event):
            return self._registered_events[event]
        else:
            d = Deferred()
            self._registered_events[event] = d
            return d

    def retrieve_event(self, event):
        """
        Get deferred by event name. Event becomes unregistered.
        """

        self._check_allowed_event(event)
        return self._registered_events.pop(event, None)


# vim:sts=4:ts=4:sw=4:expandtab:
