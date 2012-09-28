# -*- coding: utf-8 -*-

from zope.interface import Interface


"""
This module defines following structure of network application:

      [ IService ]
           /\
           ||
           \/
      [ IProtocol ]
           ||
[ network transport layer ]

IService defines interface for IProtocol.
IProtocol defines interface for IService.
"""


class IProtocol(Interface):
    def send_message(self, msg):
        """
        Send dict-like message.
        """

    def hangup(self):
        """
        Hangup connection.
        """


class IService(Interface):
    """
    Class responsible for message protocol, message handling, logic state tracking.
    """

    def handle_message(self, msg):
        """
        Handle incoming dict-like message.
        """


# vim:sts=4:ts=4:sw=4:expandtab:
