# -*- coding: utf-8 -*-

from bsddb3 import db
from bson import BSON

from lib import exception
from lib import database
from lib import bdb_helpers
from lib.common import reorder


class Action(object):
    """
    Class represent single journal action.
    Action may be in {DO|UNDO} state.
    """

    class State:
        UNDO = 0
        DO = 1

    registered_actions = {}

    @classmethod
    def register_action(cls, act_cls):
        cls.registered_actions[act_cls.__name__] = act_cls
        return act_cls

    @classmethod
    def unserialize(cls, string):
        action_data = BSON.decode(string)

        if not action_data.has_key("name"):
            raise exception.ActionError("unable to construct action: "
                              "action name required")

        if not action_data.has_key("data"):
            raise exception.ActionError("unable to construct action: "
                              "action data required")

        act_cls = cls.registered_actions[action_data["name"]]
        return act_cls.from_data(action_data["data"])

    def serialize(self):
        action_data = {
            "name": self.__class__.__name__,
            "data": self.__dict__
        }
        return BSON.encode(action_data)


    def __init__(self, state):
        if state is None:
            state = self.State.DO
        self.state = int(state)

    def invert(self):
        self.state ^= 1

    def apply(self, txn=None):
        if self.state == self.State.DO:
            self._apply_do(txn)
        elif self.state == self.State.UNDO:
            self._apply_undo(txn)
        else:
            assert 0

    def _apply_do(self, txn):
        raise exception.NotImplementedError("do action not implemented")

    def _apply_undo(self, txn):
        raise exception.NotImplementedError("undo action not implemented")


@Action.register_action
class AddArena(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]), data["state"])

    def __init__(self, arena_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name

    def _apply_do(self, txn):
        adb = database.context().arena.open()
        if adb.get(self.arena_name, txn) is None:
            adb.put(self.arena_name, '', txn)
        adb.close()

    def _apply_undo(self, txn):
        adb = database.context().arena.open()
        asdb = database.context().arena_segment.open()
        szdb = database.context().segment_zone.open()
        zddb = database.context().zone_dns_data.open()
        zdb = database.context().dns_zone.open()
        ddb = database.context().dns_data.open()
        xdb = database.context().dns_xfr.open()
        cdb = database.context().dns_client.open()

        # Selection and deletion steps separated
        # because selecting data from database
        # after removing data from this database
        # isn't allowed in the same transaction.

        # select all needed data first
        data = {}
        segments = bdb_helpers.get_all(asdb, self.arena_name, txn)
        for segm in segments:
            sz_key = self.arena_name + ' ' + segm
            zones = bdb_helpers.get_all(szdb, sz_key, txn)
            data[segm] = {}
            for rzone in zones:
                zone = reorder(rzone)
                data_keys = bdb_helpers.get_all(zddb, zone, txn)
                data[segm][rzone] = (zone, data_keys)

        # delete all selected data
        for segm in data:
            for rzone in data[segm]:
                for data_key in data[segm][rzone][1]:
                    bdb_helpers.delete(ddb, data_key, txn)

                bdb_helpers.delete(zddb, data[segm][rzone][0], txn)
                bdb_helpers.delete(xdb, data[segm][rzone][0], txn)
                bdb_helpers.delete(cdb, data[segm][rzone][0], txn)
                bdb_helpers.delete(zdb, rzone, txn)

            bdb_helpers.delete(szdb, rzone, txn)

        bdb_helpers.delete(asdb, self.arena_name, txn)
        bdb_helpers.delete(adb, self.arena_name, txn)

        adb.close()
        asdb.close()
        szdb.close()
        zddb.close()
        zdb.close()
        ddb.close()
        xdb.close()
        cdb.close()


@Action.register_action
class AddSegment(Action):
    def __init__(self, data, state=None):
        super(self.__class__, self).__init__(state)

        if not data.has_key("arena_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")
        self._arena_name = data["arena_name"]

        if not data.has_key("segment_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: segment_name required")
        self._segment_name = data["segment_name"]

    def _apply_do(self, txn):
        adb = database.context().arena.open()
        asdb = database.context().arena_segment.open()

        if adb.get(self._arena_name, txn) is None:
            raise exception.ActionError("arena '{0}' doesn't exist")

        if not self._segment_name in bdb_helpers.get_all(asdb, self._arena_name, txn):
            asdb.put(self._arena_name, self._segment_name, txn)

        asdb.close()
        adb.close()

    def _apply_undo(self, txn):
        adb = database.context().arena.open()
        asdb = database.context().arena_segment.open()


        asdb.close()
        adb.close()

class AddZone(Action): pass
class AddRecord_A(Action): pass
class AddRecord_PTR(Action): pass
class AddRecord_CNAME(Action): pass
class AddRecord_DNAME(Action): pass
class AddRecord_SOA(Action): pass
class AddRecord_NS(Action): pass
class AddRecord_MX(Action): pass
class AddRecord_SRV(Action): pass
class AddRecord_TXT(Action): pass



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
