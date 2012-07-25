# -*- coding: utf-8 -*-

from bson import BSON

from lib import database
from lib.common import required_key, required_type
from lib.service import ServiceProvider


class ActionError(Exception): pass


class Action(object):
    """
    Class represent single elementary action.
    Action may be in {DO|UNDO} state.
    Action may be serialized/unserialized.
    Serialized action keep its state and
      all data it needs to perform.
    """

    registered_actions = {}

    @classmethod
    def register_action(cls, act_cls):
        cls.registered_actions[act_cls.__name__] = act_cls
        return act_cls

    @classmethod
    def construction_failure(cls, msg):
        raise ActionError("unable to construct action: " + str(msg))

    @classmethod
    def required_data_by_key(cls, action_data, key, type):
        value = required_key(action_data, key, failure_func=cls.construction_failure,
                             failure_msg="wrong action data: {0} required".format(key))

        return required_type(value, type, failure_func=cls.construction_failure,
                             failure_msg="wrong action data: bad value "
                                         "'{0}'".format(value))

    @classmethod
    def optional_data_by_key(cls, action_data, key, type, default):
        value = required_key(action_data, key, default=default)
        return required_type(value, type, default=default)

    @classmethod
    def unserialize(cls, string):
        action_data = BSON.decode(BSON(string))

        name = cls.required_data_by_key(action_data, "name", str)
        data = cls.required_data_by_key(action_data, "data", dict)

        act_cls = required_key(cls.registered_actions, name,
                               failure_func=cls.construction_failure,
                               failure_msg="unknown action '{0}'".format(name))

        return act_cls(**data)

    def serialize(self):
        action_data = {
            "name": self.__class__.__name__,
            "data": self.__dict__
        }
        return BSON.encode(action_data)

    class State:
        DO = 1
        UNDO = 0

    def __init__(self, **kwargs):
        self.state = self.optional_data_by_key(kwargs, "state", int, self.State.DO)

    def invert(self):
        self.state ^= 1

    def apply(self, txn, database):
        if self.state == self.State.DO:
            self._apply_do(txn, database)
        elif self.state == self.State.UNDO:
            self._apply_undo(txn, database)
        else:
            assert 0, "Invalid action state"

    def _apply_do(self, txn, database):
        assert 0, "Action do part is not implemented"

    def _apply_undo(self, txn, database):
        assert 0, "Action undo part is not implemented"


@ServiceProvider.register("action_journal", deps=["database"])
class journal(object):
    """
    Class used to manage journal records.
    """

    DATABASES = {
        "action": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    def __init__(self, sp, *args, **kwargs):
        db = sp.get("database")
        self._dbpool = database.DatabasePool(self.DATABASES,
                                             db.dbenv(),
                                             db.dbfile())

    def dbpool(self):
        return self._dbpool

    def record_action(self, act, txn):
        adb = self.dbpool().action.open()

        seq = self.dbpool().action.sequence()
        newid = seq.get(1, txn)
        seq.close()

        dump = act.serialize()
        adb.put(str(newid), dump, txn)

        adb.close()


# vim:sts=4:ts=4:sw=4:expandtab:
