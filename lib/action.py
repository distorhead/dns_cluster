# -*- coding: utf-8 -*-

from bson import BSON

from lib import database
from lib.common import required_key, required_type
from lib.service import ServiceProvider
from lib import bdb_helpers


class ActionError(Exception): pass


class Action(object):
    """
    Class represent single elementary action.
    Action may be serialized/unserialized.
    Serialized action keep all data it needs to perform
      and database target dbstate, to which this action
      is applyable.
    Subclasses should implement following methods:
      _do_apply - for performing actual action
      (optional) _current_dbstate - for retrieving current database state.
        Default method allows applying action on any state.
    """

    registered_actions = {}

    @classmethod
    def register_action(cls, act_cls):
        cls.registered_actions[act_cls.__name__] = act_cls
        return act_cls

    @classmethod
    def construction_failure(cls, msg):
        raise ActionError("Unable to construct action: " + str(msg))

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
        name = self.name()
        if not self.registered_actions.has_key(name):
            raise ActionError("Unable to serialize action: action '{0}' "
                              "is not registered".format(name))

        action_data = {
            "name": name,
            "data": self.__dict__
        }
        return BSON.encode(action_data)

    def __init__(self, **kwargs):
        self.dbstate = kwargs.get("dbstate", None)

    def name(self):
        return self.__class__.__name__

    def apply(self, database, txn):
        cur_dbstate = self._current_dbstate(database, txn)

        if (self.dbstate is None) or (self.dbstate == cur_dbstate):
            self._do_apply(database, txn)
        else:
            raise ActionError("Unable to apply action '{0}': dbstates mismatch: "
                              "action target dbstate {1}, current dbstate {2}".format(
                              self.name(), repr(self.dbstate), repr(cur_dbstate)))

        if self.dbstate is None:
            self.dbstate = cur_dbstate

    def _do_apply(self, database, txn):
        assert 0, "Action do method is not implemented"

    def _current_dbstate(self, database, txn):
        return self.dbstate


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

    def record_action(self, act, txn=None, pos=None):
        adb = self.dbpool().action.dbhandle()

        if not pos is None:
            # reset sequence generator if position given
            bdb_helpers.delete(adb, database.Database.SEQUENCE_KEY, txn)

        # initial value will be ignored if sequence already exists
        seq = self.dbpool().action.sequence(initial=pos, txn=txn)
        newid = seq.get(1, txn)
        seq.close()

        dump = act.serialize()
        adb.put(str(newid), dump, txn)

    def get_position(self, txn=None):
        seq = self.dbpool().action.sequence()
        pos = seq.stat().get("last_value", None)
        seq.close()
        return pos

    def get_by_position(self, pos, txn=None):
        adb = self.dbpool().action.dbhandle()
        return adb.get(str(pos), None, txn)

    def get_since_position(self, pos, number=None, txn=None):
        res = []
        adb = self.dbpool().action.dbhandle()
        cur_pos = self.get_position(txn)

        if number is None:
            last_pos = cur_pos
        else:
            last_pos = min(cur_pos, pos + number)

        for key in range(pos + 1, last_pos + 1):
            act = adb.get(str(key), None, txn)
            if not act is None:
                res.append({"action": act, "position": key})

        return res

    def position_exists(self, pos, txn=None):
        if pos == -1:
            # such position means "any position" from begin
            return True
        else:
            adb = self.dbpool().action.dbhandle()
            return adb.exists(str(pos), txn)


# vim:sts=4:ts=4:sw=4:expandtab:
