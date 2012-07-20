# -*- coding: utf-8 -*-

from bson import BSON


class ActionError(Exception): pass


class Action(object):
    """
    Class represent single elementary journal action.
    Action may be in {DO|UNDO} state.
    Action may be serialized/unserialized.
    Serialized action keep its state and
      all data it needs to perform.
    """

    class State:
        DO = 1
        UNDO = 0

    registered_actions = {}

    @classmethod
    def register_action(cls, act_cls):
        cls.registered_actions[act_cls.__name__] = act_cls
        return act_cls

    @classmethod
    def unserialize(cls, string):
        action_data = BSON.decode(BSON(string))

        if not action_data.has_key("name"):
            raise ActionError("unable to construct action: "
                              "action name required")

        if not action_data.has_key("data"):
            raise ActionError("unable to construct action: "
                              "action data required")

        if not cls.registered_actions.has_key(action_data["name"]):
            raise ActionError("unable to construct action '{0}': "
                              "no such action existed".format(action_data["name"]))

        act_cls = cls.registered_actions[action_data["name"]]
        return act_cls.from_data(action_data["data"])

    def serialize(self):
        action_data = {
            "name": self.__class__.__name__,
            "data": self.__dict__
        }
        return BSON.encode(action_data)


    def __init__(self, state):
        try:
            self.state = int(state)
        except:
            self.state = self.State.DO

    def invert(self):
        self.state ^= 1

    def apply(self, txn):
        if self.state == self.State.DO:
            self._apply_do(txn)
        elif self.state == self.State.UNDO:
            self._apply_undo(txn)
        else:
            assert 0, "Invalid action state"

    def _apply_do(self, txn):
        assert 0, "Action do part is not implemented"

    def _apply_undo(self, txn):
        assert 0, "Action undo part is not implemented"


# vim:sts=4:ts=4:sw=4:expandtab:
