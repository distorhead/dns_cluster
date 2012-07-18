# -*- coding: utf-8 -*-

from bsddb3 import db
from bson import BSON
from exceptions import NotImplementedError
from twisted.python import log

from lib import exception
from lib import database
from lib import bdb_helpers
from lib.common import reorder, singleton


class ActionError(Exception): pass


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
        action_data = BSON.decode(BSON(string))

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

    def apply(self, sessid, txn):
        if self.state == self.State.DO:
            self._apply_do(sessid, txn)
        elif self.state == self.State.UNDO:
            self._apply_undo(sessid, txn)
        else:
            assert 0, "Invalid action state"

    def _apply_do(self, sessid, txn):
        raise NotImplementedError("do action not implemented")

    def _apply_undo(self, sessid, txn):
        raise NotImplementedError("undo action not implemented")


@singleton
class journal(object):
    """
    Class used to manage journal records
    and control sessions.
    """

    JOURNAL_DATABASES = {
        "session": {
            "type": database.bdb.DB_HASH,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE,
            "autoincrement": 1
        },
        "action": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE,
            "autoincrement": 1
        },
        "session_action": {
            "type": database.bdb.DB_BTREE,
            "flags": database.bdb.DB_DUP | database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE,
            "autoincrement": 0
        }
    }

    def __init__(self, *args, **kwargs):
        self._dbpool = database.DatabasePool(self.JOURNAL_DATABASES,
                                             database.context().dbenv(),
                                             database.context().dbfile())

    def start_session(self, txn=None):
        """
        Get new session id.
        """

        try:
            db = self.dbpool().session.open()

            if txn is None:
                txn = database.context().dbenv().txn_begin()
                is_tmp_txn = True
            else:
                is_tmp_txn = False

            dbseq = database.Database.sequence(db, txn)
            id = str(dbseq.get(1, txn))
            db.put(id, '', txn)

            if is_tmp_txn:
                txn.commit()

            dbseq.close()
            db.close()

            return int(id)
        except database.bdb.DBError, e:
            log.err("Unable to create session")
            if not txn is None:
                txn.abort()
            raise

    def rollback_session(self, sessid, txn=None):
        """
        Undo changes made in session.
        """

        sessid = str(sessid)
        try:
            if txn is None:
                txn = database.context().dbenv().txn_begin()
                is_tmp_txn = True
            else:
                is_tmp_txn = False

            new_sessid = str(self.start_session(txn))

            adb = self.dbpool().action.open()
            sadb = self.dbpool().session_action.open()
            adbseq = database.Database.sequence(adb, txn)

            actions = [adb.get(act_id, txn) for act_id
                        in bdb_helpers.get_all(sadb, sessid, txn)]

            for action_dump in reversed(actions):
                action = Action.unserialize(action_dump)
                action.invert()
                self._apply_action(action, new_sessid, txn, adb, sadb, adbseq)

            if is_tmp_txn:
                txn.commit()

            adbseq.close()
            sadb.close()
            adb.close()

        except:
            log.err("Unable to rollback session '{0}'".format(sessid))
            if not txn is None:
                txn.abort()
                log.err("Transaction aborted")
            raise

    def apply_action(self, action, sessid=None):
        """
        Apply specified action in session (created automatically
            if omitted).
        """

        txn = None
        try:
            txn = database.context().dbenv().txn_begin()

            if sessid is None:
                sessid = str(self.start_session(txn))
            else:
                sessid = str(sessid)

            adb = self.dbpool().action.open()
            sadb = self.dbpool().session_action.open()
            adbseq = Database.sequence(adb, txn)
            self._apply_action(action, sessid, txn, adb, sadb, adbseq)

            txn.commit()

            adbseq.close()
            sadb.close()
            adb.close()
        except:
            log.err("Unable to apply action {0}".format(
                     action.__class__.__name__))
            if not txn is None:
                txn.abort()
                log.err("Transaction aborted")
            raise

    def dbpool(self):
        return self._dbpool

    def _apply_action(self, action, sessid, txn, adb, sadb, adbseq):
        action.apply(txn)

        action_dump = action.serialize()
        act_id = str(adbseq.get(1, txn))

        adb.put(act_id, action_dump, txn)
        sadb.put(sessid, act_id, txn)


# vim:sts=4:ts=4:sw=4:expandtab:
