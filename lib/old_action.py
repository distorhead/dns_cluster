# -*- coding: utf-8 -*-

from bsddb3 import db
from bson import BSON
from twisted.python import log
from twisted.internet import defer

from lib import exception
from lib import database
from lib import bdb_helpers
from lib import lock
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
            raise ActionError("unable to construct action: "
                              "action name required")

        if not action_data.has_key("data"):
            raise ActionError("unable to construct action: "
                              "action data required")

        if not cls.registered_actions.has_key(action_data["name"]):
            raise ActionError("unable to construct action '{0}': "
                              "no such action existed".format(action_data["name"]));

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

    def apply(self, sessid):
        print 'apply'
        act_defer = defer.Deferred()

        lock_defer, lock_objs = self._acquire_locks(sessid)
        lock_defer.addErrback(self._acquire_locks_eb, lock_objs, act_defer)
        lock_defer.addCallback(self._on_locks_acquired, sessid, act_defer)
        print 'after adding callbacks to lock_defer'

        print 'adding act_defer callback'
        act_defer.addCallback(self._release_locks_cb, lock_objs)
        print 'adding act_defer errback'
        act_defer.addErrback(self._release_locks_eb, lock_objs)

        return act_defer

    def _on_locks_acquired(self, _, sessid, act_defer):
        print 'on locks acquired'
        try:
            with database.context().transaction() as txn:
                if self.state == self.State.DO:
                    self._apply_do(sessid, txn)
                elif self.state == self.State.UNDO:
                    self._apply_undo(sessid, txn)
                else:
                    assert 0, "Invalid action state"

                # FIXME: turn on line
                #journal().apply_action(self, sessid, txn)

            print 'calling callback'
            act_defer.callback((self, sessid))

        except (AssertionError, ActionError) as e:
            print 'calling errback'
            act_defer.errback(e)

    def _apply_do(self, sessid, txn):
        print 'in apply do'
        assert 0, "Do action not implemented"

    def _apply_undo(self, sessid, txn):
        print 'in apply undo'
        assert 0, "Undo action not implemented"

    def _get_lock_resources(self):
        """
        Method should return list of lock strings to acquire
        in given order before applying action.
        Action class may override this method to use
        own list.
        """
        return []

    def _acquire_locks(self, sessid):
        print 'acquire locks'
        defers = []
        lock_objs = []
        resources = self._get_lock_resources()
        for resource in resources:
            lck = lock.manager().lock(resource, sessid)
            lock_objs.append(lck)

            deferred = lck.acquire()
            defers.append(deferred)

        dl = defer.DeferredList(defers, consumeErrors=True,
                                fireOnOneErrback=True)
        return (dl, lock_objs)

    def _acquire_locks_eb(self, failure, lock_objs, act_defer):
        print 'acquire locks errback'
        lock_names = ", ".join([lck.resource() for lck in lock_objs])
        msg = "Unable to acquire locks '{0}' for action '{1}'".format(
                                   lock_names, self.__class__.__name__)
        log.err(msg)

        self._release_locks(lock_objs)
        act_defer.errback(ActionError(msg))

    def _release_locks_cb(self, res, lock_objs):
        print 'release locks callback'
        self._release_locks(lock_objs)
        return res

    def _release_locks_eb(self, failure, lock_objs):
        print 'release locks errback'
        self._release_locks(lock_objs)
        # forward failure to further errbacks
        return failure

    def _release_locks(self, lock_objs):
        for lck in lock_objs:
            if lck.is_locked():
                lck.release()


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
            "hasseq": 1
        },
        "action": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE,
            "hasseq": 1
        },
        "session_action": {
            "type": database.bdb.DB_BTREE,
            "flags": database.bdb.DB_DUP | database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE,
            "hasseq": 0
        }
    }

    def __init__(self, *args, **kwargs):
        self._dbpool = database.DatabasePool(self.JOURNAL_DATABASES,
                                             database.context().dbenv(),
                                             database.context().dbfile())

    def start_session(self):
        """
        Get new session id.
        """
        sdb = self.dbpool().session.open()

        with database.context().transaction() as txn:
            dbseq = database.Database.sequence(sdb, txn)
            id = str(dbseq.get(1, txn))
            sdb.put(id, '', txn)
            dbseq.close()

        sdb.close()
        return int(id)

    def rollback_session(self, sessid):
        """
        Undo changes made in session.
        """

        sessid = str(sessid)
        new_sessid = str(self.start_session())

        adb = self.dbpool().action.open()
        sadb = self.dbpool().session_action.open()

        with database.context().transaction() as txn:
            actions = [adb.get(act_id, txn) for act_id
                        in bdb_helpers.get_all(sadb, sessid, txn)]

        adb.close()
        sadb.close()

        for action_dump in reversed(actions):
            action = Action.unserialize(action_dump)
            action.invert()
            action.apply(new_sessid)

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
