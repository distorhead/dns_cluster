# -*- coding: utf-8 -*-

import functools

from lib import database
from lib import bdb_helpers
from lib.service import ServiceProvider
from lib.action import Action

from twisted.internet import reactor
from twisted.python import log


class SessionError(Exception): pass


class Session(object):
    """
    Object-like interface to begin, commit and rollback session.
    """

    def __init__(self, session_manager):
        self._session_manager = session_manager
        self._used = False

    def _check_not_used(self):
        if self._used:
            raise SessionError("Session object must not be used repeatedly")

    def _check_began(self):
        if not hasattr(self, "sessid"):
            raise SessionError("Session is not began")

    def __enter__(self):
        return self.begin()

    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        else:
            log.err("Aborting session")
            self.rollback()

    def get(self):
        self._check_not_used()
        self._check_began()
        return self.sessid

    def begin(self):
        self._check_not_used()
        self.sessid = self._session_manager.begin_session()
        return self.sessid

    def commit(self):
        self._check_not_used()
        self._check_began()
        self._session_manager.commit_session(self.sessid)

    def rollback(self):
        self._check_not_used()
        self._check_began()
        self._session_manager.rollback_session(self.sessid)


@ServiceProvider.register("session", deps=["database", "action_journal"])
class manager(object):
    """
    Class used to control user api sessions.
    """

    SESSION_TIMEOUT_SECONDS = 10

    JOURNAL_DATABASES = {
        "session": {
            "type": database.bdb.DB_HASH,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        },

        # stores id => action_dump + undo_action_dump
        "action_journal": {
            "type": database.bdb.DB_BTREE,
            "flags": 0,
            "open_flags": database.bdb.DB_CREATE
        },

        "session_action": {
            "type": database.bdb.DB_BTREE,
            "flags": database.bdb.DB_DUP | database.bdb.DB_DUPSORT,
            "open_flags": database.bdb.DB_CREATE
        }
    }

    def transactional(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            txn = kwargs.get("txn", None)
            if txn is None:
                with self._database.transaction() as txn:
                    kwargs["txn"] = txn
                    return func(self, *args, **kwargs)
            else:
                return func(self, *args, **kwargs)

        return wrapper

    def __init__(self, sp, *args, **kwargs):
        self._database = sp.get("database")
        self._action_journal = sp.get("action_journal")
        self._dbpool = database.DatabasePool(self.JOURNAL_DATABASES,
                                             self._database.dbenv(),
                                             self._database.dbfile())
        self._watchdogs = {}

    def session(self):
        return Session(self)

    def dbpool(self):
        return self._dbpool

    @transactional
    def begin_session(self, **kwargs):
        """
        Get new session id.
        """

        txn = kwargs["txn"]

        sdb = self.dbpool().session.dbhandle()
        seq = self.dbpool().session.sequence(txn=txn)
        id = str(seq.get(1, txn))
        sdb.put(id, '', txn)
        seq.close()
        sessid = int(id)

        self._set_session_watchdog(sessid)
        return sessid

    @transactional
    def commit_session(self, sessid, **kwargs):
        txn = kwargs["txn"]

        if self._is_session_exists(sessid, txn):
            self._delete_session(sessid, txn)
            actions = self._retrieve_session_actions(sessid, txn)
            actions.sort()
            for actid in actions:
                data = self._retrieve_action_record(actid, txn)
                if data:
                    act_dump = data.split(' ')[0]
                    self._action_journal.record_action_dump(act_dump, txn)
        else:
            raise SessionError("Session '{}' doesn't exist".format(sessid))

        self._unset_session_watchdog(sessid)

    @transactional
    def rollback_session(self, sessid, **kwargs):
        txn = kwargs["txn"]

        if self._is_session_exists(sessid, txn):
            self._delete_session(sessid, txn)
            actions = self._retrieve_session_actions(sessid, txn)
            actions.sort()
            actions.reverse()
            for actid in actions:
                data = self._retrieve_action_record(actid, txn)
                if data:
                    undo_act_dump = data.split(' ')[1]
                    act = Action.unserialize(undo_act_dump)
                    act.apply(self._database, txn)
        else:
            raise SessionError("Session '{}' doesn't exist".format(sessid))

        self._unset_session_watchdog(sessid)

    def keepalive_session(self, sessid):
        if self._watchdogs.has_key(sessid):
            self._unset_session_watchdog(sessid)
            self._set_session_watchdog(sessid)

    @transactional
    def apply_action(self, sessid, action, undo_action, **kwargs):
        txn = kwargs["txn"]

        if self._is_session_exists(sessid, txn):
            action.apply(self._database, txn)
            actid = self._record_action(action, undo_action, txn)
            self._link_session_action(sessid, actid, txn)
        else:
            raise SessionError("Session '{}' doesn't exist".format(sessid))


    def _is_session_exists(self, sessid, txn):
        sdb = self.dbpool().session.dbhandle()
        return sdb.exists(str(sessid), txn)

    def _delete_session(self, sessid, txn):
        sdb = self.dbpool().session.dbhandle()
        sdb.delete(str(sessid), txn)

    def _set_session_watchdog(self, sessid):
        callid = reactor.callLater(self.SESSION_TIMEOUT_SECONDS,
                    lambda: self.rollback_session(sessid))
        self._watchdogs[sessid] = callid

    def _unset_session_watchdog(self, sessid):
        if self._watchdogs.has_key(sessid):
            if self._watchdogs[sessid].active():
                self._watchdogs[sessid].cancel()
            del self._watchdogs[sessid]

    def _record_action(self, action, undo_action, txn):
        act_dump = action.serialize()
        undo_act_dump = undo_action.serialize()
        data = act_dump + ' ' + undo_act_dump

        seq = self.dbpool().action_journal.sequence(txn=txn)
        newid = seq.get(1, txn)
        seq.close()

        ajdb = self.dbpool().action_journal.dbhandle()
        ajdb.put(str(newid), data, txn)

        return newid

    def _retrieve_action_record(self, actid, txn):
        ajdb = self.dbpool().action_journal.dbhandle()
        data = ajdb.get(str(actid), None, txn)
        if not data is None:
            ajdb.delete(str(actid), txn)
        return data

    def _link_session_action(self, sessid, actid, txn):
        sadb = self.dbpool().session_action.dbhandle()
        sadb.put(str(sessid), str(actid), txn)

    def _retrieve_session_actions(self, sessid, txn):
        sadb = self.dbpool().session_action.dbhandle()
        actions = [int(actid) for actid in
                   bdb_helpers.get_all(sadb, str(sessid), txn, delete=True)]
        return actions


# vim:sts=4:ts=4:sw=4:expandtab:
