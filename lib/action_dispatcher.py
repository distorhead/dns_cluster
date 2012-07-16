# -*- coding: utf-8 -*-

import database
import bdb_helpers

from lib import exception
from lib.action import Action

from twisted.python import log


class ActionDispatcher(object):
    """
    Frontend interface to manage journal records
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
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._dbfile is None

        for dbname in self.JOURNAL_DATABASES:
            dbdesc = database.Database(database.context().dbenv(), self._dbfile, dbname,
                          self.JOURNAL_DATABASES[dbname]["type"],
                          self.JOURNAL_DATABASES[dbname]["flags"],
                          self.JOURNAL_DATABASES[dbname]["open_flags"])
            setattr(self, dbname, dbdesc)

            if self.JOURNAL_DATABASES[dbname]["autoincrement"]:
                db = dbdesc.open()
                database.Database.sequence(db, None, 0)
                db.close()

    def start_session(self, txn=None):
        """
        Get new session id.
        """

        try:
            db = self.session.open()

            if txn is None:
                txn = database.context().dbenv().txn_begin()
                is_tmp_txn = True
            else:
                is_tmp_txn = False

            dbseq = Database.sequence(db, txn)
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
                txn = databse.context().dbenv().txn_begin()
                is_tmp_txn = True
            else:
                is_tmp_txn = False

            new_sessid = str(self.start_session(txn))

            adb = self.action.open()
            sadb = self.session_action.open()
            adbseq = Database.sequence(adb, txn)

            actions = [adb.get(act_id, txn) for act_id in get_all(sadb, sessid, txn)]

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

            adb = self.action.open()
            sadb = self.session_action.open()
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

    def _apply_action(self, action, sessid, txn, adb, sadb, adbseq):
        action.apply(txn)

        action_dump = action.serialize()
        act_id = str(adbseq.get(1, txn))

        adb.put(act_id, action_dump, txn)
        sadb.put(sessid, act_id, txn)


# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
