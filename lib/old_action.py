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
