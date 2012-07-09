# -*- coding: utf-8 -*-


import cPickle

from database import Database, context, bdb
from twisted.python import log


class ActionDispatcher(object):
    """
    Frontend interface to manage journal records
    and control sessions.
    """

    JOURNAL_DATABASES = {
        "session": {
            "type": bdb.DB_HASH,
            "flags": 0,
            "open_flags": bdb.DB_CREATE,
            "autoincrement": 1
        },
        "action": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE,
            "autoincrement": 1
        },
        "session_action": {
            "type": bdb.DB_BTREE,
            "flags": bdb.DB_DUP | bdb.DB_DUPSORT,
            "open_flags": bdb.DB_CREATE,
            "autoincrement": 0
        }
    }

    def __init__(self, *args, **kwargs):
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._dbfile is None

        for dbname in self.JOURNAL_DATABASES:
            dbdesc = Database(context().dbenv(), self._dbfile, dbname,
                          self.JOURNAL_DATABASES[dbname]["type"],
                          self.JOURNAL_DATABASES[dbname]["flags"],
                          self.JOURNAL_DATABASES[dbname]["open_flags"])
            setattr(self, dbname, dbdesc)

            if self.JOURNAL_DATABASES[dbname]["autoincrement"]:
                db = dbdesc.open()
                Database.sequence(db, None, 0)
                db.close()

    def start_session(self, txn=None):
        """
        Get new session id.
        """

        try:
            db = self.session.open()

            if txn is None:
                txn = context().dbenv().txn_begin()
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

            return id
        except bdb.DBError, e:
            log.err("Unable to create session")
            if not txn is None:
                txn.abort()
            raise

    def rollback_session(self, id):
        """
        Undo changes made in session.
        """

        txn = None
        try:
            pass
        except bdb.DBError, e:
            log.err("Unable to rollback session")
            if not txn is None:
                txn.abort()

    def apply_action(self, action, sessid=None):
        """
        Apply specified action in session (created automatically
            if omitted).
        """

        txn = None
        try:
            txn = context().dbenv().txn_begin()

            if sessid is None:
                sessid = str(self.start_session(txn))
            else:
                sessid = str(sessid)

            action.apply(txn)

            action_dump = cPickle.dumps(action)
            adb = self.action.open()
            adbseq = Database.sequence(adb, txn)
            act_id = str(adbseq.get(1, txn))
            adb.put(act_id, action_dump, txn)

            sadb = self.session_action.open()
            sadb.put(sessid, act_id, txn)

            txn.commit()
            adbseq.close()
            adb.close()
            sadb.close()
        except bdb.DBError:
            log.err("Unable to apply action {0}".format(
                     action.get_name()))
            if not txn is None:
                txn.abort()
            raise


# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
