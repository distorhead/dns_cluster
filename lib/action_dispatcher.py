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
            "open_flags": bdb.DB_CREATE
        },
        "journal": {
            "type": bdb.DB_BTREE,
            "flags": 0,
            "open_flags": bdb.DB_CREATE
        }
    }

    def __init__(self, *args, **kwargs):
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._dbfile is None

        for dbname in self.JOURNAL_DATABASES:
            db = Database(context().dbenv(), self._dbfile, dbname,
                          self.JOURNAL_DATABASES[dbname]["type"],
                          self.JOURNAL_DATABASES[dbname]["flags"],
                          self.JOURNAL_DATABASES[dbname]["open_flags"])
            setattr(self, dbname, db)

        txn = context().dbenv().txn_begin()
        sdb = self.session.open(txn)
        jdb = self.journal.open(txn)
        txn.commit()

        Database.sequence(sdb, None, 0)
        sdb.close()

        Database.sequence(jdb, None, 0)
        jdb.close()


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
            id = dbseq.get(1, txn)
            db.put(str(id), '', txn)

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
        pass

    def apply_action(self, action, sessid=None):
        """
        Apply specified action in session (created automatically
            if omitted).
        """

        txn = None
        try:
            txn = context().dbenv().txn_begin()

            if sessid is None:
                sessid = self.start_session(txn)

            action.apply(txn)

            action_dump = cPickle.dumps(action)
            db = self.journal.open()
            dbseq = Database.sequence(db, txn)

            id = dbseq.get(1, txn)
            val = str(sessid) + " " + action_dump
            db.put(str(id), val, txn)

            txn.commit()
            dbseq.close()
            db.close()
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
