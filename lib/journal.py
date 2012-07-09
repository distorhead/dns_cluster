# -*- coding: utf-8 -*-


from database import Database, context, bdb
from twisted.python import log


class Journal(object):
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

    SEQUENCE_KEY = "_seq"
    SEQUENCE_FLAGS = bdb.DB_CREATE | bdb.DB_THREAD

    def __init__(self, *args, **kwargs):
        self._dbfile = kwargs.get("dbfile", None)

        assert not self._dbfile is None

        for dbname in self.JOURNAL_DATABASES:
            db = Database(context().dbenv(), self._dbfile, dbname,
                          self.JOURNAL_DATABASES[dbname]["type"],
                          self.JOURNAL_DATABASES[dbname]["flags"],
                          self.JOURNAL_DATABASES[dbname]["open_flags"])
            setattr(self, dbname, db)

        dbh = self.session.open()
        self._get_session_seq(dbh, None, 0)
        dbh.close()


    def start_session(self):
        """
        Get new session id.
        """

        dbh = self.session.open()
        dbtxnh = context().dbenv().txn_begin()
        try:
            dbseqh = self._get_session_seq(dbh, dbtxnh)

            id = dbseqh.get(1, dbtxnh)
            dbh.put(str(id), '', dbtxnh)

            dbtxnh.commit()
            dbseqh.close()
            dbh.close()

            return id
        except bdb.DBError, e:
            log.err("Unable to create session")
            dbtxnh.abort()
            raise

    def commit_session(self, id):
        """
        Make changes made in session permanent.
        """
        pass

    def rollback_session(self, id):
        """
        Undo changes made in session.
        """
        pass

    def _get_session_seq(self, dbh, dbtxnh=None, initial=None):
        dbseq = bdb.DBSequence(dbh)

        if not initial is None:
            dbseq.initial_value(initial)

        dbseq.open(self.SEQUENCE_KEY, dbtxnh, self.SEQUENCE_FLAGS)
        return dbseq



# vim: set sts=4:
# vim: set ts=4:
# vim: set sw=4:
# vim: set expandtab:
