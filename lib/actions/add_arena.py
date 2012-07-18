# -*- coding: utf-8 -*-

from lib import database
from lib import lock
from lib.action import Action, ActionError


@Action.register_action
class AddArena(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]), data["state"])

    def __init__(self, arena_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name

    def _apply_do(self, sessid, txn):
        lck = lock.context().lock(self.arena_name, sessid)
        defer = lck.acquire()
        defer.addCallback(self._apply_do_cb, txn, lck)
        defer.addErrback(self._apply_do_errb, lck)

    def _apply_do_cb(self, lock_res, txn, lck):
        adb = database.context().dbpool().arena.open()
        if not adb.exists(self.arena_name, txn):
            adb.put(self.arena_name, '', txn)
        else:
            raise ActionError("unable to add arena '{0}': "
                              "arena already exists".format(
                                            self.arena_name))
        adb.close()
        lck.release()

    def _apply_do_errb(self, failure, lck):
        lck.release()
        raise ActionError("unable to add arena '{0}': " + str(failure))


    def _apply_undo(self, sessid, txn):
        lck = lock.context().lock(self.arena_name, sessid)
        defer = lck.acquire()
        defer.addCallback(self._apply_undo_cb, txn, lck)
        defer.addErrback(self._apply_undo_errb, lck)

    def _apply_undo_cb(self, lock_res, txn, lck):
        adb = database.context().dbpool().arena.open()
        asdb = database.context().dbpool().arena_segment.open()

        if asdb.exists(self.arena_name, txn):
            raise ActionError("unable to delete arena '{0}': "
                              "arena contains segments".format(
                                            self.arena_name))

        if adb.exists(self.arena_name, txn):
            adb.delete(self.arena_name, txn)
        else:
            raise ActionError("unable to delete arena '{0}': "
                              "arena doesn't exist".format(
                                            self.arena_name))

        adb.close()
        asdb.close()
        lck.release()

    def _apply_undo_errb(self, failure, lck):
        lck.release()
        raise ActionError("unable to delete arena '{0}': " + str(failure))


# vim:sts=4:ts=4:sw=4:expandtab:
