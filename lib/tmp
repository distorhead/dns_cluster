
@Action.register_action
class AddSegment(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("segment_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]), str(data["segment_name"]), data["state"])

    def __init__(self, arena_name, segment_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name
        self.segment_name = segment_name

    def _apply_do(self, txn):
        adb = database.context().arena.open()
        asdb = database.context().arena_segment.open()

        if not adb.exists(self.arena_name, txn):
            raise exception.ActionError("unable to add segment '{0}' "
                                        "to arena '{1}': "
                                        "arena '{1}' doesn't exists".format(
                                        self.segment_name, self.arena_name))

        segments = bdb_helpers.get_all(asdb, self.arena_name, txn)
        if not self.segment_name in segments:
            asdb.put(self.arena_name, self.segment_name, txn)
        else:
            raise exception.ActionError("unable to add segment '{0}' "
                                        "to arena '{1}': "
                                        "segment already exists".format(
                                        self.segment_name, self.arena_name))

        adb.close()
        asdb.close()

    def _apply_undo(self, txn):
        asdb = database.context().arena_segment.open()
        szdb = database.context().segment_zone.open()

        if szdb.exists(self.arena_name + ' ' + self.segment_name, txn):
            raise exception.ActionError("unable to delete segment '{0}' "
                                        "from arena '{1}': "
                                        "segment contains zones".format(
                                        self.segment_name, self.arena_name))

        if bdb_helpers.pair_exists(asdb, self.arena_name, self.segment_name, txn):
            bdb_helpers.delete_pair(asdb, self.arena_name, self.segment_name, txn)
        else:
            raise exception.ActionError("unable to delete segment '{0}' "
                                        "from arena '{1}': "
                                        "segment doesn't exist".format(
                                        self.segment_name, self.arena_name))

        asdb.close()
        szdb.close()


@Action.register_action
class AddZone(Action):
    @classmethod
    def from_data(cls, data):
        if not data.has_key("arena_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("segment_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("zone_name"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: arena_name required")

        if not data.has_key("state"):
            raise exception.ActionError("unable to construct action: "
                              "wrong action data: state required")

        return cls(str(data["arena_name"]),
                   str(data["segment_name"]),
                   str(data["zone_name"]),
                   data["state"])

    def __init__(self, arena_name, segment_name, zone_name, state=None):
        super(self.__class__, self).__init__(state)
        self.arena_name = arena_name
        self.segment_name = segment_name
        self.zone_name = zone_name

    def _apply_do(self, txn):
        adb = database.context().arena.open()
        asdb = database.context().arena_segment.open()
        szdb = database.context().segment_zone.open()
        zdb = database.context().dns_zone.open()

        if not adb.exists(self.arena_name, txn):
            raise exception.ActionError("unable to add zone '{0}': "
                                        "arena '{1}' doesn't exists".format(
                                        self.zone_name, self.arena_name))

        segments = bdb_helpers.get_all(asdb, self.arena_name, txn)
        if not self.segment_name in segments:
            raise exception.ActionError("unable to add zone '{0}': "
                                        "segment '{1}' doesn't exists".format(
                                        self.zone_name, self.segment_name))

        zone_rname = reorder(self.zone_name)
        sz_key = self.arena_name + ' ' + self.segment_name

        if not zdb.exists(zone_rname, txn):
            zdb.put(zone_rname, sz_key, txn)
        else:
            arena_segm = zdb.get(zone_rname, txn)
            raise exception.ActionError("unable to add zone '{0}' "
                                        "to '{1} {2}': "
                                        "zone already exists in '{3}'".format(
                                        self.zone_name, self.arena_name,
                                        self.segment_name, arena_segm))

        zones = bdb_helpers.get_all(szdb, sz_key, txn)
        if not zone_rname in zones:
            szdb.put(sz_key, zone_rname, txn)

        adb.close()
        asdb.close()
        szdb.close()
        zdb.close()

    def _apply_undo(self, txn):
        zdb = database.context().dns_zone.open()
        xdb = database.context().dns_xfr.open()
        zddb = database.context().zone_dns_data.open()
        szdb = database.context().segment_zone.open()

        if zddb.exists(self.zone_name, txn) or xdb.exists(self.zone_name, txn):
            raise exception.ActionError("unable to delete zone '{0}': "
                                        "zone contains records".format(
                                        self.zone_name))

        zone_rname = reorder(self.zone_name)

        if zdb.exists(zone_rname, txn):
            zdb.delete(zone_rname, txn)
        else:
            raise exception.ActionError("unable to delete zone '{0}': "
                                        "zone doesn't exist".format(
                                        self.zone_name))

        bdb_helpers.delete_pair(szdb, self.arena_name + ' ' + self.segment_name,
                                zone_rname, txn)

        zdb.close()
        xdb.close()
        zddb.close()
        szdb.close()



class AddRecord_A(Action): pass
class AddRecord_PTR(Action): pass
class AddRecord_CNAME(Action): pass
class AddRecord_DNAME(Action): pass
class AddRecord_SOA(Action): pass
class AddRecord_NS(Action): pass
class AddRecord_MX(Action): pass
class AddRecord_SRV(Action): pass
class AddRecord_TXT(Action): pass


