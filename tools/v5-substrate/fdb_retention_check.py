"""Fresh FDB source-retention evidence; one row and finite control state at a time.

The caller owns the same quota-bound comparison spool as the ordinary raw
checker. Original CDC remains the full history of effects; this verifier never
turns that logical inventory into a claim of physical post-collection storage.
"""
import base64
import hashlib
from pathlib import Path

from comparison_spool import u64

FORMAT = "fdb-graph-cost-retained-v1"
PARALLEL_FORMAT = "fdb-graph-cost-retained-attempts-v1"
COLUMN_FORMAT = "fdb-graph-cost-retained-columns-v1"
META = "Floor Generation Versions Transactions Certificates Bytes Groups"
SEAL = "Request Cut Before After Originals Deleted DeletedBytes Anchors CensusSHA256 CumulativeDeleted"
OWNER = "Kind Request Cut Required Secret State PinRequest PinDigest Lineage TerminalRequest"
EXPIRED = "fdb-source: retained cut or token expired"
BUSY = "fdb-source: durable history retirement in progress"
IN_USE = "fdb-source: retained history has live owners"


def policy(facts):
    return {"MaxOwners": 64, "MaxControlBytes": 512 << 10, "PageRows": 128,
            "PageBytes": 4 << 20, "MaxWorkRows": 16 * facts + 64,
            "MaxWorkBytes": (64 << 20) + (16 * facts + 64) * (512 << 10),
            "MaxChainSteps": 8, "MaxOutputBytes": 4096}


def zero_cut():
    return {"Graph": "", "Topology": 0, "Epochs": [0, 0], "Scope": None, "Round": 0, "Groups": [0, 0]}


def zero_seal():
    return {"Request": "", "Cut": zero_cut(), "Before": [{k: 0 for k in META.split()} for _ in range(2)],
            "After": [{k: 0 for k in META.split()} for _ in range(2)],
            **{k: [0, 0] for k in ("Originals", "Deleted", "DeletedBytes", "Anchors")},
            "CensusSHA256": [0] * 32, "CumulativeDeleted": [0, 0]}


def zero_state(owners=0):
    return {"Phase": 0, "Request": "", "Target": zero_cut(),
            "Before": [{k: 0 for k in META.split()} for _ in range(2)],
            "Coordinator": 0, "Group": 0, "Sequence": 0, "Cursor": None,
            **{k: [0, 0] for k in ("Originals", "Scanned", "Deleted", "DeletedBytes", "Anchors")},
            "WorkRows": 0, "WorkBytes": 0, "Chain": [0] * 32, "Owners": owners,
            "Completed": zero_seal(), "CumulativeDeleted": [0, 0]}


def empty_outcome():
    return {"Commit": False, "Round": 0, "Digest": [0] * 32, "ID": "", "Request": "", "Reason": "", "Coordinator": 0, "Sequence": 0}


class Verification:
    def __init__(self, api, result, spool):
        self.a, self.r, self.s = api, result, spool
        self.e, self.c = result["Retention"], result["Config"]
        self.p = policy(result["spec"]["facts"])
        self.key = lambda name: api.fdb_key(self.c, 0, "source-retention-v1", name)
        self.owner_key = lambda name: api.fdb_key(self.c, 0, "source-owner-v1", name)
        self.state, self.ledger = zero_state(), None
        self.base_reservation = 0
        self.held = self.token = self.final_owner = None
        self.pages = self.resumes = self.guards = self.replays = self.aborts = 0
        self.old_rows, self.original_rows = [0, 0], [0, 0]
        self.old_key = self.original_key = None
        self.history = self.peek = None
        self.stages = 0
        self.old_eof = self.original_eof = self.released = False
        self.refusals = 0
        self.guard_body = None
        self.last_observation = None

    def admit(self):
        a, e, r = self.a, self.e, self.r
        a.fields(e, "Format PayloadSHA256 ArtifactSHA256 Binding ArtifactPolicy OldCut FinalCut Seal Metadata OldRows FinalRows OriginalRows Pages Resumes GuardRefusals Replays AbortedReplays Sources")
        for digest in (e["PayloadSHA256"], e["ArtifactSHA256"]):
            a.require(type(digest) is str and len(digest) == 64 and bytes.fromhex(digest).hex() == digest, "retention artifact digest")
        expected_binding = {"Version": 1, "Config": self.c, "Initial": r["InitialCut"], "Policy": self.p}
        artifact_policy = {"LineBytes": 512 << 10, "OriginalPage": {"PageRows": 128, "PageBytes": 4 << 20},
                           "MaxOriginalRows": 2 * self.c["Limits"]["Transactions"], "MaxReplayRows": 2 * self.c["Limits"]["Transactions"]}
        a.require(e["Format"] == FORMAT and a.same(e["Binding"], expected_binding) and a.same(e["ArtifactPolicy"], artifact_policy), "fresh source-retention policy/binding")
        a.require(a.same(e["OldCut"], r["OldCut"]) and a.same(e["FinalCut"], r["FinalCut"]) and a.same(e["Sources"], r["StreamEnds"]), "retention cut/source EOF join")
        a.fields(e["Seal"], SEAL)
        seal = e["Seal"]
        a.require(seal["Request"] == "source-final" and a.same(seal["Cut"], r["FinalCut"]) and a.same(seal["After"], e["Metadata"]), "immutable retirement seal/cut")
        for name in ("OldRows", "FinalRows", "OriginalRows"):
            a.require(type(e[name]) is list and len(e[name]) == 2, "retention census pair")
            for n in e[name]: a.integer(n)
        for name in ("Pages", "Resumes", "GuardRefusals", "Replays", "AbortedReplays"): a.integer(e[name])
        for name in ("Originals", "Deleted", "DeletedBytes", "Anchors", "CumulativeDeleted"):
            a.require(type(seal[name]) is list and len(seal[name]) == 2, "seal counter pair")
            for n in seal[name]: a.integer(n)
        a.sha_array(seal["CensusSHA256"])
        a.require(len(seal["Before"]) == len(e["Metadata"]) == 2, "retirement metadata pair")
        for g, (before, after) in enumerate(zip(seal["Before"], e["Metadata"])):
            for m in (before, after):
                a.fields(m, META)
                for n in m.values(): a.integer(n)
            a.require(before["Versions"] >= seal["Deleted"][g] and before["Bytes"] >= seal["DeletedBytes"][g], "retirement subtraction underflow")
            expected = before | {"Versions": before["Versions"] - seal["Deleted"][g], "Bytes": before["Bytes"] - seal["DeletedBytes"][g]}
            a.require(a.same(after, expected) and after["Floor"] == r["FinalCut"]["Round"] and after["Groups"] == r["FinalCut"]["Groups"][g], "retirement exact physical deltas")
            a.require(after["Versions"] == e["FinalRows"][g] == seal["Anchors"][g] and after["Transactions"] == e["OriginalRows"][g], "retirement originals/anchors census")
        facts = r["spec"]["facts"]
        a.require(sum(e["OldRows"]) == sum(e["FinalRows"]) == 3 + 3 * facts and sum(seal["Originals"]) == 3 + 6 * facts and sum(seal["Deleted"]) == 3 * facts and a.same(seal["CumulativeDeleted"], seal["Deleted"]), "fixed slice complete history/deletion census")
        a.require(e["Resumes"] == 1 and e["GuardRefusals"] == 2 and e["Pages"] >= 2 and e["Pages"] <= self.p["MaxWorkRows"] + 4 and e["Replays"] == sum(e["OriginalRows"]) == 3 + 2 * facts + e["AbortedReplays"] <= artifact_policy["MaxReplayRows"], "retention finite completed invocation/census contract")
        for i, source in enumerate(e["Sources"]):
            a.require(a.same(source, {"Rows": r["FinalCut"]["Groups"] if i == 2 else e["FinalRows"], "Groups": r["FinalCut"]["Groups"], "Metadata": e["Metadata"]}), "retained physical source EOF")

    def observation(self, value, *, diagnostic=None):
        a = self.a
        a.fields(value, "Metadata State Records Originals")
        a.require(a.same(value["State"], self.state), "actual persisted retirement progress")
        metadata = self.e["Seal"]["Before"]
        if self.state["Phase"]:
            metadata = [m | {"Versions": m["Versions"] - self.state["Deleted"][g], "Bytes": m["Bytes"] - self.state["DeletedBytes"][g]} for g, m in enumerate(metadata)]
        a.require(a.same(value["Metadata"], metadata), "actual page metadata debit")
        records = a.column_records(value["Records"])
        expected = {self.key("state"): a.go_json(value["State"])}
        expected.update({a.fdb_key(self.c, g, "meta", "state"): a.go_json(m) for g, m in enumerate(metadata)})
        a.require(records == expected, "same-observation raw metadata/state binding")
        originals = value["Originals"]
        if diagnostic is None:
            a.require(a.same(originals, [{"Key": None, "Value": None}] * 2), "unexpected observation originals")
        else:
            a.require(type(originals) is list and len(originals) == 2, "diagnostic original absence pair")
            for rec, kind in zip(originals, ("id", "request")):
                a.fields(rec, "Key Value")
                a.require(a.raw_bytes(rec["Key"]) == a.fdb_key(self.c, 0, kind, diagnostic["ID" if kind == "id" else "Request"]) and rec["Value"] is None, "diagnostic genuine original absence")

    def owner(self, raw, *, kind, cut, required, state, terminal, token=None):
        a = self.a
        o = a.decoded_body(raw)
        a.fields(o, OWNER)
        secret = a.sha_array(o["Secret"])
        a.require(any(secret) and o["Kind"] == kind and a.same(o["Cut"], cut) and a.same(o["Required"], required) and type(o["State"]) is int and o["State"] == state and o["TerminalRequest"] == terminal, "held source owner identity/lifetime")
        if kind == "read": a.require(o["Request"] == "source-read-" + secret[:16].hex(), "genuine source reader request")
        a.require(o["PinRequest"] == ("source-old" if token else "") and a.sha_array(o["PinDigest"]) == (hashlib.sha256(a.go_json(token)).digest() if token else bytes(32)) and a.sha_array(o["Lineage"]) == bytes(32), "held source exact pin lineage")
        return o

    def stage(self, row):
        a, r = self.a, self.r
        stages = ("old-held", "expired-held", "old-closed", "retirement-admitted", "retirement-complete", "final-read-open", "final-read-closed", "terminal")
        a.fields(row, "Type Name Records")
        i = self.stages
        a.require(i < len(stages) and row["Name"] == stages[i], "retention lifecycle stage order")
        records = a.column_records(row["Records"])
        columns = "Columns" in r
        # The source old-held stage precedes columns.holdOld in the actual
        # runner; the column reference exists only at subsequent stages.
        owners = (2, 1 + int(columns), int(columns), 0, 0, 1, 0, 0)[i]
        if i < 3: self.state = zero_state(owners)
        elif i == 3:
            a.require(self.old_eof and (not columns or self.released), "retirement before held source/reference release")
            self.state = zero_state() | {"Phase": 1, "Request": "source-final", "Target": r["FinalCut"], "Before": self.e["Seal"]["Before"]}
        else:
            a.require(self.state["Phase"] == 3, "source seal before final read")
            self.state = self.state | {"Owners": owners}
        state = a.decoded_body(records.get(self.key("state"), b""))
        a.require(a.same(state, self.state), "retention source stage state")
        # Preserve actual canonical field order for the exact raw binding.
        self.state = state
        a.require(records.get(self.key("binding")) == a.go_json(self.e["Binding"]), "retention immutable binding changed")
        ledger = a.decoded_body(records.get(self.key("ledger"), b""))
        self.check_ledger(ledger)
        if self.ledger is not None:
            a.require(ledger["Records"] >= self.ledger["Records"] and ledger["Bytes"] >= self.ledger["Bytes"], "source control reservations refunded")
            if i == 3:
                added = len(self.key("intent/source-final")) + len(self.key("seal/source-final")) + 8192
                a.require(ledger["Records"] == self.ledger["Records"] + 2 and ledger["Bytes"] == self.ledger["Bytes"] + added, "seal reservation not prepaid before deletion")
        self.ledger = ledger
        metadata = a.column_old_metadata(r, self.s) if i == 0 else self.e["Seal"]["Before"] if i < 4 else self.e["Metadata"]
        expected = {self.key("binding"), self.key("ledger"), self.key("state")}
        for g, m in enumerate(metadata):
            key = a.fdb_key(self.c, g, "meta", "state")
            a.require(records.get(key) == a.go_json(m), "source lifecycle native metadata")
            expected.add(key)
        if self.held is None:
            candidates = [k for k in records if k.startswith(a.fdb_key(self.c, 0, "source-owner-v1", "")) and k != self.owner_key("source-old")]
            a.require(len(candidates) == 1 and self.token is not None, "genuine held source reader inventory")
            self.held = self.owner(records[candidates[0]], kind="read", cut=r["OldCut"], required=r["InitialCut"], state=1, terminal="", token=self.token)
        held_state = 1 if i < 2 else 2
        held = self.owner(records.get(self.owner_key(self.held["Request"]), b""), kind="read", cut=r["OldCut"], required=r["InitialCut"], state=held_state, terminal="" if held_state == 1 else "close", token=self.token)
        a.require(a.same(held | {"State": 1, "TerminalRequest": ""}, self.held), "held source substituted across mutation/expiry")
        pin = self.owner(records.get(self.owner_key("source-old"), b""), kind="pin", cut=r["OldCut"], required=zero_cut(), state=1 if i == 0 else 3, terminal="" if i == 0 else "source-old-expire")
        a.require(pin["Request"] == self.token["Request"] and a.same(pin["Secret"], self.token["Secret"]), "pin token secret/owner join")
        expected.update((self.owner_key(self.held["Request"]), self.owner_key("source-old")))
        if i in (3, 4):
            key = self.key("intent/source-final")
            a.require(records.get(key) == a.go_json({"Request": "source-final", "Target": r["FinalCut"]}), "immutable funded retirement intent")
            expected.add(key)
        if i in (4, 7):
            a.require(records.get(self.key("seal/source-final")) == a.go_json(self.e["Seal"]), "actual immutable complete seal")
            expected.add(self.key("seal/source-final"))
        if i in (5, 6):
            rest = set(records) - expected
            a.require(len(rest) == 1, "final owned source inventory")
            key = next(iter(rest))
            o = self.owner(records[key], kind="read", cut=r["FinalCut"], required=r["FinalCut"], state=1 if i == 5 else 2, terminal="" if i == 5 else "close")
            a.require(key == self.owner_key(o["Request"]), "final source owner raw key")
            if i == 5: self.final_owner = o
            else: a.require(a.same(o | {"State": 1, "TerminalRequest": ""}, self.final_owner), "final source cleanup changed owner")
            expected.add(key)
        a.require(set(records) == expected, "complete exact source lifecycle control inventory")
        charge = self.base_reservation + len(self.owner_key("source-old")) + len(self.owner_key(self.held["Request"])) + 8192
        reserved_records = 2
        if columns and i >= 1:
            # All eight actual column readers, one issued pin and both head
            # revisions have fixed hashed request lengths. These lengths price
            # reservations; actual reader/token/reference identity is verified
            # in the complete column artifact and the detach record.
            for kind, count in (("read", 8), ("pin", 1), ("reference", 2)):
                charge += count * (len(self.owner_key("column-" + kind + "-" + "0" * 64)) + 4096)
                reserved_records += count
        if i >= 3:
            charge += len(self.key("intent/source-final")) + len(self.key("seal/source-final")) + 8192
            reserved_records += 2
        if i >= 5:
            charge += len(self.owner_key(self.final_owner["Request"])) + 4096
            reserved_records += 1
        a.require(a.same(ledger, {"Records": reserved_records, "Bytes": charge}), "exact cumulative source control reservation ledger")
        if i == 7: a.require(self.original_eof, "terminal before original/source census")
        self.stages += 1

    def check_ledger(self, ledger):
        a = self.a
        a.fields(ledger, "Records Bytes")
        a.integer(ledger["Records"], 0, self.p["MaxOwners"])
        a.integer(ledger["Bytes"], 1, self.p["MaxControlBytes"])

    def original(self, group, logical, value):
        """Mirror authentic native ID/request/CDC reads, including absent peer ID."""
        a, work, match = self.a, 0, None
        for coordinator in range(2):
            key = a.fdb_key(self.c, coordinator, "id", value["TxID"])
            raw = self.s.find(b"fdb-id", key)
            work += len(key) + (len(raw) if raw is not None else 0)
            if raw is None: continue
            o = a.decoded_body(raw)
            if not o["Commit"] or o["Round"] != value["Round"] or o["Coordinator"] != coordinator: continue
            request = a.fdb_key(self.c, coordinator, "request", o["Request"])
            work += len(request) + len(self.s.lookup(b"fdb-request", request))
            body = self.s.lookup(b"retention-cdc", u64(coordinator) + u64(o["Sequence"]))
            key = self.change_key(o)
            work += len(key) + len(body)
            original = a.decoded_body(body)
            for write in original["Writes"]:
                if write["Group"] == group and write["Key"] == logical and a.same(write["After"], value):
                    a.require(match is None, "ambiguous physical original")
                    match = original
        a.require(match is not None, "physical row has no genuine original")
        return match, work

    def change_key(self, o):
        return (f"g{o['Coordinator']}/{self.c['Graph'].encode().hex()}/changes/{o['Sequence']:016x}/" + o["Request"].encode().hex()).encode()

    def deletion(self, key, raw):
        a = self.a
        parts = key.split(b"/", 4)
        a.require(len(parts) == 5 and parts[2] == b"history", "history native key")
        g, logical, v = int(parts[0][1:]), bytes.fromhex(parts[4].decode()).decode(), a.decoded_body(raw)
        _, used = self.original(g, logical, v)
        current_key = a.fdb_key(self.c, g, "current", logical)
        current_raw = self.s.lookup(b"fdb-current", current_key)
        anchor = a.decoded_body(current_raw)
        _, anchor_used = self.original(g, logical, anchor)
        work = len(key) + len(raw) + used + len(current_key) + len(current_raw) + anchor_used
        floor = self.r["FinalCut"]["Round"]
        a.require(anchor["Round"] <= floor, "foreign future physical anchor")
        remove = 0 < v["Round"] < floor and 0 < v["Version"] < anchor["Version"] and v["Round"] < anchor["Round"]
        if remove:
            cursor = anchor
            for _ in range(self.p["MaxChainSteps"]):
                if a.same(cursor, v): break
                source, extra = self.original(g, logical, cursor)
                work += extra
                writes = [w for w in source["Writes"] if w["Group"] == g and w["Key"] == logical and a.same(w["After"], cursor)]
                a.require(len(writes) == 1 and writes[0]["Before"] is not None, "retired candidate missing actual predecessor")
                before = writes[0]["Before"]
                a.require(before["Version"] + 1 == cursor["Version"] and before["Round"] < cursor["Round"], "retirement predecessor identity")
                cursor = before
            a.require(a.same(cursor, v), "retired candidate not exact ancestor")
        else: a.require(a.same(v, anchor), "non-anchor history retained")
        return g, remove, work

    def collect(self, row):
        a = self.a
        a.fields(row, "Type Budget Page Observation")
        a.require(self.stages == 4 and self.guards == 2 and self.state["Phase"] in (1, 2), "misplaced retirement page")
        budget = {"Rows": 128, "Bytes": 4 << 20, "OutputBytes": 4096}
        a.require(a.same(row["Budget"], budget), "retirement page admission changed")
        s = self.state
        phase, count, work = s["Phase"], 0, 0
        deleted, deleted_bytes = [0, 0], [0, 0]
        # Fresh fixed slice has no preceding seal: all census effects are still
        # physical. Only the subsequent deleting phase may remove them.
        if phase == 1:
            g = s["Coordinator"]
            while count < 128 and s["Sequence"] < self.r["FinalCut"]["Groups"][g]:
                body = self.s.lookup(b"retention-cdc", u64(g) + u64(s["Sequence"] + 1))
                original = a.decoded_body(body)
                o = original["Outcome"]
                key = self.change_key(o)
                used = len(key) + len(body)
                for kind, identity in (("id", o["ID"]), ("request", o["Request"])):
                    k = a.fdb_key(self.c, g, kind, identity)
                    used += len(k) + len(self.s.lookup(b"fdb-" + kind.encode(), k))
                for write in original["Writes"]:
                    v = write["After"]
                    k = (f"g{write['Group']}/{self.c['Graph'].encode().hex()}/history/{v['Round']:016x}/" + write["Key"].encode().hex()).encode()
                    used += len(k) + len(self.s.lookup(b"fdb-history", k))
                    s["Originals"][write["Group"]] += 1
                work += used
                count += 1
                s["Sequence"] = o["Sequence"]
                chain = a.sha_array(s["Chain"])
                s["Chain"] = list(hashlib.sha256(chain + u64(len(key)) + key + u64(len(body)) + body).digest())
            if count < 128:
                s["Coordinator"] += 1
                s["Sequence"] = 0
                if s["Coordinator"] == 2:
                    a.require(s["Originals"] == [m["Versions"] for m in s["Before"]], "complete original census before deletion")
                    s["Phase"] = 2
                    self.history = self.s.walk(b"fdb-history")
        else:
            g = s["Group"]
            while count < 128:
                if self.peek is None: self.peek = next(self.history, None)
                if self.peek is None or not self.peek[0].startswith(f"g{g}/".encode()): break
                key, raw = self.peek
                self.peek = None
                actual_g, remove, used = self.deletion(key, raw)
                a.require(actual_g == g, "retirement scan group")
                work += used
                count += 1
                s["Scanned"][g] += 1
                s["Cursor"] = base64.b64encode(key).decode()
                if remove:
                    size = len(key) + len(raw)
                    deleted[g] += 1; deleted_bytes[g] += size
                    s["Deleted"][g] += 1; s["DeletedBytes"][g] += size
                else: s["Anchors"][g] += 1
            if count < 128:
                a.require(s["Scanned"][g] == s["Before"][g]["Versions"], "retirement skipped physical row")
                s["Group"] += 1; s["Cursor"] = None
                if s["Group"] == 2:
                    a.require(self.peek is None and next(self.history, None) is None, "extra physical history after EOF")
                    self.history.close(); self.history = None
                    s["Phase"] = 3
                    s["CumulativeDeleted"] = list(s["Deleted"])
                    s["Completed"] = {"Request": "source-final", "Cut": self.r["FinalCut"], "Before": s["Before"], "After": self.e["Metadata"],
                                      **{k: s[k] for k in ("Originals", "Deleted", "DeletedBytes", "Anchors")}, "CensusSHA256": s["Chain"], "CumulativeDeleted": s["CumulativeDeleted"]}
                    a.require(a.same(s["Completed"], self.e["Seal"]), "retirement seal differs from complete exact scan")
        a.require(work <= budget["Bytes"], "retirement page exceeds native work admission")
        s["WorkRows"] += count; s["WorkBytes"] += work
        a.require(s["WorkRows"] <= self.p["MaxWorkRows"] and s["WorkBytes"] <= self.p["MaxWorkBytes"], "retirement cumulative work exhausted")
        expected = {"Phase": phase, "WorkRows": count, "WorkBytes": work, "Deleted": deleted, "DeletedBytes": deleted_bytes,
                    "Done": s["Phase"] == 3, "Seal": s["Completed"] if s["Phase"] == 3 else zero_seal()}
        a.require(a.same(row["Page"], expected) and len(a.go_json(row["Page"])) <= budget["OutputBytes"], "exact native retirement page/accounting")
        self.observation(row["Observation"])
        self.last_observation = row["Observation"]
        self.pages += 1

    def proof(self, proof, *, inventory):
        a = self.a
        a.fields(proof, "Pair Original Group Outcome")
        o = proof["Outcome"]
        a.fields(o, "Commit Round Digest ID Request Reason Coordinator Sequence")
        g = a.integer(o["Coordinator"], 0, 1)
        a.require(type(o["Commit"]) is bool and type(proof["Pair"]) is list and len(proof["Pair"]) == 2, "complete genuine original proof")
        id_key = a.fdb_key(self.c, g, "id", o["ID"])
        namespace = b"fdb-id" if o["Commit"] else b"fdb-attempt-id"
        a.require(self.s.lookup(namespace, id_key) == a.go_json(o), "retained original not actual business/attempt receipt")
        for rec, kind in zip(proof["Pair"], ("id", "request")):
            a.raw_binding(rec, a.fdb_key(self.c, g, kind, o["ID"] if kind == "id" else o["Request"]), o)
            a.require(self.s.lookup(b"fdb-" + (b"attempt-" if not o["Commit"] else b"") + kind.encode(), a.raw_bytes(rec["Key"])) == a.raw_bytes(rec["Value"]), "paired retained request identity")
        if o["Commit"]:
            group = a.decoded_body(self.s.lookup(b"retention-cdc", u64(g) + u64(o["Sequence"])))
            a.require(a.same(proof["Group"], group) and a.same(group["Outcome"], o), "retained committed CDC/original binding")
            a.raw_binding(proof["Original"], self.change_key(o), group)
            body = a.go_json(group["Tx"])
        else:
            attempt = a.decoded_body(self.s.lookup(b"retention-attempt", id_key))
            a.require(attempt["Classification"] == "aborted" and o["Round"] == o["Sequence"] == 0, "unknown empty original is not ABORT")
            a.fields(proof["Original"], "Key Value")
            a.require(a.same(proof["Original"], {"Key": base64.b64encode(self.change_key(o)).decode() if inventory else None, "Value": None}), "ABORT genuine CDC absence")
            a.require(a.same(proof["Group"], {"Tx": {"Graph": "", "Topology": 0, "ID": "", "Request": "", "Coordinator": 0, "Dependency": 0, "Participants": None}, "Outcome": empty_outcome(), "Writes": None}), "ABORT invented decoded CDC")
            body = a.raw_bytes(attempt["Proposal"])
        a.require(a.sha_array(o["Digest"]) == hashlib.sha256(body).digest(), "original frozen transaction digest")
        return id_key, o, body

    def replay(self, row):
        a = self.a
        a.fields(row, "Type Body BodySHA256 Submission Proof Before After")
        a.require(self.stages == 5 and self.refusals == 3 and not self.original_eof, "misplaced original replay")
        key, o, body = self.proof(row["Proof"], inventory=False)
        a.require(row["Type"] == ("original-replay" if o["Commit"] else "aborted-original-replay") and a.raw_bytes(row["Body"]) == body and a.sha_array(row["BodySHA256"]) == hashlib.sha256(body).digest(), "SAME-body original replay")
        self.submission(row["Submission"], 1, o, "")
        self.observation(row["Before"]); self.observation(row["After"])
        a.require(a.same(row["Before"], row["After"]), "original replay mutated metadata/source progress")
        self.s.insert(b"retention-replay", key, a.go_json(o))
        self.replays += 1; self.aborts += int(not o["Commit"])

    def submission(self, s, ordinal, outcome, error):
        a = self.a
        a.fields(s, "Ordinal Callbacks Outcome Error Retryable MaybeCommitted")
        a.require(type(s["Ordinal"]) is int and s["Ordinal"] == ordinal and a.same(s["Outcome"], outcome) and s["Error"] == error and s["Retryable"] is False and s["MaybeCommitted"] is False, "actual original submission classification")
        a.integer(s["Callbacks"], 1, 16)


def verify(api, path, result, spool):
    v = Verification(api, result, spool)
    v.admit()
    a, e = api, v.e
    payload, whole, footer = hashlib.sha256(), hashlib.sha256(), False
    # Derived from the same finite source/attempt profile, including empty scan
    # boundary pages and all genuine COMMIT/ABORT inventory/replay frames.
    maximum = 64 + e["OldRows"][0] + e["OldRows"][1] + v.p["MaxWorkRows"] + 4 + 2 * e["ArtifactPolicy"]["MaxOriginalRows"]
    try:
        for index, (row, line) in enumerate(a.parallel_lines(Path(path), maximum)):
            a.require(not footer, "extra retention frame after EOF")
            whole.update(line)
            kind = row.get("Type")
            if kind == "footer":
                a.fields(row, "Type End")
                a.require(a.same(row["End"], e | {"ArtifactSHA256": ""}) and payload.hexdigest() == e["PayloadSHA256"], "retention footer/result seal")
                footer = True
                continue
            payload.update(line)
            if index == 0:
                a.fields(row, "Type Format Binding ArtifactPolicy Records")
                a.require(kind == "header" and row["Format"] == FORMAT and a.same(row["Binding"], e["Binding"]) and a.same(row["ArtifactPolicy"], e["ArtifactPolicy"]), "retention header fresh identity")
                records = a.column_records(row["Records"])
                ledger = a.decoded_body(records.get(v.key("ledger"), b""))
                reserved = len(v.key("binding")) + len(a.go_json(e["Binding"])) + len(v.key("ledger")) + 256 + len(v.key("state")) + 4096
                expected = {v.key("binding"): a.go_json(e["Binding"]), v.key("ledger"): a.go_json({"Records": 0, "Bytes": reserved}), v.key("state"): a.go_json(zero_state())}
                expected.update({a.fdb_key(v.c, g, "meta", "config"): a.go_json(v.c) for g in range(2)})
                a.require(records == expected, "actual empty initial control reservation/config")
                v.check_ledger(ledger); v.ledger = ledger
                v.base_reservation = reserved
            elif kind == "pin-intent":
                a.fields(row, "Type Token")
                a.require(v.token is None and v.stages == 0, "duplicate/misplaced source token")
                token = row["Token"]
                a.fields(token, "Request Cut Secret")
                a.require(token["Request"] == "source-old" and a.same(token["Cut"], result["OldCut"]) and any(a.sha_array(token["Secret"])), "genuine issued retained old-cut token")
                v.token = token
            elif kind == "stage": v.stage(row)
            elif kind == "refusal":
                a.fields(row, "Type Name Error")
                expected = (("expired-new-open", EXPIRED, 1), ("held-source-retirement", IN_USE, 1), ("below-floor-new-read", EXPIRED, 5))
                a.require(v.refusals < 3 and (row["Name"], row["Error"], v.stages) == expected[v.refusals], "actual expiry/held/floor refusal")
                v.refusals += 1
            elif kind == "row":
                a.fields(row, "Type Name Row")
                a.require(v.stages == 2 and v.refusals == 2 and not v.old_eof and row["Name"] == "old-held", "old source row lifetime/EOF")
                raw = row["Row"]
                a.fields(raw, "Group Key Value")
                g = a.integer(raw["Group"], 0, 1)
                key, body = a.raw_bytes(raw["Key"]), a.raw_bytes(raw["Value"])
                a.require(v.old_key is None or v.old_key < (g, key), "old-held duplicate/reordered row")
                logical = bytes.fromhex(key.rsplit(b"/", 1)[1].decode()).decode()
                expected_body = spool.lookup(b"fdb-old", a.fdb_key(v.c, g, "current", logical))
                value = a.decoded_body(body)
                expected_key = (f"g{g}/{v.c['Graph'].encode().hex()}/history/{value['Round']:016x}/" + logical.encode().hex()).encode()
                a.require(key == expected_key and body == expected_body and body == spool.lookup(b"fdb-history", key), "old-held current substitution/foreign original")
                v.old_rows[g] += 1; v.old_key = g, key
            elif kind == "read-eof":
                a.fields(row, "Type Name Cut Rows")
                a.require(v.stages == 2 and not v.old_eof and row["Name"] == "old-held" and a.same(row["Cut"], result["OldCut"]) and a.same(row["Rows"], v.old_rows) and v.old_rows == e["OldRows"], "genuine held-old complete EOF")
                v.old_eof = True
            elif kind == "column-reference-release":
                release(v, row)
            elif kind == "guard-submission":
                diagnostic(v, row)
            elif kind == "collect": v.collect(row)
            elif kind == "durable-resume":
                a.fields(row, "Type Request Before After")
                a.require(v.stages == 4 and v.resumes == 0 and row["Request"] == "source-final" and v.state["Phase"] == 2 and sum(v.state["Deleted"]) > 0 and a.same(row["Before"], v.last_observation) and a.same(row["Before"], row["After"]), "same persisted owner/progress on reconstructed helper")
                v.observation(row["Before"]); v.resumes += 1
            elif kind in ("original-replay", "aborted-original-replay"): v.replay(row)
            elif kind == "retained-original":
                a.fields(row, "Type Group Proof")
                a.require(v.stages == 5 and not v.original_eof and v.replays == e["Replays"], "original inventory before complete replays")
                key, o, _ = v.proof(row["Proof"], inventory=True)
                g = a.integer(row["Group"], 0, 1)
                a.require(g == o["Coordinator"] and (v.original_key is None or v.original_key < (g, key)), "unique native ID-prefix inventory order")
                a.require(spool.lookup(b"retention-replay", key) == a.go_json(o), "original inventory missing its exact replay")
                spool.insert(b"retention-inventory", key, a.go_json(o))
                v.original_rows[g] += 1; v.original_key = g, key
            elif kind == "originals-eof":
                a.fields(row, "Type Rows Observation")
                a.require(v.stages == 5 and not v.original_eof and a.same(row["Rows"], v.original_rows) and v.original_rows == e["OriginalRows"], "complete unique retained original EOF")
                v.observation(row["Observation"]); v.original_eof = True
            else: a.require(False, "unknown/misplaced source retention frame")
        a.require(footer and whole.hexdigest() == e["ArtifactSHA256"] and v.stages == 8 and v.original_eof and v.old_eof and v.refusals == 3 and (v.pages, v.resumes, v.guards, v.replays, v.aborts) == tuple(e[k] for k in ("Pages", "Resumes", "GuardRefusals", "Replays", "AbortedReplays")), "complete retention lifecycle/sealed artifact EOF")
        for namespace in (b"fdb-id", b"fdb-attempt-id"):
            for key, body in spool.walk(namespace):
                a.require(spool.lookup(b"retention-inventory", key) == body and spool.lookup(b"retention-replay", key) == body, "omitted genuine COMMIT/ABORT from unique prefix/replay inventory")
        return {"format": FORMAT, "artifactSHA256": whole.hexdigest(), "oldHeldRows": v.old_rows, "physicalFinalRows": e["FinalRows"], "originalRows": v.original_rows,
                "pages": v.pages, "successfulPageWorkRows": v.state["WorkRows"], "successfulPageWorkBytes": v.state["WorkBytes"], "resumes": v.resumes,
                "accounting": "actual successful source census/deletion data reads; excludes control/config/cut checks and SDK retries; post-SDK admission, not RSS",
                "profileDifference": "FDB source-v1 retires physical superseded history; native cost source remains retain-all; semantic agreement does not establish equal retention policy or storage efficiency"}
    finally:
        if v.history is not None: v.history.close()


def diagnostic(v, row):
    a = v.a
    a.fields(row, "Type Classification Diagnostic Ordinal Body BodySHA256 Submission Before After")
    a.require(v.stages == 4 and v.pages == 0 and v.guards < 2 and row["Classification"] == "retirement-busy" and row["Diagnostic"] is True and type(row["Ordinal"]) is int and row["Ordinal"] == v.guards + 1, "separate non-business guard submission")
    body = a.raw_bytes(row["Body"])
    a.require(len(body) <= 65536 and a.sha_array(row["BodySHA256"]) == hashlib.sha256(body).digest() and (v.guard_body is None or body == v.guard_body), "diagnostic SAME immutable bytes")
    tx = a.decoded_body(body)
    a.fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    a.require(tx["Graph"] == v.c["Graph"] and tx["Topology"] == v.c["Topology"] and tx["ID"] == "source-retirement-diagnostic" and tx["Request"] == "request/source-retirement-diagnostic" and tx["Coordinator"] == 0 and tx["Dependency"] == 0, "diagnostic exact non-business namespace")
    # The owner captured this same declared first-edge update against the final
    # state, before the native busy guard. It is never folded into useful work.
    command, lifecycle, effects, reads = a.fdb_contract(v.r["spec"], 4 + v.r["spec"]["facts"])
    a.require(a.same(tx["GraphChange"], command) and a.same(tx["Lifecycle"], lifecycle) and [p["Group"] for p in tx["Participants"]] == sorted(effects), "diagnostic genuine fixed operation shape")
    for p in tx["Participants"]:
        g = p["Group"]
        expected_reads = [{"Key": key, "Version": a.decoded_body(v.s.lookup(b"fdb-current", a.fdb_key(v.c, g, "current", key)))["Version"]} for key in reads[g]]
        a.require(a.same(p, {"Group": g, "Epoch": v.c["Epochs"][g], "Generation": v.e["Seal"]["Before"][g]["Generation"], "Reads": expected_reads, "Effects": effects[g]}), "diagnostic actual dependencies/final revisions")
    v.submission(row["Submission"], v.guards + 1, empty_outcome(), BUSY)
    v.observation(row["Before"], diagnostic=tx); v.observation(row["After"], diagnostic=tx)
    a.require(a.same(row["Before"], row["After"]), "busy diagnostic left original/metadata effects")
    v.guard_body = body; v.guards += 1


def release(v, row):
    a = v.a
    a.fields(row, "Type Before After Reference Records")
    a.require("Columns" in v.r and v.stages == 3 and not v.released, "undeclared/duplicate column source-reference release")
    # Both observations use the same idle state except its actual owner debit.
    v.observation(row["Before"])
    v.state = v.state | {"Owners": 0}
    v.observation(row["After"])
    a.require(a.same(row["Before"] | {"State": row["After"]["State"], "Records": row["After"]["Records"]}, row["After"]), "reference release changed source metadata")
    ref = row["Reference"]
    expected_ref = {"Name": "head", "Generation": "", "Version": 3, "Manifest": [0] * 32}
    a.require(a.same(ref, expected_ref), "authenticated head revision detach")
    records = a.column_records(row["Records"])
    prefix = f"g0/{v.c['Graph'].encode().hex()}/columns-v1/".encode()
    key = lambda kind, name: prefix + b"retention-v1/" + kind.encode() + b"/" + name.encode().hex().encode()
    reference_key, request_key = key("reference", "head"), key("reference-request", "head-source-retirement")
    intent = hashlib.sha256(a.go_json({"Name": "head", "Generation": "", "Expected": 2})).digest()
    a.require(records.get(reference_key) == a.go_json(ref) and records.get(request_key) == a.go_json({"Intent": list(intent), "Reference": ref}), "actual immutable detach request/reference")
    manifest = v.r["Columns"]["Final"]
    guard_key = prefix + b"generation/" + b"final".hex().encode() + b"/owners"
    expected_guard = {"Manifest": list(hashlib.sha256(a.go_json(manifest)).digest()), "State": 1, "References": 0, "Pins": 0, "Readers": 0, "Cursor": 0, "DeletedBytes": 0, "Chain": [0] * 32}
    a.require(records.get(guard_key) == a.go_json(expected_guard), "source detach generation guard")
    ledger_key = prefix + b"retention-v1/ledger"
    ledger = a.decoded_body(records.get(ledger_key, b""))
    before_ledger = v.r["Columns"]["PinLedger"]
    a.require(a.same(ledger, before_ledger | {"Records": before_ledger["Records"] + 1, "Bytes": before_ledger["Bytes"] + len(request_key) + 4096, "ReservedRecords": 0, "ReservedBytes": 0}), "actual prepaid reference-detach ledger")
    old_ref = {"Name": "head", "Generation": "final", "Version": 2, "Manifest": list(hashlib.sha256(a.go_json(manifest)).digest())}
    digest = hashlib.sha256(a.go_json(old_ref)).digest()
    request = "column-reference-" + hashlib.sha256(b"head/2").hexdigest()
    owner_key = v.owner_key(request)
    owner = a.decoded_body(records.get(owner_key, b""))
    a.fields(owner, OWNER)
    a.require(a.same(owner, {"Kind": "column-pin", "Request": request, "Cut": v.r["InitialCut"], "Required": zero_cut(), "Secret": list(digest), "State": 2, "PinRequest": "", "PinDigest": [0] * 32, "Lineage": list(digest), "TerminalRequest": "head-source-retirement"}), "released column reference source lineage")
    a.require(set(records) == {reference_key, request_key, guard_key, ledger_key, owner_key}, "exact detached reference control inventory")
    v.released = True
