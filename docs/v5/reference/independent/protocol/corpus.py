"""Explicit fault traces and bounded interleavings; expected answers authored independently."""
from __future__ import annotations

import copy
import itertools
import json
from pathlib import Path

from oracle import check, digest

ROOT = Path(__file__).resolve().parent


def put(value):
    return {"op": "put", "value": value}


def transaction(writes, reads=(), participants=None, coordinator="B", historical=False, ids=()):
    participants = participants or sorted({k.split("/")[0] for k in writes})
    return {"participants": participants, "coordinator": coordinator, "epochs": {g: 1 for g in participants}, "reads": list(reads), "writes": writes, "historical": historical, "request_key": "request", "ids": list(ids)}


def history(name, initial=None, txs=None):
    return {"schema_version": 1, "name": name, "groups": {"A": ["A1", "A2", "A3"], "B": ["B1", "B2", "B3"]}, "epochs": {"A": 1, "B": 1}, "topology": 1, "observation": {"durable_events_complete": True, "client_reads_complete": True, "groups": ["A", "B"], "assumption": "All accepted durable group transitions are captured through certificate watermarks; absence of an event alone is not evidence."}, "initial": initial or {}, "transactions": txs or {}, "events": []}


class Trace:
    def __init__(self, h):
        self.h = h
        self.e = h["events"]
        self.positions = {"A": 0, "B": 0}
        self.pending = {"A": set(), "B": set()}
        self.fences = {}
        self.cuts = {}
        self.owners = dict(h.get("ownership", {"A": "A", "B": "B"}))

    def add(self, kind, **kw):
        self.e.append({"kind": kind, **kw})
        return len(self.e) - 1

    def durable(self, kind, group=None, **kw):
        g = group or self.h["transactions"][kw["tx"]]["coordinator"]
        self.positions[g] += 1
        return self.add(kind, replicas=[g + "1", g + "2"], position=self.positions[g], **({"group": group} if group else {}), **kw)

    def register(self, tx):
        s = self.h["transactions"][tx]
        self.durable("register", tx=tx, digest=digest(s), participants=s["participants"], epochs=s["epochs"])

    def prepare(self, tx, g, floor=0):
        s = self.h["transactions"][tx]
        self.durable("prepare", g, tx=tx, digest=digest(s), epochs=s["epochs"], floor=floor)
        self.pending[g].add(tx)

    def decision(self, tx, outcome="commit", round=10):
        self.durable("decision", tx=tx, outcome=outcome, round=round if outcome == "commit" else None)

    def install(self, tx, g, aborted=False):
        s = self.h["transactions"][tx]
        effects = {} if aborted else {k: v for k, v in s["writes"].items() if self.owners[k.split("/")[0]] == g}
        self.durable("install", g, tx=tx, effects=effects)
        self.pending[g].discard(tx)

    def ack(self, tx, outcome="commit", round=10):
        s = self.h["transactions"][tx]
        result = {"tx": tx, "digest": digest(s), "decision": outcome, "round": round if outcome == "commit" else None, "ids": copy.deepcopy(s.get("ids", []))}
        self.add("ack", tx=tx, result=result)
        return result

    def fence(self, g, bound):
        self.fences[g] = self.durable("fence", g, epoch=self.h["epochs"][g], round=bound)

    def cert(self, ident, bound, scope=("A", "B"), topology=1):
        self.cuts[ident] = {b: self.owners[b] for b in scope}
        self.add("certify", cut={"id": ident, "round": bound, "scope": list(scope), "topology": topology, "owners": {b: self.owners[b] for b in scope}}, coverage={g: {"complete": True, "through": self.positions[g], "prepared": sorted(self.pending[g])} for g in {self.owners[b] for b in scope}}, fence_events={g: self.fences[g] for g in {self.owners[b] for b in scope}}, durable={g: [g + "1", g + "2"] for g in {self.owners[b] for b in scope}})

    def read(self, cut, values, scope=("A", "B"), mode="At", topology=1, **kw):
        self.add("read", cut=cut, values=values, scope=list(scope), mode=mode, topology=topology, served_by=self.cuts[cut], status="ok", **kw)


def cross_trace():
    s = transaction({"A/node/1": put({"life": 1, "label": "old"}), "A/edge/r": put({"from": 1, "to": 2}), "A/out/1/r": put(1), "B/node/2": put({"life": 1}), "B/in/2/r": put(1)}, ids=[1, 2, 3])
    h = history("E15-cross-effects-apply-lag-crash-duplicate", txs={"t1": s})
    t = Trace(h)
    t.add("enqueue", message="registration")
    t.add("drop", message="first-registration-reply")
    t.register("t1")
    t.prepare("t1", "A")
    t.add("crash", group="A", replica="A1")
    t.add("restart", group="A", replica="A1")
    t.add("recover", group="A", prepared=["t1"], decisions=[], installed=[], floor=0, epoch=1)
    t.prepare("t1", "B")
    t.decision("t1")
    result = t.ack("t1")
    t.add("unknown_reply", tx="t1")
    t.install("t1", "A")
    t.add("read", mode="Fresh", status="unavailable", scope=["A", "B"], reason="B installation unresolved")
    t.add("crash", group="B", replica="B1")
    t.add("restart", group="B", replica="B1")
    t.add("recover", group="B", prepared=["t1"], decisions=["t1"], installed=[], floor=0, epoch=1)
    t.add("duplicate", message="commit-resolution-B")
    t.install("t1", "B")
    t.add("read_begin", id="fresh-request", scope=["A", "B"])
    t.add("collect", request="fresh-request", id="fresh", scope=["A", "B"], available=True, quorums={g: [g + "1", g + "2"] for g in ("A", "B")})
    for g in ("A", "B"):
        t.fence(g, 20)
    t.cert("c20", 20)
    expected = {k: v["value"] for k, v in s["writes"].items()}
    t.read("c20", expected, mode="Fresh", collection="fresh")
    t.read("c20", expected, mode="After", after="t1")
    t.add("replay", request_key="request", digest=digest(s), status="known", result=result)
    t.add("replay", request_key="request", digest="different-payload", status="conflict")
    t.add("final", scope=["A", "B"], values=expected)
    return h


def unresolved_trace():
    s = transaction({"A/x": put(1), "B/y": put(1)})
    h = history("E16-unresolved-old-prepare-must-block", {"A/x": 0, "B/y": 0}, {"t1": s})
    t = Trace(h)
    t.register("t1")
    t.prepare("t1", "A")
    t.prepare("t1", "B")
    for g in ("A", "B"):
        t.fence(g, 100)
    t.add("timeout", tx="t1", reason="coordinator unavailable; cannot infer abort")
    t.add("network", group="B", reachable=["B1"])
    t.add("read", mode="Fresh", status="unavailable", scope=["A"], reason="undecided coordinator outside scope")
    t.add("network", group="B", reachable=["B1", "B2", "B3"])
    t.decision("t1", round=90)
    for g in ("A", "B"):
        t.install("t1", g)
    t.ack("t1", round=90)
    t.cert("c100", 100)
    t.read("c100", {"A/x": 1, "B/y": 1})
    return h


def local_and_abort_trace():
    a = transaction({"A/x": put(1)}, coordinator="A")
    b = transaction({"A/phantom": put(99), "B/y": put(99)}, reads=[{"kind": "key", "key": "A/missing", "exists": False, "value": None}])
    b["request_key"] = "abort-request"
    h = history("local-fast-path-abort-private-effects", {"A/x": 0}, {"local": a, "aborted": b})
    t = Trace(h)
    t.durable("local_commit", tx="local", epoch=1, round=5)
    t.ack("local", round=5)
    t.register("aborted")
    t.prepare("aborted", "A", floor=5)
    t.add("unknown_reply", tx="aborted")
    t.decision("aborted", "abort")
    t.install("aborted", "A", aborted=True)
    t.ack("aborted", "abort")
    t.fence("A", 10)
    t.fence("B", 10)
    t.cert("c10", 10)
    t.read("c10", {"A/x": 1})
    t.add("final", scope=["A", "B"], values={"A/x": 1})
    return h


def historical_trace():
    s1 = transaction({"A/node/1": put({"label": "old", "life": 1}), "B/index/old/1": put(1)})
    s2 = transaction({"A/node/1": put({"label": "new", "life": 1}), "B/index/old/1": {"op": "delete"}, "B/index/new/1": put(1)}, reads=[{"kind": "key", "key": "A/node/1", "exists": True, "value": {"label": "old", "life": 1}, "revision": "t1"}])
    s2["request_key"] = "correction"
    h = history("E17-E18-retained-cut-correction-transfer-reopen", txs={"t1": s1, "t2": s2})
    t = Trace(h)
    for tx, rnd in (("t1", 10), ("t2", 30)):
        t.register(tx)
        for g in ("A", "B"):
            t.prepare(tx, g, floor=0 if tx == "t1" else 20)
        t.decision(tx, round=rnd)
        for g in ("A", "B"):
            t.install(tx, g)
        t.ack(tx, round=rnd)
        if tx == "t1":
            for g in ("A", "B"):
                t.fence(g, 20)
            t.cert("old", 20)
    t.durable("transfer", "A", old_epoch=1, new_epoch=2, retained_lineage=True)
    t.add("stale_write", group="A", epoch=1, status="rejected")
    t.add("crash", group="A", replica="A1")
    t.add("restart", group="A", replica="A1")
    t.add("recover", group="A", prepared=[], decisions=[], installed=["t1", "t2"], floor=30, epoch=2)
    t.read("old", {"A/node/1": {"label": "old", "life": 1}, "B/index/old/1": 1})
    t.add("expire", cut="old")
    t.add("read", mode="At", status="expired", cut="old", scope=["A", "B"])
    return h


def allocator_trace():
    h = history("P08-reservation-uncertainty-restart-stale-allocator")
    t = Trace(h)
    t.durable("reserve", "A", id="r1", epoch=1, lo=1, hi=10)
    t.add("mint", group="A", epoch=1, reservation="r1", value=1, status="issued")
    t.add("crash", group="A", replica="A1")
    t.add("restart", group="A", replica="A1")
    t.durable("reserve", "A", id="r2", epoch=1, lo=10, hi=20)
    t.add("mint", group="A", epoch=1, reservation="r2", value=10, status="issued")
    t.durable("transfer", "A", old_epoch=1, new_epoch=2, retained_lineage=True)
    t.add("mint", group="A", epoch=1, reservation="r1", value=2, status="rejected")
    t.add("mint", group="A", epoch=2, reservation="r2", value=11, status="rejected")
    return h


def serial_two_trace():
    s1 = transaction({"A/x": put(1)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 0}], coordinator="A")
    s2 = transaction({"A/x": put(2)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 1, "revision": "t1"}], coordinator="A", historical=True)
    s2["request_key"] = "second"
    h = history("two-conflicting-transactions-revalidated-history", {"A/x": 0}, {"t1": s1, "t2": s2})
    t = Trace(h)
    for tx, rnd in (("t1", 10), ("t2", 20)):
        t.register(tx)
        t.add("validate", tx=tx, reads=h["transactions"][tx]["reads"])
        t.prepare(tx, "A", floor=0 if tx == "t1" else 10)
        t.decision(tx, round=rnd)
        t.install(tx, "A")
        t.ack(tx, round=rnd)
    t.fence("A", 30)
    t.cert("c30", 30, scope=["A"])
    t.read("c30", {"A/x": 2}, scope=["A"])
    return h


def empty_range_write_skew():
    r1 = {"kind": "range", "lo": "B/unique/", "hi": "B/unique0", "values": {}}
    r2 = {"kind": "range", "lo": "A/unique/", "hi": "A/unique0", "values": {}}
    s1 = transaction({"A/unique/1": put(1)}, [r1], participants=["A", "B"])
    s2 = transaction({"B/unique/2": put(2)}, [r2], participants=["A", "B"])
    s2["request_key"] = "second"
    h = history("weak-ignore-empty-range-locks-write-skew", txs={"t1": s1, "t2": s2})
    t = Trace(h)
    for tx in ("t1", "t2"):
        t.register(tx)
        for g in ("A", "B"):
            t.prepare(tx, g)
    for tx, rnd in (("t1", 10), ("t2", 20)):
        t.decision(tx, round=rnd)
        for g in ("A", "B"):
            t.install(tx, g)
        t.ack(tx, round=rnd)
    t.add("final", scope=["A", "B"], values={"A/unique/1": 1, "B/unique/2": 2})
    return h


def ownership_move_trace():
    first = transaction({"A/node/1": put({"label": "old", "life": 1})}, coordinator="A")
    second = transaction({"A/node/1": put({"label": "new", "life": 1})}, reads=[{"kind": "key", "key": "A/node/1", "exists": True, "value": {"label": "old", "life": 1}, "revision": "t1"}], participants=["B"], coordinator="B")
    second["epochs"] = {"A": 2}
    second["request_key"] = "second"
    h = history("E17-stable-bucket-A-moves-to-owner-B", txs={"t1": first, "t2": second})
    h["ownership"] = {"A": "A", "B": "B"}
    t = Trace(h)
    t.register("t1")
    t.prepare("t1", "A")
    t.decision("t1", round=10)
    t.install("t1", "A")
    result = t.ack("t1", round=10)
    t.fence("A", 20)
    t.cert("old", 20, scope=["A"])
    t.durable("transfer", "A", bucket="A", **{"from": "A", "to": "B"}, old_epoch=1, new_epoch=2, retained_lineage=True, target_replicas=["B1", "B2"], installed=["t1"], prepared=[], floor=20)
    t.positions["B"] += 1  # Target quorum handoff publication.
    t.owners["A"] = "B"
    t.add("stale_write", group="A", bucket="A", epoch=1, status="rejected")
    t.register("t2")
    t.prepare("t2", "B", floor=20)
    t.decision("t2", round=30)
    t.install("t2", "B")
    t.ack("t2", round=30)
    t.add("replay", request_key="request", digest=digest(first), status="known", result=result)
    t.fence("B", 40)
    t.cert("new", 40, scope=["A"], topology=2)
    t.read("new", {"A/node/1": {"label": "new", "life": 1}}, scope=["A"], topology=2)
    t.read("old", {"A/node/1": {"label": "old", "life": 1}}, scope=["A"], topology=1)
    return h


def physical_lock_trace():
    first = transaction({"A/a": put(1), "B/x": put(1)})
    second = transaction({"A/b": put(2), "B/x": put(2)}, reads=[{"kind": "key", "key": "B/x", "exists": True, "value": 1, "revision": "t1"}])
    second["request_key"] = "second"
    h = history("fine-grained-A-prepares-concurrent-B-serializes", {"B/x": 0}, {"t1": first, "t2": second})
    t = Trace(h)
    t.register("t1")
    t.prepare("t1", "A")
    t.prepare("t1", "B")
    t.decision("t1", round=10)
    t.install("t1", "B")
    # A still retains t1's disjoint mutation lock; B's dependency is resolved.
    t.register("t2")
    t.prepare("t2", "A")
    t.prepare("t2", "B", floor=10)
    t.decision("t2", round=20)
    t.install("t1", "A")
    t.install("t2", "A")
    t.install("t2", "B")
    t.add("final", scope=["A", "B"], values={"A/a": 1, "A/b": 2, "B/x": 2})
    return h


def fresh_unacked_trace():
    first = transaction({"A/x": put(1)}, coordinator="A")
    second = transaction({"A/x": put(2), "B/y": put(2)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 1}], coordinator="B")
    second["request_key"] = "second"
    h = history("Fresh-includes-unacked-outside-scope-decision", txs={"t1": first, "t2": second})
    t = Trace(h)
    t.register("t1")
    t.prepare("t1", "A")
    t.decision("t1", round=10)
    t.install("t1", "A")
    t.fence("A", 20)
    t.cert("old", 20, scope=["A"])
    t.register("t2")
    t.prepare("t2", "A", floor=20)
    t.prepare("t2", "B")
    t.decision("t2", round=30)
    t.add("drop", message="decision-reply-to-client")
    t.add("read_begin", id="r", scope=["A"])
    t.add("collect", id="collect", request="r", scope=["A"], available=True, quorums={"A": ["A1", "A2"], "B": ["B1", "B2"]})
    t.install("t2", "A")
    t.fence("A", 40)
    t.cert("new", 40, scope=["A"])
    t.read("new", {"A/x": 2}, scope=["A"], mode="Fresh", collection="collect")
    return h


def active_transfer_trace():
    h = history("unsupported-active-intent-transfer", txs={"t1": transaction({"A/x": put(1)}, coordinator="A")})
    t = Trace(h)
    t.register("t1")
    t.prepare("t1", "A")
    t.durable("transfer", "A", bucket="A", **{"from": "A", "to": "B"}, old_epoch=1, new_epoch=2, retained_lineage=True, target_replicas=["B1", "B2"], installed=[], prepared=["t1"], floor=0)
    return h


def durable_recovery_trace(outcome="commit", acknowledged=False):
    name = "durable-" + ("acked-" if acknowledged else "unacked-") + outcome + "-survives-restart"
    h = history(name, txs={"t": transaction({"A/x": put(1), "B/y": put(1)})})
    t = Trace(h)
    t.register("t")
    t.prepare("t", "A")
    t.prepare("t", "B")
    t.decision("t", outcome, round=10)
    if acknowledged:
        t.ack("t", outcome, round=10)
    t.add("crash", group="B", replica="B1")
    t.add("restart", group="B", replica="B1")
    t.add("recover", group="B", prepared=["t"], decisions=["t"], installed=[], floor=0, epoch=1)
    return h


def local_prepared_lock_trace(local_before_resolution=False):
    a = transaction({"A/x": put(1), "B/y": put(1)})
    b = transaction({"A/x": put(2)}, coordinator="A")
    b["request_key"] = "local"
    h = history("local-commit-after-prepared-abort-resolution", {"A/x": 0}, {"prepared": a, "local": b})
    t = Trace(h)
    t.register("prepared")
    t.prepare("prepared", "A")
    t.prepare("prepared", "B")
    if local_before_resolution:
        t.durable("local_commit", tx="local", epoch=1, round=5)
    t.decision("prepared", "abort")
    t.install("prepared", "A", aborted=True)
    t.install("prepared", "B", aborted=True)
    if not local_before_resolution:
        t.durable("local_commit", tx="local", epoch=1, round=5)
    t.add("final", scope=["A", "B"], values={"A/x": 2})
    return h


def local_bucket_epoch_trace():
    h = history("local-after-move-original-bucket-epoch")
    t = Trace(h)
    t.durable("transfer", "A", bucket="A", **{"from": "A", "to": "B"},
              old_epoch=1, new_epoch=2, retained_lineage=True,
              target_replicas=["B1", "B2"], installed=[], prepared=[], floor=0)
    t.positions["B"] += 1  # Target quorum handoff is a durable transition.
    s = transaction({"A/x": put(1)}, participants=["B"], coordinator="B")
    s["epochs"] = {"A": 2}
    h["transactions"]["local"] = s
    t.durable("local_commit", tx="local", epoch=1, round=5)
    t.add("final", scope=["A"], values={"A/x": 1})
    return h


def local_read_footprint_trace(kind="key"):
    read = {"kind": "key", "key": "B/y", "exists": True, "value": 0} if kind == "key" else {
        "kind": "range", "lo": "A/", "hi": "B0", "values": {"B/y": 0}}
    s = transaction({"A/x": put(1)}, reads=[read], participants=["A"], coordinator="A")
    s["epochs"] = {"A": 1, "B": 2}
    h = history("local-full-" + kind + "-read-bucket-footprint", {"B/y": 0}, {"local": s})
    h["ownership"] = {"A": "A", "B": "A"}
    h["epochs"]["B"] = 2
    t = Trace(h)
    t.durable("local_commit", tx="local", epoch=1, round=5)
    t.add("final", scope=["A", "B"], values={"A/x": 1, "B/y": 0})
    return h


def local_historical_trace(validated=True):
    s = transaction({"A/x": put(1)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 0}], coordinator="A", historical=True)
    h = history("local-historical-write-revalidated", {"A/x": 0}, {"local": s})
    t = Trace(h)
    if validated:
        t.add("validate", tx="local", reads=s["reads"])
    t.durable("local_commit", tx="local", epoch=1, round=5)
    t.add("final", scope=["A"], values={"A/x": 1})
    return h


def corpus():
    positives = [cross_trace(), unresolved_trace(), local_and_abort_trace(), historical_trace(), allocator_trace(), serial_two_trace(), ownership_move_trace(), physical_lock_trace(), fresh_unacked_trace()]
    negatives = []

    def weak(base, name, mutate, code):
        h = copy.deepcopy(base)
        h["name"] = name
        mutate(h)
        negatives.append((h, code))

    cross, unresolved, local, historical, allocator, serial = positives[:6]
    weak(cross, "weak-mixed-graph-and-endpoint-postings", lambda h: next(e for e in h["events"] if e["kind"] == "read" and e.get("status") == "ok")["values"].pop("B/in/2/r"), "nonserializable_or_wrong_snapshot")
    weak(cross, "weak-acknowledged-decision-lost-after-restart", lambda h: next(e for e in h["events"] if e["kind"] == "recover" and e["group"] == "B")["decisions"].clear(), "lost_acknowledged_decision")
    weak(cross, "weak-wrong-request-key-replay-ids", lambda h: next(e for e in h["events"] if e["kind"] == "replay" and e["status"] == "known")["result"]["ids"].append(999), "replay_binding")
    weak(cross, "weak-one-replica-durability-ack", lambda h: next(e for e in h["events"] if e["kind"] == "decision").update(replicas=["B1"]), "quorum")
    weak(historical, "weak-current-index-substituted-for-old-cut", lambda h: next(e for e in h["events"] if e["kind"] == "read" and e.get("status") == "ok").update(values={"A/node/1": {"label": "new", "life": 1}, "B/index/new/1": 1}), "nonserializable_or_wrong_snapshot")
    weak(historical, "weak-stale-owner-accepts-delayed-write", lambda h: next(e for e in h["events"] if e["kind"] == "stale_write").update(status="accepted"), "stale_owner_accepted")
    weak(allocator, "weak-recycle-id-after-crash", lambda h: next(e for e in h["events"] if e["kind"] == "mint" and e["value"] == 10).update(value=1), "invalid_id_issuance")
    weak(allocator, "weak-old-allocator-issues-after-transfer", lambda h: next(e for e in h["events"] if e["kind"] == "mint" and e["value"] == 2).update(status="issued"), "invalid_id_issuance")
    weak(local, "weak-aborted-private-write-visible", lambda h: next(e for e in h["events"] if e["kind"] == "read")["values"].update({"A/phantom": 99}), "nonserializable_or_wrong_snapshot")
    weak(serial, "weak-conflicting-transactions-share-round", lambda h: [e.update(round=10) for e in h["events"] if e["kind"] == "decision" and e["tx"] == "t2"], "nonserializable_or_wrong_snapshot")
    weak(serial, "weak-historical-write-without-revalidation", lambda h: h["events"].__setitem__(slice(None), [e for e in h["events"] if not (e["kind"] == "validate" and e["tx"] == "t2")]), "historical_write_unvalidated")
    weak(serial, "weak-prepare-before-delayed-registration", lambda h: h["events"].__setitem__(slice(0, 3), [h["events"][2], h["events"][1], h["events"][0]]), "prepare_before_registration")
    weak(serial, "weak-certificate-without-complete-capture", lambda h: h["observation"].update(durable_events_complete=False), "incomplete_history")
    weak(historical, "weak-scope-expansion-and-stale-topology", lambda h: next(e for e in h["events"] if e["kind"] == "read" and e.get("status") == "ok").update(topology=2, scope=["A"]), "scope_expansion")

    # Certify while the old prepare can still choose round90. Coordinator B is
    # outside requested A scope, so A's applied high-water cannot close it.
    bad = copy.deepcopy(unresolved)
    bad["name"] = "weak-ignore-undecided-coordinator-outside-scope"
    prefix = bad["events"][:5]
    bad["events"] = prefix
    t = Trace(bad)
    t.positions = {"A": 2, "B": 3}
    t.pending = {"A": {"t1"}, "B": {"t1"}}
    t.fences = {"A": 3, "B": 4}
    t.cert("bad", 100, scope=["A"])
    t.read("bad", {"A/x": 0}, scope=["A"], mode="Stable")
    negatives.append((bad, "unresolved_prepare"))

    negatives.append((empty_range_write_skew(), "nonserializable_or_wrong_snapshot"))
    skew = empty_range_write_skew()
    skew["name"] = "weak-absent-key-read-locks-write-skew"
    skew["transactions"]["t1"]["reads"] = [{"kind": "key", "key": "B/unique/2", "exists": False, "value": None}]
    skew["transactions"]["t2"]["reads"] = [{"kind": "key", "key": "A/unique/1", "exists": False, "value": None}]
    for e in skew["events"]:
        if e["kind"] in ("register", "prepare"):
            e["digest"] = digest(skew["transactions"][e["tx"]])
    negatives.append((skew, "nonserializable_or_wrong_snapshot"))

    late = history("weak-post-fence-prepare-votes-stale-floor", {"A/x": 0}, {"t1": transaction({"A/x": put(1)}, coordinator="A")})
    t = Trace(late)
    t.register("t1")
    t.fence("A", 100)
    t.cert("before", 100, scope=["A"])
    t.read("before", {"A/x": 0}, scope=["A"])
    t.prepare("t1", "A", floor=90)
    t.decision("t1", round=95)
    t.install("t1", "A")
    negatives.append((late, "stale_floor"))

    split = copy.deepcopy(cross)
    split["name"] = "weak-timeout-overwrites-durable-commit"
    first_decision = next(i for i, e in enumerate(split["events"]) if e["kind"] == "decision")
    second = copy.deepcopy(split["events"][first_decision])
    second.update(outcome="abort", round=None)
    split["events"].insert(first_decision + 1, second)
    negatives.append((split, "decision_change"))
    move, _, fresh = positives[6:]
    weak(move, "weak-new-tx-routed-to-old-owner-after-move", lambda h: h["transactions"]["t2"].update(participants=["A"]), "registration_routing")
    weak(move, "weak-new-cut-served-with-old-owner-coverage", lambda h: next(e for e in h["events"] if e["kind"] == "certify" and e["cut"]["id"] == "new")["cut"].update(owners={"A": "A"}), "topology")
    weak(move, "weak-move-discards-retained-history", lambda h: next(e for e in h["events"] if e["kind"] == "transfer").update(installed=[]), "incomplete_handoff")
    weak(fresh, "weak-Fresh-reuses-old-cut-before-unacked-decision", lambda h: next(e for e in h["events"] if e["kind"] == "read").update(cut="old", values={"A/x": 1}), "fresh_missing_durable_decision")
    weak(fresh, "weak-Fresh-ignores-outside-scope-coordinator", lambda h: next(e for e in h["events"] if e["kind"] == "collect")["quorums"].pop("B"), "incomplete_authoritative_collection")
    weak(move, "weak-current-read-served-by-retired-owner", lambda h: next(e for e in h["events"] if e["kind"] == "read" and e["cut"] == "new").update(served_by={"A": "A"}), "scope_expansion")
    negatives.append((active_transfer_trace(), "unsupported_active_intent_transfer"))
    # Review corrections: every refusal has a matching complete/authorized
    # counterpart. These remain declared histories, not engine fault evidence.
    for outcome in ("commit", "abort"):
        for acknowledged in (False, True):
            good = durable_recovery_trace(outcome, acknowledged)
            positives.append(good)
            weak(good, "weak-" + good["name"].replace("survives", "lost"),
                 lambda h: h["events"][-1].update(decisions=[]), "lost_durable_decision")
    positives.append(local_prepared_lock_trace())
    bad = local_prepared_lock_trace(True)
    bad["name"] = "weak-local-commit-bypasses-prepared-lock"
    negatives.append((bad, "prepared_conflict"))
    good = local_bucket_epoch_trace()
    positives.append(good)
    weak(good, "weak-local-after-move-stale-logical-bucket-epoch",
         lambda h: h["transactions"]["local"].update(epochs={"A": 1}), "local_routing")
    for kind in ("key", "range"):
        good = local_read_footprint_trace(kind)
        positives.append(good)
        weak(good, "weak-local-" + kind + "-read-routed-outside-single-participant",
             lambda h: h["ownership"].update(B="B"), "local_routing")
        weak(good, "weak-local-" + kind + "-read-missing-bucket-epoch",
             lambda h: h["transactions"]["local"]["epochs"].pop("B"), "local_routing")
        weak(good, "weak-local-" + kind + "-read-stale-bucket-epoch",
             lambda h: h["transactions"]["local"]["epochs"].update(B=1), "local_routing")
    positives.append(local_historical_trace())
    bad = local_historical_trace(False)
    bad["name"] = "weak-local-historical-write-without-revalidation"
    negatives.append((bad, "historical_write_unvalidated"))
    return positives, negatives


def interleavings():
    """Two disjoint transactions commute at same round; arbitrary delivery labels.

    Enumerate register->prepare->decision->install order for each chain, keeping
    all 70 valid interleavings of 8 durable transitions. Neither chain's round
    is required to follow client wall-clock order.
    """
    outcomes = []
    for schedule in itertools.permutations(range(8)):
        if any(schedule.index(i) > schedule.index(i + 1) for i in (0, 1, 2, 4, 5, 6)):
            continue
        s1 = transaction({"A/x": put(1)}, coordinator="A")
        s2 = transaction({"B/y": put(2)}, coordinator="B")
        s2["request_key"] = "second"
        h = history("independent-same-round-" + "".join(map(str, schedule)), txs={"t1": s1, "t2": s2})
        t = Trace(h)
        for action in schedule:
            tx, g = ("t1", "A") if action < 4 else ("t2", "B")
            step = action % 4
            if step == 0: t.register(tx)
            elif step == 1: t.prepare(tx, g)
            elif step == 2: t.decision(tx, round=10)
            else: t.install(tx, g)
            t.add("duplicate" if action % 2 else "deliver", message=f"message-{action}")
        t.add("final", scope=["A", "B"], values={"A/x": 1, "B/y": 2})
        outcomes.append(h)
    return outcomes


def main():
    positives, negatives = corpus()
    schedules = interleavings()
    folder = ROOT / "histories"
    folder.mkdir(exist_ok=True)
    reports = []
    for h, expected, code in [(h, True, None) for h in positives + schedules] + [(h, False, code) for h, code in negatives]:
        result = check(h)
        if result["accepted"] != expected or code and code not in {v["code"] for v in result["violations"]}:
            raise AssertionError(json.dumps(result, indent=2))
        (folder / (h["name"] + ".json")).write_text(json.dumps(h, indent=2, sort_keys=True) + "\n")
        reports.append({"name": h["name"], "expected_accept": expected, "required_counterexample": code, "result": result})
    summary = {"schema_version": 1, "positive_named_histories": len(positives), "positive_interleavings": len(schedules), "weakened_variants": len(negatives), "all_expected_outcomes_matched": True, "scope": "independent bounded declarative oracle; not production, Raft, fsync, process-kill or V2 acceptance evidence", "reports": reports}
    (ROOT / "results.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "reports"}, indent=2))


if __name__ == "__main__":
    main()
