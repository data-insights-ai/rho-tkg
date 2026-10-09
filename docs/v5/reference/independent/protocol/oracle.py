"""Independent bounded history oracle; no production reducer/Raft dependencies."""
from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
from pathlib import Path

SCHEMA = 1
MAX_TRANSACTIONS = 8


def digest(spec):
    """Immutable request payload; TxID and returned IDs are not client payload."""
    payload = {k: spec[k] for k in ("participants", "coordinator", "epochs", "reads", "writes", "historical") if k in spec}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def group(key):
    return key.split("/", 1)[0]


def observe(state, revisions, read):
    if read["kind"] == "key":
        key = read["key"]
        # Explicit exists distinguishes absent from a stored JSON null.
        result = {"exists": key in state, "value": state.get(key)}
        if "revision" in read:
            result["revision"] = revisions.get(key)
        return result
    if read["kind"] == "range":
        selected = {k: state[k] for k in sorted(state) if read["lo"] <= k < read["hi"]}
        result = {"values": selected}
        if "revisions" in read:
            result["revisions"] = {k: revisions[k] for k in selected}
        return result
    raise ValueError("unsupported read kind")


def expected_read(read):
    return {k: read[k] for k in ("exists", "value", "revision", "values", "revisions") if k in read}


def apply(state, revisions, txid, spec):
    for key, mutation in spec["writes"].items():
        if mutation["op"] == "delete":
            state.pop(key, None)
            revisions.pop(key, None)
        elif mutation["op"] == "put":
            state[key] = copy.deepcopy(mutation["value"])
            revisions[key] = txid
        else:
            raise ValueError("unsupported mutation")


def touches(read, key):
    return read["key"] == key if read["kind"] == "key" else read["lo"] <= key < read["hi"]


def conflict(left, right):
    lw, rw = set(left["writes"]), set(right["writes"])
    return bool(lw & rw) or any(touches(r, k) for r in left["reads"] for k in rw) or any(touches(r, k) for r in right["reads"] for k in lw)


def projected(state, scope):
    return {k: v for k, v in sorted(state.items()) if group(k) in scope}


def serial_witness(history, committed, rounds, observations, finals):
    """Search exact map semantics, independent of prepare/floor/fence guards.

    No real-time edges are inferred from transaction begin/end/ack order. Only
    client observations and strict rounds for actual conflicting dependencies
    constrain the serial order. Fresh snapshot inclusion is checked separately.
    """
    transactions = history["transactions"]
    if len(committed) > MAX_TRANSACTIONS:
        return None, {"reason": "bounded oracle limit", "limit": MAX_TRANSACTIONS}
    tried = 0
    first_failure = None
    for order in itertools.permutations(sorted(committed)):
        tried += 1
        state = copy.deepcopy(history["initial"])
        revisions = {k: "base" for k in state}
        mismatch = None
        for i, tx in enumerate(order):
            spec = transactions[tx]
            for read in spec["reads"]:
                actual = observe(state, revisions, read)
                if actual != expected_read(read):
                    mismatch = {"transaction": tx, "read": read, "serial_state_observation": actual}
                    break
            if mismatch:
                break
            for earlier in order[:i]:
                if conflict(transactions[earlier], spec) and rounds[earlier] >= rounds[tx]:
                    mismatch = {"dependency": [earlier, tx], "rounds": [rounds[earlier], rounds[tx]]}
                    break
            if mismatch:
                break
            apply(state, revisions, tx, spec)
        if not mismatch:
            for observation in observations:
                expected = copy.deepcopy(history["initial"])
                rev = {k: "base" for k in expected}
                # Whole transactions are included; application lag never changes
                # mathematical snapshot contents.
                for tx in order:
                    if rounds[tx] <= observation["round"]:
                        apply(expected, rev, tx, transactions[tx])
                want = projected(expected, observation["scope"])
                if want != observation["values"]:
                    mismatch = {"read_event": observation["event"], "expected": want, "actual": observation["values"]}
                    break
        if not mismatch:
            for final in finals:
                want = projected(state, final["scope"])
                if want != final["values"]:
                    mismatch = {"final_event": final["event"], "expected": want, "actual": final["values"]}
                    break
        if not mismatch:
            return list(order), {"orders_tried": tried}
        first_failure = first_failure or mismatch
    return None, {"orders_tried": tried, "first_mismatch": first_failure}


class Evidence:
    """Checks stated protocol evidence; does not manufacture state/read answers.

    Durable-event completeness is a declared capture assumption, not inferred
    from a missing event. Replica receipts are claims to validate against real
    adapters later; this file neither performs fsync nor simulates Raft.
    """
    def __init__(self, history):
        self.h = history
        self.tx = history["transactions"]
        self.groups = history["groups"]
        self.live = {g: set(ids) for g, ids in self.groups.items()}
        self.epochs = dict(history["epochs"])
        self.topology = history["topology"]
        self.ownership = {self.topology: dict(history.get("ownership", {g: g for g in self.groups}))}
        self.tx_owners = {}
        self.registered = {}
        self.validated = set()
        self.prepared = {}
        self.decisions = {}
        self.installed = set()
        self.floors = {g: 0 for g in self.groups}
        self.fences = {g: 0 for g in self.groups}
        self.fence_ordinals = {}
        self.positions = {g: 0 for g in self.groups}
        self.records = {r: set() for ids in self.groups.values() for r in ids}
        self.acked = {}
        self.collections = {}
        self.begins = {}
        self.cuts = {}
        self.expired = set()
        self.reservations = []
        self.issued = set()
        self.observations = []
        self.finals = []
        self.violations = []
        self.ordinal = -1

    def fail(self, code, detail):
        self.violations.append({"event": self.ordinal, "code": code, "detail": detail})

    def quorum(self, event, g, record):
        ids = event.get("replicas", [])
        if len(ids) != len(set(ids)) or not set(ids) <= self.live[g] or len(ids) < len(self.groups[g]) // 2 + 1:
            self.fail("quorum", {"group": g, "replicas": ids, "reachable": sorted(self.live[g])})
            return False
        for replica in ids:
            self.records[replica].add(record)
        position = event.get("position", self.positions[g] + 1)
        if not isinstance(position, int) or position < self.positions[g]:
            self.fail("position", {"group": g, "position": position})
        self.positions[g] = max(position, self.positions[g])
        return True

    def epoch(self, event, g):
        if event.get("epoch") != self.epochs[g]:
            self.fail("stale_epoch", {"group": g, "current": self.epochs[g], "provided": event.get("epoch")})
            return False
        return True

    def read_buckets(self, read):
        if read["kind"] == "key":
            return {group(read["key"])}
        if read["kind"] != "range":
            raise ValueError("unsupported read kind")
        if read["lo"] >= read["hi"]:
            return set()
        # Keys in logical bucket b have prefix b+"/", exactly the lexical
        # interval [b+"/", b+"0"). A range can cover multiple empty buckets;
        # routing by only its lower endpoint would omit those dependencies.
        return {b for b in self.ownership[self.topology]
                if read["lo"] < b + "0" and b + "/" < read["hi"]}

    def transaction_routing(self, tx, code):
        spec = self.tx[tx]
        buckets = {group(k) for k in spec["writes"]}
        for read in spec["reads"]:
            buckets.update(self.read_buckets(read))
        owners = self.ownership[self.topology]
        routing = {b: owners[b] for b in buckets if b in owners}
        if (not buckets <= owners.keys()
                or set(spec["participants"]) != set(routing.values())
                or set(spec["epochs"]) != buckets
                or any(spec["epochs"][b] != self.epochs[b] for b in buckets)):
            self.fail(code, {"tx": tx, "owners": routing,
                             "participants": spec["participants"], "epochs": spec["epochs"]})
        return routing

    def historical_validation(self, tx):
        if self.tx[tx].get("historical") and tx not in self.validated:
            self.fail("historical_write_unvalidated", tx)

    def prepared_lock_conflicts(self, tx, physical):
        for other, owner in self.prepared:
            if owner == physical and other != tx and (other, owner) not in self.installed:
                if self.lock_conflict(tx, other, physical):
                    self.fail("prepared_conflict", {"tx": tx, "holds_locks": other, "group": physical})

    def lock_conflict(self, left, right, physical):
        def local(tx):
            spec = self.tx[tx]
            owners = self.tx_owners.get(tx, self.ownership[self.topology])
            return {"writes": {k: v for k, v in spec["writes"].items() if owners.get(group(k), group(k)) == physical}, "reads": [r for r in spec["reads"] if any(owners.get(b) == physical for b in self.read_buckets(r))]}
        return conflict(local(left), local(right))

    def binding(self, tx):
        return {"tx": tx, "digest": digest(self.tx[tx]), "decision": self.decisions[tx]["outcome"], "round": self.decisions[tx].get("round"), "ids": self.tx[tx].get("ids", [])}

    def committed(self):
        return {tx for tx, decision in self.decisions.items() if decision["outcome"] == "commit"}

    def certificate(self, event):
        cut = event["cut"]
        scope = set(cut["scope"])
        owners = self.ownership[self.topology]
        physical = {owners[b] for b in scope}
        bound = cut["round"]
        if not scope or not scope <= owners.keys() or cut["topology"] != self.topology or cut.get("owners") != {b: owners[b] for b in scope}:
            self.fail("topology", cut)
            return
        complete = self.h.get("observation", {})
        if complete.get("durable_events_complete") is not True or set(complete.get("groups", [])) != set(self.groups):
            self.fail("incomplete_history", "capture must explicitly include every accepted durable event in declared groups")
        for g in physical:
            coverage = event.get("coverage", {}).get(g, {})
            # A capture watermark plus complete inventory is an explicit input
            # assertion; its absence can never prove closure.
            actual_prepares = sorted(tx for tx, p in self.prepared if p == g and (tx, g) not in self.installed)
            if coverage.get("complete") is not True or coverage.get("through") != self.positions[g] or coverage.get("prepared") != actual_prepares:
                self.fail("incomplete_inventory", {"group": g, "expected_pending": actual_prepares, "coverage": coverage})
            if event["fence_events"].get(g) != self.fence_ordinals.get(g):
                self.fail("fence_evidence", {"group": g, "provided": event["fence_events"].get(g), "actual": self.fence_ordinals.get(g)})
            if self.fences[g] < bound:
                self.fail("missing_fence", {"group": g, "bound": bound})
            for (tx, p), vote in self.prepared.items():
                if p != g or vote["at"] >= event["fence_events"].get(g, -1):
                    continue
                decision = self.decisions.get(tx)
                if decision is None:
                    self.fail("unresolved_prepare", {"tx": tx, "group": g, "coordinator": self.tx[tx]["coordinator"]})
                elif decision["outcome"] == "abort" and (tx, g) not in self.installed:
                    self.fail("unremoved_abort", {"tx": tx, "group": g})
                elif decision["outcome"] == "commit" and decision["round"] <= bound and (tx, g) not in self.installed:
                    self.fail("uninstalled_commit", {"tx": tx, "group": g})
            for tx in self.committed():
                if self.decisions[tx]["round"] <= bound and any(owners[group(k)] == g and group(k) in scope for k in self.tx[tx]["writes"]) and (tx, g) not in self.installed:
                    self.fail("uninstalled_commit", {"tx": tx, "group": g})
            replicas = event.get("durable", {}).get(g, [])
            self.quorum({"replicas": replicas, "position": self.positions[g]}, g, "cut:" + cut["id"])
        if cut["id"] in self.cuts and any(self.cuts[cut["id"]].get(k) != v for k, v in cut.items()):
            self.fail("cut_redefinition", cut)
        self.cuts[cut["id"]] = {**copy.deepcopy(cut), "published_at": self.ordinal, "fence_events": copy.deepcopy(event["fence_events"])}

    def event(self, event):
        kind = event["kind"]
        tx = event.get("tx")
        spec = self.tx.get(tx)
        if kind == "network":
            g = event["group"]
            if not set(event["reachable"]) <= set(self.groups[g]):
                self.fail("unknown_replica", event)
            self.live[g] = set(event["reachable"])
        elif kind == "crash":
            self.live[event["group"]].discard(event["replica"])
            # Durable copies survive; no fabricated rollback/abort follows crash.
        elif kind == "restart":
            self.live[event["group"]].add(event["replica"])
        elif kind in ("enqueue", "drop", "deliver", "duplicate", "timeout", "unknown_reply"):
            # Transport labels carry schedule evidence, no durable effects.
            pass
        elif kind == "register":
            g = spec["coordinator"]
            routing = self.transaction_routing(tx, "registration_routing")
            if sorted(event["participants"]) != sorted(spec["participants"]) or event["digest"] != digest(spec) or event["epochs"] != spec["epochs"]:
                self.fail("registration_binding", event)
            previous = self.registered.get(tx)
            binding = (digest(spec), tuple(sorted(spec["participants"])), spec["epochs"])
            if previous is not None and previous != binding:
                self.fail("participant_change", tx)
            if self.quorum(event, g, "register:" + tx):
                self.registered[tx] = binding
                self.tx_owners[tx] = routing
        elif kind == "validate":
            if event["reads"] != spec["reads"]:
                self.fail("read_footprint", tx)
            self.validated.add(tx)
        elif kind == "prepare":
            g = event["group"]
            if tx not in self.registered:
                self.fail("prepare_before_registration", tx)
            if g not in spec["participants"] or event["digest"] != digest(spec) or (event.get("epochs") != spec["epochs"] if "epochs" in event else event.get("epoch") != spec["epochs"].get(g)):
                self.fail("prepare_binding", event)
            if "epochs" in event:
                if any(event["epochs"].get(b) != self.epochs[b] for b in spec["epochs"]):
                    self.fail("stale_epoch", event)
            else:
                self.epoch(event, g)
            self.historical_validation(tx)
            if event["floor"] < self.floors[g]:
                self.fail("stale_floor", {"tx": tx, "group": g, "floor": event["floor"], "current": self.floors[g]})
            vote = {"floor": event["floor"], "digest": event["digest"], "at": self.ordinal}
            prior = self.prepared.get((tx, g))
            if prior and any(prior[k] != vote[k] for k in ("floor", "digest")):
                self.fail("prepare_change", event)
            # Shared read locks + exclusive mutation locks including empty ranges.
            self.prepared_lock_conflicts(tx, g)
            if self.quorum(event, g, "prepare:" + tx):
                self.prepared.setdefault((tx, g), vote)
        elif kind in ("decision", "local_commit"):
            g = spec["coordinator"]
            decision = {"outcome": event.get("outcome", "commit"), "round": event.get("round")}
            prior = self.decisions.get(tx)
            if prior and prior != decision:
                self.fail("decision_change", {"tx": tx, "previous": prior, "new": decision})
            if kind == "local_commit":
                routing = self.transaction_routing(tx, "local_routing")
                if spec["participants"] != [g] or any(owner != g for owner in routing.values()):
                    self.fail("not_local", tx)
                self.epoch(event, g)
                self.historical_validation(tx)
                self.prepared_lock_conflicts(tx, g)
                if decision["round"] <= self.floors[g]:
                    self.fail("stale_floor", tx)
            elif tx not in self.registered:
                self.fail("decision_without_registration", tx)
            elif decision["outcome"] == "commit":
                for p in spec["participants"]:
                    vote = self.prepared.get((tx, p))
                    if vote is None or vote["digest"] != digest(spec):
                        self.fail("missing_prepare", {"tx": tx, "group": p})
                    elif decision["round"] <= vote["floor"]:
                        self.fail("decision_floor", {"tx": tx, "group": p, "round": decision["round"], "floor": vote["floor"]})
            for prior_tx in self.decisions:
                if prior_tx != tx and self.tx[prior_tx].get("request_key") == spec.get("request_key"):
                    self.fail("duplicate_request_effect", {"old": prior_tx, "new": tx})
            if self.quorum(event, g, "decision:" + tx):
                self.decisions.setdefault(tx, decision)
                if kind == "local_commit":
                    self.tx_owners[tx] = routing
                    self.installed.add((tx, g))
                    self.floors[g] = max(self.floors[g], decision["round"])
        elif kind == "install":
            g = event["group"]
            decision = self.decisions.get(tx)
            if not decision:
                self.fail("install_without_decision", tx)
            expected = {k: v for k, v in spec["writes"].items() if self.tx_owners.get(tx, {}).get(group(k), group(k)) == g} if decision and decision["outcome"] == "commit" else {}
            if event["effects"] != expected:
                self.fail("partial_effects", {"tx": tx, "group": g, "expected": expected, "actual": event["effects"]})
            if self.quorum(event, g, "install:" + tx):
                self.installed.add((tx, g))
                if decision and decision["outcome"] == "commit":
                    self.floors[g] = max(self.floors[g], decision["round"])
        elif kind == "ack":
            if tx not in self.decisions:
                self.fail("ack_without_decision", tx)
            else:
                want = self.binding(tx)
                if event["result"] != want:
                    self.fail("ack_binding", {"expected": want, "actual": event["result"]})
                self.acked[tx] = {"at": self.ordinal, "result": want}
        elif kind == "replay":
            originals = [t for t, s in self.tx.items() if s.get("request_key") == event["request_key"] and t in self.decisions]
            if not originals:
                if event["status"] != "unknown":
                    self.fail("invented_replay", event)
            else:
                original = originals[0]
                if event["digest"] != digest(self.tx[original]):
                    if event["status"] != "conflict":
                        self.fail("request_key_conflict", event)
                elif event["status"] != "known" or event.get("result") != self.binding(original):
                    self.fail("replay_binding", event)
        elif kind == "fence":
            g = event["group"]
            self.epoch(event, g)
            if event["round"] < self.floors[g]:
                self.fail("regressing_fence", event)
            if self.quorum(event, g, "fence:" + str(event["round"])):
                self.fences[g] = self.floors[g] = max(self.floors[g], event["round"])
                self.fence_ordinals[g] = self.ordinal
        elif kind == "read_begin":
            self.begins[event["id"]] = {"at": self.ordinal, "scope": event["scope"]}
        elif kind == "collect":
            marks = {t: self.decisions[t]["round"] for t in self.committed() if {group(k) for k in self.tx[t]["writes"]} & set(event["scope"])}
            required_authorities = {self.ownership[self.topology][b] for b in event["scope"]} | {self.tx[t]["coordinator"] for t in marks}
            required_authorities |= {self.tx[t]["coordinator"] for t, p in self.prepared if {group(k) for k in self.tx[t]["writes"]} & set(event["scope"])}
            if event.get("available") and set(event.get("quorums", {})) != required_authorities:
                self.fail("incomplete_authoritative_collection", {"required": sorted(required_authorities), "provided": sorted(event.get("quorums", {}))})
            begin = self.begins.get(event.get("request"))
            if event.get("available") and (begin is None or begin["scope"] != event["scope"] or begin["at"] >= self.ordinal):
                self.fail("missing_fresh_request_start", event)
            self.collections[event["id"]] = {"at": self.ordinal, "scope": event["scope"], "topology": self.topology, "available": event.get("available", False), "marks": marks}
            if event.get("available"):
                for g in required_authorities:
                    self.quorum({"replicas": event.get("quorums", {}).get(g, []), "position": self.positions[g]}, g, "collect:" + event["id"])
        elif kind == "certify":
            self.certificate(event)
        elif kind == "read":
            mode, status = event["mode"], event["status"]
            if status in ("unavailable", "expired"):
                if status == "expired" and event.get("cut") not in self.expired:
                    self.fail("unjustified_expired", event)
                return
            if status != "ok" or event.get("cut") not in self.cuts:
                self.fail("uncertified_read", event)
                return
            cut = self.cuts[event["cut"]]
            if event["scope"] != cut["scope"] or event["topology"] != cut["topology"] or event.get("served_by") != cut["owners"]:
                self.fail("scope_expansion", event)
            if cut["id"] in self.expired:
                self.fail("expired_success", event)
            if mode == "Fresh":
                collection = self.collections.get(event.get("collection"))
                if collection is None or not collection["available"] or collection["scope"] != cut["scope"] or collection["topology"] != cut["topology"]:
                    self.fail("fresh_collection", event)
                else:
                    if cut["published_at"] <= collection["at"] or any(f <= collection["at"] for f in cut["fence_events"].values()):
                        self.fail("fresh_reuses_pre_request_barrier", {"cut": cut["id"], "collection_at": collection["at"]})
                    for t, round in collection["marks"].items():
                        if round > cut["round"]:
                            self.fail("fresh_missing_durable_decision", {"tx": t, "round": round, "cut_round": cut["round"]})
                    for t, ack in self.acked.items():
                        if ack["at"] < collection["at"] and {group(k) for k in self.tx[t]["writes"]} & set(cut["scope"]) and self.decisions[t]["outcome"] == "commit" and self.decisions[t]["round"] > cut["round"]:
                            self.fail("fresh_missing_ack", {"tx": t, "cut": cut})
            elif mode == "After":
                receipt = event["after"]
                if receipt not in self.acked or self.decisions[receipt]["round"] > cut["round"]:
                    self.fail("after_missing_receipt", event)
            elif mode not in ("Stable", "At"):
                self.fail("read_mode", mode)
            self.observations.append({"event": self.ordinal, "round": cut["round"], "scope": cut["scope"], "values": event["values"]})
        elif kind == "recover":
            g = event["group"]
            # Every quorum-durable decision survives, including an ABORT and a
            # COMMIT whose client reply was lost. No decision reclamation event
            # exists in this bounded schema; ack status cannot authorize loss.
            for decided_tx, decision in self.decisions.items():
                if self.tx[decided_tx]["coordinator"] == g and decided_tx not in event["decisions"]:
                    self.fail("lost_durable_decision", {"group": g, "tx": decided_tx,
                                                       "outcome": decision["outcome"]})
            for installed_tx, physical in self.installed:
                if physical == g and installed_tx not in event["installed"]:
                    self.fail("lost_installed_effects", {"group": g, "tx": installed_tx})
            for tx in self.acked:
                if self.tx[tx]["coordinator"] == g and tx not in event["decisions"]:
                    self.fail("lost_acknowledged_decision", {"group": g, "tx": tx})
                if (tx, g) in self.installed and tx not in event["installed"]:
                    self.fail("lost_installed_effects", {"group": g, "tx": tx})
            for (tx, p), _ in self.prepared.items():
                if p == g and (tx, g) not in self.installed and tx not in event["prepared"]:
                    self.fail("lost_prepared_locks", {"group": g, "tx": tx})
            if event["floor"] < self.floors[g] or event["epoch"] != self.epochs[g]:
                self.fail("lost_authority", event)
        elif kind == "final":
            self.finals.append({"event": self.ordinal, "scope": event["scope"], "values": event["values"]})
        elif kind == "transfer":
            g = event["group"]
            bucket = event.get("bucket", g)
            old_owner = self.ownership[self.topology][bucket]
            new_owner = event.get("to", old_owner)
            pending = sorted(tx for tx, p in self.prepared if p == old_owner and (tx, p) not in self.installed)
            if pending or event.get("prepared"):
                self.fail("unsupported_active_intent_transfer", {"observed_prepared": pending, "declared_prepared": event.get("prepared", []), "outstanding_requirement": "V2/V5 active-intent handoff semantics are unimplemented; resolve prepares before this artifact can check quiescent transfer"})
                return
            if event.get("from", old_owner) != old_owner or g != old_owner or new_owner not in self.groups:
                self.fail("transfer_routing", event)
            if event["old_epoch"] != self.epochs[bucket] or event["new_epoch"] <= self.epochs[bucket] or not event["retained_lineage"]:
                self.fail("transfer_authority", event)
            if self.quorum(event, g, "transfer:" + str(event["new_epoch"])):
                if new_owner != old_owner:
                    receipts = event.get("target_replicas", [])
                    self.quorum({"replicas": receipts}, new_owner, "handoff:" + bucket)
                    required = sorted(tx for tx, p in self.installed if p == old_owner and any(group(k) == bucket for k in self.tx[tx]["writes"]))
                    if event.get("installed") != required or event.get("prepared") != pending or event.get("floor", -1) < self.floors[old_owner]:
                        self.fail("incomplete_handoff", {"required_installed": required, "required_prepared": pending, "event": event})
                    for tx in required:
                        self.installed.add((tx, new_owner))
                    self.floors[new_owner] = max(self.floors[new_owner], self.floors[old_owner])
                self.epochs[bucket] = event["new_epoch"]
                owners = dict(self.ownership[self.topology])
                owners[bucket] = new_owner
                self.topology += 1
                self.ownership[self.topology] = owners
        elif kind == "stale_write":
            bucket = event.get("bucket", event["group"])
            if (event["epoch"] == self.epochs[bucket] and event["group"] == self.ownership[self.topology][bucket]) or event["status"] != "rejected":
                self.fail("stale_owner_accepted", event)
        elif kind == "reserve":
            g = event["group"]
            self.epoch(event, g)
            if event["lo"] < 1 or event["hi"] <= event["lo"] or event["hi"] > 2**64:
                self.fail("id_range", event)
            for reservation in self.reservations:
                if max(event["lo"], reservation["lo"]) < min(event["hi"], reservation["hi"]):
                    self.fail("overlapping_reservation", event)
            if self.quorum(event, g, "reserve:" + event["id"]):
                self.reservations.append(copy.deepcopy(event))
        elif kind == "mint":
            g = event["group"]
            valid = event["epoch"] == self.epochs[g] and any(r["id"] == event["reservation"] and r["group"] == g and r["epoch"] == event["epoch"] and r["lo"] <= event["value"] < r["hi"] for r in self.reservations) and event["value"] not in self.issued
            if event["status"] == "issued":
                if not valid:
                    self.fail("invalid_id_issuance", event)
                self.issued.add(event["value"])
            elif valid or event["status"] != "rejected":
                self.fail("mint_result", event)
        elif kind == "expire":
            self.expired.add(event["cut"])
        else:
            raise ValueError("unsupported event " + kind)


def check(history):
    if history.get("schema_version") != SCHEMA:
        raise ValueError("unsupported history schema")
    if len(history["transactions"]) > MAX_TRANSACTIONS:
        raise ValueError("bounded transaction limit exceeded")
    evidence = Evidence(history)
    for index, event in enumerate(history["events"]):
        evidence.ordinal = index
        evidence.event(event)
    committed = evidence.committed()
    rounds = {t: evidence.decisions[t]["round"] for t in committed}
    witness, detail = serial_witness(history, committed, rounds, evidence.observations, evidence.finals)
    if witness is None:
        evidence.fail("nonserializable_or_wrong_snapshot", detail)
    return {"schema_version": SCHEMA, "history": history["name"], "accepted": not evidence.violations, "serial_witness": witness, "search": detail, "violations": sorted(evidence.violations, key=lambda v: json.dumps(v, sort_keys=True)), "scope": "bounded declarative history validation; not implementation/consensus/disk proof"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("history", type=Path)
    args = parser.parse_args()
    result = check(json.loads(args.history.read_text()))
    print(json.dumps(result, indent=2, sort_keys=True))
    raise SystemExit(0 if result["accepted"] else 1)


if __name__ == "__main__":
    main()
