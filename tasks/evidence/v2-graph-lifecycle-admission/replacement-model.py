#!/usr/bin/env python3
"""Finite independent replacement model; not Raft/implementation conformance.

Three logical voters A/B/C; C loses its disk after voting for A in term 5.
A has a term-5 committed entry at index 8. B previously campaigned in term 5
with a term-4/index-7 log. A current-term ReadIndex AFTER C retires requires
A+B, and its heartbeat makes B a follower. Replacement has no voting role
until a complete snapshot is activated. Control request nonce and physical
replica incarnation are distinct: retained-disk restart invalidates receiving
control sessions but may receive delayed native Raft packets for that same
unique physical incarnation. A preactivation restart needs fresh authority.

Native vote predicate models term/log ordering and known-leader suppression;
no implementation imports, mutable test hooks or fabricated quorum replies.
The current-term snapshot assumption is tested independently of constructor
readiness. No model result establishes actual storage durability or full V2.
"""
from dataclasses import dataclass, replace
from itertools import permutations
import hashlib
import json
from pathlib import Path


@dataclass(frozen=True)
class Node:
    name: str
    term: int
    log_term: int
    log_index: int
    vote: str | None = None
    leader: str | None = None
    role: str = "follower"
    receiving: bool = False


@dataclass(frozen=True)
class Grant:
    issuer: str
    leader: str
    target_voter: str
    nonce: int
    term: int
    barrier: int
    physical: str
    scope: str


def native_vote(node, candidate, term, log_term, log_index):
    """Reduced voting transition; matching higher-term log test is explicit."""
    if term < node.term:
        return node, False
    if term > node.term:
        node = replace(node, term=term, vote=None, leader=None, role="follower")
    can_vote = node.vote == candidate or node.vote is None and node.leader is None
    up_to_date = (log_term, log_index) >= (node.log_term, node.log_index)
    if can_vote and up_to_date:
        return replace(node, vote=candidate), True
    return node, False


class Trial:
    def __init__(self, quorum=True):
        self.a = Node("A", 5, 5, 8, vote="A", leader="A", role="leader")
        self.b = Node("B", 5, 4, 7, vote="B", role="candidate")
        # Ghost voter history is the independent oracle; disk loss erases C's
        # local copy, not its prior participation in the original election.
        self.votes = {5: {"A"}}
        self.target = Node("C", 0, 0, 0, receiving=True)
        self.nonce = 1
        self.physical = "C-new"
        self.issuer = "A-session-1"
        self.known_issuer = None
        self.scope = "graph-1/partition-1/group-1"
        self.grant = None
        self.current_quorum = quorum
        self.authorized = []
        self.violation = []
        self.frames = []
        self.authorize()

    def authorize(self):
        if not self.current_quorum:
            return False
        # Actual abstract heartbeat delivery/response, not a caller flag.
        self.b = replace(self.b, term=self.a.term, leader=self.a.name, role="follower")
        acknowledgments = {self.a.name, self.b.name}
        if len(acknowledgments) < 2 or self.a.role != "leader" or self.a.log_term != self.a.term:
            return False
        self.grant = Grant(self.issuer, self.a.name, self.target.name, self.nonce, self.a.term, self.a.log_index,
                           self.physical, self.scope)
        self.authorized.append(self.grant)
        self.known_issuer = self.issuer  # Explicit delivered native completion.
        return True

    def matches(self, grant):
        return (any(issued is grant for issued in self.authorized) and grant.issuer == self.known_issuer and
                grant.target_voter == self.target.name and grant.nonce == self.nonce and grant.physical == self.physical and
                grant.scope == self.scope)

    def activate(self, index=8, entry_term=5, message_term=5, strict=True):
        if self.target.name not in {"A", "B", "C"} or not self.target.receiving or not self.matches(self.grant):
            return False
        # Actual Raft.Step ignores a lower-term snapshot. Neither Prepare nor
        # a still-valid historical grant can override the durable local term.
        if message_term < self.target.term:
            return False
        if strict and (index < self.grant.barrier or
                       entry_term != message_term or message_term != self.grant.term):
            return False
        # Complete application data/claims and actual Ready are assumptions of
        # this voting model. They need separate native storage tests.
        self.target = replace(self.target, term=max(self.target.term, message_term),
                              log_term=entry_term, log_index=index, vote=None,
                              leader=self.grant.leader, receiving=False)
        return True

    def restart(self):
        self.target = replace(self.target, leader=None, role="follower")
        if self.target.receiving:
            self.nonce += 1
            # Receiving authority is ephemeral. Durable term survives but the
            # old control completion cannot create/activate this new session.

    def delayed_vote(self, early_gate=True):
        if self.target.receiving and early_gate:
            return False
        target, granted = native_vote(self.target, "B", 5, 4, 7)
        if self.target.receiving and granted:
            self.violation.append("receiving-owner-mutated-volatile-vote")
            # The independent nonvoting-Ready persistence rule would still
            # forbid releasing this response. Do not claim a durable fork here.
            self.target = target
            return False
        self.target = target
        if granted:
            self.votes.setdefault(5, set()).add("B")
            self.frames.append(("B", 5))
            if len(self.votes[5]) > 1:
                self.violation.append("logical-voter-double-vote")
        return granted

    def response(self):
        if ("B", 5) in self.frames and self.b.role == "candidate" and self.b.term == 5:
            self.b = replace(self.b, role="leader", leader="B")
        if self.a.role == self.b.role == "leader" and self.a.term == self.b.term:
            self.violation.append("two-leaders-in-one-term")

    def event(self, name):
        if name == "probe":
            if self.target.receiving and self.matches(self.grant):
                # A genuine rejection persists only nonvoting term/Commit0.
                self.target = replace(self.target, term=self.a.term, leader="A")
        elif name == "activate":
            self.activate()
        elif name == "restart":
            self.restart()
        elif name == "reauthorize":
            self.authorize()
        elif name == "delayed-vote":
            self.delayed_vote()
        elif name == "delayed-response":
            self.response()
        elif name == "future-vote":
            if not self.target.receiving:
                self.target, _ = native_vote(self.target, "B", 6, 5, 8)
        else:
            raise AssertionError(name)


def run():
    checks = []
    traces = 0
    events = ("probe", "activate", "restart", "reauthorize", "delayed-vote",
              "delayed-response", "future-vote")
    for schedule in permutations(events):
        trial = Trial()
        for event in schedule:
            trial.event(event)
            assert not trial.violation, (schedule, trial.violation)
        traces += 1
    checks.append({"name": "strict-event-order-enumeration", "schedules": traces,
                   "events_per_schedule": len(events), "violations": 0})

    # Conservative snapshot requirement prevents losing the original term-5
    # voting promise after restart. With a real quorum read, B is now follower:
    # this counterexample is double-voting, NOT a demonstrated second leader.
    weak = Trial()
    assert weak.activate(7, 4, 5, strict=False)
    weak.restart()
    assert weak.delayed_vote()
    weak.response()
    assert "logical-voter-double-vote" in weak.violation
    assert "two-leaders-in-one-term" not in weak.violation
    checks.append({"name": "older-snapshot-after-current-quorum-read",
                   "omitted": ["snapshot-covers-barrier", "snapshot-entry-term-is-current-message-term"],
                   "counterexample": ["snapshot(7,4,message-term5)", "restart", "delayed-vote(B,term5,log4:7)"],
                   "violations": weak.violation, "second_leader": False})

    # Manifest-only authority neither contacts the surviving quorum nor updates
    # B's candidate role. Reusing C now permits a second term-5 majority {B,C}.
    fork = Trial(quorum=False)
    fork.grant = Grant(fork.issuer, "A", "C", 1, 5, 0, fork.physical, fork.scope)
    fork.known_issuer = fork.issuer
    fork.authorized.append(fork.grant)  # Intentional mutant: copied fields as authority.
    assert fork.activate(7, 4, 5, strict=False)
    fork.restart(); fork.delayed_vote(); fork.response()
    assert "two-leaders-in-one-term" in fork.violation
    checks.append({"name": "manifest-only-replacement-forks-election",
                   "omitted": ["actual-surviving-quorum-completion", "current-term-complete-snapshot"],
                   "counterexample": ["A elected by A+C-old", "retire/loss C-old", "B stays candidate5", "accept old snapshot7:4", "restart C-new", "B gets votes B+C-new"],
                   "violations": fork.violation})

    pending = Trial()
    pending.delayed_vote(early_gate=False)
    assert pending.violation == ["receiving-owner-mutated-volatile-vote"]
    checks.append({"name": "empty-membership-is-not-a-vote-gate",
                   "omitted": ["receiving-packet-whitelist"],
                   "violations": pending.violation,
                   "qualification": "Commit0/Vote0 Ready persistence separately prevents response release; no durable fork is claimed."})

    session = Trial()
    old = session.grant
    session.restart()
    assert not session.matches(old) and not session.activate()
    session.authorize()
    assert session.matches(session.grant) and session.activate()
    session.restart()
    assert not session.target.receiving
    checks.append({"name": "preactivation-restart-needs-new-control-authority-postactivation-keeps-snapshot", "passed": True})

    delayed = Trial()
    old = delayed.grant
    # These are REMOTE transitions. Receiver has heard none of them and may
    # still accept its previously authorized complete emitted term-5 snapshot.
    delayed.issuer = "B-session-2"
    delayed.a = Node("B", 6, 6, 9, vote="B", leader="B", role="leader")
    delayed.b = Node("A", 6, 6, 9, vote="B", leader="B")
    assert delayed.matches(old) and delayed.activate(8, 5, 5)
    delayed.restart()
    assert not delayed.delayed_vote()  # Prior term5 request is out-of-date.
    target, granted = native_vote(delayed.target, "C", 6, 5, 8)
    assert granted  # Recovered C may vote for itself in a newer term.
    # It still cannot win: BOTH survivors have the term6/index9 log and deny
    # a stale C candidate even at term7. No stale-state write can get quorum.
    denied = []
    for survivor in (delayed.a, delayed.b):
        _, vote = native_vote(survivor, "C", 7, 5, 8)
        denied.append(not vote)
    assert all(denied)
    checks.append({"name": "emitted-valid-snapshot-activates-before-undelivered-leadership-change",
                   "passed": True, "receiver_remote_role_oracle": False,
                   "sequence": ["quorum grant A-session1 term5 barrier8", "complete MsgSnap8:5 emitted", "remote B elected/committed6:9", "receiver activates old8:5 before hearing term6", "receiver restarts", "prior vote5 denied", "stale C campaign denied by both surviving logs6:9"],
                   "qualification": "Historical local restoration is safe in this reduced voting model; it is not fresh graph-operation authority."})

    source = Trial()
    old = source.grant
    source.issuer = "B-session-2"
    source.a = Node("B", 6, 6, 9, vote="B", leader="B", role="leader")
    source.b = Node("A", 6, 6, 9, vote="B", leader="B")
    assert source.matches(old)  # No instantaneous remote knowledge.
    source.nonce += 1  # Explicit local cancel/switch creates a new request.
    assert not source.matches(old)
    source.authorize()  # Actual delivered new-term source completion.
    assert source.known_issuer == "B-session-2" and source.grant.leader == "B"
    assert not source.activate(8, 5, 6)
    assert source.activate(9, 6, 6)
    assert not source.matches(old)
    checks.append({"name": "delivered-new-source-completion-invalidates-old-control-owner", "passed": True})

    copied = Trial()
    body_only = replace(copied.grant)
    assert body_only == copied.grant and body_only is not copied.grant
    assert not copied.matches(body_only)
    copied.grant = body_only
    assert not copied.activate()
    checks.append({"name": "identical-decoded-fields-are-not-native-completion-provenance", "passed": True})

    for other in ("A", "B", "D"):
        membership = Trial()
        membership.target = replace(membership.target, name=other)
        assert not membership.matches(membership.grant)
        assert not membership.activate()
    checks.append({"name": "grant-binds-exact-target-not-another-existing-or-new-voter",
                   "refused_targets": ["A", "B", "D"], "passed": True})

    local_term = Trial()
    emitted = local_term.grant
    # A durable local term increase from an already processed Raft exchange
    # precedes delivery of this older, complete snapshot. No remote oracle.
    local_term.target = replace(local_term.target, term=6)
    before = local_term.target
    assert local_term.matches(emitted)
    assert not local_term.activate(8, 5, 5)
    assert local_term.target == before and local_term.target.receiving
    checks.append({"name": "delivered-snapshot-below-durable-local-term-cannot-activate",
                   "sequence": ["grant5 and MsgSnap8:5 emitted", "receiver durably advances local term to6", "old snapshot5 delivered"],
                   "passed": True, "qualification": "Actual RawNode term admission is an explicit activation assumption, not max(localTerm,messageTerm)."})

    # External unique-incarnation precondition is necessary even with a good
    # snapshot: two physical C replicas can each vote once in term6.
    cold = Node("C", 5, 5, 8, vote="A", leader="A")
    fresh = Node("C", 5, 5, 8, leader=None)
    cold, one = native_vote(cold, "A", 6, 5, 8)
    fresh, two = native_vote(fresh, "B", 6, 5, 8)
    assert one and two and cold.vote != fresh.vote
    checks.append({"name": "unretired-duplicate-physical-voter-breaks-quorum-intersection",
                   "omitted": ["embedding-exclusive-live-incarnation-association"],
                   "counterexample": "Term6 votes {A,C-old} and {B,C-new} elect distinct leaders; local snapshots cannot detect the copied credential set.",
                   "qualification": "Intentional violation of embedding trust precondition, not a requested global clone detector."})

    result = {"schema_version": 1, "status": "finite_model_checks_pass",
              "scope": "Independent reduced voting and control-lifetime model; not production, storage, Raft conformance or full V2 acceptance.",
              "assumptions": ["Three fixed voters; A's term5 election/commit originally used A+C-old.",
                              "A completed a current-term heartbeat barrier with A+B AFTER old C retired.",
                              "Only complete receiver-local verified/claimed snapshot and actual Ready may activate (native storage obligation, not modeled).",
                              "Embedding supplies unique active physical incarnation associations and excludes retired connections.",
                              "Source and receiver request sessions are independently matched; manifest/peer fields alone never authorize.",
                              "Model restart preserves durable term/log/vote and loses volatile leader/receiving-control session.",
                              "Receiver activation consults only locally accepted grant/session and delivered snapshot, never instantaneous remote role/term.",
                              "Actual RawNode ignores a snapshot message below its durable local term; the model activation gate enforces this."],
              "checks": checks,
              "remaining_native_tests": ["Actual source ReadIndex/publish/SnapshotSend term/index relationships and message authority.",
                                         "Durable nonvoting empty-store Ready admission and genuine native reject->snapshot flow.",
                                         "Same-store claim, actual Ready, sync, Restore, ACK ordering; fail-stop after uncertain I/O.",
                                         "Transport retirement/session changes, two-source import/claim races and source leadership changes.",
                                         "Node/relationship history, ID high-water, dedup, prepared locks, decisions and old certified cuts after replacement."],
              "model_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    run()
