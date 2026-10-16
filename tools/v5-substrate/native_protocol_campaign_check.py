#!/usr/bin/env python3
"""Independent joins for finite actual protocol/native-storage conjunctions.

Diagnostic recipes and decoded observation bodies never create protocol
authority. Every completed cell must additionally supply the real native batch,
process history, complete original prefix and its protocol-specific oracle.
"""
import argparse
import hashlib
import json
import os
import stat
from pathlib import Path
import native_transition_initial_check as I

C, P, B, F, T = I.C, I.P, I.B, I.F, I.T
require, integer, fixed = I.require, I.integer, I.fixed
PROFILE = "native-protocol-storage-campaign-v1"
EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Successor OriginalCall InitialPhases InitialCapture Hold VoteRead Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened Completion".split())
COMPLETION = {"P06", "Calls", "FirstReopened", "RecoveryBVoter", "Capture"}
SCOPE_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened P01".split())
Q = C.module("campaign_scope_semantics", "native_transition_scope_check.py")
L = C.module("campaign_fence_semantics", "check-p02-history.py")
M = C.module("campaign_coordinator_semantics", "check-p03-history.py")
N = C.module("campaign_conditional_semantics", "check-p05-history.py")
CONDITIONAL_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened P05 From Through Records TraceName TraceSHA256 TraceBytes Capture".split())
CONDITIONAL = set("Schedule Condition Cut Old Retained Current Attempts Steps Phases Controls Processes Records".split())
FROZEN_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened P04 Frozen CapacityCapture".split())
QUEUED = set("Version Profile Case Records From Through OriginalA OriginalB ManifestA Capacity Queue Pending DecisionA DecisionB Before Retained Current Reopened ReopenedCurrent Processes Duplicates".split())
COORDINATOR_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened P03 TraceName TraceSHA256 TraceBytes Records From Through Boundary OriginalPending OutagePending InitialPhases InitialCapture RecoveryPhases RecoveryCapture RecoveryCalls RecoveryActor BeforeRecovery".split())
PARTIAL_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened OriginalActor Reader RecoveryActor Partial Completed Outage".split())
PARTIAL = set("Version Profile Case Original Manifest PendingCaller InstalledA WithheldB PartialReads Before Retained Recovered Reopened Processes From Through Records PacketEvents IO Refused RefusedReplay".split())
FENCE_EVIDENCE = set("Recipe Run DeadlineUnixNanos Actor Target Arm Batch SelectedCommand TargetTransportLoss ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork TargetProcesses TargetReopened P02 From Through Capture Planning Hold Boundary Calls Cohort Reopened PostRetry".split())
P02 = set("Schedule Trace Phases SourceCaptures Encounters Fences Stage Processes Old Current Retained DelayedLocal DelayedLocalReplay Retries PendingCaller Records".split())


def fence_recipe(row):
    require(type(row) is dict and set(row) == {"Schedule", "Phase", "Partition", "Kind", "Side"}, "actual P02 causal recipe grammar")
    if row["Schedule"] == "persisted-fence-before-application":
        require(type(row["Phase"]) is int and type(row["Partition"]) is int and
                (row["Phase"], row["Partition"]) in ((0, 3), (1, 3), (2, 3), (2, 8), (3, 3), (4, 3), (4, 8)) and
                type(row["Kind"]) is int and row["Kind"] in (1, 2) and row["Side"] in ("before", "after"), "actual local fence or post-election original phase")
    else:
        require(row["Schedule"] in ("delayed-A-prepare", "A-before-B-after"), "actual P02 live causal ordering")
        scope_recipe({"Mode":1, **{key:row[key] for key in ("Phase", "Partition", "Kind", "Side")}})
    return f'p02-{row["Schedule"]}-phase{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'


def fence_planning_floor(stage, floor, bound):
    require(type(stage) is bool and type(floor) is int and type(bound) is int and 0 <= floor < 1 << 64 and 0 < bound < 1 << 64,
            "actual finite planning floor/fence coordinates")
    require(floor >= bound if stage else floor < bound, "post-fence planning versus genuine pre-fence live capture")


def scope_recipe(row):
    require(type(row) is dict and set(row) == {"Mode", "Phase", "Partition", "Kind", "Side"}, "exact P01 native recipe")
    require(type(row["Mode"]) is int and row["Mode"] in (1, 4) and type(row["Phase"]) is int and
            type(row["Partition"]) is int and (row["Phase"], row["Partition"]) in ((1, 8), (2, 3), (2, 8), (3, 8), (4, 3), (4, 8)) and
            type(row["Kind"]) is int and row["Kind"] in (1, 2) and row["Side"] in ("before", "after"), "actual P01 scope/participant/native transition")
    return f'p01-mode{row["Mode"]}-phase{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'


def live_recipe(row):
    require(type(row) is dict and set(row) == C.RECIPE | {"Phase"}, "exact first-live causal recipe")
    C.recipe({key: row[key] for key in C.RECIPE})
    require(type(row["Phase"]) is int and (row["Phase"] == 2 or row["Phase"] == 3 and row["Coordinator"] == 8),
            "first-live endpoint set cannot invent A replacement or later Resolve authority")
    return f'p06-co{row["Coordinator"]}-commit{str(row["Commit"]).lower()}-first-live-B{row["Phase"]}-k{row["Kind"]}-{row["Side"]}'


def coordinator_recipe(row):
    require(type(row) is dict and set(row) == {"Coordinator", "Phase", "Partition", "Kind", "Side"}, "actual P03 native recipe grammar")
    co = row["Coordinator"]
    require(type(co) is int and co in (3, 8) and type(row["Phase"]) is int and type(row["Partition"]) is int and
            (row["Phase"], row["Partition"]) in ((1, co), (2, 3), (2, 8), (3, co), (4, 3), (4, 8)) and
            type(row["Kind"]) is int and row["Kind"] in (1, 2) and row["Side"] in ("before", "after"), "actual coordinator/participant stable transition")
    return f'p03-co{co}-phase{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'


def partial_recipe(row):
    require(type(row) is dict and set(row) == {"Phase", "Partition", "Kind", "Side"}, "actual P03 partial causal recipe")
    scope_recipe(dict(Mode=1, **row))
    return f'p03-partial-phase{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'


def conditional_recipe(row):
    require(type(row) is dict and set(row) == {"Schedule", "Operation", "Phase", "Partition", "Kind", "Side"} and
            row["Schedule"] in N.SCHEDULES, "actual five P05 dependency scenarios")
    count = 4 if row["Schedule"] == "temporal-life-value-ABA" else 2 if row["Schedule"] == "empty-range-and-absent-key-insertion" else 1
    require(type(row["Operation"]) is int and 1 <= row["Operation"] <= count, "actual preflight/insertion/correction occurrence")
    co = 8 if row["Schedule"] == "write-skew-B-first" else 3
    coordinator_recipe(dict(Coordinator=co, **{key:row[key] for key in ("Phase", "Partition", "Kind", "Side")}))
    return f'p05-{row["Schedule"]}-op{row["Operation"]}-phase{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'


def frozen_recipe(row):
    require(type(row) is dict and set(row) == {"Order", "Original", "Phase", "Partition", "Candidate", "Kind", "Side"} and
            row["Order"] in ("commit-first", "abort-first") and row["Original"] in ("A", "B") and
            type(row["Phase"]) is type(row["Partition"]) is type(row["Kind"]) is int and row["Kind"] in (1, 2) and row["Side"] in ("before", "after"),
            "actual P04 private queue/native recipe")
    if row["Original"] == "A":
        require(row["Phase"] in (1, 2, 3, 4) and row["Partition"] == 3 and row["Candidate"] == "", "A's real NO has no B Prepare/Resolve")
    elif row["Phase"] == 3:
        require(row["Partition"] == 8 and row["Candidate"] in ("capacity-ABORT", "all-YES"), "actual two private B candidates")
    else:
        require((row["Phase"], row["Partition"]) in ((1, 8), (2, 3), (2, 8), (4, 3), (4, 8)) and row["Candidate"] == "", "actual ordinary B source submissions")
    candidate = row["Candidate"] or "ordinary"
    return f'p04-{row["Order"]}-{row["Original"]}-phase{row["Phase"]}-p{row["Partition"]}-{candidate}-k{row["Kind"]}-{row["Side"]}'


def load(path, *, program=False):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded campaign frame/artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after actual campaign EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "campaign header")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == {"Version", "Profile", "Case", "Evidence"}, "fresh campaign grammar")
                evidence = header["Evidence"]
                require(type(evidence) is dict, "actual protocol evidence object")
                if "P04" in evidence:
                    require(set(evidence) == FROZEN_EVIDENCE | ({"Program", "ProgramCohort"} if program else set()) and type(evidence["P04"]) is dict and
                            set(evidence["P04"]) == QUEUED and evidence["P04"]["Records"] is None,
                            "actual queued-original prefix must be streamed")
                elif "P05" in evidence:
                    extra = ({"Program", "ProgramCohort"} | ({"ProgramReopenAfter"} if evidence["Recipe"]["Schedule"] == "write-skew-A-first" else set())) if program else set()
                    require(set(evidence) == CONDITIONAL_EVIDENCE | extra and evidence["P05"] is None and evidence["Records"] is None,
                            "actual conditional full prefix must be streamed")
                elif "Partial" in evidence:
                    require(set(evidence) == PARTIAL_EVIDENCE | ({"Program", "ProgramCohort"} if program else set()) and type(evidence["Partial"]) is dict and
                            set(evidence["Partial"]) == PARTIAL and evidence["Partial"]["Records"] is None,
                            "actual partial-install prefix must be streamed")
                elif "P03" in evidence:
                    require(set(evidence) == COORDINATOR_EVIDENCE | ({"Program"} if program else set()) and evidence["P03"] is None and evidence["Records"] is None,
                            "actual P03 full prefix must be streamed")
                elif "P02" in evidence:
                    extra = {"Program"} if program else set()
                    require(set(evidence) in (FENCE_EVIDENCE | extra, FENCE_EVIDENCE | {"FenceTransition"} | extra) and type(evidence["P02"]) is dict and
                            set(evidence["P02"]) == P02 and evidence["P02"]["Records"] is None,
                            "actual P02 prefix must be streamed")
                elif "P01" in evidence:
                    require(set(evidence) == SCOPE_EVIDENCE | ({"Program"} if program else set()) and type(evidence["P01"]) is dict and
                            set(evidence["P01"]) == Q.HEADER and evidence["P01"]["Records"] is None,
                            "actual P01 prefix must be streamed")
                else:
                    require(set(evidence) == EVIDENCE | ({"Program", "ProgramCohort"} if program else set()) and type(evidence["Completion"]) is dict and
                            set(evidence["Completion"]) == COMPLETION and type(evidence["Completion"]["P06"]) is dict and
                            set(evidence["Completion"]["P06"]) == C.P06 and evidence["Completion"]["P06"]["Records"] is None,
                            "complete original prefix must be streamed, never inline/omitted")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "bounded actual retained original row")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer" and
                        type(row["Rows"]) is int and row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(),
                        "complete actual original-prefix census/hash")
                footer = row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None, "missing actual campaign EOF")
    evidence = header["Evidence"]
    semantic = evidence["P04"] if "P04" in evidence else evidence["Partial"] if "Partial" in evidence else evidence if "P03" in evidence or "P05" in evidence else evidence["P02"] if "P02" in evidence else evidence["P01"] if "P01" in evidence else evidence["Completion"]["P06"]
    require(footer["Through"] == (evidence["Through"] if "P02" in evidence or "P03" in evidence or "P05" in evidence else semantic["Through"]), "foreign actual campaign EOF")
    semantic["Records"] = records
    if "P03" in evidence:
        require(type(header["Version"]) is int and header["Version"] == (2 if program else 1) and header["Profile"] == ("native-protocol-causal-program-v1" if program else PROFILE) and
                header["Case"] == coordinator_recipe(evidence["Recipe"]), "actual P03 profile before sidecar ownership")
        evidence["P03"] = coordinator_sidecar(path, evidence, total)
    elif "P05" in evidence:
        require(type(header["Version"]) is int and header["Version"] == (2 if program else 1) and header["Profile"] == ("native-protocol-causal-program-v1" if program else PROFILE) and
                header["Case"] == conditional_recipe(evidence["Recipe"]), "actual conditional profile before sidecar ownership")
        evidence["P05"] = conditional_sidecar(path, evidence, total)
        require(type(evidence["P05"]) is dict and set(evidence["P05"]) == CONDITIONAL and evidence["P05"]["Records"] == records,
                "complete actual P05 trace prefix differs from independently sealed streamed census")
    return header


def coordinator_sidecar(path, evidence, main_bytes):
    return trace_sidecar(path, evidence, main_bytes, coordinator_recipe(evidence["Recipe"]))


def conditional_sidecar(path, evidence, main_bytes):
    return trace_sidecar(path, evidence, main_bytes, conditional_recipe(evidence["Recipe"]))


def trace_sidecar(path, evidence, main_bytes, recipe_name):
    name, expected = evidence["TraceName"], integer(evidence["TraceBytes"], 8 << 20, 1)
    require(type(name) is str and name == recipe_name+"-history.json" and
            Path(name).name == name and name not in (".", "..") and len(name) <= 160 and
            type(main_bytes) is int and 0 < main_bytes <= 16 << 20 and expected+main_bytes <= 24 << 20,
            "pre-admitted basename-only sibling and combined diagnostic ceiling")
    digest = fixed(evidence["TraceSHA256"], 32, True)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            require(stat.S_ISREG(metadata.st_mode) and metadata.st_size == expected and metadata.st_nlink == 1,
                    "actual regular sibling, no symlink/hardlink/truncated/foreign trace")
            wire = source.read(expected+1)
            require(len(wire) == expected and wire.endswith(b"\n") and hashlib.sha256(wire).digest() == digest,
                    "exact actual complete sidecar size/hash before decode")
            return T.decoded(wire)
    finally:
        os.close(directory)


def source_reply(wire):
    c = P.Cursor(P.body(wire, b"GSR\1"))
    scope = P.body(c.field(156), b"APS\1")
    voter, issuer, kind = c.number(), c.take(16), c.number(1)
    sequence, nonce, mask = c.number(), c.take(16), c.number(2)
    body = c.field()
    c.end()
    require(voter in range(1, 7) and any(issuer) and sequence > 0 and any(nonce) and mask == 0,
            "actual successful service reply identity")
    return dict(scope=scope, voter=voter, issuer=issuer, kind=kind, sequence=sequence, nonce=nonce, body=body)


def prepared_vote_read(boundary, submit, scopes, actor, voter):
    """Join actual completed GSE read to the earlier, separately observed GSR.

    Only production consumes/seals the original live reply. This parser returns
    inert coordinates for the independent checker and cannot resume a native
    operation or manufacture a successful source reply.
    """
    require(type(boundary) is dict and set(boundary) == {"Request", "Reply"}, "actual held vote-read grammar")
    command, decoded, submit_correlation = C.source_request(submit, scopes, actor)
    _, manifest, yes, outcome = P.peer_phase(P.raw(submit["Request"]), P.raw(submit["Reply"]), scopes, voter)
    require(decoded["phase"] == yes["phase"] == 2 and yes["yes"] and not yes["commit"] and yes["round"] == 0 and
            yes["index"] > 0 and any(yes["tx"]) and P.body(outcome, b"GCS\1")[-1] == 0 and decoded["partition"] == yes["partition"],
            "separate genuine first Prepare YES Submit")
    first = source_reply(P.raw(submit["Reply"]))
    request, reply = P.raw(boundary["Request"]), P.raw(boundary["Reply"])
    require(0 < len(request)+len(reply) <= 128 << 10 and len(reply) <= len(manifest)+4096,
            "prepaid bounded held authority observation")
    q = P.Cursor(P.body(request, b"GSQ\1"))
    kind, sequence, nonce = q.number(1), q.number(), q.take(16)
    require(q.take(48) == bytes(48) and all(q.field() == b"" for _ in range(4)),
            "ordinary exact CrossRead request fields; no capacity/feed/pin command")
    tx, phase = q.take(16), q.number(1)
    q.end()
    actual = source_reply(reply)
    require(kind == actual["kind"] == 6 and sequence == actual["sequence"] > submit_correlation[1] and
            nonce == actual["nonce"] and any(nonce) and tx == yes["tx"] and phase == 2 and
            actual["scope"] == first["scope"] == scopes[yes["partition"]] and
            actual["voter"] == first["voter"] == voter and actual["issuer"] == first["issuer"],
            "later real vote read must keep the authenticated source peer/stream")
    g = P.Cursor(P.body(actual["body"], b"GSE\1"))
    scope = P.body(g.field(156), b"APS\1")
    session, source_sequence = g.take(16), g.number()
    observed_tx, observed_nonce, observed_phase = g.take(16), g.take(16), g.number(1)
    source_index, status_wire, original_manifest = g.number(), g.field(256), g.field()
    g.end()
    require(scope == actual["scope"] and session == actual["issuer"] and source_sequence > 0 and
            (observed_tx, observed_nonce, observed_phase) == (tx, nonce, phase) and
            status_wire == outcome and P.status(status_wire) == yes and source_index >= yes["index"] and
            original_manifest == manifest and hashlib.sha256(original_manifest).digest() == yes["digest"],
            "actual native vote-read identity/status/original-manifest join")
    return dict(status=yes, command=command, manifest=manifest, source_index=source_index,
                correlation=(yes["partition"], sequence, nonce))


def live_initial(rows, scopes, actor, coordinator, phase, manifest, tx, records, floors, voters=None):
    voters = {3:1, 8:4} if voters is None else voters
    order = [(1, coordinator), (2, 3), (2, 8)] + ([(3, 8)] if phase == 3 else [])
    require(type(rows) is list and len(rows) == len(order), "actual SAME-first-run source submission inventory")
    seen, originals, selected = set(), {}, None
    for i, (row, expected) in enumerate(zip(rows, order)):
        command, decoded, correlation = C.source_request(row, scopes, actor)
        require(correlation not in seen and (decoded["phase"], decoded["partition"]) == expected and
                decoded["manifest"] == manifest and decoded["conflict"] is None, "first-run order/original/correlation")
        seen.add(correlation)
        if i == 0:
            proof_keys = []
        elif i < 3:
            proof_keys = [(coordinator, 1)] + ([(3, 2)] if i == 2 else [])
        else:
            proof_keys = [(3, 2), (8, 2)]
        require(decoded["proof_wires"] == [originals[key] for key in proof_keys], "actual prior immutable original authority bytes")
        if i == len(order)-1:
            require(not P.raw(row["Reply"]), "selected unknown cannot borrow a successful reply")
            selected = command
            continue
        _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes,
                                                        voters[row["Partition"]])
        stored = records[status["partition"], status["index"]]
        if expected[0] == 1:
            matched = status["floor"] == 0 and P.status(P.raw(stored["Outcome"])) == dict(status, floor=floors[coordinator])
        else:
            matched = P.raw(stored["Outcome"]) == outcome
        require(actual_manifest == manifest and status["tx"] == tx and status["phase"] == expected[0] and matched,
                "first-live success differs from its actual retained original")
        originals[expected] = outcome
    return selected


def check_scope(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    require(set(e) == SCOPE_EVIDENCE | ({"Program"} if program_roles is not None else set()) and a["Case"] == scope_recipe(r), "actual P01 selected recipe grammar")
    d, selected = e["P01"], P.raw(e["SelectedCommand"])
    target = (3 if r["Partition"] == 3 else 6) if program_roles is None else Q.program_scope_roles(program_roles)[1]
    require(type(target) is int and 1 <= target <= 6 and (target <= 3) == (r["Partition"] == 3), "actual native target belongs to selected partition")
    require(type(e["Actor"]) is int and e["Actor"] == 5 and type(e["Target"]) is int and e["Target"] == target,
            "actual P01 coordinator survives its selected native follower")
    command = C.C.decision_command(selected)
    require((command["phase"], command["partition"]) == (r["Phase"], r["Partition"]) and
            command["manifest"] == P.raw(d["Manifest"]) and d["Mode"] == ("Fresh" if r["Mode"] == 1 else "After"),
            "native selected actual P01 original command/scope")
    run, deadline = fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1)
    event = C.batch_pair(e["Arm"], e["Batch"], run, deadline, target, r["Kind"], r["Side"], selected)
    # Native process/batch checks precede the optional semantic-checker seam.
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    require(type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096,
            "actual selected native target transport death")
    reader = Q.scope_reader(selected_processes=e["TargetProcesses"], program_roles=program_roles)
    result = Q.check(d, failure_member=reader, selected_processes=e["TargetProcesses"], program_roles=program_roles)
    originals = {}
    for row in d["Records"]:
        outcome = P.raw(row["Outcome"])
        if outcome[:4] == b"GCS\1":
            status = P.status(outcome)
            if status["index"] == row["Index"]:
                originals[status["partition"], status["phase"]] = outcome
    phase, part = r["Phase"], r["Partition"]
    if phase == 1:
        keys = []
    elif phase == 2:
        keys = [(8, 1)] + ([(3, 2)] if part == 8 else [])
    else:
        keys = [(3, 2), (8, 2)] if phase == 3 else [(8, 3)]
    expected_proofs = [dict(P.status(originals[key]), floor=0) if key[1] == 1 else P.status(originals[key]) for key in keys]
    require(command["proofs"] == expected_proofs and command["conflict"] is None,
            "actual selected command binds complete genuine earlier original phase proofs")
    if r["Kind"] == 2:
        row = next((row for row in d["Records"] if (row["Partition"], row["Index"]) == (part, event["Index"])), None)
        require(row is not None and P.raw(row["Outcome"]) == originals[part, phase], "actual P01 native installation original")
    T.semantic(e["TargetReopened"] == d["Before"], "old-cut:P01-actual-selected-replacement-native-data-and-index")
    require(integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "P01 attempted diagnostic grants")
    return dict(case=a["Case"], **result)


BATCH_FIELDS = SCOPE_EVIDENCE - {"Recipe", "Run", "DeadlineUnixNanos", "P01"}
PERSISTED_FENCE = BATCH_FIELDS | {"Proposal", "Submission", "Run", "DeadlineUnixNanos", "Kind", "Side", "Successor", "Original", "Applied", "Duplicate"}


def program_fence_roles(context, recipe):
    require(type(context) is dict and set(context) == {"initial_leaders", "leaders", "target"}, "authenticated actual persisted-fence role context")
    initial, leaders, target = context["initial_leaders"], context["leaders"], context["target"]
    require(type(initial) is tuple and initial in ((1, 4), (2, 6)) and all(type(v) is int for v in initial) and
            type(leaders) is tuple and leaders == (3-initial[0], initial[1]) and all(type(v) is int for v in leaders),
            "genuine initial leader death followed by the surviving A voter election")
    expected = initial[0] if recipe["Partition"] == 3 else next(member for member in range(4, 7) if member != leaders[1])
    require(type(target) is int and target == expected and recipe["Schedule"] == "persisted-fence-before-application", "actual selected native member after causal successor")
    return initial, leaders, target


def check_persisted_fence(e, scopes, original, sources, records, bound, *, program_roles=None):
    p, r = e["FenceTransition"], e["Recipe"]
    leader, successor, extra = 1, 2, set()
    if program_roles is not None:
        initial, current, _ = program_fence_roles(program_roles, r)
        leader, successor = initial[0], current[0]
        extra = {"SuccessorEvidence"} | ({"Program"} if r["Phase"] == 0 else set())
    require(type(p) is dict and set(p) == PERSISTED_FENCE | extra and (p["Actor"], p["Target"], p["Successor"]) == (leader, leader, successor),
            "actual local fence leader-loss/successor recipe")
    require(p["Run"] == e["Run"] and p["DeadlineUnixNanos"] == e["DeadlineUnixNanos"], "original fixed fence/run identity/deadline")
    if r["Phase"] == 0:
        require((p["Kind"], p["Side"]) == (r["Kind"], r["Side"]) and all(p[key] == e[key] for key in BATCH_FIELDS),
                "actual selected fence receipt is not another business-phase receipt")
        if program_roles is not None:
            require(p["Program"] == e["Program"], "same actual local-fence message window, not another diagnostic owner")
    else:
        require((p["Kind"], p["Side"]) == (2, "before"), "actual committed pre-application fence predecessor")
        causal = p["TargetProcesses"] + e["TargetProcesses"]
        events = [integer(row["EventID"], minimum=1) for row in causal]
        require(events == sorted(set(events)), "restore quorum after real fence leader death before next selected native fault")
    wire = P.raw(p["Proposal"])
    c = P.Cursor(P.body(wire, b"GSQ\1"))
    require(c.number(1) == 1 and c.take(120) == scopes[3] and c.take(32) == original["configuration"] and c.number() == bound,
            "actual configured PrepareGraphFence command, never a cross-phase label")
    c.end()
    require(wire == P.raw(p["SelectedCommand"]), "actual native fence selector/source input")
    event = C.batch_pair(p["Arm"], p["Batch"], fixed(p["Run"], 16, True), integer(p["DeadlineUnixNanos"], (1 << 63)-1, 1),
                         leader, p["Kind"], p["Side"], wire)
    B.processes(p["TargetProcesses"], None, "P02-actual-local-fence", p["Batch"])
    if program_roles is not None:
        stopped = Q.processes(e["Cohort"])
        rejoined = p["TargetProcesses"][1]
        later = e["TargetProcesses"][0] if r["Phase"] != 0 and e["Target"] == leader else stopped[leader][0]
        require(rejoined["Instance"] == later["Instance"] and rejoined["Volume"] == later["Volume"],
                "actual same-volume successor/rejoin lineage before later selected loss or cohort stop")
    C.terminal_submission(p["Submission"], scopes, 3, leader, wire, event)
    require(type(p["TargetTransportLoss"]) is str and 0 < len(p["TargetTransportLoss"]) <= 4096, "actual fence leader transport death")
    require(p["Applied"] == e["P02"]["Fences"][0] and p["Duplicate"] == e["P02"]["Fences"][1], "successor fence outputs differ from whole artifact")
    for row in (p["Applied"], p["Duplicate"]):
        require(type(row) is dict and set(row) == C.LOCAL and row["Partition"] == 3 and P.raw(row["Proposal"]) == wire and
                L.fence(row) == bound and records.get((3, row["Index"])) == {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")},
                "actual same fence control/application-prefix join")
    require(p["Applied"]["Index"] < p["Duplicate"]["Index"] <= sources[3]["index"] <= sources[3]["source_index"],
            "genuine successor fence established before planning; captured installed floor is a separate coordinate")
    if p["Kind"] == 2:
        row = p["Original"]
        require(type(row) is dict and set(row) == C.LOCAL and row["Partition"] == 3 and row["Index"] == event["Index"] and
                P.raw(row["Proposal"]) == wire and L.fence(row) == bound and records.get((3, row["Index"])) ==
                {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")}, "actual committed survivor original before missing leader apply reply")
    else:
        require(p["Original"] is None, "Ready persistence is not quorum/applied-original evidence")
    T.semantic(p["TargetReopened"] == e["P02"]["Old"], "old-cut:P02-fence-leader-actual-local-recovery")
    require(integer(p["ReceiptReads"], 129, 1)*65536 == p["ReceiptGrantBytes"] and integer(p["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == p["GateDecodeWork"],
            "separate fully spent native fence diagnostic grant")


def check_delayed_local(d, records, recovered, current, decision_round, bound):
    local, duplicate = d["DelayedLocal"], d["DelayedLocalReplay"]
    require(type(local) is dict and type(duplicate) is dict and set(local) == set(duplicate) == C.LOCAL, "actual delayed local/replay grammar")
    wire, outcome, replay = P.raw(local["Proposal"]), P.body(P.raw(local["Outcome"]), b"GRO\4"), P.body(P.raw(duplicate["Outcome"]), b"GRO\4")
    request = P.pgq(wire)
    require(request["partition"] == local["Partition"] == duplicate["Partition"] == 3 and len(outcome) == len(replay) == 84 and
            outcome[0] == request["kind"] and outcome[1:17] == P.GRAPH and int.from_bytes(outcome[17:25], "big") == 3 and
            outcome[25:41] == request["request"] and outcome[41:73] == hashlib.sha256(wire).digest() and
            int.from_bytes(outcome[73:81], "big") == local["Index"] and outcome[81:] == b"\1\0\0" and bool(P.raw(local["CDC"])),
            "actual pre-fence local input and useful immutable outcome")
    require(P.raw(duplicate["Proposal"]) == wire and duplicate["Index"] > local["Index"] and replay[:81] == outcome[:81] and
            replay[81:] == b"\2\0\0" and not P.raw(duplicate["CDC"]), "same local original index/requestReplay after real cohort replacement")
    for row in (local, duplicate):
        require(records.get((3, row["Index"])) == {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")}, "delayed local/replay missing from complete actual prefix")
    envelope = P.Cursor(P.body(P.raw(local["CDC"]), b"GCE\1"))
    require(envelope.number(1) == request["kind"] and envelope.take(16) == request["request"] and
            envelope.take(32) == hashlib.sha256(wire).digest() and envelope.number() == local["Index"], "delayed local CDC binds actual original")
    logical = P.body(envelope.field(), b"GCD\5")
    envelope.end()
    require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == 3, "actual delayed local CDC namespace")
    cell = current["A/presence/1"]
    round_ = integer(cell["revision"], minimum=1)
    require(round_ > max(decision_round, bound), "ordinary delayed local proposal must commit above actual fence/decision")
    expected = dict(recovered)
    expected["A/presence/1"] = dict(present=False, null=False, handle=0, revision=round_, provenance=41, kind="absent", i64=0)
    T.semantic(current == expected and cell == expected["A/presence/1"], "P02-delayed-local-exact-effect-and-no-phantoms")
    return round_


def fence_prefix(records, operations, local_rows, names=("node-a", "node-b", "edge-create")):
    """Account every genuine entry; missing output never becomes a business row."""
    known = {}
    for name in names:
        op = operations[name]
        encoded = op["Transaction"]
        require(type(encoded) is str and len(encoded) == 32 and encoded == encoded.lower(), "actual qualified business transaction ID")
        tx = bytes.fromhex(encoded)
        require(any(tx) and tx.hex() == encoded and tx not in known and op["Coordinator"] in (3, 8), "distinct actual business originals")
        known[tx] = op["Coordinator"]
    locals_ = {(row["Partition"], row["Index"]):row for row in local_rows}
    originals, digests = {}, {}
    for coordinate, row in records.items():
        outcome, cdc = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not outcome:
            require(not cdc, "no-output native prefix entry cannot carry phantom CDC")
            continue
        if outcome[:4] == b"GSO\1":
            body = P.body(outcome, b"GSO\1")
            require(len(body) == 18 and body[0] in (1, 2) and 1 <= body[1] <= 4 and
                    int.from_bytes(body[2:10], "big") > 0 and int.from_bytes(body[10:18], "big") == coordinate[1] and not cdc,
                    "actual bounded cut-control prefix entry")
            continue
        if outcome[:4] == b"GRO\4":
            require(coordinate in locals_ and row == {key:locals_[coordinate][key] for key in ("Partition", "Index", "Outcome", "CDC")},
                    "unreported local mutation cannot hide in the complete native prefix")
            continue
        status = P.status(outcome)
        tx, original_index = status["tx"], status["index"]
        require(tx in known and status["partition"] == coordinate[0] and 0 < original_index <= coordinate[1] and
                (tx not in digests or digests[tx] == status["digest"]), "foreign original/digest in actual prefix")
        digests[tx] = status["digest"]
        if original_index != coordinate[1]:
            require(records.get((coordinate[0], original_index), {}).get("Outcome") == row["Outcome"] and not cdc,
                    "actual later replay entry must join its earlier unchanged original")
            continue
        key = tx, status["partition"], status["phase"]
        require(key not in originals and P.body(outcome, b"GCS\1")[-1] == 0 and
                (status["yes"] if status["phase"] == 2 else True) and
                (status["commit"] if status["phase"] >= 3 else True) and bool(cdc) == (status["phase"] == 4),
                "complete real YES/COMMIT/Resolve original and CDC-only-on-install")
        originals[key] = status
        if cdc:
            envelope = P.Cursor(P.body(cdc, b"GCE\1"))
            require(envelope.number(1) == 6 and envelope.take(16) == tx and envelope.take(32) == status["digest"] and
                    envelope.number() == original_index, "actual whole original CDC envelope")
            logical = P.body(envelope.field(), b"GCD\5")
            envelope.end()
            require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == coordinate[0], "actual CDC source namespace")
    expected = {(tx, part, phase) for tx, co in known.items() for part, phase in ((co, 1), (3, 2), (8, 2), (co, 3), (3, 4), (8, 4))}
    require(set(originals) == expected, "complete actual transaction original census; no omitted phase or extra business original")
    for tx, co in known.items():
        decision = originals[tx, co, 3]
        require(decision["round"] > max(originals[tx, part, 2]["floor"] for part in (3, 8)) and
                all(originals[tx, part, 4]["round"] == decision["round"] for part in (3, 8)), "both actual effects bind the first durable decision")
    return originals


def fence_phases(rows, records, scopes, peers, actor, co, sources):
    require(type(rows) is list and len(rows) == 6, "actual six ordered P02 phase replies")
    order = [(1, co), (2, 3), (2, 8), (3, co), (4, 3), (4, 8)]
    statuses, outcomes, commands, manifest, correlations = {}, {}, {}, None, set()
    for row, expected in zip(rows, order):
        command_wire, command, correlation = C.source_request(row, scopes, actor)
        _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes, peers[row["Partition"]])
        manifest = actual_manifest if manifest is None else manifest
        require((command["phase"], command["partition"]) == (status["phase"], status["partition"]) == expected and
                actual_manifest == command["manifest"] == manifest and correlation not in correlations and command["conflict"] is None,
                "actual first P02 source order/correlation/immutable manifest")
        correlations.add(correlation)
        if expected[0] == 1:
            keys = []
        elif expected[0] == 2:
            keys = [(co, 1)] + ([(3, 2)] if expected[1] == 8 else [])
        else:
            keys = [(3, 2), (8, 2)] if expected[0] == 3 else [(co, 3)]
        require(command["proof_wires"] == [outcomes[key] for key in keys], "actual earlier original phase authority bytes")
        actual = records.get((expected[1], status["index"]))
        require(actual is not None, "actual P02 phase original missing from complete source prefix")
        if expected[0] == 1:
            require(P.status(P.raw(actual["Outcome"])) == dict(status, floor=sources[co]["floor"]) and status["floor"] == status["round"] == 0,
                    "actual Register projection versus stored installed floor")
        else:
            require(P.raw(actual["Outcome"]) == outcome, "actual returned P02 phase differs from retained original")
        coordinate = expected[1], expected[0] # Native original namespace, then phase.
        statuses[coordinate], outcomes[coordinate], commands[expected] = status, outcome, command_wire
    return statuses, outcomes, commands, manifest


def check_fence(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    stage = r["Schedule"] == "persisted-fence-before-application"
    require(set(e) == (FENCE_EVIDENCE | {"FenceTransition"} if stage else FENCE_EVIDENCE) | ({"Program"} if program_roles is not None else set()) and a["Case"] == fence_recipe(r), "actual P02 finite native recipe")
    actor, co, peers = (3, 3, {3:2, 8:4}) if stage else (5, 8, {3:1, 8:4})
    if program_roles is not None:
        if stage:
            initial, leaders, selected_target = program_fence_roles(program_roles, r)
        else:
            leaders, selected_target = Q.program_scope_roles(program_roles)
        peers = dict(zip((3, 8), leaders))
    d = e["P02"]
    require(type(d) is dict and set(d) == P02 and d["Schedule"] == r["Schedule"] and
            d["Stage"] is None and d["Processes"] is None and d["Encounters"] is None and not P.raw(d["PendingCaller"]) and
            ((d["DelayedLocal"] is not None and d["DelayedLocalReplay"] is not None) if stage else
             (d["DelayedLocal"] is None and d["DelayedLocalReplay"] is None)), "actual P02 delayed native-local versus live-call inventory")
    records = F.prefix({"Records":d["Records"], "From":e["From"], "Through":e["Through"]})
    scopes, old_round = P.cut(d["Old"])
    native_actor = 1 if stage and r["Phase"] == 0 else actor
    if program_roles is not None and stage and r["Phase"] == 0:
        native_actor = initial[0]
    target = (1 if stage else 3) if r["Partition"] == 3 else 6
    if program_roles is not None:
        target = selected_target
    require((e["Actor"], e["Target"]) == (native_actor, target), "actual P02 source/coordinator/native-target route")
    trace = L.FAULT.lc.normalize(d)["Trace"]
    require(trace["engine"] == "native-raft" and len(trace["steps"]) == 3, "actual two nodes then useful cross-edge")
    names = {row["Logical"]:row for row in trace["identityMapping"]}
    raw_ids = {row["Raw"]:row for row in trace["identityMapping"]}
    operations = {row["Logical"]:row for row in trace["operationMapping"]}
    rounds = {row["Round"]:row["Logical"] for row in operations.values()}
    require(len(names) == len(raw_ids) == len(trace["identityMapping"]) and len(rounds) == len(operations) == (4 if stage else 3) and
            sorted((name, row["Role"], row["Owner"]) for name, row in names.items()) == sorted(tuple(row) for row in L.FAULT.SPEC["identityRoles"]),
            "actual graph ID/original/revision bijection")
    for n, step in enumerate(trace["steps"], 1):
        require(step["Operation"] == ("node-a", "node-b", "edge-create")[n-1], "actual P02 business order")
        L.FAULT.observation(trace, {"Cut":step["Cut"], "View":step["After"], "RawObservation":step["RawObservation"]}, n,
                            (names, raw_ids, operations, rounds))
        L.FAULT.raw_records(trace, step["RawRecords"], n, (names, raw_ids, operations, rounds))
        L.FAULT.logical_group(step, step["Before"], n)
    for observed in (d["Old"], d["Current"], d["Retained"], e["Reopened"], e["PostRetry"]):
        L.FAULT.lc.view(observed["View"], names, operations)
        L.FAULT.lc.native_raw(observed["Native"], observed["View"], raw_ids, rounds)
        L.decoded_observation(observed["Native"], observed["View"], raw_ids)
        P.cut(observed)
    require(type(d["Phases"]) is list and len(d["Phases"]) == 6 and type(d["SourceCaptures"]) is list and len(d["SourceCaptures"]) == 2,
            "actual six replies and two genuine pre-fence planning captures")
    sources = {row["Partition"]:L.source_info(row, scopes, peers[row["Partition"]]) for row in d["SourceCaptures"]}
    require(set(sources) == {3, 8} and all(row["Origin"] == actor for row in d["SourceCaptures"]), "same actual planning caller/partitions")
    statuses, outcomes, commands, manifest = fence_phases(d["Phases"], records, scopes, peers, actor, co, sources)
    original = C.C.decision_manifest(P.raw(trace["steps"][2]["OriginalRequest"]["Payload"]), manifest, co, scopes, d["Old"])
    require(all(original["floors"][part] == sources[part]["floor"] for part in (3, 8)), "both actual planning opens bind the frozen installed floors")
    tx, digest = original["tx"], original["digest"]
    decision = statuses[co, 3]
    require(all(row["tx"] == tx and row["digest"] == digest for row in statuses.values()) and statuses[3, 2]["yes"] and statuses[8, 2]["yes"] and
            all(statuses[key]["commit"] and statuses[key]["round"] == decision["round"] for key in ((co, 3), (3, 4), (8, 4))) and
            decision["round"] > max(statuses[part, 2]["floor"] for part in (3, 8)), "actual complete YES/COMMIT above both apply-time floors")
    require(type(d["Fences"]) is list and len(d["Fences"]) == (2 if stage or r["Schedule"] == "A-before-B-after" else 1), "actual finite fence inventory")
    bound = L.fence(d["Fences"][0])
    fence_planning_floor(stage, sources[3]["floor"], bound)
    require(bound > old_round and all(L.fence(row) == bound and records.get((row["Partition"], row["Index"])) ==
            {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")} for row in d["Fences"]), "actual native fence originals/captured floor")
    require(statuses[3, 2]["floor"] < bound <= statuses[8, 2]["floor"] if r["Schedule"] == "A-before-B-after" else statuses[3, 2]["floor"] >= bound,
            "actual reducer Prepare/fence ordering")
    boundary = e["Boundary"]
    if stage:
        require(type(boundary) is dict and set(boundary) == {"Request", "Reply"} and not any(P.raw(value) for value in boundary.values()), "post-election planning cannot invent a held first reply")
    elif r["Schedule"] == "delayed-A-prepare":
        require(type(boundary) is dict and set(boundary) == {"Request", "Reply"} and boundary["Request"] == d["Phases"][1]["Request"] and
                not P.raw(boundary["Reply"]), "actual SAME-call pending Prepare before effects")
    else:
        require(prepared_vote_read(boundary, d["Phases"][1], scopes, 5, peers[3])["status"] == statuses[3, 2], "actual completed A authority read before B fence")
    selected = P.raw(e["SelectedCommand"])
    require(selected == (P.raw(e["FenceTransition"]["Proposal"]) if stage and r["Phase"] == 0 else commands[r["Phase"], r["Partition"]]), "actual P02 selected native command")
    event = C.batch_pair(e["Arm"], e["Batch"], fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1), e["Target"], r["Kind"], r["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    Q.processes(e["Cohort"])
    if r["Kind"] == 2 and r["Phase"] != 0:
        require(records.get((r["Partition"], event["Index"])) is not None and P.status(P.raw(records[r["Partition"], event["Index"]]["Outcome"])) ==
                P.status(P.raw(records[r["Partition"], statuses[r["Partition"], r["Phase"]]["index"]]["Outcome"])), "selected P02 native application original")
    require(type(e["Calls"]) is list and len(e["Calls"]) == 1, "one actual initial P02 coordinator invocation")
    P.resumed(e["Calls"][0], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 0, outcomes[co, 3])
    call = F.process(P.raw(e["Calls"][0]))
    require(call["code"] == call["mask"] == 0, "native refusal/unknown cannot be called useful success")
    for observed in (d["Retained"], e["Reopened"], e["TargetReopened"]):
        T.semantic(observed == d["Old"], "old-cut:P02-actual-native-replacement-and-cohort")
    T.semantic(e["PostRetry"]["View"] == d["Current"]["View"], "P02-original-replay-current-answer")
    require(type(d["Retries"]) is list and len(d["Retries"]) == 2, "actual before/after-restart original retries")
    for retry in d["Retries"]:
        require(P.raw(retry["Payload"]) == P.raw(trace["steps"][2]["OriginalRequest"]["Payload"]) and retry["Before"]["View"] == retry["After"]["View"],
                "same useful immutable original/input/answer")
        P.resumed(retry["RawResult"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 0, outcomes[co, 3])
    I.charge(e["Capture"], d["Phases"])
    planning = e["Planning"]
    require(type(planning) is dict and set(planning) == {"Output", "Retained"} and planning["Output"] == 2*(8*(4096+8192)+8192) and
            planning["Retained"] == sum(len(P.raw(row["Request"]))+len(P.raw(row["Reply"])) for row in d["SourceCaptures"]), "separate prepaid two-source ownership")
    hold = e["Hold"]
    require(type(hold) is dict and set(hold) == {"Candidates", "Work", "Output", "Retained"}, "actual hold charge grammar")
    if stage:
        require(all(type(value) is int and value == 0 for value in hold.values()), "post-election source cannot borrow a live hold")
    else:
        require(integer(hold["Candidates"], 128, 1)*(4096+32*(2 << 20)) == hold["Work"] and hold["Output"] == 8*(128 << 10)+8192 and
                hold["Retained"] == len(P.raw(boundary["Request"]))+len(P.raw(boundary["Reply"])), "same live hold's spent diagnostic grant")
    require(integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual P02 native diagnostic work")
    initial, current = P.projected(d["Old"]), P.projected(d["Current"])
    recovered = P.projected({"Native":trace["steps"][2]["RawObservation"]})
    transactions = {"edge-create":dict(reads=[P.read_key(initial, key) for key in ("A/presence/1", "B/presence/17", "A/entity/3")],
                                     writes=P.writes(initial, recovered, ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18")))}
    tx_rounds = {"edge-create":decision["round"]}
    if stage:
        check_persisted_fence(e, scopes, original, sources, records, bound, program_roles=program_roles)
        local_round = check_delayed_local(d, records, recovered, current, decision["round"], bound)
        transactions["node-close"] = dict(reads=[P.read_key(recovered, "A/presence/1")], writes=P.writes(recovered, current, ("A/presence/1",)))
        tx_rounds["node-close"] = local_round
    else:
        T.semantic(current == recovered, "P02-live-no-extra-business-effect")
    fence_prefix(records, operations, [d["DelayedLocal"], d["DelayedLocalReplay"]] if stage else [])
    observations = [dict(event=key, scope=["A", "B"], round=P.cut(d[key])[1], values=P.projected(d[key])) for key in ("Old", "Current", "Retained")]
    witness, detail = P.oracle()({"initial":initial, "transactions":transactions}, set(transactions), tx_rounds, observations,
                                [dict(event="final", scope=["A", "B"], values=current)])
    T.semantic(witness is not None, "P02 independent exact serial/cut witness: " + str(detail))
    return dict(case=a["Case"], serial_witness=witness, records=len(records), scope="actual finite P02 native/source/old-cut conjunction; leader/message programs remain distinct")


def coordinator_history(e, records, *, program_roles=None):
    """The original six-operation oracle, with the fresh real outage ordering.

    No altered process rows are passed to the old P03 entry point. Its complete
    semantic readers and pinned independent oracle consume the actual trace;
    this wrapper separately validates its additional second-loss chronology.
    """
    trace = M.FAULT.lc.normalize(e["P03"])
    co, context = M.mapping(trace)
    operations = context[2]
    require(co == e["Recipe"]["Coordinator"], "actual P03 coordinator mapping")
    initial = M.projected(trace["steps"][0]["Before"])
    previous, transactions, observations, inputs = initial, {}, [], {}
    footprints = [
        {"A/entity/node-a", "A/life/life-a1"}, {"B/entity/node-b", "B/life/life-b1"},
        {"A/entity/edge", "A/life/life-edge", "A/posting/node-a/", "A/posting/node-a/life-a1", "B/posting/node-b/", "B/posting/node-b/life-b1"},
        {"A/entity/edge"}, {"A/entity/edge"}, {"A/entity/edge"},
    ]
    for n, step in enumerate(trace["steps"], 1):
        require(M.projected(step["Before"]) == previous, "actual P03 step continuity")
        M.FAULT.fixture_view(step["Before"], n-1)
        M.FAULT.observation(trace, {key:step[key] for key in ("Cut", "RawObservation") } | {"View":step["After"]}, n, context)
        M.FAULT.raw_records(trace, step["RawRecords"], n, context)
        M.FAULT.logical_group(step, step["Before"], n)
        inputs[step["Operation"]] = M.FAULT.request(trace, step["OriginalRequest"], operations[step["Operation"]], context)
        for row in step["RawRecords"]:
            require(records.get((row["Partition"], row["Index"])) == {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")}, "P03 step record missing from actual complete prefix")
        after = M.projected(step["After"])
        changed = {key for key in previous.keys() | after.keys() if previous.get(key) != after.get(key)}
        T.semantic(changed == footprints[n-1], "P03-exact-original-effect-footprint")
        transactions[step["Operation"]] = {"reads":[{"kind":"range", "lo":"A/", "hi":"B0", "values":previous}],
                                            "writes":{key:{"op":"put", "value":after[key]} for key in changed}}
        observations.append({"event":step["Operation"], "scope":["A", "B"], "round":M.cut_round(step["Cut"]), "values":after})
        previous = after
    originals = fence_prefix(records, operations, [], tuple(operations))
    require(len(originals) == 36, "P03 complete six-business-original phase census")
    inventory = trace["rawCounts"]["completeRawRecords"]
    M.FAULT.raw_records(trace, inventory, 6, context)
    require(type(inventory) is list and len(inventory) == len(records) and
            {(row["Partition"], row["Index"]):{key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")} for row in inventory} == records,
            "complete qualified native no-op/original sidecars join every streamed physical entry")
    fault, cohort = trace["faults"]
    require([row["Case"] for row in trace["faults"]] == ["coordinator-leader-loss-after-commit", "same-volume-reopen"], "actual P03 and full cohort")
    commit, killed, rejoined = fault["ProcessEvidence"]
    for row in [commit, killed, rejoined, *cohort["ProcessEvidence"]]:
        M.FAULT.process_event(row, "native-raft")
    leader = 1 if co == 3 else 4
    if program_roles is not None:
        leader = program_coordinator_roles(program_roles, e["Recipe"])[1]
    require([row["Action"] for row in (commit, killed, rejoined)] == ["commit", "kill", "reopen"] and
            commit["Member"] == killed["Member"] == rejoined["Member"] == leader and
            commit["EventID"] < killed["EventID"] < rejoined["EventID"] and
            rejoined["PreviousInstance"] == killed["Instance"] and rejoined["Volume"] == killed["Volume"] and
            e["BeforeRecovery"] == killed and fault["CallerClassification"] == "pending" and
            fault["CallerRaw"] == e["OriginalPending"] and fault["MarkerRaw"] == e["Boundary"]["Reply"], "real durable-D leader kill/reopen and original caller identity")
    op = operations["postcommit-update"]
    M.FAULT.coordinates(commit["Result"], op)
    captured = M.FAULT.lc.parse(P.raw(commit["Raw"]))
    require(captured["Request"] == e["Boundary"]["Request"] and captured["Response"] == e["Boundary"]["Reply"], "actual committed source reply capture")
    voters = M.FAULT.lc.parse(P.raw(captured["VoterRecords"]))
    voter_first = (1 if co == 3 else 4) if program_roles is not None else leader
    require([row["Member"] for row in voters] == list(range(voter_first, voter_first+3)), "all three original D voters before leader death")
    decision_tx = bytes.fromhex(op["Transaction"])
    decision = originals[decision_tx, co, 3]
    decision_wire = P.raw(records[co, decision["index"]]["Outcome"])
    for row in voters:
        actual = F.process(P.raw(row["Record"]))
        require(actual["outcome"] == decision_wire and not actual["changes"] and actual["applied"] >= decision["index"] and
                actual["stats"][1] >= decision["index"] and actual["stats"][4] >= decision["index"], "actual immutable D voter record/checkpoint/commit")
    for retry in [fault["Retry"], *(fault["ReopenRetries"] or [])]:
        M.FAULT.replay(trace, retry, 5, op, inputs["postcommit-update"], context)
    outage = e["Recipe"]["Phase"] == 4 and e["Recipe"]["Partition"] == co
    require((rejoined["EventID"] < fault["Retry"]["EventID"] if outage else killed["EventID"] < fault["Retry"]["EventID"] < rejoined["EventID"]),
            "P03 recovery order must reflect actual quorum loss, never a success during outage")
    require(type(cohort["ProcessEvidence"]) is list and len(cohort["ProcessEvidence"]) == 12, "six stopped then reopened processes")
    stops, starts = cohort["ProcessEvidence"][:6], cohort["ProcessEvidence"][6:]
    require({row["Member"] for row in stops} == {row["Member"] for row in starts} == set(range(1, 7)) and
            all(row["Action"] == "stop" for row in stops) and all(row["Action"] == "reopen" for row in starts) and
            max(row["EventID"] for row in stops) < min(row["EventID"] for row in starts), "complete actual cohort barrier")
    for start in starts:
        stop = next(row for row in stops if row["Member"] == start["Member"])
        require(start["PreviousInstance"] == stop["Instance"] and start["Volume"] == stop["Volume"], "same-volume cohort identity")
    require(len(cohort["ReopenRetries"]) == 6, "all six immutable originals after restart")
    for retry, step in zip(cohort["ReopenRetries"], trace["steps"]):
        require(retry["Operation"] == step["Operation"], "ordered actual original replays")
        M.FAULT.replay(trace, retry, 6, operations[step["Operation"]], inputs[step["Operation"]], context)
    expected = {(trace["steps"][i-1]["Operation"], "while-coordinator-down") for i in (3, 4)} | {(trace["steps"][i-1]["Operation"], "after-cohort-reopen") for i in range(1, 6)}
    require(len(trace["oldReads"]) == 7 and {(row["Operation"], row["Checkpoint"]) for row in trace["oldReads"]} == expected, "complete actual old-cut inventory")
    for row in trace["oldReads"]:
        n = next(i+1 for i, step in enumerate(trace["steps"]) if step["Operation"] == row["Operation"])
        step = trace["steps"][n-1]
        T.semantic(row["Cut"] == step["Cut"] and row["View"] == step["After"], "P03-retained-cut-exact-data-and-index")
        require(killed["EventID"] < row["EventID"] < rejoined["EventID"] if row["Checkpoint"] == "while-coordinator-down" else row["EventID"] > max(start["EventID"] for start in starts), "actual old-read timing")
        M.FAULT.observation(trace, row, n, context)
        observations.append({"event":row["Checkpoint"]+row["Operation"], "scope":["A", "B"], "round":M.cut_round(row["Cut"]), "values":M.projected(row["View"])})
    require(hashlib.sha256(M.ORACLE.read_bytes()).hexdigest() == M.ORACLE_SHA, "reviewed independent serial oracle")
    witness, detail = M.module("campaign_p03_serial", M.ORACLE).serial_witness({"initial":initial, "transactions":transactions}, set(transactions),
            {name:op["Round"] for name, op in operations.items()}, observations, [{"event":"final", "scope":["A", "B"], "values":previous}])
    T.semantic(witness is not None, "P03-independent-serial-cut-witness:"+str(detail))
    return trace, context, originals, witness


def program_coordinator_roles(context, recipe):
    require(type(context) is dict and set(context) == {"initial_leaders", "leaders", "target"}, "authenticated P03 actual-role context")
    initial, current = context["initial_leaders"], context["leaders"]
    require(type(initial) is tuple and initial in ((1, 4), (2, 6)) and all(type(v) is int for v in initial), "actual P03 initial leader layout")
    co, phase, part = recipe["Coordinator"], recipe["Phase"], recipe["Partition"]
    group = 0 if co == 3 else 1
    leader = initial[group]
    caller, actor = ((2, 3) if initial == (1, 4) else (3, 1)) if co == 3 else ((5, 6) if initial == (1, 4) else (5, 4))
    expected = list(initial)
    if phase == 4:
        expected[group] = caller
    require(type(current) is tuple and current == tuple(expected) and all(type(v) is int for v in current), "actual durable-D successor precedes only selected recovery phases")
    first = 1 if part == 3 else 4
    target = caller if phase == 4 and part == co else next(v for v in range(first, first+3) if v != initial[0 if part == 3 else 1] and v != caller)
    require(type(context["target"]) is int and context["target"] == target, "actual P03 caller/recoverer/selected voter remain distinct where required")
    return initial, leader, caller, actor, target


def check_coordinator(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    require(set(e) == COORDINATOR_EVIDENCE | ({"Program"} if program_roles is not None else set()) and a["Case"] == coordinator_recipe(r), "actual finite P03 route")
    co, leader, caller, actor = r["Coordinator"], 1 if r["Coordinator"] == 3 else 4, 2 if r["Coordinator"] == 3 else 5, 3 if r["Coordinator"] == 3 else 6
    target = (caller if r["Partition"] == co else 3 if r["Partition"] == 3 else 6) if r["Phase"] == 4 else 3 if r["Partition"] == 3 else 6
    if program_roles is not None:
        initial_leaders, leader, caller, actor, target = program_coordinator_roles(program_roles, r)
    require((e["Actor"], e["Target"], e["RecoveryActor"]) == (actor if r["Phase"] == 4 else caller, target, actor), "actual P03 surviving coordinator and source target")
    selected = P.raw(e["SelectedCommand"])
    event = C.batch_pair(e["Arm"], e["Batch"], fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1), target, r["Kind"], r["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    require(type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096 and
            integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual selected pipe loss and diagnostic funding")
    records = F.prefix(e)
    trace, context, originals, witness = coordinator_history(e, records, program_roles=program_roles)
    op = context[2]["postcommit-update"]
    tx = bytes.fromhex(op["Transaction"])
    decision = originals[tx, co, 3]
    wire = P.raw(records[co, decision["index"]]["Outcome"])
    prior = trace["steps"][3]
    old = {"Cut":prior["Cut"], "View":prior["After"], "Native":prior["RawObservation"]}
    scopes, _ = P.cut(old)
    decoded = C.C.decision_command(selected)
    require((decoded["phase"], decoded["partition"]) == (r["Phase"], r["Partition"]) and
            P.body(decoded["manifest"], b"GCM\1")[:16] == tx, "selected actual P03 command/qualified original")
    original = C.C.decision_manifest(P.raw(trace["steps"][4]["OriginalRequest"]["Payload"]), decoded["manifest"], co, scopes, old)
    require(original["tx"] == tx and original["digest"] == decision["digest"], "whole actual business/manifest/configuration identity")
    T.semantic(e["TargetReopened"] == old, "P03-same-directory-old-data-index")
    killed = e["BeforeRecovery"]
    first_rejoin = trace["faults"][0]["ProcessEvidence"][2]
    native_kill, native_rejoin = e["TargetProcesses"]
    if r["Phase"] == 4:
        require(killed["EventID"] < native_kill["EventID"] < native_rejoin["EventID"], "selected recovery occurrence follows real durable-D leader loss")
    else:
        require(native_kill["EventID"] < native_rejoin["EventID"] < trace["faults"][0]["ProcessEvidence"][0]["EventID"] < killed["EventID"], "early native occurrence rejoins before all-voter D witness and leader death")
    initial, recovery = e["InitialPhases"], e["RecoveryPhases"]
    require(type(initial) is list and len(initial) == 4 and type(recovery) is list and 1 <= len(recovery) <= 16, "actual first R/Prepare/Prepare/D and recovery attempts")
    selected_rows, correlations = [], set()
    outage = r["Phase"] == 4 and r["Partition"] == co
    for rows, origin in ((initial, caller), (recovery, actor)):
        peers = {3:(1,), 8:(4,)} if program_roles is None else dict(zip((3, 8), ((v,) for v in initial_leaders)))
        if rows is recovery:
            peers[co] = (caller, leader) if outage else (caller,)
        for i, row in enumerate(rows):
            command, actual, correlation = C.source_request(row, scopes, origin)
            require(actual["manifest"] == decoded["manifest"] and correlation not in correlations, "same original and unique real source correlation")
            correlations.add(correlation)
            if rows is initial:
                require((actual["phase"], actual["partition"]) == ((1, co), (2, 3), (2, 8), (3, co))[i] and P.raw(row["Reply"]), "actual first invocation phase order")
            if command == selected:
                selected_rows.append(row)
            for proof in actual["proof_wires"]:
                status = P.status(proof)
                expected = originals.get((tx, status["partition"], status["phase"]))
                require(expected is not None and status == (dict(expected, floor=0) if status["phase"] == 1 else expected),
                        "actual source phase proof joins immutable original, never nominal DTO authority")
            if not P.raw(row["Reply"]):
                require(rows is recovery and command == selected and r["Phase"] == 4, "only the genuinely selected recovery call may be unanswered")
                continue
            response = source_reply(P.raw(row["Reply"]))
            status = P.status(response["body"])
            expected = originals[tx, status["partition"], status["phase"]]
            require(response["kind"] == 8 and (row["Partition"], response["sequence"], response["nonce"]) == correlation and
                    response["scope"] == scopes[row["Partition"]] and response["voter"] in peers[row["Partition"]] and
                    status == (dict(expected, floor=0) if actual["phase"] == status["phase"] == 1 else expected), "actual returned projection/current original versus retained phase")
    require(1 <= len(selected_rows) <= 2 and (len(selected_rows) == 1 or not P.raw(selected_rows[0]["Reply"]) and P.raw(selected_rows[1]["Reply"])) and
            e["Boundary"] == {key:initial[3][key] for key in ("Request", "Reply")}, "actual first selected source call, optional SAME-original retry, and same held durable-D reply")
    if r["Kind"] == 2:
        installed = records.get((r["Partition"], event["Index"]))
        require(installed is not None and P.status(P.raw(installed["Outcome"])) == originals[tx, r["Partition"], r["Phase"]], "selected application install joins actual original")
    P.resumed(e["OriginalPending"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 1, b"")
    original_pending = F.process(P.raw(e["OriginalPending"]))
    require(original_pending["code"] in (3, 6) and original_pending["sequence"] > 0, "actual canceled original pending, never invalid/limit/corrupt")
    calls = e["RecoveryCalls"]
    require(type(calls) is list and len(calls) == (2 if r["Phase"] == 4 else 1), "actual original recovery invocations")
    for i, call in enumerate(calls):
        require(type(call) is dict and set(call) == {"Reply", "Transport", "Pending"} and type(call["Pending"]) is bool, "actual recovery call grammar")
        response = F.process(P.raw(call["Reply"]))
        if call["Pending"]:
            require(i == 0 and r["Phase"] == 4, "only selected recovery may be pending")
            C.unknown_invocation(response, call["Transport"])
        else:
            require(response["code"] == response["mask"] == 0 and not call["Transport"], "real successful original recovery")
        P.resumed(call["Reply"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), int(call["Pending"]), b"" if call["Pending"] else wire)
    require(calls[-1]["Pending"] is False and (bool(P.raw(e["OutagePending"])) == outage), "no source-success invention during lost quorum")
    if outage:
        require(calls[0]["Pending"] is True and e["OutagePending"] == calls[0]["Reply"] and
                killed["Member"] != native_kill["Member"] and native_kill["EventID"] < first_rejoin["EventID"] < native_rejoin["EventID"], "two distinct losses, actual pending, then owned-volume quorum reconstruction")
    I.charge(e["InitialCapture"], initial)
    I.charge(e["RecoveryCapture"], recovery)
    return dict(case=a["Case"], records=len(records), serial_witness=witness, scope="P03 actual D and applicable native occurrence only; broader D remains distinct")


def program_partial_roles(context, recipe):
    require(type(context) is dict and set(context) == {"initial_leaders", "leaders", "target"}, "authenticated actual partial-install role context")
    initial, leaders = context["initial_leaders"], context["leaders"]
    require(type(initial) is tuple and initial in ((1, 4), (2, 6)) and all(type(v) is int for v in initial), "actual initial partial sources")
    outage = recipe["Phase"] == 4 and recipe["Partition"] == 8
    require(type(leaders) is tuple and leaders == (initial[0], 5 if outage else initial[1]) and all(type(v) is int for v in leaders) and
            type(context["target"]) is int and context["target"] == (3 if recipe["Partition"] == 3 else 5),
            "actual native member and causal B successor, no relabelled endpoint")
    return initial, 6 if initial == (1, 4) else 4, context["target"]


def check_partial(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    require(set(e) == PARTIAL_EVIDENCE | ({"Program", "ProgramCohort"} if program_roles is not None else set()) and a["Case"] == partial_recipe(r), "finite P03 partial-install route")
    actor, source_roles = 6, None
    if program_roles is not None:
        initial_leaders, actor, _ = program_partial_roles(program_roles, r)
        source_roles = dict(leaders=initial_leaders)
    d = e["Partial"]
    require(type(d) is dict and set(d) == PARTIAL and d["Version"] == 1 and d["Profile"] == C.C.PROFILE and
            d["Case"] == "partial-install-decision-loss" and d["IO"] is None and
            not d["PacketEvents"] and d["Refused"] is None and d["RefusedReplay"] is None and
            (e["OriginalActor"], e["Reader"], e["RecoveryActor"], e["Actor"]) == (actor, 2, actor, actor),
            "actual partial original, independent certification reader and surviving private owner")
    selected = P.raw(e["SelectedCommand"])
    command = C.C.decision_command(selected)
    target = 3 if r["Partition"] == 3 else 5
    require(e["Target"] == target and (command["phase"], command["partition"]) == (r["Phase"], r["Partition"]) and
            command["manifest"] == P.raw(d["Manifest"]), "actual selected phase belongs to the SAME partial original")
    event = C.batch_pair(e["Arm"], e["Batch"], fixed(e["Run"], 16, True),
                         integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1), target, r["Kind"], r["Side"], selected)
    # Authenticate both real loss/reopen rosters before lending them to any
    # semantic reader. These are separate actual processes, never DTO recipes.
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    C.C.processes(d["Processes"], None, d["Case"], program_roles=source_roles)
    require(type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096 and
            integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual native loss and separately prepaid diagnostics")
    killed, rejoined = d["Processes"]
    native_kill, native_rejoin = e["TargetProcesses"]
    outage = r["Phase"] == 4 and r["Partition"] == 8
    require((e["Outage"] is not None) is outage, "real second-voter outage only on recovery B Resolve")
    if outage:
        require(killed["EventID"] < native_kill["EventID"] < rejoined["EventID"] < native_rejoin["EventID"] and
                killed["Member"] != native_kill["Member"], "two actual losses precede quorum reconstruction")
    else:
        require(native_kill["EventID"] < native_rejoin["EventID"] < killed["EventID"] < rejoined["EventID"],
                "earlier native occurrence rejoins while original partial caller/reply remain live")
    records = F.prefix(d)
    original = C.C.decision_manifest(P.raw(d["Original"]), command["manifest"], 8, P.cut(d["Before"])[0], d["Before"])
    tx, scopes = original["tx"], P.cut(d["Before"])[0]
    decision = None
    for record in records.values():
        raw = P.raw(record["Outcome"])
        if raw[:4] == b"GCS\1":
            status = P.status(raw)
            if status["tx"] == tx and status["phase"] == 3:
                require(decision in (None, raw), "immutable first decision changed")
                decision = raw
    require(decision is not None, "actual durable decision missing")
    if r["Kind"] == 2:
        installed = records.get((r["Partition"], event["Index"]))
        require(installed is not None, "selected application entry missing")
        status = P.status(P.raw(installed["Outcome"]))
        require((status["tx"], status["digest"], status["phase"], status["partition"]) ==
                (tx, original["digest"], r["Phase"], r["Partition"]), "actual native install joins selected original")
    pending = F.process(P.raw(d["PendingCaller"]))
    require((pending["code"], pending["mask"]) == (6, 0), "real canceled unknown original, never arbitrary native refusal")
    response = F.process(P.raw(e["Completed"]))
    require(response["code"] == response["mask"] == 0 and response["incarnation"] == pending["incarnation"] and
            response["sequence"] > pending["sequence"], "SAME surviving original owner completed after recovery")
    P.resumed(e["Completed"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], 8), 0, decision)
    if outage:
        call = e["Outage"]
        require(type(call) is dict and set(call) == {"Reply", "Transport", "Pending"} and call["Pending"] is True, "actual outage call grammar")
        unknown = F.process(P.raw(call["Reply"]))
        C.unknown_invocation(unknown, call["Transport"])
        require(unknown["incarnation"] == pending["incarnation"] and pending["sequence"] < unknown["sequence"] < response["sequence"],
                "same private owner pending while its two voters are absent")
        P.resumed(call["Reply"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], 8), 1, b"")
    T.semantic(e["TargetReopened"] == d["Before"], "P03-partial-native-volume-old-cut-and-index")
    # Reuse the unchanged raw original/CDC/typed graph and independent serial
    # checker only after all fresh native/process bindings above authenticate.
    if program_roles is None:
        result = C.C.check_history(d, semantic_require=T.semantic)
    else:
        result = C.C.check_history(d, semantic_require=T.semantic, program_roles=source_roles,
                                 process_check=lambda rows, io, case: C.C.processes(rows, io, case, program_roles=source_roles))
    return dict(case=a["Case"], records=len(records), serial_witness=result["serial_witness"],
                scope="P03 actual partial interval plus selected native transition; no full-D acceptance")


def conditional_charge(charge, rows):
    require(type(charge) is dict and set(charge) == {"Candidates", "Attempts", "Work", "Output", "Retained"}, "actual cumulative P05 observation ledger")
    candidates = integer(charge["Candidates"], 2*(4096+96*4*1024), 1)
    require(integer(charge["Attempts"], 128, 1) == len(rows) <= candidates, "finite actual selected full-frame count")
    lengths = [len(P.raw(row["Request"]))+len(P.raw(row["Reply"])) for row in rows]
    headers = 128*64+2*64
    require(all(0 < n <= 2*(2 << 20) for n in lengths) and
            sum(4096+32*n for n in lengths) <= integer(charge["Work"], candidates*(4096+64*(2 << 20)), 1) and
            headers+sum(8192+8*n for n in lengths) <= integer(charge["Output"], headers+candidates*(8192+16*(2 << 20)), 1) and
            charge["Retained"] == headers+sum(64+n for n in lengths), "full traversal plus exact owned copies and prepaid JSON (including unretained Register-gate observations), no reset across reopen")


def program_conditional_roles(context, recipe):
    require(type(context) is dict and set(context) == {"leaders", "target", "reopen_after"}, "authenticated actual conditional program roles")
    leaders = C.C.program_leaders(dict(leaders=context["leaders"]))
    actor = 4 if recipe["Schedule"] == "write-skew-B-first" else 1
    target = 3 if recipe["Partition"] == 3 else 6 if leaders == (1, 4) else 5
    boundary = context["reopen_after"]
    require(type(context["target"]) is int and context["target"] == target and type(boundary) is int and
            (1 <= boundary <= 128 if recipe["Schedule"] == "write-skew-A-first" else boundary == 0),
            "actual surviving conditional caller/source and sole cohort capture boundary")
    return leaders, actor, target, dict(leaders=leaders, reopen_after=boundary)


def check_conditional(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    extra = ({"Program", "ProgramCohort"} | ({"ProgramReopenAfter"} if r["Schedule"] == "write-skew-A-first" else set())) if program_roles is not None else set()
    require(set(e) == CONDITIONAL_EVIDENCE | extra and a["Case"] == conditional_recipe(r), "finite actual P05 route")
    d = e["P05"]
    require(type(d) is dict and set(d) == CONDITIONAL and d["Schedule"] == r["Schedule"] and
            e["TargetReopened"] is None, "no invented third-source/local-history observation")
    actor, target = (4 if r["Schedule"] == "write-skew-B-first" else 1), (3 if r["Partition"] == 3 else 6)
    sources = None
    if program_roles is not None:
        _, actor, target, sources = program_conditional_roles(program_roles, r)
    require((e["Actor"], e["Target"]) == (actor, target), "actual source/caller remains live while selected follower dies")
    selected = P.raw(e["SelectedCommand"])
    command = C.C.decision_command(selected)
    require((command["phase"], command["partition"]) == (r["Phase"], r["Partition"]), "selected actual conditional native command")
    event = C.batch_pair(e["Arm"], e["Batch"], fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1),
                         target, r["Kind"], r["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    require(type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096 and
            integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual selected loss and prepaid diagnostic work")
    require(e["Records"] == d["Records"], "actual P05 trace and main EOF prefix mismatch")
    prefix = F.prefix(e)
    scopes, _, _ = N.cut(d["Cut"])
    manifests, _, _, _ = N.frames(d["Phases"], scopes, program_sources=sources)
    retained, decisions = N.records(d["Records"], manifests)
    require(retained == prefix and command["manifest"] in [row[0] for row in manifests.values()], "selected command belongs to actual complete retained source prefix")
    m = N.manifest(command["manifest"])
    if r["Schedule"] == "authority-fence-before-prepare":
        require(m["condition"] == P.raw(d["Condition"]) and m["tx"] in decisions and not decisions[m["tx"]]["commit"],
                "actual conditional original rejected by changed participant prefix")
    else:
        require(type(d["Steps"]) is list and len(d["Steps"]) >= r["Operation"], "selected actual useful operation missing")
        result = N.result(P.raw(d["Steps"][r["Operation"]-1]["Result"]), scopes)
        require(result["tx"] == m["tx"] and result["business"] == m["business"] and result["decision"] == decisions[m["tx"]],
                "occurrence names actual immutable business original, never a later substitute")
    if r["Kind"] == 2:
        installed = prefix.get((r["Partition"], event["Index"]))
        require(installed is not None, "actual selected application install absent")
        status = P.status(P.raw(installed["Outcome"]))
        require((status["tx"], status["digest"], status["phase"], status["partition"]) ==
                (m["tx"], m["digest"], r["Phase"], r["Partition"]), "native install joins same original and command phase")
    conditional_charge(e["Capture"], d["Phases"])
    require(all(not attempt.get("NativePending", False) for attempt in d["Attempts"]), "surviving source quorum must not relabel unknown/error as completed dependency scenario")
    processes = d["Processes"] or []
    require(len(processes) == (12 if r["Schedule"] == "write-skew-A-first" else 0), "only genuine declared complete cohort replacement")
    if processes:
        stops, starts = processes[:6], processes[6:]
        require({row["Member"] for row in stops} == {row["Member"] for row in starts} == set(range(1, 7)) and
                all(row["Action"] == "stop" for row in stops) and all(row["Action"] == "reopen" and row["Ready"] for row in starts) and
                max(row["EventID"] for row in stops) < min(row["EventID"] for row in starts), "real cohort stop/reopen barrier")
        for start in starts:
            stop = next(row for row in stops if row["Member"] == start["Member"])
            require(start["PreviousInstance"] == stop["Instance"] and start["Volume"] == stop["Volume"], "actual same-volume conditional cohort")
    result = N.check(d, program_sources=sources) # Existing exact absent/range/temporal/life literals and independent serial witness.
    return dict(case=a["Case"], records=len(prefix), serial_witness=result["serial_witness"],
                scope="P05 five genuine dependency scenarios plus selected native transition; source-only")


def manifest_fields(wire):
    """Inert bounded structure, also used for deliberately invalid probes.

    Successful originals are separately authenticated by decision_manifest;
    parsing a hostile but checksummed body cannot confer private authority.
    """
    c = P.Cursor(P.body(wire, b"GCM\1"))
    head = (c.take(16), c.number(), c.field(), c.take(128))
    count = c.number(4)
    require(1 <= count <= 2, "finite hostile participant observation")
    parts = [(c.number(), c.number(), c.field(64 << 10)) for _ in range(count)]
    c.end()
    return head, parts


def frozen_identity(receipt, d, prefix, scopes, *, member=4):
    require(type(receipt) is dict and set(receipt) == set("Version Cookie Member PID GrantBytes Manifest BeforeApplied AfterApplied BeforeLastIndex AfterLastIndex Before After Attempts".split()) and
            receipt["Version"] == 1 and receipt["Cookie"] == d["Queue"]["Cookie"] and
            receipt["Member"] == member and receipt["PID"] == d["Queue"]["PID"] and receipt["Manifest"] == d["Queue"]["Manifest"],
            "actual child retained-B frozen-identity owner")
    manifest, foreign = P.raw(receipt["Manifest"]), P.raw(d["ManifestA"])
    head, parts = manifest_fields(manifest)
    foreign_head, foreign_parts = manifest_fields(foreign)
    require(head[0] != foreign_head[0] and [p[0] for p in parts] == [p[0] for p in foreign_parts] == [3, 8] and parts[1][2] != foreign_parts[1][2] and
            P.body(parts[1][2], b"GPK\1")[:16] == head[0] and P.body(foreign_parts[1][2], b"GPK\1")[:16] == foreign_head[0] and
            receipt["GrantBytes"] == 12*(64 << 10)+8192+32*(len(manifest)+len(foreign)), "genuine distinct original kernels and bounded prepaid hostile decode/read/output")
    applied = integer(receipt["BeforeApplied"], (1 << 64)-1, 1)
    last = integer(receipt["BeforeLastIndex"], (1 << 64)-1, applied)
    require(receipt["AfterApplied"] == applied and receipt["AfterLastIndex"] == last and
            last <= d["Queue"]["BeforeLastIndex"] and receipt["Before"] == receipt["After"], "refusals changed neither native entry prefix nor frozen controls")
    controls = receipt["Before"]
    require(type(controls) is list and len(controls) == 4 and [row["Tag"] for row in controls] == list(b"RPKD"), "actual fixed control census")
    for row in controls:
        require(type(row) is dict and set(row) == {"Tag", "Found", "Index", "Value"} and type(row["Found"]) is bool, "actual native control row")
        if row["Tag"] == ord("D"):
            require(not row["Found"] and row["Index"] == 0 and not P.raw(row["Value"]), "pre-queue decision must actually be absent")
            continue
        require(row["Found"] and integer(row["Index"], applied, 1), "genuine retained R/P/K index")
        if row["Tag"] == ord("R"):
            require(P.raw(row["Value"]) == manifest, "frozen complete participant manifest changed")
            status = P.status(P.raw(prefix[8, row["Index"]]["Outcome"]))
            require(status["phase"] == 1 and status["tx"] == head[0], "R joins actual original register")
        else:
            status = P.status(P.raw(prefix[8, row["Index"]]["Outcome"]))
            require(status["phase"] == 2 and status["yes"] and status["tx"] == head[0], "P/K join actual successful retained Prepare")
            require(P.raw(row["Value"]) == (P.raw(prefix[8, row["Index"]]["Outcome"]) if row["Tag"] == ord("P") else parts[1][2]), "frozen actual vote/kernel bytes changed")
    attempts = receipt["Attempts"]
    require(type(attempts) is list and len(attempts) == 3 and [row["Kind"] for row in attempts] ==
            ["missing-participant", "foreign-positive-ownership", "foreign-real-kernel"], "actual hostile admission inventory")
    for i, row in enumerate(attempts):
        require(type(row) is dict and set(row) == {"Kind", "Target", "Command", "ErrorMask"} and row["ErrorMask"] == (4 if i == 2 else 1),
                "exact known invalid/corrupt admission, never native limit/unknown")
        target = P.body(P.raw(row["Target"]), b"APS\1")
        if i == 1:
            require(target[:40] == scopes[8][:40] and target[48:] == scopes[8][48:] and
                    int.from_bytes(target[40:48], "big") == int.from_bytes(scopes[8][40:48], "big")+1, "actual positive foreign ownership target")
        else:
            require(target == scopes[8], "actual local source target for hostile participant/kernel")
        command = C.C.decision_command(P.raw(row["Command"]))
        require(command["phase"] == 1 and command["partition"] == 8 and not command["proofs"] and command["conflict"] is None, "hostile Register has no manufactured authority")
        observed_head, observed_parts = manifest_fields(command["manifest"])
        require(observed_head == head and observed_parts == (parts[:1] if i == 0 else [parts[0], foreign_parts[1]] if i == 2 else parts),
                "actual shortened/foreign-epoch/real-kernel mutant, no arbitrary hostile substitute")


def program_frozen_roles(context, recipe):
    require(type(context) is dict and set(context) == {"leaders", "target"}, "authenticated native frozen-owner program roles")
    leaders = C.C.program_leaders(dict(leaders=context["leaders"]))
    actor = 3 if recipe["Original"] == "A" else leaders[1]
    target = (2 if recipe["Partition"] == 3 else 6) if leaders == (1, 4) else (1 if recipe["Partition"] == 3 else 4)
    require(type(context["target"]) is int and context["target"] == target and actor != target,
            "actual native follower loss preserves current B queue's private owner")
    return leaders, actor, target


def check_frozen(a, *, program_roles=None):
    e, r = a["Evidence"], a["Evidence"]["Recipe"]
    require(set(e) == FROZEN_EVIDENCE | ({"Program", "ProgramCohort"} if program_roles is not None else set()) and a["Case"] == frozen_recipe(r), "finite actual P04 native/frozen recipe")
    d = e["P04"]
    require(type(d) is dict and set(d) == QUEUED and d["Case"] == r["Order"], "actual two queued original order")
    actor, target = 3 if r["Original"] == "A" else 4, 2 if r["Partition"] == 3 else 6
    source_roles, member = None, 4
    if program_roles is not None:
        leaders, actor, target = program_frozen_roles(program_roles, r)
        source_roles, member = dict(leaders=leaders), leaders[1]
    require((e["Actor"], e["Target"]) == (actor, target), "original caller and authority remain alive while a real follower dies")
    selected = P.raw(e["SelectedCommand"])
    command = C.C.decision_command(selected)
    manifest = P.raw(d["ManifestA"] if r["Original"] == "A" else d["Queue"]["Manifest"])
    require((command["phase"], command["partition"], command["manifest"]) == (r["Phase"], r["Partition"], manifest), "actual selected native entry belongs to exact frozen original")
    if r["Candidate"]:
        require(selected == P.raw(d["Queue"]["Candidates"][0 if r["Candidate"] == "capacity-ABORT" else 1]), "selected Decide is actual retained private candidate, never reconstructed authority")
    event = C.batch_pair(e["Arm"], e["Batch"], fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1), target, r["Kind"], r["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    C.C.processes(d["Processes"], None, "partial-install-decision-loss", program_roles=source_roles)
    require(e["TargetProcesses"][1]["EventID"] < d["Processes"][0]["EventID"], "selected native process rejoins before genuine owner death/reopen")
    require(type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096 and
            integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual matched native transport loss and diagnostic admission")
    prefix = F.prefix(d)
    scopes, _ = P.cut(d["Before"])
    frozen_identity(e["Frozen"], d, prefix, scopes, member=member)
    charge, boundary = e["CapacityCapture"], d["Capacity"]
    size = len(P.raw(boundary["Request"]))+len(P.raw(boundary["Reply"]))
    require(type(charge) is dict and set(charge) == {"Candidates", "Work", "Output", "Retained"} and
            0 < size <= 128 << 10 and 4096+32*size <= integer(charge["Work"], integer(charge["Candidates"], 128, 1)*(4096+64*(2 << 20)), 1) and
            charge["Output"] == 8192+8*size and charge["Retained"] == size,
            "complete actual capacity request/reply funded before full decode/copy")
    if r["Kind"] == 2:
        installed = prefix.get((r["Partition"], event["Index"]))
        require(installed is not None, "actual selected application entry missing")
        status = P.status(P.raw(installed["Outcome"]))
        require(status["tx"] == manifest_fields(manifest)[0][0] and status["phase"] == r["Phase"] and
                status["digest"] == hashlib.sha256(manifest).digest() and status["index"] <= event["Index"], "actual installed original/current replay joins native entry")
    T.semantic(e["TargetReopened"] == d["Before"], "P04-replacement-local-old-cut-data-index")
    result = C.C.decision_check(d, program_roles=source_roles) # Full two-original/first-D/CDC/exact graph/index and serial checks.
    return dict(case=a["Case"], records=len(prefix), serial_witness=result["serial_witness"], scope="P04 actual OS-child frozen refusals and queue order plus selected native transition; source-only")


def check(a):
    require(type(a) is dict and set(a) == {"Version", "Profile", "Case", "Evidence"} and type(a["Version"]) is int and
            a["Version"] == 1 and a["Profile"] == PROFILE, "fresh actual protocol campaign profile")
    require(type(a["Evidence"]) is dict, "actual protocol evidence")
    if "P04" in a["Evidence"]:
        return check_frozen(a)
    if "P05" in a["Evidence"]:
        return check_conditional(a)
    if "Partial" in a["Evidence"]:
        return check_partial(a)
    if "P03" in a["Evidence"]:
        return check_coordinator(a)
    if "P02" in a["Evidence"]:
        return check_fence(a)
    if "P01" in a["Evidence"]:
        return check_scope(a)
    return check_live(a)


def program_live_roles(context):
    require(type(context) is dict and set(context) == {"initial_leaders", "leaders", "target", "successor"}, "authenticated first-live elected-role context")
    initial, leaders = context["initial_leaders"], context["leaders"]
    require(type(initial) is tuple and initial in ((1, 4), (2, 6)) and all(type(v) is int for v in initial) and
            type(leaders) is tuple and leaders == (3-initial[0], initial[1]) and all(type(v) is int for v in leaders) and
            type(context["target"]) is type(context["successor"]) is int and context["target"] == initial[1] and
            context["successor"] == 10-initial[1], "SAME live caller survives actual A then B source leader losses")
    return initial, leaders, context["target"], context["successor"]


def check_live(a, *, program_roles=None):
    e = a["Evidence"]
    require(type(e) is dict and set(e) == EVIDENCE | ({"Program", "ProgramCohort"} if program_roles is not None else set()) and a["Case"] == live_recipe(e["Recipe"]), "actual causal inventory route")
    r, completion = e["Recipe"], e["Completion"]
    require(type(completion) is dict and set(completion) == COMPLETION, "complete same-original recovery grammar")
    d = completion["P06"]
    co, commit, actor = r["Coordinator"], r["Commit"], 3 if r["Coordinator"] == 3 else 5
    first, recovery_a, target, recovery_b = 1, 2, 4, 6
    if program_roles is not None:
        initial, leaders, target, recovery_b = program_live_roles(program_roles)
        first, recovery_a = initial[0], leaders[0]
    require(type(d) is dict and set(d) == C.P06 and type(d["Version"]) is int and d["Version"] == 1 and
            (d["Coordinator"], d["Commit"]) == (co, commit) and (e["Actor"], e["Target"], e["Successor"]) == (actor, target, recovery_b) and
            type(completion["RecoveryBVoter"]) is int and completion["RecoveryBVoter"] == recovery_b, "actual surviving private owner/native routes")
    scopes, _ = P.cut(d["Before"])
    first_manifest, manifest, yes, yes_wire = P.peer_phase(P.raw(d["FirstYES"]["Request"]), P.raw(d["FirstYES"]["Reply"]), scopes, first)
    require(first_manifest == P.raw(d["Manifest"]) and yes["partition"] == 3 and yes["phase"] == 2 and yes["yes"], "actual first A Submit YES")
    original = C.C.decision_manifest(P.raw(d["Original"]), manifest, co, scopes, d["Before"])
    tx, digest = original["tx"], original["digest"]
    records, decision = C.phase_census(d, tx, digest, co, commit)
    require(type(e["InitialPhases"]) is list and len(e["InitialPhases"]) in (3, 4), "actual initial source rows")
    require(yes["tx"] == tx and P.raw(records[3, yes["index"]]["Outcome"]) == yes_wire and
            d["FirstYES"] == {key:e["InitialPhases"][1][key] for key in ("Request", "Reply")}, "earlier YES actual submission identity")
    selected = live_initial(e["InitialPhases"], scopes, actor, co, r["Phase"], manifest, tx, records, original["floors"], {3:first, 8:target})
    require(selected == P.raw(e["SelectedCommand"]), "first-live native selected command differs from unanswered original")
    observed = prepared_vote_read(e["VoteRead"], e["InitialPhases"][1], scopes, actor, first)
    require(observed["status"] == yes, "held original authority reply differs from actual first YES")
    run, deadline = fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1)
    event = C.batch_pair(e["Arm"], e["Batch"], run, deadline, target, r["Kind"], r["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    restored = I.first_processes(d["Processes"], first)
    require(restored["applied"] >= yes["index"], "actual first YES replacement lacks local original")
    processes = [d["Processes"][0], *e["TargetProcesses"], d["Processes"][1]]
    ids = [integer(row["EventID"], minimum=1) for row in processes]
    require(ids == sorted(set(ids)), "actual A death then B native death/reopen then A reopen")
    if r["Kind"] == 2:
        stored = records.get((8, event["Index"]))
        require(stored is not None and P.status(P.raw(stored["Outcome"]))["phase"] == r["Phase"], "actual B application batch missing selected original")
    call = e["OriginalCall"]
    require(type(call) is dict and set(call) == {"Reply", "Transport", "Pending"} and call["Pending"] is True and
            call["Reply"] == d["Pending"] and type(e["TargetTransportLoss"]) is str and 0 < len(e["TargetTransportLoss"]) <= 4096,
            "actual first invocation/typed selected target loss")
    pending = F.process(P.raw(call["Reply"]))
    C.unknown_invocation(pending, call["Transport"])
    P.resumed(call["Reply"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 1, b"")
    decision_wire = P.raw(records[co, decision["index"]]["Outcome"])
    require(type(completion["Calls"]) is list and len(completion["Calls"]) == 2 and completion["Calls"][0] == d["Decision"], "actual recovery plus same-directory replay")
    for wire in completion["Calls"]:
        actual = F.process(P.raw(wire))
        require(actual["incarnation"] == pending["incarnation"] and actual["sequence"] > pending["sequence"] and
                actual["code"] == actual["mask"] == 0 and not actual["outcome"] and not actual["changes"] and
                not actual["packets"] and not actual["reads"], "same actual coordinator owner/successful original replay")
        P.resumed(wire, scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 0, decision_wire)
        pending = actual
    if len(d["Phases"]) == 5:
        I.recovery(d["Phases"], scopes, actor, co, {3:recovery_a, 8:recovery_b}, manifest, tx, records)
    else:
        require(len(d["Phases"]) == 2 and r["Phase"] == 3, "known D must have two actual Resolve replies; no padded capture")
        for row, part in zip(d["Phases"], (3, 8)):
            _, command, _ = C.source_request(row, scopes, actor)
            _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes, recovery_a if part == 3 else recovery_b)
            require((command["phase"], command["partition"]) == (4, part) and command["manifest"] == actual_manifest == manifest and
                    command["proof_wires"] == [decision_wire] and command["conflict"] is None and
                    outcome == P.raw(records[part, status["index"]]["Outcome"]) and status["tx"] == tx and
                    status["phase"] == 4 and status["round"] == decision["round"] and status["commit"] is commit,
                    "actual known-D Resolve/immutable original")
    require(type(d["Duplicates"]) is list and len(d["Duplicates"]) == 3, "actual Prepare/two Resolve duplicates")
    for i, row in enumerate(d["Duplicates"]):
        require(type(row) is dict and set(row) == C.LOCAL, "actual terminal replay grammar")
        command, status = C.C.decision_command(P.raw(row["Proposal"])), P.status(P.raw(row["Outcome"]))
        part = 3 if i < 2 else 8
        require((command["phase"], command["partition"]) == (2 if i == 0 else 4, part) and command["manifest"] == manifest and
                status["phase"] == 4 and status["tx"] == tx and status["digest"] == digest and status["index"] < row["Index"] and
                not P.raw(row["CDC"]) and records.get((part, row["Index"])) == {key:row[key] for key in ("Partition", "Index", "Outcome", "CDC")},
                "late Prepare returns current Resolve without new original/CDC")
    for actual in (d["Retained"], completion["FirstReopened"], e["TargetReopened"]):
        T.semantic(actual == d["Before"], "old-cut:first-live-real-replacement-data-and-index")
    I.charge(e["InitialCapture"], e["InitialPhases"]); I.charge(completion["Capture"], d["Phases"])
    hold = e["Hold"]
    require(type(hold) is dict and set(hold) == {"Candidates", "Work", "Output", "Retained"} and
            integer(hold["Candidates"], 128, 1)*(4096+32*(2 << 20)) == hold["Work"] and hold["Output"] == 8*(128 << 10)+8192 and
            hold["Retained"] == len(P.raw(e["VoteRead"]["Request"]))+len(P.raw(e["VoteRead"]["Reply"])), "separate prepaid completed reply ownership")
    require(integer(e["ReceiptReads"], 129, 1)*65536 == e["ReceiptGrantBytes"] and
            integer(e["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == e["GateDecodeWork"], "actual native diagnostic prepay")
    return dict(case=a["Case"], records=len(records), serial_witness=C.literal_and_serial(d, decision),
                scope="first-live P06 only; other protocol/state/leader/message compositions remain distinct")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 524, "finite actual implemented P01/P02/P03/P04/P05/first-live-run invocation")
    seen, results = set(), []
    for path in args.artifacts:
        result = check(load(path))
        require(result["case"] not in seen, "duplicate actual causal recipe")
        seen.add(result["case"]); results.append(result)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
