#!/usr/bin/env python3
"""Inert native codec guards, never vote/owner/source authority."""
import copy
import base64
import json
import zlib
import struct
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import native_protocol_campaign_check as C
import native_transition_conjunction_check_test as G


def sample(*, source_index=12, observed_tx=None, read_issuer=bytes([4])*16):
    submit, scopes, manifest, command = G.unanswered(2, 3)
    scope, tx, digest = scopes[3], bytes([2])*16, C.hashlib.sha256(manifest).digest()
    status = G.sealed(b"GCS\1", C.P.GRAPH+struct.pack(">Q", 3)+tx+digest+b"\2"+struct.pack(">QQQ", 9, 1, 0)+b"\1\0")
    def reply(kind, sequence, nonce, body, issuer=bytes([4])*16):
        return G.sealed(b"GSR\1", G.field(G.sealed(b"APS\1", scope))+struct.pack(">Q", 1)+issuer+
                        bytes([kind])+struct.pack(">Q", sequence)+nonce+bytes(2)+G.field(body))
    submit["Reply"] = G.encoded(reply(8, 7, bytes([3])*16, status))
    nonce = bytes([5])*16
    request = G.sealed(b"GSQ\1", b"\6"+struct.pack(">Q", 8)+nonce+bytes(48)+bytes(16)+tx+b"\2")
    observed = G.sealed(b"GSE\1", G.field(G.sealed(b"APS\1", scope))+bytes([4])*16+struct.pack(">Q", 11)+
                        (tx if observed_tx is None else observed_tx)+nonce+b"\2"+struct.pack(">Q", source_index)+G.field(status)+G.field(manifest))
    boundary = dict(Request=G.encoded(request), Reply=G.encoded(reply(6, 8, nonce, observed, read_issuer)))
    return boundary, submit, scopes


P02_PHASES = (
    'eNrtnVuToki3hv9L3ToRIqDCRMwF5/P5oPDFvgBRQFQQUJCJ+e9bq/pr7RqsU1f39OydGVERVFlCrsyVmStXPq/++aAnQbWsHn7/'
    'z58PWpnG6e7h9/FvD3pQ1mmd5uffsN8ezOX+sKzqh98fzJFqEkZM/LeokKBa8GpizOGjyR3pkjmMBylEfHqh2Oj8bJowztdiSzqs'
    'Qj5e3y8C9fId6evlbhPL+6Wxcl1U2MGkb1WhtNnODX9GeiHElIbsmW7mB5N9kzGpEyCljs1dR6Nz5lyJbGgaa2dAjRB7AFXOqp2p'
    'ESnoLKpMbWKlcDyGN2N0p0/MoEwsMY2qQTwWIE/AoR0N1R2fLs72Ua4JqQ5hMOTL1T63PUUa5/9VCfsYp4k3Qm2J3qVDJMGKMTa/'
    'Nsqj/WhkmCOGJGPm5eYlExiFKqpOy2u7uPtxFfLLxCCmsW7Aujaw09lMwwOUHiSF2XZxqeW1CrcrR9JmxKEYk7g7j9OVx26kttrP'
    '/IjceGxyNKyaqBN2Zm7KfUm5Ssnty8ZmyZO6NrbioZDbzYbBVNwwhdGyPu6qpkbhYddebJFnDhPphOG91i5edFBlovT2TfPfv7HB'
    '9fXF9fLxdWZ1p3kfnyPx5HEBb1B57h6CeVz7c/LowfFEsTNEoaBGsc4/rtEodP70w+aTaP3YxnI421Qht+n8mXoMt8ZTV8R9fvf4'
    '2uPzVNe0CUPorVMyHwkHjdguMnV05MMppefJsPGM4YKh0q2fITMxmW0CLtUwrr9tzI3PRZvr71dfYAyGu6nbXZf7UqwXfYj4J238'
    'tEI+eUp2M2CcZ//y5feG6Z9X+uYlzUpTNq/RSZNUfDQQKZnKZ8KzZnp6/h1f6fX/sSQJsxRZlxTXRAiaCZWUePHNva49Tfcaa/TZ'
    'QGd5ngTyUquEkERWqZE2ezbt8x9i5uWizR12NaqPDW9qi8XKQ+AJXPlxeDhuD4gZIjEWQsiO4HaJzk1Hx32NDNOKwY4ndrtcrOKm'
    '7Zy55iDLrEOXlguz8fafnnfWi7xzcH+hDDJNhIII4j15OPLVZmJEiuyOJSiP7Ek1WDuP/SY8m1sey8VPXR+KDKthpqiQz9Qlc4rH'
    '6BFHV9JyxUKvrF83/SX9e9rDxsTCMsxqbC1ybyuIR4+vilQ9jMz1cnRclIujRCeJ0tVH5O7cLBS80TCvre803/Qu+09jiUqaEFGL'
    'JTUahdvzPD5zmxBWc23NjNWOqBWaOf+wjkITk6cfMtB2Xvs4j8GjIuTaYgG76xAeZ++Ym84DSjcdxuVu6v7YjyUTWUTjvLKGKX1z'
    'Lnv9Y4olBbwZcGG5sgm7nM1V1jbsY5CVe79bl7tdEJ8KHD6tJta1bcTecTt4u59Uk5iloyqeu6NAcbx0LkQpqy4WOFwGJiFknXes'
    'V65KBFjVN58ty9hABINpd4JQDfbDqDFV7NS4LEtymYxnCLVUpq0BDYn9Y8dV5kig++clom+cUX3zNCWfuF0wR/1DjSukVvsLyFVJ'
    'Dd9bh2KnaYdltKelHRKKw0K+vj/rqz899qjJLuNH7pIS8XjmlNyp8DcQ58YzDicliIIZ1+dmh30ldXvCUeETGc3dvDVTcZHAgjNq'
    'OySoC4ZIbTUdjHAHIsvTpf5Syzuxwr59HhBu5u2r0xyFICpWyXaCQA0zGW0604GYKVxIk+0CnYzHcK7sV1G1G2TX/qGMvjVCf3sc'
    'XHFbRZXYVsIcy28hEyGZGi5yGI1K0Yzw0hqsSCxbCo7xOM6Ziz9TDP12Wy9jgTqYkELdWWf7fIEmbuPc67PMR0O5c91h5yfFfRa7'
    'clIUpfyTgcyw6FgJg5QfkgmNN4Yk+Vs78rZs4OaBMG28qw9TGfO+NrrpRLq9zJsff//z8URe/U0w3j53Pfom/TiHv8O3n8dYd+Iv'
    'OuNeu6fPlxYzDytCZNfMHElPE5lMt7PyCAneInMgzJ8JGawi2hZ7fNDZx17dH9342LUdGIb/5Lrc9IJ5aT/+I+2nxvHb15nbNbTv'
    'WTx7qYfwEZ/6Zr15Kt8b+/dN+ghFvGN+uBr7dD1/bq/4YXvJp2tycXu7vhhFcHFLXsfiTqSjsIUHzXoirDCO0RGSyIutr45Szy1m'
    'Sb4+Xt+U34zF7M39+9hOUn2xSXy7TfHb/YN2HadhqO+1k+zfg/bFLqTW9O5vPtY+8dM6/L72yfvah3m6Zp+3xMi95KoSrDep9Y41'
    'on/P3dcWdtO/r/z6JkZ+u62XmIiZXuZI6u3rsPjG+eDH+ZLs0sYn3OtrG/5YP7+bi7gZir2NRz6f2y51tR7jvOQd/XWJgUmdic7x'
    'WfX2sfOeefWmfGYeqHYuOWHhM9aVfzBHxfTvYa5jlH/W/pe6kvLTWDbePpaZx0zvZSwzxHvjHaV/HTDe8lyCfFPc2LN+Xl/MzLfY'
    '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'
    '8tdf/wu+nt8x'
)


# Actual rotated/reorder first-live P06 artifact e75607235958d6ed8fff230d307b9b8484aa8ebab8a6b91cc13a506fcbecb301.
P06_LIVE_PHASES = (
    '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'
    '5uOrOb5cIf1l57f/Bykqiiw='
)


P03_REOPENED = (
    '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'
    'DRpu0HCDhhuWbXh1vIaGG//+BWJFcdE='
)

class ActualVoteReadGuards(unittest.TestCase):
    def test_frozen_registry_has_actual_a_no_path_and_b_two_private_candidates(self):
        names, counts, candidates = set(), {"A":0, "B":0}, 0
        for order in ("commit-first", "abort-first"):
            for original, phases in (("A", ((1, 3, ""), (2, 3, ""), (3, 3, ""), (4, 3, ""))),
                                     ("B", ((1, 8, ""), (2, 3, ""), (2, 8, ""), (4, 3, ""), (4, 8, ""), (3, 8, "capacity-ABORT"), (3, 8, "all-YES")))):
                for phase, part, candidate in phases:
                    for kind in (1, 2):
                        for side in ("before", "after"):
                            names.add(C.frozen_recipe(dict(Order=order, Original=original, Phase=phase, Partition=part, Candidate=candidate, Kind=kind, Side=side)))
                            counts[original] += 1
                            candidates += bool(candidate)
        self.assertEqual((len(names), counts, candidates), (88, {"A":32, "B":56}, 16))
        for original, phase, part, candidate in (("A", 2, 8, ""), ("B", 3, 8, ""), ("A", 3, 3, "all-YES")):
            with self.assertRaises(ValueError):
                C.frozen_recipe(dict(Order="commit-first", Original=original, Phase=phase, Partition=part, Candidate=candidate, Kind=1, Side="before"))

    def test_frozen_identity_cannot_accept_missing_owner_or_unknown_probe(self):
        for receipt in ({}, dict(Version=1), dict(Attempts=[dict(ErrorMask=1 << 13)])):
            with self.assertRaises(ValueError):
                C.frozen_identity(receipt, {}, {}, {})

    def test_conditional_inventory_is_exact_five_scenarios_nine_mutations(self):
        names, counts = set(), {}
        for schedule in C.N.SCHEDULES:
            co = 8 if schedule == "write-skew-B-first" else 3
            count = 4 if schedule == "temporal-life-value-ABA" else 2 if schedule == "empty-range-and-absent-key-insertion" else 1
            for operation in range(1, count+1):
                for phase, part in ((1, co), (2, 3), (2, 8), (3, co), (4, 3), (4, 8)):
                    for kind in (1, 2):
                        for side in ("before", "after"):
                            names.add(C.conditional_recipe(dict(Schedule=schedule, Operation=operation, Phase=phase, Partition=part, Kind=kind, Side=side)))
                            counts[schedule] = counts.get(schedule, 0)+1
        self.assertEqual(len(names), 216)
        self.assertEqual(sorted(counts.values()), [24, 24, 24, 48, 96])
        for operation, phase, part in ((0, 2, 3), (2, 2, 3), (1, 3, 3)):
            with self.assertRaises(ValueError):
                C.conditional_recipe(dict(Schedule="write-skew-B-first", Operation=operation, Phase=phase, Partition=part, Kind=1, Side="after"))

    def test_conditional_copy_accounting_is_cumulative_and_has_no_hidden_capacity(self):
        rows = [dict(Request=G.encoded(b"inert"), Reply=G.encoded(b"bytes"))]
        headers, size = 128*64+2*64, 10
        charge = dict(Candidates=3, Attempts=1, Work=3*4096+32*size, Output=headers+8192+8*size, Retained=headers+64+size)
        C.conditional_charge(charge, rows)
        for changed in (dict(Attempts=0), dict(Work=1), dict(Output=charge["Output"]-1), dict(Retained=charge["Retained"]+1)):
            with self.assertRaises(ValueError):
                C.conditional_charge({**charge, **changed}, rows)

    def test_partial_process_mask_is_not_graph_wire_mask(self):
        tx = bytes([1])*16
        rows = [dict(Mode=name, Classification="unavailable", Request=G.encoded(G.sealed(b"GEQ\1", bytes([mode])+
                    (bytes(16) if mode == 1 else tx)+struct.pack(">Q", 0 if mode == 1 else 8)+bytes(4))), Response=None)
                for name, mode in (("Fresh", 1), ("After", 4))]
        response = dict(code=3, mask=1 << 7, aux=b"", outcome=b"", changes=b"")
        with mock.patch.object(C.C.C, "process_response", return_value=response):
            C.C.C.partial_reads(rows, tx, 8)
        for mask in (0, 1 << 3, 1 << 12, 1 << 13, (1 << 7) | (1 << 3), (1 << 7) | (1 << 12)):
            with mock.patch.object(C.C.C, "process_response", return_value={**response, "mask":mask}):
                with self.assertRaises(ValueError):
                    C.C.C.partial_reads(rows, tx, 8)

    def test_p03_partial_inventory_keeps_original_and_recovery_occurrences_distinct(self):
        names, early, outage = set(), 0, 0
        for phase, part in ((1, 8), (2, 3), (2, 8), (3, 8), (4, 3), (4, 8)):
            for kind in (1, 2):
                for side in ("before", "after"):
                    names.add(C.partial_recipe(dict(Phase=phase, Partition=part, Kind=kind, Side=side)))
                    outage += phase == 4 and part == 8
                    early += (phase, part) != (4, 8)
        self.assertEqual((len(names), early, outage), (24, 20, 4))
        for phase, part in ((1, 3), (3, 3), (0, 3)):
            with self.assertRaises(ValueError):
                C.partial_recipe(dict(Phase=phase, Partition=part, Kind=1, Side="before"))

    def test_p02_captured_phase_authority_uses_partition_then_phase(self):
        # Six actual RPCs + native original projections from the immutable
        # rotated/reorder P02 artifact, SHA25633a5c18c03069f51df220f21b453ee16
        # f99a2c36ec10774f03e72b5d82e0a18d. No reconstructed authority.
        f=json.loads(zlib.decompress(base64.b64decode(P02_PHASES)))
        rows=f["Phases"];scopes={int(k):base64.b64decode(v) for k,v in f["Scopes"].items()}
        records={(r["Partition"],r["Index"]):r for r in f["Records"]}
        sources={int(k):dict(floor=v) for k,v in f["Floors"].items()};peers={3:2,8:6}
        def check(rows=rows,records=records):return C.fence_phases(rows,records,scopes,peers,5,8,sources)
        statuses,outcomes,commands,manifest=check()
        expected={(8,1),(3,2),(8,2),(8,3),(3,4),(8,4)}
        self.assertEqual(set(statuses),expected);self.assertEqual(set(outcomes),expected)
        self.assertEqual(set(commands),{(phase,part) for part,phase in expected})
        self.assertEqual(statuses[8,1]["index"],22)
        for phase,part in ((1,8),(2,3),(2,8),(3,8),(4,3),(4,8)):
            parsed=C.C.C.decision_command(commands[phase,part])
            keys=[] if phase==1 else [(8,1)]+([(3,2)] if part==8 else []) if phase==2 else [(3,2),(8,2)] if phase==3 else [(8,3)]
            self.assertEqual(parsed["proof_wires"],[outcomes[k] for k in keys])
            self.assertEqual(parsed["manifest"],manifest)
        with self.assertRaises(ValueError):check(rows=rows[1:])
        missing=dict(records);del missing[8,22]
        with self.assertRaises(ValueError):check(records=missing)
        conflicted=copy.deepcopy(records);row=conflicted[8,22]
        body=bytearray(C.P.body(C.P.raw(row["Outcome"]),b"GCS\1"));body[81:89]=struct.pack(">Q",sources[8]["floor"]+1)
        row["Outcome"]=G.encoded(G.sealed(b"GCS\1",body))
        with self.assertRaises(ValueError):check(records=conflicted)
        def replace_proofs(row,proofs):
            row=copy.deepcopy(row);command,parsed,_=C.C.source_request(row,scopes,5)
            body=bytes([parsed["phase"]])+C.P.GRAPH+struct.pack(">Q",parsed["partition"])+G.field(parsed["manifest"])+struct.pack(">I",len(proofs))
            body+=b"".join(C.P.body(p,b"GCS\1") for p in proofs)
            altered=G.sealed(b"GCT\1",body)
            request=C.P.raw(row["Request"]);q=C.P.Cursor(C.P.body(request,b"GSQ\1"))
            header=q.take(1+8+16+48);fields=[q.field() for _ in range(3)]
            proposal=q.field();tail=q.take(17);q.end()
            p=C.P.Cursor(C.P.body(proposal,b"GSC\1"));target=p.field();self.assertEqual(p.field(),command);p.end()
            proposal_new=G.sealed(b"GSC\1",G.field(target)+G.field(altered))
            row["Request"]=G.encoded(G.sealed(b"GSQ\1",header+b"".join(G.field(v) for v in fields)+G.field(proposal_new)+tail))
            return row
        for proofs in ([],[outcomes[3,2]]):
            changed=copy.deepcopy(rows);changed[1]=replace_proofs(rows[1],proofs)
            with self.assertRaisesRegex(ValueError,"earlier original phase authority"):
                check(rows=changed)

    def test_first_live_prior_authority_uses_native_partition_phase_coordinates(self):
        f = json.loads(zlib.decompress(base64.b64decode(P06_LIVE_PHASES)))
        rows = f["Rows"]
        scopes = {int(k):base64.b64decode(v) for k,v in f["Scopes"].items()}
        records = {(r["Partition"], r["Index"]):r for r in f["Records"]}
        floors = {int(k):v for k,v in f["Floors"].items()}
        manifest, tx = C.P.raw(f["Manifest"]), bytes.fromhex(f["Tx"])
        def check(rows=rows, records=records):
            return C.live_initial(rows, scopes, 5, 8, 2, manifest, tx, records, floors, {3:2,8:6})
        selected = check()
        self.assertEqual(selected, C.C.source_request(rows[-1], scopes, 5)[0])
        self.assertFalse(C.P.raw(rows[-1]["Reply"]))  # Actual captured unknown, never a fabricated reply.
        registration = C.P.peer_phase(C.P.raw(rows[0]["Request"]), C.P.raw(rows[0]["Reply"]), scopes, 6)[2]
        key = (8, registration["index"])
        missing = dict(records); del missing[key]
        with self.assertRaisesRegex(ValueError, "retained original"):
            check(records=missing)
        wrong_phase = copy.deepcopy(records)
        wrong_phase[key]["Outcome"] = next(r["Outcome"] for r in f["Records"] if C.P.status(C.P.raw(r["Outcome"]))["phase"] == 2)
        with self.assertRaisesRegex(ValueError, "retained original"):
            check(records=wrong_phase)
        # A different REAL captured Register is still foreign authority, even
        # when its command phase/partition match. No status is synthesized.
        other = json.loads(zlib.decompress(base64.b64decode(P02_PHASES)))
        foreign = next(r["Outcome"] for r in other["Records"] if C.P.raw(r["Outcome"])[:4] == b"GCS\1" and C.P.status(C.P.raw(r["Outcome"]))["phase"] == 1)
        self.assertNotEqual(C.P.status(C.P.raw(foreign))["tx"], tx)
        changed = copy.deepcopy(records); changed[key]["Outcome"] = foreign
        with self.assertRaisesRegex(ValueError, "retained original"):
            check(records=changed)
        with self.assertRaises(ValueError):
            check(rows=list(reversed(rows)))

    def test_p02_planning_floor_split_preserves_before_and_after_fence_order(self):
        for stage, floor, bound in ((False, 9, 10), (True, 10, 10), (True, 11, 10)):
            C.fence_planning_floor(stage, floor, bound)
        for stage, floor, bound in ((False, 10, 10), (False, 11, 10), (True, 9, 10), (True, 0, 10), (True, 10, 0)):
            with self.assertRaises(ValueError):
                C.fence_planning_floor(stage, floor, bound)

    def test_p03_actual_reopened_observation_joins_raw_original_cut(self):
        # Exact observations from immutable artifact SHA256
        # 2ebf8df287fa2a4f3a82af89515b25779a270e212898121ab7b102b19fe452bd;
        # authenticated history SHA256
        # 4fe6cf35da7cca97dfebb510f519f202dfdc1680a012d96d6478dfde601a4418.
        # This fixture is inert; the full oracle authenticates native authority.
        evidence = json.loads(zlib.decompress(base64.b64decode(P03_REOPENED)))
        prior = evidence["P03"]["steps"][3]
        old = {"Cut":prior["Cut"], "View":prior["After"], "Native":prior["RawObservation"]}
        self.assertIsNone(old["View"]["Entities"][0]["Labels"])
        self.assertEqual(C.coordinator_reopened(evidence), old)
        for section, field, value in (("Cut", "Certificates", []),):
            changed = copy.deepcopy(evidence)
            changed["TargetReopened"][section][field] = value
            with self.assertRaises(ValueError):
                C.coordinator_reopened(changed)
        for path, value in (
            (("View", "Entities", 0, "Labels"), ["phantom"]),
            (("View", "Entities", 0, "Labels"), []),
            (("Native", "lives", 0, "life"), 99),
            (("Native", "entities", 1, "property", "i64"), 99),
        ):
            changed = copy.deepcopy(evidence)
            owner = changed["TargetReopened"]
            for key in path[:-1]:
                owner = owner[key]
            owner[path[-1]] = value
            with self.assertRaises(ValueError):
                C.coordinator_reopened(changed)

    def test_p03_program_recovery_keeps_surviving_original_caller(self):
        # Inert role derivation only. Native originals and process receipts
        # remain authenticated by check_coordinator and the program oracle.
        for initial in ((1, 4), (2, 6)):
            for co in (3, 8):
                group = 0 if co == 3 else 1
                caller, recoverer = ((2, 3) if initial == (1, 4) else (3, 1)) if co == 3 else ((5, 6) if initial == (1, 4) else (5, 4))
                for phase, part in ((1, co), (2, co), (3, co), (2, 11-co), (4, co)):
                    recipe = dict(Coordinator=co, Phase=phase, Partition=part, Kind=2, Side="after")
                    current = list(initial)
                    if phase == 4:
                        current[group] = caller
                    first = 1 if part == 3 else 4
                    target = caller if phase == 4 else next(v for v in range(first, first+3) if v != initial[0 if part == 3 else 1] and v != caller)
                    context = dict(initial_leaders=initial, leaders=tuple(current), target=target)
                    want = caller if phase != 4 and target == recoverer else recoverer
                    self.assertEqual(C.program_coordinator_roles(context, recipe), (initial, initial[group], caller, want, target))
                    for changed in (dict(target=caller if phase != 4 else recoverer), dict(leaders=tuple(reversed(current))), dict(initial_leaders=(2, 4))):
                        with self.assertRaises(ValueError):
                            C.program_coordinator_roles({**context, **changed}, recipe)
        pending = dict(incarnation=bytes([1])*16, sequence=20)
        response = dict(incarnation=pending["incarnation"], sequence=21)
        C.coordinator_surviving_call(response, pending)
        for changed in (dict(incarnation=bytes([2])*16), dict(incarnation=bytes(16)), dict(sequence=20), dict(sequence=19)):
            with self.assertRaises(ValueError):
                C.coordinator_surviving_call({**response, **changed}, pending)

    def test_p03_registry_keeps_actual_predecision_and_quorum_loss_orders(self):
        names, early, outage, other = set(), 0, 0, 0
        for co in (3, 8):
            for phase, part in ((1, co), (2, 3), (2, 8), (3, co), (4, 3), (4, 8)):
                for kind in (1, 2):
                    for side in ("before", "after"):
                        names.add(C.coordinator_recipe(dict(Coordinator=co, Phase=phase, Partition=part, Kind=kind, Side=side)))
                        early += phase != 4
                        outage += phase == 4 and part == co
                        other += phase == 4 and part != co
        self.assertEqual((len(names), early, outage, other), (48, 32, 8, 8))
        for co, phase, part in ((3, 1, 8), (8, 3, 3)):
            with self.assertRaises(ValueError):
                C.coordinator_recipe(dict(Coordinator=co, Phase=phase, Partition=part, Kind=1, Side="before"))

    def test_p03_sidecar_pre_admission_and_actual_bytes_reject_missing_foreign_links(self):
        # Tiny inert JSON is not a semantic source artifact. This guard only
        # exercises the bounded file/hash reader, never protocol authority.
        recipe = dict(Coordinator=3, Phase=1, Partition=3, Kind=1, Side="before")
        wire = b"{}\n"
        name = C.coordinator_recipe(recipe)+"-history.json"
        evidence = dict(Recipe=recipe, TraceName=name, TraceBytes=len(wire), TraceSHA256=list(C.hashlib.sha256(wire).digest()))
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            path = parent/"campaign.jsonl"
            for bad in (dict(TraceName="../"+name), dict(TraceName="/"+name), dict(TraceBytes=(8 << 20)+1)):
                with mock.patch.object(C.os, "open", side_effect=AssertionError("effect before admission")):
                    with self.assertRaises(ValueError):
                        C.coordinator_sidecar(path, {**evidence, **bad}, 1)
            with self.assertRaises(FileNotFoundError):
                C.coordinator_sidecar(path, evidence, 1)
            sibling = parent/name
            sibling.write_bytes(wire)
            self.assertEqual(C.coordinator_sidecar(path, evidence, 1), {})
            with self.assertRaises(ValueError):
                C.coordinator_sidecar(path, {**evidence, "TraceSHA256":[1]*32}, 1)
            sibling.write_bytes(wire[:-1])
            with self.assertRaises(ValueError):
                C.coordinator_sidecar(path, evidence, 1)
            sibling.unlink()
            target = parent/"foreign.json"
            target.write_bytes(wire)
            sibling.symlink_to(target.name)
            with self.assertRaises(OSError):
                C.coordinator_sidecar(path, evidence, 1)

    def test_p05_uses_same_bounded_exact_sidecar_reader_before_semantic_decode(self):
        recipe = dict(Schedule="temporal-life-value-ABA", Operation=4, Phase=4, Partition=8, Kind=2, Side="after")
        wire = b"{}\n" # Inert parser guard, never a native semantic trace.
        name = C.conditional_recipe(recipe)+"-history.json"
        evidence = dict(Recipe=recipe, TraceName=name, TraceBytes=len(wire), TraceSHA256=list(C.hashlib.sha256(wire).digest()))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"campaign.jsonl"
            (path.parent/name).write_bytes(wire)
            self.assertEqual(C.conditional_sidecar(path, evidence, 1), {})
            for changed in (dict(TraceName="../"+name), dict(TraceSHA256=[1]*32), dict(TraceBytes=(8 << 20)+1)):
                with self.assertRaises(ValueError):
                    C.conditional_sidecar(path, {**evidence, **changed}, 1)

    def test_fence_prefix_requires_actual_originals_and_never_accepts_phantom_cdc(self):
        operations = {name:dict(Transaction=(bytes([i])*16).hex(), Coordinator=3 if i == 1 else 8)
                      for i, name in enumerate(("node-a", "node-b", "edge-create"), 1)}
        with self.assertRaises(ValueError):
            C.fence_prefix({}, operations, [])
        row = dict(Partition=3, Index=1, Outcome=None, CDC=G.encoded(b"inert nonempty output"))
        with self.assertRaises(ValueError):
            C.fence_prefix({(3, 1):row}, operations, [])
        with self.assertRaises(ValueError):
            C.check_persisted_fence(dict(FenceTransition={}, Recipe={}), {}, {}, {}, {}, 1)

    def test_p02_live_registry_never_relabels_persistence_or_later_recovery(self):
        names = set()
        for schedule in ("delayed-A-prepare", "A-before-B-after"):
            for phase, part in ((1, 8), (2, 3), (2, 8), (3, 8), (4, 3), (4, 8)):
                for kind in (1, 2):
                    for side in ("before", "after"):
                        names.add(C.fence_recipe(dict(Schedule=schedule, Phase=phase, Partition=part, Kind=kind, Side=side)))
        self.assertEqual(len(names), 48)
        for schedule in ("recovery", ""):
            with self.assertRaises(ValueError):
                C.fence_recipe(dict(Schedule=schedule, Phase=2, Partition=3, Kind=1, Side="after"))
        stage = set()
        for phase, part in ((0, 3), (1, 3), (2, 3), (2, 8), (3, 3), (4, 3), (4, 8)):
            for kind in (1, 2):
                for side in ("before", "after"):
                    stage.add(C.fence_recipe(dict(Schedule="persisted-fence-before-application", Phase=phase, Partition=part, Kind=kind, Side=side)))
        self.assertEqual(len(stage), 28)
        self.assertFalse(names & stage)
        for phase, part in ((0, 8), (1, 8), (3, 8)):
            with self.assertRaises(ValueError):
                C.fence_recipe(dict(Schedule="persisted-fence-before-application", Phase=phase, Partition=part, Kind=1, Side="after"))

    def test_p01_registry_names_only_actual_full_original_participants(self):
        names = set()
        for mode in (1, 4):
            for phase, part in ((1, 8), (2, 3), (2, 8), (3, 8), (4, 3), (4, 8)):
                for kind in (1, 2):
                    for side in ("before", "after"):
                        names.add(C.scope_recipe(dict(Mode=mode, Phase=phase, Partition=part, Kind=kind, Side=side)))
        self.assertEqual(len(names), 48)
        for mode, phase, part in ((3, 2, 3), (1, 1, 3), (4, 3, 3), (1, 2, 0)):
            with self.assertRaises(ValueError):
                C.scope_recipe(dict(Mode=mode, Phase=phase, Partition=part, Kind=1, Side="before"))
        # The ordinary owning checker cannot silently select the fresh actor.
        with self.assertRaises(ValueError):
            C.Q.check({}, failure_member=2)
        with self.assertRaises(ValueError):
            C.Q.check({}, failure_member=3, selected_processes=[])

    def test_exact_first_live_registry_refuses_causally_unreachable_endpoints(self):
        names = set()
        for co in (3, 8):
            for commit in (True, False):
                for kind in (1, 2):
                    for side in ("before", "after"):
                        for phase in ((2,) if co == 3 else (2, 3)):
                            names.add(C.live_recipe(dict(Coordinator=co, Commit=commit, Phase=phase, Kind=kind, Side=side)))
        self.assertEqual(len(names), 24)
        for phase in (1, 3, 4):
            with self.assertRaises(ValueError):
                C.live_recipe(dict(Coordinator=3, Commit=True, Phase=phase, Kind=1, Side="before"))
        with self.assertRaises(ValueError):
            C.live_recipe(dict(Coordinator=8, Commit=True, Phase=3, Kind=1, Side="before", Replacement=6))

    def test_inert_codec_shape_joins_same_stream_but_grants_no_authority(self):
        boundary, submit, scopes = sample()
        observed = C.prepared_vote_read(boundary, submit, scopes, 3, 1)
        self.assertEqual(observed["source_index"], 12)
        self.assertEqual(observed["status"]["index"], 9)
        self.assertNotIn("stamp", observed)

    def test_submit_ack_or_foreign_peer_cannot_replace_actual_vote_read(self):
        boundary, submit, scopes = sample()
        for changed in ({**boundary, "Reply":submit["Reply"]}, {**boundary, "Request":submit["Request"]},
                        {**boundary, "Reply":None}, {**boundary, "extra":True}):
            with self.assertRaises(ValueError):
                C.prepared_vote_read(changed, submit, scopes, 3, 1)
        for actor, voter in ((5, 1), (3, 2)):
            with self.assertRaises(ValueError):
                C.prepared_vote_read(boundary, submit, scopes, actor, voter)
        foreign = copy.deepcopy(scopes); foreign[3] = bytes([7])*120
        with self.assertRaises(ValueError):
            C.prepared_vote_read(boundary, submit, foreign, 3, 1)

    def test_checksummed_native_question_prefix_and_issuer_still_must_match(self):
        for changes in (dict(source_index=8), dict(observed_tx=bytes([9])*16), dict(read_issuer=bytes([6])*16)):
            boundary, submit, scopes = sample(**changes)
            with self.assertRaises(ValueError):
                C.prepared_vote_read(boundary, submit, scopes, 3, 1)


if __name__ == "__main__":
    unittest.main()
