#!/usr/bin/env python3
"""Inert native codec guards, never vote/owner/source authority."""
import copy
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

    def test_p02_planning_floor_split_preserves_before_and_after_fence_order(self):
        for stage, floor, bound in ((False, 9, 10), (True, 10, 10), (True, 11, 10)):
            C.fence_planning_floor(stage, floor, bound)
        for stage, floor, bound in ((False, 10, 10), (False, 11, 10), (True, 9, 10), (True, 0, 10), (True, 10, 0)):
            with self.assertRaises(ValueError):
                C.fence_planning_floor(stage, floor, bound)

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
