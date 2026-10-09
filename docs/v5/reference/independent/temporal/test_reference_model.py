"""Hand-checked failures and literal-membership differential tests.

Expected support is computed from raw comparisons, not model normalization.
Finite dense witnesses cover every endpoint and every open endpoint cell.
QN witnesses also cover natural-microstep endpoint cells and dense model gaps.
"""
from dataclasses import replace
from fractions import Fraction
from itertools import product
from pathlib import Path
import random
import unittest

from generate_corpus import HISTORICAL, corpus, create, edge, mutation
from reference_model import (ABSENT, Axis, Cell, Component, Interval, Limits, ModelError,
                             Region, Snapshot, coord, convert_units, knowledge_predicate,
                             instant_ms_codec, metadata_bytes, run_historical, unown)


class ReferenceTests(unittest.TestCase):
    def error(self, code, fn):
        with self.assertRaises(ModelError) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)

    def test_original_corpus_unchanged_16_52(self):
        r = run_historical(HISTORICAL)
        self.assertEqual((r["cases"], r["assertions"]), (16, 52))

    def test_literal_membership_differential(self):
        counts = {}
        for profile in ("Q", "Z", "QN"):
            axis = Axis("differential-" + profile, profile)
            if profile == "Q":
                values = [Fraction(-1), Fraction(0), Fraction(1, 3), Fraction(1)]
                probes = sorted(set(values + [(a + b) / 2 for a, b in zip(values, values[1:])]))
            elif profile == "Z":
                values = [-1, 0, 1, 2]
                probes = range(-2, 4)
            else:
                values = [(Fraction(-1), 0), (Fraction(0), 0), (Fraction(0), 1),
                          (Fraction(0), 2), (Fraction(1), 0)]
                probes = list(product([Fraction(-1), Fraction(-1, 2), Fraction(0), Fraction(1, 2), Fraction(1)], range(4)))
            samples = []
            for lo, hi, lc, hc in product(values, values, (False, True), (False, True)):
                raw = Interval(coord(axis, lo), coord(axis, hi), lc, hc)
                scope = Region.build(axis, [raw])
                # Direct raw endpoint predicates do not use emptiness/normalization.
                expected = {i for i, p in enumerate(probes) if raw.contains(coord(axis, p))}
                actual = {i for i, p in enumerate(probes) if scope.contains(p)}
                self.assertEqual(actual, expected, (profile, raw))
                samples.append((scope, expected))
            for (a, ea), (b, eb) in product(samples, repeat=2):
                for result, expected in ((a.intersection(b), ea & eb),
                                         (a.difference(b), ea - eb), (a.union(b), ea | eb)):
                    actual = {i for i, p in enumerate(probes) if result.contains(p)}
                    self.assertEqual(actual, expected, (profile, a, b, result))
            counts[profile] = len(samples) ** 2
        self.assertEqual(counts, {"Q": 4096, "Z": 4096, "QN": 10000})

    def test_exact_unbounded_history_boundary_and_singletons(self):
        q = Axis("Q", "Q")
        all_q = Region.span(q, "-inf", "+inf", False, False)
        closed = Region.span(q, 0, 2, True, True)
        inside = Region.span(q, 0, 2, False, False)
        tails = closed.difference(inside)
        self.assertEqual([tails.contains(p) for p in (0, 1, 2)], [True, False, True])
        self.assertEqual(len(tails.parts), 2)
        self.assertFalse(all_q.difference(all_q).parts)
        self.assertEqual(all_q.intersection(closed), closed)
        z = Axis("Z", "Z")
        unbounded = Region.span(z, "-inf", "+inf", False, False)
        self.assertTrue(unbounded.contains(10**60))
        self.assertTrue(Region.span(z, 0, 1).contains(0))
        self.assertFalse(Region.span(z, 0, 1, False, False).parts)

    def test_lex_dense_cross_model_gap_and_microstep_natural_boundary(self):
        axis = Axis("QN", "QN")
        span = Region.span(axis, (0, 0), (1, 0), False, False)
        self.assertTrue(span.contains((Fraction(1, 3), 0)))
        self.assertTrue(span.contains((0, 10000)))
        self.assertFalse(span.contains((0, 0)))
        self.assertFalse(span.contains((1, 0)))
        self.assertFalse(Region.span(axis, (0, 0), (0, 1), False, False).parts)
        self.error("INVALID_MICROSTEP", lambda: coord(axis, (0, -1)))
        for value in ((True, 0), (1.25, 0), (0, True), (0, 0.0)):
            self.error("INVALID_COORDINATE" if type(value[0]) in (bool, float) else "INVALID_MICROSTEP", lambda: coord(axis, value))
        for profile in ("Z", "Q"):
            for value in (True, 1.25):
                self.error("INVALID_COORDINATE", lambda: coord(Axis(profile, profile), value))

    def test_axis_definition_checked_before_empty_fast_path(self):
        a, b = Axis("same", "Q", unit="unit-a"), Axis("same", "Q", unit="unit-b")
        for method in ("union", "intersection", "difference", "subset"):
            self.error("AXIS_MISMATCH", lambda: getattr(Region(a), method)(Region(b)))
        self.error("AXIS_MISMATCH", lambda: knowledge_predicate("confidence", Region(a), Region(b)))
        self.error("UNSUPPORTED_PROFILE", lambda: Axis("symbolic", "unsupported"))
        self.error("UNSUPPORTED_VERSION", lambda: Axis("versioned", "Q", version=2))

    def test_full_cell_identity_typed_value_provenance_and_null(self):
        axis = Axis("cells", "Q")
        region = Region.span(axis, 0, 10)
        empty = Component(axis)
        boolean, _ = empty.replace(region, Cell(True, True, "r", "p"))
        integer, changes = boolean.replace(region, Cell(True, 1, "r", "p"))
        self.assertEqual(len(changes), 1)  # Python True==1 must not erase typed change.
        self.assertIs(type(integer.at(5).value), int)
        provenance, changes = integer.replace(region, Cell(True, 1, "r", "p2"))
        self.assertEqual(len(changes), 1)
        replay, changes = provenance.replace(region, Cell(True, 1, "r", "p2"))
        self.assertEqual(changes, ())
        self.assertEqual(replay.at(5), provenance.at(5))
        null, _ = empty.replace(region, Cell(True, None, "null", "p"))
        unset, _ = null.replace(Region.span(axis, 3, 7), Cell(False, None, "unset", "p2"))
        self.assertTrue(null.at(5).present)
        self.assertIsNone(null.at(5).value)
        self.assertFalse(unset.at(5).present)
        self.assertEqual(unset.at(5).revision, "unset")
        self.assertNotEqual(unset.at(5), ABSENT)
        self.error("REVISION_REQUIRED", lambda: empty.replace(region, Cell(True, "x")))

    def test_owned_payload_and_prior_snapshot_are_immutable(self):
        payload = {"scope": {"axis": "property-other-axis", "positions": [50, 60]}, "kind": "TimeSpan"}
        snap, _ = Snapshot("g", Axis("owner", "Q")).apply([create("A", {"t": payload})], "r1")
        payload["scope"]["positions"][0] = 999
        projected = snap.project(5)
        self.assertEqual(projected["nodes"]["A"]["properties"]["t"]["scope"]["positions"], [50, 60])
        projected["nodes"]["A"]["properties"]["t"]["scope"]["positions"].clear()
        self.assertEqual(snap.project(5)["nodes"]["A"]["properties"]["t"]["scope"]["positions"], [50, 60])
        self.assertEqual(set(snap.project(5)["nodes"]), {"A"})
        self.assertFalse(snap.project(55)["nodes"])  # Property scope never expands owner life.

    def test_node_rel_parity_old_snapshot_after_mutation(self):
        old, _ = Snapshot("g", Axis("history", "Q")).apply([
            create("A", {"v": "before"}, labels=["OldLabel"]), create("B"),
            edge("E", properties={"v": "before"}), edge("parallel")], "r1", "p1")
        current, _ = old.apply([mutation("set", owner, (3, 7), key="v", value="after") for owner in ("A", "E")]
                               + [mutation("remove_label", "A", (3, 7), label="OldLabel")], "r2", "p2")
        for bucket, owner in (("nodes", "A"), ("edges", "E")):
            self.assertEqual(old.project(5)[bucket][owner]["properties"]["v"], "before")
            self.assertEqual(current.project(5)[bucket][owner]["properties"]["v"], "after")
            self.assertEqual(current.project(8)[bucket][owner]["properties"]["v"], "before")
        self.assertEqual(old.project(5)["nodes"]["A"]["labels"], ["OldLabel"])
        self.assertEqual(current.project(5)["nodes"]["A"]["labels"], [])
        self.assertEqual(set(current.project(5)["nodes"]), {"A", "B"})
        self.assertEqual(set(current.project(5)["edges"]), {"E", "parallel"})
        self.assertFalse(current.project(10)["edges"])

    def test_disjoint_native_region_graph_mutation_and_axis_validation(self):
        old, _ = Snapshot("g", Axis("regions", "Q")).apply([create("A", {"v": "old"})], "r1")
        scope = {"kind": "region", "pieces": [{"lower": 0, "upper": 2}, {"lower": 4, "upper": 6}]}
        changed, _ = old.apply([{"op": "set", "owner": "A", "valid": scope, "key": "v", "value": "new"}], "r2")
        self.assertEqual([changed.project(p)["nodes"]["A"]["properties"]["v"] for p in (1, 2, 3, 4, 6)], ["new", "old", "old", "new", "old"])
        self.assertEqual(old.project(1)["nodes"]["A"]["properties"]["v"], "old")
        bad_empty_part = {"kind": "region", "pieces": [{"lower": 1, "upper": 1, "axis": "other"}]}
        self.error("AXIS_MISMATCH", lambda: old.apply([{"op": "set", "owner": "A", "valid": bad_empty_part, "key": "v", "value": "new"}], "bad"))

    def test_close_hole_reopens_same_life_but_reopen_never_rebinds(self):
        old, _ = Snapshot("g", Axis("life", "Q")).apply([
            create("A", {"v": "old"}), create("B"), edge("E"),
            edge("self", "A", "A"), edge("observation", mode="identity_reference")], "r1")
        hole, _ = old.apply([mutation("close", "A", (3, 5), life=1)], "r2")
        self.assertEqual(set(hole.project(4)["edges"]), {"observation"})
        self.assertEqual(set(hole.project(5)["edges"]), {"E", "self", "observation"})
        self.assertEqual(hole.project(5)["nodes"]["A"]["life"], 1)
        closed, _ = hole.apply([mutation("close", "A", (5, 10), life=1)], "r3")
        reopened, _ = closed.apply([mutation("reopen", "A", (6, 10), life=2)], "r4")
        self.assertEqual(set(reopened.project(7)["edges"]), {"observation"})
        self.assertEqual(set(reopened.project(7, "declared")["edges"]), {"E", "self", "observation"})
        status = reopened.project(7, lifecycle_status=True)["edges"]["observation"]["endpoint_status"][0]
        self.assertEqual(status, {"identity": "A", "exists": True, "active_life": 2,
                                  "bound_life": None, "bound_life_active": False})
        self.assertEqual(reopened.project(7)["nodes"]["A"]["properties"], {})
        self.assertEqual(set(old.project(7)["edges"]), {"E", "self", "observation"})
        self.error("LIFECYCLE_OVERLAP", lambda: reopened.apply([mutation("correct", "A", (3, 10), life=1, present=True)], "bad"))

    def test_life_bound_presence_corrections_verify_original_endpoint_lives(self):
        old, _ = Snapshot("g", Axis("strict-life", "Q")).apply([
            create("A"), create("B"), edge("E", properties={"v": 1})], "r1")
        closed, _ = old.apply([mutation("close", "A", (3, 10), life=1)], "r2")
        masked, _ = closed.apply([mutation("set", "E", (3, 7), key="v", value=2)], "r3")
        self.assertFalse(masked.project(5)["edges"])
        self.assertEqual(masked.project(5, "declared")["edges"]["E"]["properties"], {"v": 2})
        reopened, _ = masked.apply([mutation("reopen", "A", (6, 10), life=2)], "r4")
        before = reopened.project(7, "declared", True)
        for base, scope in ((masked, (3, 7)), (reopened, (6, 10))):
            self.error("OWNER_VALIDITY", lambda: base.apply([
                mutation("correct", "E", scope, life=1, present=True)], "bad"))
        self.assertEqual(reopened.project(7, "declared", True), before)
        restored, _ = reopened.apply([mutation("correct", "A", (3, 6), life=1, present=True)], "r5")
        corrected, _ = restored.apply([mutation("correct", "E", (3, 6), life=1, present=True)], "r6")
        self.assertEqual(corrected.project(4)["edges"]["E"]["properties"], {"v": 2})
        self.assertEqual(corrected.entity("E").lives[0].endpoint_lives, (1, 1))
        self.assertFalse(corrected.project(7)["edges"])
        self.assertEqual(old.project(5)["edges"]["E"]["properties"], {"v": 1})
        short, _ = Snapshot("g", Axis("strict-short", "Q")).apply([
            create("A", valid=(0, 5)), create("B", valid=(0, 5)), edge("E", valid=(0, 5))], "r1")
        self.error("OWNER_VALIDITY", lambda: short.apply([
            mutation("correct", "E", (5, 10), life=1, present=True)], "bad-extension"))
        self.assertFalse(short.project(7, "declared")["edges"])

    def test_declared_rel_edit_while_masked_and_atomic_failure(self):
        old, _ = Snapshot("g", Axis("masked", "Q")).apply([create("A"), create("B"), edge("E", properties={"v": 1})], "r1")
        masked, _ = old.apply([mutation("close", "A", (3, 7), life=1)], "r2")
        changed, _ = masked.apply([mutation("set", "E", (3, 7), key="v", value=9)], "r3")
        self.assertFalse(changed.project(5)["edges"])
        self.assertEqual(changed.project(5, "declared")["edges"]["E"]["properties"], {"v": 9})
        restored, _ = changed.apply([mutation("correct", "A", (3, 7), life=1, present=True)], "r4")
        self.assertEqual(restored.project(5)["edges"]["E"]["properties"], {"v": 9})
        self.assertEqual(old.project(5)["edges"]["E"]["properties"], {"v": 1})
        self.error("NOT_FOUND", lambda: old.apply([mutation("set", "A", (0, 10), key="v", value=99), edge("bad", target="MISSING")], "bad"))
        self.assertEqual(old.project(5)["nodes"]["A"]["properties"], {})
        # Corpus revision 2 isolates missing-target failure at fully live B,
        # after a tentative edit of the still-declared masked relationship.
        old_before, masked_before = old.project(5), changed.project(5, "declared")
        self.error("NOT_FOUND", lambda: changed.apply([
            mutation("set", "E", (0, 10), key="v", value=99),
            edge("bad-isolated", source="B", target="MISSING")], "bad-isolated"))
        self.assertEqual(old.project(5), old_before)
        self.assertEqual(changed.project(5, "declared"), masked_before)
        self.assertEqual(changed.project(5, "declared")["edges"]["E"]["properties"], {"v": 9})
        self.assertFalse(changed.project(5)["edges"])

    def test_limit_exact_fit_one_byte_and_intermediate_fragment_failures(self):
        self.error("INVALID_LIMITS", lambda: Limits(operations=0))
        axis = Axis("budgets", "Q", reference="reference" * 30)
        base = Snapshot("g", axis)
        operations = [create("A", {"variable-name-" * 20: "payload"}), create("B"), edge("E")]
        fitting, changes = base.apply(operations, "r", "p")
        fit = replace(Limits(), snapshot_json_bytes=metadata_bytes(fitting), change_json_bytes=metadata_bytes(changes))
        self.assertEqual(base.apply(operations, "r", "p", limits=fit)[0], fitting)
        for tight in (replace(fit, snapshot_json_bytes=fit.snapshot_json_bytes - 1),
                      replace(fit, change_json_bytes=fit.change_json_bytes - 1),
                      replace(fit, total_fragments=1), replace(fit, entities=2), replace(fit, operations=2),
                      replace(fit, input_json_bytes=1)):
            self.error("RESOURCE_LIMIT", lambda: base.apply(operations, "r", "p", limits=tight))
            self.assertFalse(base.entities)
        one_piece = replace(Limits(), region_fragments=1)
        a, b = Region.span(axis, 0, 3), Region.span(axis, 2, 5)
        # Input/intermediate pieces count even though final union is one piece.
        self.error("RESOURCE_LIMIT", lambda: a.union(b, one_piece))
        self.error("RESOURCE_LIMIT", lambda: a.difference(Region.span(axis, 1, 2), one_piece))
        component, _ = Component(axis).replace(Region.span(axis, 0, 10), Cell(True, "A", "r1"))
        self.error("RESOURCE_LIMIT", lambda: component.replace(Region.span(axis, 3, 7), Cell(True, "B", "r2"), replace(Limits(), component_fragments=2)))
        self.assertEqual(component.at(5).value, "A")
        self.error("RESOURCE_LIMIT", lambda: coord(Axis("QN-b", "QN"), (0, 8), replace(Limits(), coordinate_bits=3)))

    def test_tight_discrete_replay_and_coalescing_do_not_charge_transient_boundaries(self):
        z = Axis("scratch-Z", "Z")
        tight = replace(Limits(), coordinate_bits=2, region_fragments=1, component_fragments=1)
        all_z = Region.span(z, "-inf", "+inf", False, False)
        old, _ = Component(z).replace(all_z, Cell(True, "A", "r", "p"), tight)
        point3 = Region.span(z, 3, 3, True, True, tight)
        replay, changes = old.replace(point3, Cell(True, "A", "r", "p"), tight)
        self.assertEqual(changes, ())
        self.assertEqual(replay, old)
        self.assertEqual(replay.assertions[0].region, all_z)
        # Filling a gap joins two pieces of the same Cell into one final atom.
        left = Region.span(z, "-inf", 0, False, True)
        right = Region.span(z, 2, "+inf", True, False)
        fragmented, _ = Component(z).replace(left.union(right), Cell(True, "A", "r", "p"))
        filled, changes = fragmented.replace(Region.span(z, 1, 1, True, True), Cell(True, "A", "r", "p"), tight)
        self.assertEqual(filled.assertions[0].region, all_z)
        self.assertEqual(len(changes), 1)

    def test_generated_component_histories_against_literal_cells(self):
        rng = random.Random(20261009)
        for profile in ("Q", "Z", "QN"):
            axis = Axis("cell-model-" + profile, profile)
            endpoints = [-1, 0, 1, 2] if profile != "QN" else [(-1, 0), (0, 0), (0, 1), (1, 0), (1, 2)]
            probes = [Fraction(n, 2) for n in range(-2, 5)] if profile == "Q" else endpoints
            if profile == "QN":
                probes = list(product([Fraction(-1), Fraction(-1, 2), Fraction(0), Fraction(1, 2), Fraction(1)], range(4)))
            for sequence in range(20):
                state = Component(axis)
                truth = [ABSENT] * len(probes)
                saved = []
                for step in range(10):
                    lo, hi = rng.choice(endpoints), rng.choice(endpoints)
                    lc, hc = rng.choice((False, True)), rng.choice((False, True))
                    raw = Interval(coord(axis, lo), coord(axis, hi), lc, hc)
                    scope = Region.build(axis, [raw])
                    present = rng.choice((True, False))
                    value = rng.choice((None, True, 1, "A", "B")) if present else None
                    cell = Cell(present, value, f"{sequence}:{step}", f"p{step % 3}")
                    saved.append((state, tuple(truth)))
                    state, _ = state.replace(scope, cell)
                    truth = [cell if raw.contains(coord(axis, p)) else old for p, old in zip(probes, truth)]
                    self.assertEqual(tuple(state.at(p) for p in probes), tuple(truth))
                for historical, expected in saved:
                    self.assertEqual(tuple(historical.at(p) for p in probes), expected)

    def test_knowledge_preservation_and_empty_axis_failures(self):
        axis = Axis("knowledge", "Q")
        support = Region.span(axis, "119/10", "121/10", True, True)
        narrow, broad = Region.span(axis, 12, 13), Region.span(axis, 11, 13)
        self.assertEqual(knowledge_predicate("hard_support", support, narrow), {"holds": True, "consistency": "consistent"})
        self.assertFalse(knowledge_predicate("hard_support", support, narrow, "definite")["holds"])
        self.assertTrue(knowledge_predicate("hard_support", support, broad, "definite")["holds"])
        for predicate in ("possible", "definite"):
            self.assertEqual(knowledge_predicate("hard_support", Region(axis), broad, predicate), {"holds": False, "consistency": "inconsistent"})
        for kind in ("confidence95", "confidence100", "nominal", "opaque", "unspecified"):
            self.error("UNSUPPORTED_PREDICATE", lambda: knowledge_predicate(kind, support, broad))
        self.error("UNSUPPORTED_PREDICATE", lambda: knowledge_predicate("hard_support", support, broad, "invented"))

    def test_exact_ms_unit_mapping_and_ordinal_decline(self):
        ms_z, ms_q = Axis("ms-Z", "Z", unit="millisecond"), Axis("ms-Q", "Q", unit="millisecond")
        self.assertEqual(convert_units(-123, "millisecond", ms_z), -123)
        self.assertEqual(convert_units(1000, "microsecond", ms_z), 1)
        self.assertEqual(convert_units(1, "microsecond", ms_q), Fraction(1, 1000))
        self.assertEqual(convert_units(1001, "microsecond", ms_q), Fraction(1001, 1000))
        self.error("INCOMPATIBLE_DOMAIN", lambda: convert_units(1, "microsecond", ms_z))
        self.error("EXPLICIT_MAPPING_REQUIRED", lambda: convert_units(1, "ordinal", ms_q))
        for value in (-(1 << 63), (1 << 63) - 1, 0, -123):
            self.assertEqual(instant_ms_codec(value), value)
        for value in (-(1 << 63) - 1, 1 << 63):
            self.error("INSTANT_CODEC_RANGE", lambda: instant_ms_codec(value))
        self.error("NONINTEGRAL_INSTANT_CODEC", lambda: instant_ms_codec(Fraction(1, 1000)))
        self.assertEqual(coord(ms_z, 1 << 63)[1], 1 << 63)  # Domain is not the codec.
        for value in (True, 1.0, "broken", "1/0"):
            self.error("INVALID_COORDINATE", lambda: convert_units(value, "millisecond", ms_q))

    def test_graph_failures_and_typed_set_members(self):
        snap, _ = Snapshot("g", Axis("g", "Q")).apply([create("A")], "r1")
        self.error("IDENTITY_EXISTS", lambda: snap.apply([create("A")], "r2"))
        self.error("GRAPH_MISMATCH", lambda: snap.apply([mutation("set", "A", (0, 1), key="x", value=1, graph="other")], "r2"))
        self.error("AXIS_MISMATCH", lambda: snap.apply([mutation("set", "A", (0, 1), key="x", value=1, axis="other")], "r2"))
        self.error("VALIDITY_REQUIRED", lambda: snap.apply([{"op": "set", "owner": "A", "key": "x", "value": 1}], "r2"))
        self.error("UNSUPPORTED_PLACEMENT", lambda: snap.apply([{"op": "set", "owner": "A", "valid": "unplaced", "key": "x", "value": 1}], "r2"))
        self.error("NULL_SET_MEMBER_UNSUPPORTED", lambda: snap.apply([mutation("add", "A", (0, 1), key="x", value=None)], "r2"))
        both, _ = snap.apply([mutation("add", "A", (0, 10), key="x", value=True), mutation("add", "A", (0, 10), key="x", value=1)], "r2")
        members = both.project(5)["nodes"]["A"]["properties"]["x"]
        self.assertEqual([type(x) for x in members], [bool, int])
        self.error("TYPE_MISMATCH", lambda: Snapshot._validate_value(True, "I64", "scalar"))
        self.error("TYPE_MISMATCH", lambda: Snapshot._validate_value(1 << 63, "I64", "scalar"))
        self.error("TYPE_MISMATCH", lambda: Snapshot._validate_value(-(1 << 63) - 1, "I64", "scalar"))

    def test_revised_corpus_nonempty_all_requested_examples(self):
        document = corpus()
        ids = [c["id"] for c in document["cases"]]
        self.assertEqual(len(ids), len(set(ids)))
        for eid in [f"E{i:02}" for i in range(1, 11)] + ["E13", "E14", "E19", "E20"]:
            self.assertTrue(any(eid in name for name in ids), eid)

    def test_interpretation_and_role_do_not_follow_canonical_support(self):
        axis = Axis("interpretation-Z", "Z")
        point, span = Region.span(axis, 0, 0, True, True), Region.span(axis, 0, 1)
        self.assertEqual(point, span)
        snap, _ = Snapshot("g", axis).apply([
            dict(create("event", valid=(0, 0)), upper_closed=True, interpretation="occurrence", temporal_role="source_occurrence"),
            dict(create("state", valid=(0, 1)), interpretation="state", temporal_role="validity")], "r1")
        nodes = snap.project(0)["nodes"]
        self.assertEqual(set(nodes), {"event", "state"})
        self.assertEqual((nodes["event"]["interpretation"], nodes["event"]["temporal_role"]), ("occurrence", "source_occurrence"))
        self.assertEqual((nodes["state"]["interpretation"], nodes["state"]["temporal_role"]), ("state", "validity"))
        self.assertFalse(snap.project(1)["nodes"])


if __name__ == "__main__":
    unittest.main()
