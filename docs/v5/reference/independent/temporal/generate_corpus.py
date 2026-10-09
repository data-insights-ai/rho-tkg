"""Generate versioned independent mathematical expected results."""
from pathlib import Path
from dataclasses import replace
from fractions import Fraction
import argparse
import hashlib
import json

from reference_model import (Axis, Cell, Component, Limits, ModelError, Region,
                             Snapshot, convert_units, knowledge_predicate, metadata_bytes, run_historical)

ROOT = Path(__file__).resolve().parent
HISTORICAL = ROOT.parent.parent / "temporal-fixtures.json"


def qregion(lo, hi, lc=True, hc=False, axis=None):
    return Region.span(axis or Axis("abstract-Q", "Q"), lo, hi, lc, hc)


def create(identity, properties=None, valid=(0, 10), labels=None):
    return {"op": "create_vertex", "id": identity, "life": 1,
            "valid": list(valid), "properties": properties or {}, "labels": labels or []}


def edge(identity, source="A", target="B", mode="life_bound", valid=(0, 10), properties=None):
    return {"op": "create_edge", "id": identity, "life": 1, "source": source,
            "target": target, "type": "LINK", "reference_mode": mode,
            "valid": list(valid), "properties": properties or {}}


def mutation(op, owner, valid, **fields):
    return {"op": op, "owner": owner, "valid": list(valid), **fields}


def graph_case(identity, semantics, steps, reads, failures=(), profile="Q"):
    axis = Axis(f"{identity}-axis", profile)
    current = Snapshot(identity, axis)
    snapshots = {}
    encoded_steps = []
    for name, ops in steps:
        current, changes = current.apply(ops, name + ":rev", name + ":provenance")
        snapshots[name] = current
        encoded_steps.append({"snapshot": name, "revision": name + ":rev",
                              "provenance": name + ":provenance", "operations": ops,
                              "change_count": len(changes)})
    outcomes = []
    for name, coordinate, mode, status in reads:
        outcomes.append({"snapshot": name, "coordinate": coordinate, "mode": mode,
                         "lifecycle_status": status,
                         "expected": snapshots[name].project(coordinate, mode, status)})
    errors = []
    for base, ops, expected in failures:
        before = snapshots[base]
        try:
            before.apply(ops, "failed:rev", "failed:provenance")
        except ModelError as error:
            if error.code != expected:
                raise AssertionError((identity, error.code, expected))
        else:
            raise AssertionError((identity, "expected error did not occur"))
        errors.append({"base_snapshot": base, "operations": ops, "expected_error": expected,
                       "expected_atomic": True})
    return {"id": identity, "kind": "graph", "semantics": semantics, "axis": axis.__dict__,
            "steps": encoded_steps, "reads": outcomes, "failures": errors}


def corpus():
    cases = []
    for profile in ("Q", "Z", "QN"):
        axis = Axis("support-" + profile, profile)
        values = ("0", "1") if profile != "QN" else (("0", 0), ("0", 1))
        opened = Region.span(axis, *values, False, False)
        point = Region.span(axis, values[0], values[0], True, True)
        half = Region.span(axis, *values)
        assert bool(opened.parts) == (profile == "Q")
        assert point.subset(half)
        assert half.subset(point) == (profile != "Q")
        cases.append({"id": "E03-E08-support-" + profile, "kind": "region",
                      "semantics": "Domain support, not endpoint encoding; QN has no elapsed-unit inference.",
                      "axis": axis.__dict__, "input": {"lower": values[0], "upper": values[1]},
                      "expected": {"open_interval": opened.to_json(), "point": point.to_json(),
                                   "half_open": half.to_json(), "point_equals_half_open": half.subset(point)}})
    axis = Axis("E02-Q", "Q")
    base = Component(axis)
    old, _ = base.replace(qregion(0, 10, axis=axis), Cell(True, "A", "r1", "p1"))
    corrected, delta = old.replace(qregion(3, 7, axis=axis), Cell(True, "B", "r2", "p2"))
    retracted, _ = corrected.replace(qregion(4, 6, False, False, axis), Cell(False, None, "r3", "p3"))
    probes = ["0", "3", "4", "5", "6", "7", "10"]
    assert old.at(5).value == "A" and corrected.at(5).value == "B"
    assert not retracted.at(5).present and retracted.at(5).revision == "r3"
    assert retracted.at(4).value == "B" and retracted.at(6).value == "B"
    cases.append({"id": "E02-components-correction-retraction", "kind": "component",
                  "semantics": "Exact region correction; singleton boundary tails; immutable earlier snapshots.",
                  "axis": axis.__dict__, "operations": [
                      {"scope": qregion(0, 10, axis=axis).to_json(), "cell": Cell(True, "A", "r1", "p1").to_json()},
                      {"scope": qregion(3, 7, axis=axis).to_json(), "cell": Cell(True, "B", "r2", "p2").to_json()},
                      {"scope": qregion(4, 6, False, False, axis).to_json(), "cell": Cell(False, None, "r3", "p3").to_json()}],
                  "probes": probes, "expected": {
                      "old": [old.at(p).to_json() for p in probes],
                      "corrected": [corrected.at(p).to_json() for p in probes],
                      "retracted": [retracted.at(p).to_json() for p in probes]},
                  "changes": [{"scope": r.to_json(), "before": b.to_json(), "after": a.to_json()} for r, b, a in delta]})
    null_state, _ = base.replace(qregion(0, 10, axis=axis), Cell(True, None, "null-r1", "null-p1"))
    same_value_new_revision, changes = null_state.replace(qregion(3, 7, axis=axis), Cell(True, None, "null-r2", "null-p1"))
    replayed, replay = same_value_new_revision.replace(qregion(3, 7, axis=axis), Cell(True, None, "null-r2", "null-p1"))
    absent, _ = replayed.replace(qregion(4, 6, axis=axis), Cell(False, None, "unset-r3", "p3"))
    assert changes and not replay and absent.at(5) != base.at(5)
    cases.append({"id": "SEM12-null-revision-replay", "kind": "component",
                  "semantics": "Present-null, retracted and never asserted differ; equal payload new revision changes; exact replay does not.",
                  "axis": axis.__dict__, "operations": [
                      {"scope": qregion(0, 10, axis=axis).to_json(), "cell": Cell(True, None, "null-r1", "null-p1").to_json()},
                      {"scope": qregion(3, 7, axis=axis).to_json(), "cell": Cell(True, None, "null-r2", "null-p1").to_json()},
                      {"scope": qregion(3, 7, axis=axis).to_json(), "cell": Cell(True, None, "null-r2", "null-p1").to_json()},
                      {"scope": qregion(4, 6, axis=axis).to_json(), "cell": Cell(False, None, "unset-r3", "p3").to_json()}],
                  "expected": {"null": null_state.at(5).to_json(),
                      "corrected_null": same_value_new_revision.at(5).to_json(),
                      "retracted": absent.at(5).to_json(), "never_asserted": base.at(5).to_json(),
                      "new_revision_change_count": len(changes), "replay_change_count": len(replay)}})
    events = [dict(create(name, valid=(12, 12)), upper_closed=True, interpretation="occurrence", temporal_role="occurrence_time") for name in ("login-1", "login-2")]
    cases.append(graph_case("E01-event-multiplicity", "Same point preserves two identities; point does not establish persistence.",
        [("S1", events)], [("S1", 12, "effective", False), ("S1", 13, "effective", False)]))
    cases.append(graph_case("E01-E03-interpretation-independent-of-support-Z", "Point(0) and span[0,1) have equal support on Z but retain distinct supplied interpretation and temporal role; canonical support must not assign assertion meaning.",
        [("S1", [dict(create("event", valid=(0, 0)), upper_closed=True, interpretation="occurrence", temporal_role="source_occurrence"),
                 dict(create("state", valid=(0, 1)), interpretation="state", temporal_role="validity")])],
        [("S1", 0, "effective", False), ("S1", 1, "effective", False)], profile="Z"))
    cases.append(graph_case("E19-close-reopen-identity-reference", "Close masks bound edges but preserves declared evidence; reopen uses new LifeID; identity reference remains addressable with status.",
        [("S1", [create("A", {"name": "old-A"}), create("B"), edge("bound"), edge("observation", mode="identity_reference")]),
         ("S2", [mutation("close", "A", (4, 10), life=1)]),
         ("S3", [mutation("reopen", "A", (6, 10), life=2)]),
         ("S4", [mutation("correct", "A", (4, 6), life=1, present=True)])],
        [(s, p, mode, True) for s, p, mode in [("S1", 7, "effective"), ("S2", 5, "effective"),
            ("S3", 7, "effective"), ("S3", 7, "declared"), ("S4", 5, "effective"), ("S4", 7, "effective")]],
        [("S3", [mutation("correct", "A", (4, 10), life=1, present=True)], "LIFECYCLE_OVERLAP"),
         ("S3", [mutation("reopen", "A", (4, 5), life=1)], "LIFE_EXISTS")]))
    cases.append(graph_case("E19-strict-life-bound-presence-correction", "A presence correction verifies the original endpoint LifeIDs; a reopened life cannot authorize it, while masked declared property edits remain valid.",
        [("S1", [create("A"), create("B"), edge("E", properties={"v": 1})]),
         ("S2", [mutation("close", "A", (3, 10), life=1), mutation("set", "E", (3, 7), key="v", value=2)]),
         ("S3", [mutation("reopen", "A", (6, 10), life=2)]),
         ("S4", [mutation("correct", "A", (3, 6), life=1, present=True),
                 mutation("correct", "E", (3, 6), life=1, present=True)])],
        [("S1", 5, "effective", True), ("S2", 5, "effective", True),
         ("S2", 5, "declared", True), ("S3", 7, "effective", True),
         ("S3", 7, "declared", True), ("S4", 4, "effective", True),
         ("S4", 7, "effective", True)],
        [("S2", [mutation("correct", "E", (3, 7), life=1, present=True)], "OWNER_VALIDITY"),
         ("S3", [mutation("correct", "E", (6, 10), life=1, present=True)], "OWNER_VALIDITY")]))
    cases.append(graph_case("SEM03-06-node-rel-property-history", "Independent component corrections; relationship edits check declared life even when endpoint masks effective projection.",
        [("S1", [create("A", {"name": None}, labels=["Person"]), create("B"), edge("E", properties={"weight": 1})]),
         ("S2", [mutation("close", "A", (3, 7), life=1),
                 mutation("set", "E", (3, 7), life=1, key="weight", value=9)]),
         ("S3", [mutation("correct", "A", (3, 7), life=1, present=True),
                 mutation("unset", "A", (4, 6), life=1, key="name"),
                 mutation("remove_label", "A", (4, 6), life=1, label="Person"),
                 mutation("unset", "E", (4, 6), life=1, key="weight")])],
        [(s, p, mode, False) for s, p, mode in [("S1", 5, "effective"), ("S2", 5, "effective"),
         ("S2", 5, "declared"), ("S3", 3, "effective"), ("S3", 5, "effective"), ("S3", 6, "effective")]],
        [("S2", [mutation("set", "E", (0, 10), key="weight", value=99), edge("bad", source="B", target="MISSING")], "NOT_FOUND")]))
    cases.append(graph_case("SEM14-16-endpoint-boundary", "Endpoint half-open boundary has no common support; failed late-bound reference and covered-life creation are atomic.",
        [("S1", [create("A", valid=(0, 5)), create("B", valid=(5, 10)), create("C")])],
        [("S1", p, "effective", False) for p in (4, 5, 10)],
        [("S1", [edge("bad", valid=(4, 6))], "OWNER_VALIDITY"),
         ("S1", [edge("late", target="MISSING", mode="identity_reference")], "NOT_FOUND"),
         ("S1", [mutation("set", "A", (0, 1), key="x", value=1, graph="wrong")], "GRAPH_MISMATCH"),
         ("S1", [mutation("set", "A", (0, 1), key="x", value=1, axis="wrong")], "AXIS_MISMATCH")]))
    pieces = {"kind": "region", "pieces": [{"lower": "0", "upper": "2"}, {"lower": "4", "upper": "6"}]}
    cases.append(graph_case("E02-disjoint-native-region", "A disjoint native correction preserves both gaps and earlier full-cell history; graph projection uses exact finite-region support.",
        [("S1", [create("A", {"v": "old"}), create("B"), edge("E", properties={"v": "old"})]),
         ("S2", [{"op": "set", "owner": owner, "valid": pieces, "key": "v", "value": "new"} for owner in ("A", "E")])],
        [(s, p, "effective", False) for s in ("S1", "S2") for p in (1, 2, 3, 4, 6)]))
    # Information-preservation fixtures intentionally do not run a solver.
    preserved = {
        "E06": {"interpretation": "shared-constraint", "correlation": "clock-offset-x",
                "sources": [{"id": "A", "nominal": "10", "latent": "x"}, {"id": "B", "nominal": "11", "latent": "x"}],
                "constraint": {"x_lower": "-1", "x_upper": "1"}, "schema_version": 3},
        "E10": {"interpretation": "constraint", "schema_version": 2,
                "constraints": [{"difference": ["b", "a"], "lower": "2", "upper": "4"},
                                {"difference": ["c", "b"], "lower": "3", "upper": "5"}]},
        "E13": {"interpretation": "calendar", "timezone": "Europe/Vienna", "tzdb_version": "fixture-pinned-v1",
                "ambiguity_policy": "explicit-unresolved", "calendar_period": "P1D", "elapsed_duration": "PT24H"},
        "E14": {"interpretation": "property-value", "kind": "TimeSpan", "axis": "property-axis-Q", "scope": ["50", "60"]},
        "E20": {"interpretation": "summary", "quantifier": "Any", "window": ["0", "10"],
                "input_snapshot": "named-fixture-input-S1", "rule_version": "summary-v1"}}
    for eid, payload in preserved.items():
        case = graph_case(eid + "-preservation", "Opaque typed source metadata survives correction; storage does not execute a solver/calendar/summary substitution.",
            [("S1", [create("evidence", {"descriptor": payload})]),
             ("S2", [mutation("set", "evidence", (3, 7), key="descriptor", value={**payload, "source_correction": "v2"})])],
            [("S1", 5, "effective", False), ("S2", 5, "effective", False), ("S2", 8, "effective", False)])
        case["native_predicates"] = "preservation_only"
        case["must_not_infer"] = {"E06": "independent uncertainty envelopes", "E10": "STN satisfiability or consequences",
             "E13": "calendar day equals 24 elapsed hours", "E14": "property span sets owner life", "E20": "Any implies All"}[eid]
        cases.append(case)
    cases.append(graph_case("E07-causal-records", "Two asserted precedence edges enumerate without inventing A-vs-B precedence or running transitive closure.",
        [("S1", [create("A"), create("B"), create("C"),
                 dict(edge("A-before-C", "A", "C", "identity_reference"), type="PRECEDES", interpretation="asserted_relation", temporal_role="causal_order"),
                 dict(edge("B-before-C", "B", "C", "identity_reference"), type="PRECEDES", interpretation="asserted_relation", temporal_role="causal_order")])], [("S1", 5, "effective", False)]))
    precise_a, precise_b = Fraction("11.9999999"), Fraction("12.0000001")
    cases.append({"id": "E04-exact-nearby-values", "kind": "value", "semantics": "Explicit rounding may equate derived coordinates; no event identity collapse.",
                  "input": [str(precise_a), str(precise_b)], "expected": {"equal": False, "difference_seconds": str(precise_b - precise_a),
                  "explicit_nearest_microsecond": [str(round(precise_a * 1000000)), str(round(precise_b * 1000000))], "event_ids": ["a", "b"]}})
    source_p, source_q = qregion(0, 4), qregion(2, 6)
    expected_join = qregion(2, 4)
    assert source_p.intersection(source_q) == expected_join
    cases.append({"id": "E09-source-history-derived-fraction", "kind": "region", "semantics": "Rho supplies exact source regions and accepts sigma-supplied result; model does not own join execution.",
                  "source_regions": [source_p.to_json(), source_q.to_json()],
                  "supplied_result": expected_join.to_json(), "supplied_shifted_result": qregion("7/3", "13/3").to_json(),
                  "expected": {"exact_fraction": "1/3", "no_rounding": True}})
    support = qregion("119/10", "121/10", True, True)
    for name, window in (("possible-only", qregion(12, 13)), ("definite", qregion(11, 13))):
        cases.append({"id": "E05-" + name, "kind": "knowledge", "semantics": "Hard possible support is not occupied duration.",
                      "support": support.to_json(), "window": window.to_json(),
                      "expected": {p: knowledge_predicate("hard_support", support, window, p) for p in ("possible", "definite")}})
    cases.append({"id": "E05-empty-hard-support", "kind": "knowledge", "support": Region(support.axis).to_json(),
                  "window": qregion(11, 13).to_json(), "expected": {p: knowledge_predicate("hard_support", Region(support.axis), qregion(11, 13), p) for p in ("possible", "definite")}})
    for kind in ("confidence_95_percent", "confidence_100_percent", "nominal", "opaque_correlated_constraint", "unspecified"):
        try:
            knowledge_predicate(kind, support, qregion(11, 13))
        except ModelError as e:
            assert e.code == "UNSUPPORTED_PREDICATE"
        cases.append({"id": "E05-decline-" + kind, "kind": "knowledge", "knowledge_kind": kind,
                      "support": support.to_json(), "window": qregion(11, 13).to_json(), "expected_error": "UNSUPPORTED_PREDICATE"})
    for profile, source, expected in (("Z", "1000", "1"), ("Q", "1", "1/1000"), ("Q", "1001", "1001/1000"), ("Z", "1", None)):
        target = Axis("ms-" + profile, profile, "explicit-ms-test-reference", "millisecond")
        case = {"id": "V0-units-us-to-ms-" + profile + "-" + source, "kind": "unit_mapping", "source_unit": "microsecond",
                "source_coordinate": source, "target_axis": target.__dict__, "mapping": "divide_by_1000"}
        try:
            result = convert_units(source, "microsecond", target)
        except ModelError as error:
            assert expected is None and error.code == "INCOMPATIBLE_DOMAIN"
            case["expected_error"] = error.code
        else:
            assert str(result) == expected
            case["expected"] = expected
        cases.append(case)
    default_ms = Axis("default-ms-explicit-test", "Z", "fixture-default-reference", "millisecond")
    assert convert_units("-123", "millisecond", default_ms) == -123
    cases.append({"id": "V0-default-ms-identity", "kind": "unit_mapping", "source_unit": "millisecond", "source_coordinate": "-123",
                  "target_axis": default_ms.__dict__, "mapping": "identity", "expected": "-123",
                  "note": "Spec pins default Instant int64 millisecond codec; axis identity/reference itself is not guessed by this oracle."})
    cases.append({"id": "V0-default-instant-codec-range", "kind": "codec_contract",
                  "semantics": "Instant's signed int64 millisecond codec range is distinct from mathematical Z/Q domain.",
                  "expected": {"minimum_ms": str(-(1 << 63)), "maximum_ms": str((1 << 63) - 1),
                               "below_minimum": "INSTANT_CODEC_RANGE", "above_maximum": "INSTANT_CODEC_RANGE",
                               "fractional_ms": "NONINTEGRAL_INSTANT_CODEC", "general_dense_Q_value": "1/1000 remains exact"}})
    cases.append({"id": "V0-order-only-no-seconds", "kind": "unit_mapping", "source_unit": "ordinal",
                  "source_coordinate": "7", "target_axis": default_ms.__dict__, "expected_error": "EXPLICIT_MAPPING_REQUIRED"})
    cases.append({"id": "V6-one-tick-import-ambiguity", "kind": "import_contract", "valid_from_ms": "0", "valid_to_ms": "1",
                  "writer_provenance": None, "expected": "preserve_legacy_evidence_and_report_ambiguity",
                  "must_not_infer": "event from one-tick width or post-2026-06-12 dates alone"})
    cases.append({"id": "strict-symbolic-placement-decline", "kind": "failure", "operation": {"op": "set", "valid": {"kind": "opaque_constraint"}},
                  "expected_error": "UNSUPPORTED_PLACEMENT", "caller_solver_evidence": "not_a_database_verifier"})
    cases.append({"id": "axis-conflicting-definition-empty", "kind": "failure", "left_axis": Axis("same-id", "Q", unit="abstract-a").__dict__,
                  "right_axis": Axis("same-id", "Q", unit="abstract-b").__dict__, "left_scope": "empty", "right_scope": "empty",
                  "expected_error": "AXIS_MISMATCH", "semantics": "Validate axes even for empty operands; no implicit conversion."})
    budget_base = Snapshot("budget-graph", Axis("budget-axis", "Q"))
    budget_ops = [create("A", {"long-" + "name" * 50: "payload"}), create("B"), edge("E")]
    fitting, changes = budget_base.apply(budget_ops, "r-budget", "p-budget")
    cases.append({"id": "budget-variable-name-axis-change-ledger", "kind": "budget",
                  "semantics": "Count variable component names, complete axis definitions, revisions/provenance and before/after data; per-component caps do not replace aggregate caps.",
                  "axis": budget_base.axis.__dict__, "operations": budget_ops,
                  "accounting": "Independent canonical oracle JSON envelope; not Go wire or heap measurements.",
                  "snapshot_bytes": metadata_bytes(fitting), "change_bytes": metadata_bytes(changes),
                  "exact_fit": {"snapshot_json_bytes": metadata_bytes(fitting), "change_json_bytes": metadata_bytes(changes)},
                  "one_byte_less": "RESOURCE_LIMIT", "expected_atomic": True})
    cases.append({"id": "dependency-absence-phantom-budget-contract", "kind": "transaction_contract",
                  "semantics": "Initial exact planner may use coarse original snapshot dependency. Otherwise it must include checked absence, all-life phantom predicates, owner/endpoint lives and external values; batch overlay reads are not original snapshot evidence.",
                  "reads": [{"kind": "entity_absence", "entity": "new-E", "source": "original_snapshot"},
                            {"kind": "owner_lives_predicate", "owner": "A", "including_empty": True, "source": "original_snapshot"},
                            {"kind": "endpoint_life", "owner": "A", "life": 1, "source": "original_snapshot"},
                            {"kind": "component_absence", "name": "new-property", "source": "batch_overlay"}],
                  "expected": "validate_complete_original_read_footprint_before_atomic_install",
                  "budget_failure": "RESOURCE_LIMIT when full dependency metadata cannot be represented within configured transaction cap",
                  "model_status": "normative input/validation obligation; no serializability, lock, transaction or cut simulator in this model"})
    cases.append({"id": "replay-request-vs-revision-contract", "kind": "transaction_contract",
                  "component_replay": "same region + presence/value + revision + provenance yields zero changes",
                  "new_revision_same_value": "change required", "new_provenance_same_revision": "change required",
                  "create_identity_reuse": "IDENTITY_EXISTS in this reference graph adapter",
                  "request_key_deduplication": "not modeled; V2 requires durable decision and retention-lease contract"})
    z = Axis("tight-replay-Z", "Z")
    tight = replace(Limits(), coordinate_bits=2, region_fragments=1, component_fragments=1)
    all_z = Region.span(z, "-inf", "+inf", False, False)
    original, _ = Component(z).replace(all_z, Cell(True, "A", "r", "p"), tight)
    replayed, changes = original.replace(Region.span(z, 3, 3, True, True, tight), Cell(True, "A", "r", "p"), tight)
    assert replayed == original and changes == ()
    cases.append({"id": "budget-tight-Z-replay-no-transient-successor", "kind": "component",
                  "semantics": "Exact replay inside All does not charge a transient coordinate4 absent from final state. Algebra working caps and final policy are distinct.",
                  "axis": z.__dict__, "limits": tight.__dict__, "initial_scope": all_z.to_json(),
                  "initial_cell": Cell(True, "A", "r", "p").to_json(),
                  "mutation_scope": Region.span(z, 3, 3, True, True, tight).to_json(),
                  "mutation_cell": Cell(True, "A", "r", "p").to_json(),
                  "expected": {"state_unchanged": True, "change_count": 0, "scope": all_z.to_json()}})
    return {"schema": "rho-v5-independent-acceptance", "version": 1, "corpus_revision": 3,
            "input_corrections": [{"case": "SEM03-06-node-rel-property-history", "change": "Atomic-failure bad edge source A to fully live B; missing target unchanged.",
                                   "reason": "Isolate intended NOT_FOUND; original input also violated closed source life and no error precedence is specified.",
                                   "expected_error": "NOT_FOUND", "original_historical_16_52": "unchanged"},
                                  {"case": "E19-strict-life-bound-presence-correction", "change": "Presence corrections validate coverage by originally bound endpoint LifeIDs; declared property edits while masked remain allowed.",
                                   "reason": "Review found false acceptance of a strict presence placement outside endpoint lives; a reopened different life must not authorize that placement.",
                                   "expected_error": "OWNER_VALIDITY", "original_historical_16_52": "unchanged"}],
            "profile": "mathematical-reference-not-production-limits", "limits": Limits().__dict__,
            "physical_units": "Revised graph cases use abstract axes unless explicit unit mapping is named; historical corpus retains its UTC microsecond test instants.",
            "cases": cases}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--fixture", type=Path, default=HISTORICAL,
                        help="explicit historical fixture; defaults to the reference corpus beside independent/")
    args = parser.parse_args()
    results = {
        "revised-acceptance-v1.json": corpus(),
        "historical-parity-v1.json": {"schema": "rho-v5-independent-historical-parity", "version": 1,
            "source_sha256": hashlib.sha256(args.fixture.read_bytes()).hexdigest(),
            "source_units": "integer UTC microsecond test instants", "default_codec_units": "int64 milliseconds",
            "normalization": "Explicit us/1000; integral Z can refuse nonintegral ms; dense Q preserves exact fractions.",
            **run_historical(args.fixture)},
    }
    for name, value in results.items():
        text = json.dumps(value, indent=2, sort_keys=True) + "\n"
        path = ROOT / name
        if args.check:
            if path.read_text() != text:
                raise SystemExit("stale generated artifact: " + str(path))
        else:
            path.write_text(text)
    print(json.dumps({"revised_cases": len(results["revised-acceptance-v1.json"]["cases"]),
                      "historical_cases": results["historical-parity-v1.json"]["cases"],
                      "historical_assertions": results["historical-parity-v1.json"]["assertions"]}))


if __name__ == "__main__":
    main()
