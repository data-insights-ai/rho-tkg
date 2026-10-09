"""Executable v5 design examples, NOT production code or a protocol proof.

Run with: python3 -B docs/v5/reference/design_checks.py
Interval checks exhaust finite endpoint/boundary combinations and compare set
operations against independent point membership. Dense witnesses are constructed
from each boundary partition; they are not a fixed sampling of rational time.
Value/state and cut checks inform rho's database contract. STN, projection,
join and recursive examples illustrate sigma consumer semantics; they are not
proposed rho execution operators or production integration tests.
The distributed example enumerates application/fence/decision/read schedules for
one already-prepared transaction, treating consensus decisions as atomic.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction as Q
from itertools import permutations, product
from pathlib import Path
import importlib.util
import json
import math


@dataclass(frozen=True)
class Interval:
    lo: Q
    hi: Q
    lc: bool
    uc: bool

    def contains(self, x: Q) -> bool:
        return (x > self.lo or x == self.lo and self.lc) and (
            x < self.hi or x == self.hi and self.uc
        )


def empty(a: Interval, discrete: bool) -> bool:
    if discrete:
        lo = math.ceil(a.lo)
        hi = math.floor(a.hi)
        if Q(lo) == a.lo and not a.lc:
            lo += 1
        if Q(hi) == a.hi and not a.uc:
            hi -= 1
        return lo > hi
    return a.lo > a.hi or a.lo == a.hi and not (a.lc and a.uc)


def intersection(a: Interval, b: Interval) -> Interval:
    lo, hi = max(a.lo, b.lo), min(a.hi, b.hi)
    lc = (a.lc if a.lo >= b.lo else True) and (b.lc if b.lo >= a.lo else True)
    uc = (a.uc if a.hi <= b.hi else True) and (b.uc if b.hi <= a.hi else True)
    return Interval(lo, hi, lc, uc)


def difference(a: Interval, b: Interval, discrete: bool) -> list[Interval]:
    if empty(a, discrete):
        return []
    i = intersection(a, b)
    if empty(i, discrete):
        return [a]
    pieces = [
        Interval(a.lo, i.lo, a.lc, not i.lc),
        Interval(i.hi, a.hi, not i.uc, a.uc),
    ]
    return [p for p in pieces if not empty(p, discrete)]


def witnesses(a: Interval, b: Interval, discrete: bool) -> list[Q]:
    bounds = sorted({a.lo, a.hi, b.lo, b.hi})
    if discrete:
        return [Q(n) for n in range(math.floor(bounds[0]) - 1,
                                     math.ceil(bounds[-1]) + 2)]
    return sorted(set(bounds + [(x + y) / 2 for x, y in zip(bounds, bounds[1:])]
                      + [bounds[0] - 1, bounds[-1] + 1]))


def check_intervals() -> int:
    checked = 0
    for discrete in (False, True):
        bounds = ([Q(-2), Q(-1), Q(0), Q(1), Q(2)] if discrete else
                  [Q(-1), Q(0), Q(1, 3), Q(1), Q(2)])
        intervals = [Interval(a, b, lc, uc) for a, b, lc, uc in
                     product(bounds, bounds, (False, True), (False, True))]
        for a, b in product(intervals, repeat=2):
            points = witnesses(a, b, discrete)
            assert empty(a, discrete) == (not any(a.contains(x) for x in points))
            i, remaining = intersection(a, b), difference(a, b, discrete)
            for x in points:
                assert i.contains(x) == (a.contains(x) and b.contains(x))
                assert any(p.contains(x) for p in remaining) == (
                    a.contains(x) and not b.contains(x)
                )
            checked += 1
    assert empty(Interval(Q(0), Q(1), False, False), True)
    assert not empty(Interval(Q(0), Q(1), False, False), False)
    tails = difference(Interval(Q(0), Q(2), True, True),
                       Interval(Q(0), Q(2), False, False), False)
    assert tails == [Interval(Q(0), Q(0), True, True),
                     Interval(Q(2), Q(2), True, True)]
    shifted = Q(1, 3) + Q(1, 7)
    assert shifted == Q(10, 21)
    assert Q(str(shifted)) == shifted
    return checked


def check_knowledge() -> None:
    a, b = Decimal("11.9999999"), Decimal("12.0000001")
    assert (b - a) * 10**9 == 200
    assert a.quantize(Decimal(".000001")) == b.quantize(Decimal(".000001"))
    x, y, z, tolerance = map(Q, ("0", ".09", ".18", ".1"))
    assert abs(x-y) <= tolerance and abs(y-z) <= tolerance
    assert abs(x-z) > tolerance
    support = Interval(Q("11.9"), Q("12.1"), True, True)
    for window, definite in [(Interval(Q(12), Q(13), True, False), False),
                             (Interval(Q(11), Q(13), True, False), True)]:
        assert not empty(intersection(support, window), False)
        assert (not difference(support, window, False)) == definite
    for offset in (Q(-1), Q(0), Q(1)):
        assert (Q(11) + offset) - (Q(10) + offset) == 1
    # Independent endpoint combinations admit a different difference.
    assert (Q(11)-1) - (Q(10)+1) == -1
    assert (Q(12), 0) < (Q(12), 1)
    assert Q(12) - Q(12) == 0
    # Interval separation is not a metric.
    def gap(a, b):
        return max(Q(0), b[0]-a[1], a[0]-b[1])
    a, b, c = (Q(0), Q(2)), (Q(1), Q(3)), (Q(3), Q(4))
    assert gap(a, c) > gap(a, b) + gap(b, c)


def stn(constraints: list[tuple[int, int, int, int]]) -> list[list]:
    d = [[0 if i == j else math.inf for j in range(3)] for i in range(3)]
    for a, b, lo, hi in constraints:
        d[a][b] = min(d[a][b], hi)
        d[b][a] = min(d[b][a], -lo)
    for k, i, j in product(range(3), repeat=3):
        d[i][j] = min(d[i][j], d[i][k] + d[k][j])
    return d


def check_constraints() -> None:
    d = stn([(0, 1, 2, 4), (1, 2, 3, 5)])
    assert -d[2][0] == 5 and d[0][2] == 9
    bad = stn([(0, 1, 2, 4), (1, 2, 3, 5), (0, 2, 0, 4)])
    assert any(bad[i][i] < 0 for i in range(3))


def check_projection_before_box() -> None:
    # P(x,y1) on [0,1), P(x,y2) on [1,2). Projecting y away before
    # universal coverage differs from projecting independently boxed rows.
    runs = [Interval(Q(0), Q(1), True, False),
            Interval(Q(1), Q(2), True, False)]
    window_points = [Q(0), Q(1, 2), Q(1), Q(3, 2)]
    project_then_box = all(any(r.contains(t) for r in runs) for t in window_points)
    box_then_project = any(all(r.contains(t) for t in window_points) for r in runs)
    assert project_then_box and not box_then_project


def sum_weights(*relations: dict) -> dict:
    result = defaultdict(int)
    for relation in relations:
        for row, weight in relation.items():
            result[row] += weight
    return {row: weight for row, weight in result.items() if weight}


def join(left: dict, right: dict) -> dict:
    result = defaultdict(int)
    for (lk, lv), lw in left.items():
        for (rk, rv), rw in right.items():
            if lk == rk:
                result[(lk, lv, rv)] += lw * rw
    return {row: weight for row, weight in result.items() if weight}


def reachable(edges: set[tuple[str, str]], start: str) -> set[str]:
    seen, pending = set(), [start]
    while pending:
        current = pending.pop()
        for a, b in edges:
            if a == current and b not in seen:
                seen.add(b)
                pending.append(b)
    return seen


def check_incremental() -> None:
    left = {("k", "a"): 1, ("k", "b"): 1}
    right = {("k", "x"): 1, ("k", "y"): 1}
    dl = {("k", "a"): -1, ("k", "c"): 1}
    dr = {("k", "x"): -1, ("k", "z"): 1}
    old = join(left, right)
    new = join(sum_weights(left, dl), sum_weights(right, dr))
    delta = sum_weights(join(dl, right), join(left, dr), join(dl, dr))
    assert sum_weights(old, delta) == new
    broken_delta = sum_weights(join(dl, right), join(left, dr))
    assert sum_weights(old, broken_delta) != new
    # Reference outcomes, not an implementation of incremental recursion.
    edges = {("a", "b"), ("b", "d"), ("a", "c"), ("c", "d"), ("d", "c")}
    assert "d" in reachable(edges, "a")
    edges.remove(("b", "d"))
    assert "d" in reachable(edges, "a")
    edges.remove(("a", "c"))
    assert "d" not in reachable(edges, "a")
    causal = {("a", "c"), ("b", "c")}
    assert "b" not in reachable(causal, "a")
    assert "a" not in reachable(causal, "b")


def check_cut_schedules() -> dict:
    """Explore a prepared transaction at round 90 and fences at round 100.

    The reference snapshot derives from the whole transaction decision.
    The read implementation sees per-partition installed values.
    Weak certification deliberately ignores pending intents.
    No consensus, locking, crash recovery or rebalance is modeled here.
    """
    checked = certified = 0
    weak_counterexample = None
    actions = ("decision", "apply_a", "apply_b", "fence_a", "fence_b", "read")
    for committed, schedule in product((False, True), permutations(actions)):
        if any(schedule.index("decision") > schedule.index(a)
               for a in ("apply_a", "apply_b")):
            continue
        decision = None
        applied, fenced = set(), set()
        values = {"a": 0, "b": 0}
        for action in schedule:
            if action == "decision":
                decision = committed
            elif action.startswith("apply_"):
                owner = action[-1]
                assert decision is not None
                values[owner] = int(decision)
                applied.add(owner)
            elif action.startswith("fence_"):
                fenced.add(action[-1])
            elif action == "read":
                expected = (int(decision is True),) * 2
                actual = (values["a"], values["b"])
                if fenced == applied == {"a", "b"}:
                    assert actual == expected
                    certified += 1
                if fenced == {"a", "b"} and actual != expected:
                    weak_counterexample = weak_counterexample or {
                        "schedule": schedule, "actual": actual, "expected": expected
                    }
        checked += 1
    assert certified > 0 and weak_counterexample is not None
    return {"schedules": checked, "certified_reads": certified,
            "weak_cut_counterexample": weak_counterexample}


def original_oracle() -> dict:
    folder = Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location("temporal_oracle", folder/"temporal_oracle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cases = json.loads((folder/"temporal-fixtures.json").read_text())["cases"]
    return {"cases": len(cases), "assertions": sum(module.run_fixture(c) for c in cases)}


def main() -> None:
    boundary_cases = check_intervals()
    check_knowledge()
    check_constraints()
    check_projection_before_box()
    check_incremental()
    print(json.dumps({
        "original_state_oracle": original_oracle(),
        "interval_domain_pair_cases": boundary_cases,
        "worked_examples": {
            "value_and_knowledge_contracts": "passed",
            "sigma_consumer_mathematics": "passed",
        },
        "prepared_transaction_model": check_cut_schedules(),
        "scope": "design examples only; not a distributed protocol proof",
    }, indent=2))


if __name__ == "__main__":
    main()
