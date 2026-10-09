# v5 design references and executable checks

`temporal_oracle.py` and `temporal-fixtures.json` are copied unchanged from the
"temporal graph implementation v1.3" handoff (`reference/temporal_oracle.py`,
`fixtures/temporal.json`, 2026-09-17). They define the original exact-state
subset, not the complete revised v5 semantics. See [the revised plan](../PLAN.md)
and [discussion paper](../DISCUSSION.md). Time unit: integer UTC microsecond
test instants; system cuts are named commits (S1, S2, …).

Phase V1 ports the oracle to Go for the value/state reducer; engine adapters run
the same fixtures as storage lands. Until then these files are the state-subset
reference; do not edit them to match an implementation. The broader model and
distributed fault coverage require additional independent fixtures and models.

Ownership matters: rho-tkg implements graph storage, value predicates and
database consistency. Sigma-tkgd implements reasoning. The STN, correlation,
projection-before-box, join and recursive examples below are mathematical
consumer fixtures showing information rho must preserve. They are not proposed
rho solver/operator implementations or evidence that sigma already supports
each case. A passing fixture alone is not a passing production integration test.

`PLAN-2026-09-24.md` preserves the original uncommitted draft before the 2026-10-09
revision. It is historical, not the current plan. In particular, its D6 segment
deferral and universal interval model are superseded.

Run the design examples and original corpus together:

```sh
python3 -B docs/v5/reference/design_checks.py
```

Validation on 2026-10-09:

- Original state oracle: 16 cases, 52 read/error assertions passed.
- 20,000 interval/domain pair cases: emptiness, intersection and difference
  against independent membership, including open/closed bounds, singletons,
  inverted intervals, rational endpoints and discrete versus dense semantics.
- Worked value/knowledge examples and sigma consumer mathematics passed.
  The join check detects omission of the simultaneous-delta cross term;
  recursive reachability outcomes are reference recomputations.
- 480 decision/application/fence/read schedules for one already-prepared
  two-partition transaction: 80 certified reads agree with the transaction-level
  reference snapshot. Weakening certification to ignore pending application
  produces a concrete mixed-snapshot counterexample.

Dense witnesses are generated from the endpoints and open cells of each tested
interval pair, where membership is constant; they are not a claim that sampling
an arbitrary dense timeline is exhaustive. This finite corpus is not a proof
for unbounded inputs.

The distributed example treats consensus decisions as atomic. It does not model
lock conflicts, crashes, delayed prepare registration, topology changes or
multi-transaction certification. It is a counterexample/check of the stated
visibility distinction, not validation of the complete proposed protocol.
V0/V2 require the independent state-machine and fault tests in the plan.

No production engine or Go API is implemented by these reference files.
