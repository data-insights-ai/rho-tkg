# Focused CI cost qualification

[cost-final-source-20261013.json](cost-final-source-20261013.json) adapts the
immutable [final-source receipt](../../../../tasks/evidence/v2-fdb-cost-final-source-20261013/receipt.json)
to `scripts/ci-changes.py`'s existing external-source contract. Both normal and
race logs record the same two passing SDK parents without failures or skips;
the same launcher terminal records exit and cleanup zero. Source hashes are
relative to the FDB module, as the validator requires. The receipt also binds
the original source capture, controller, evidence receipts and raw archives.
No validator policy or qualified executable source changed.

The final source checkpoint is `406dbb4c24c8c60cfb5b2ac5c9e5c38391fd7669`.
Its actual instrumented two-fact cross100 client checks retained old/current
graph state, phantom/posting refusals, complete local CDC and artifact IO.
Configured statics are carried only from identical executable source. Earlier
missing-client-RSS, monitor and security failures remain in their original
bundles. This adapter grants only recorded focused source qualification, with
no cost/RSS, matched 10k, five-run matrix, engine-selection or full V2 acceptance.

The controller at that source checkpoint checked retained graph values and
postings, with the component-change total following the declared fact count
and runner validation. It did not compare complete CDC streams across engines.

The later Python-only `compare-cost-cdc.py` and controller call now stream every
logical group, before/after effect and entity/life/value declaration, validating
all FDB physical writes and original bindings. Native inputs require decoded
cost-only originals, named empty-output classifications, raw hash bindings,
same-index source roots and complete prefix bounds tied to the final certified
cut. Missing metadata fails closed. Native partitions 3/8 correspond to FDB
owners 0/1; numeric revision coordinates and physical protocol records retain
their own validation rather than being compared as equal bytes.

This is a later Python-only source delta. The frozen 34 FDB SDK inputs and their
receipts remain unchanged; the original controller hash in the adapter names
the historical controller. The instrumented FDB run did not execute the updated
controller. Replay tests use genuine retained FDB two-fact LOCAL/CROSS exports
and explicitly synthetic native metadata fixtures. Genuine later native
two-fact exports additionally match complete LOCAL/CROSS CDC, with seven groups
and nine logical effects per pair. The LOCAL FDB source is the historical CBC
profile; CROSS uses the frozen final source. Separate actual native 17-fact
checks establish only native parsing. No matched measured cost cell, RSS,
five-run matrix or engine acceptance follows.

Input lines are capped at 64 KiB and the native join holds at most 16 pending
operations. The offline verifier's predecessor maps and original-identity sets
grow with the fact count; this is not a constant-memory comparison. The
controller also retains its existing 64 MiB point-view limit. One-million-fact
and larger-layout resource qualification remains pending, including measured
state bounds or spooling if needed. The fixed cost contract admits only the
declared participant ownership and protocol phases; alternate graph footprints
are refused and receive no broader public graph semantics acceptance.

The bounded source-only checks are:

```sh
python3 -B scripts/ci-changes-test.py
python3 -B scripts/ci-changes.py external --base 9e805ddc --head HEAD
python3 -B tools/v5-substrate/compare_cost_cdc_test.py
```
