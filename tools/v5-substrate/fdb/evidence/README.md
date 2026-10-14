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

`cost-cell.py --compare` checks retained graph values and postings. It does not
read the engines' complete CDC/group streams for cross-engine equality; its
component-change total follows the declared fact count and runner validation.
Complete CDC comparison remains a separate gap before comparative acceptance.

The bounded source-only checks are:

```sh
python3 -B scripts/ci-changes-test.py
python3 -B scripts/ci-changes.py external --base 9e805ddc --head HEAD
```
