# Finite P06 first-YES process recovery evidence

This receipt qualifies one actual six-process parent in four cells: coordinator
A/B × COMMIT/ABORT after participant A1 dies by SIGKILL while the first genuine
YES reply is held. A successor retains the prepared endpoint-life guard,
recovers the exact original transaction, and releases the lock only after the
original decision settles. Duplicate Prepare/Resolve retain the original
outcomes without new business CDC. Exact whole-graph/index, current/old certified
cut, endpoint-effective life, canonical CDC and original-byte bindings are
asserted by the Go test. The unchanged independent `serial_witness` oracle
checks the bounded observed histories.

Normal, race and scoped coverage each passed one parent/four cases, with no
failures or skips. Affected vet, configured lint (zero issues), gosec and
symbol-level govulncheck passed. The latter retains two uncalled imported-package
and two uncalled required-module advisories. Scanner defaults exclude test-only
helpers; production inputs are unchanged. Coverage is diagnostic production
package coverage (5.7%); added test helpers are not instrumented.

`receipt.json` binds qualified source commits/hashes, exact commands, tool and
input manifests, sampled process-tree RSS, terminal cleanup, twelve raw captures,
serial witnesses and every `raw.tar.gz` member hash. The archive was read back
and every member verified before commit. It excludes database directories.
All 84 incarnations from the successful lanes were reaped; all 24 named killed
and reopened PIDs were absent at the terminal checks. The heavy lease is released.

The first RED is preserved under `normal-1`: the ABORT fixture initially changed
a label that violated the existing lifecycle observer contract. Endpoint-close
replaced that fixture mutation. A later Python-only one-line correction selects
a genuine COMMIT capture for wrong-after sensitivity rather than whichever
capture sorts first. Final offline tests pass against each successful lane;
qualified Go and checker bytes did not change.

To inspect the normal raw captures, extract the archive into a fresh directory
and run these commands from the matching source checkout (Python only):

```sh
python3 -B tools/v5-substrate/check-p06-history.py /path/to/raw/normal-2/*/p06-original-recovery.json
python3 -B tools/v5-substrate/check_p06_history_test.py /path/to/raw/normal-2/*/p06-original-recovery.json
```

The four replay tests reject equal-count missing/duplicate/reordered records,
changed original/provenance/request bindings, wrong after-values and current
state substituted for the old cut. They also exercise the existing mathematical
oracle's stale-key, write-skew and empty-range rejection controls; they do not
run a backend or implement another graph reducer.

Qualification is limited to these selected single-machine distributed P06
cells. Predicate/range guard campaigns, other fault cells, storage/VFS/network
and physical-host power-loss proofs, full P06/V2 acceptance, FDB comparison,
capacity qualification and engine selection remain open. JSON replay relies on
the qualified authentic extraction; hashes alone do not establish process
provenance or fsync durability. No full suite, push, tag, release or MR is implied.
