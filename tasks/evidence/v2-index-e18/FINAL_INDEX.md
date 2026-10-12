# Finite E18 remote historical index evidence

Base: `13612eb315c0b15c29200d67a2cbeebfdd882fbd`. This change adds private test IPC and acceptance tests; production code, schemas, dependencies and source/session quotas are unchanged.

The six-OS-process test starts with `UniqueCandidates` or `IncidentRelationships`, not supplied candidate IDs. It hydrates each returned owner through Entity, Life, ComponentPage, Value and effective Project at the same certified cut. Node1 changes p9/ref5 to p10/ref6 and old-label to new-label; closing node17/life18 masks the life-bound edge3/4. Distinct node19/20 retains p11/ref21. Exact raw candidates preserve historical supersets; effective point-at-zero results separately check value equality and owner life. Both endpoint directions and canonical/declared posting doors retain edge3 with original endpoint LifeIDs and component revision/provenance. Held-old and newly opened At(old) answers preserve exact typed values, references, component bytes and revisions after correction. Current reads exclude stale effective matches; phantom999 is empty.

Actual source RPC traffic is observed after opening the held session: partition3 Get/Scan=[161,2], partition8=[136,2]. Matching voter Resolve outcomes/CDC/root bytes and persisted certificates are checked across all six voters. These checks establish replica parity; they are not a newly independent full CDC semantic oracle. Both final normal and race process runs reap six children successfully with zero owned views/pins; counted IPC=12,316 plus268 bootstrap packets. The fixed fixture allowance is32,768: default4,096 + seven admitted operations × four IPC events × existing1,024-call operation ceiling. This is an explicit test allowance, not a proved algorithmic worst-case or production SLO. Default4,096, packet/frame/queue guards, session caps2, wall/RSS limits are preserved.

`FINAL_RECEIPT.json` records terminal gates and scope. `go-source.json` seals520 Go files; `ENVIRONMENT.json` records the actual pinned toolchain and resource environment. `COVERAGE_SUMMARY.json` names instrumented receiver methods; no new production code/public method exists, and unchanged methods are not claimed to have exhaustive coverage from this focused selection.

Normal and race each pass19 parents +43 subtests in the principal selection, plus3 precise shared-IPC parents in the supplemental selection (22 unique parents total, no failure/skip). The principal selection includes the native local E18 baseline and remote E15/E16. New acceptance passes before mutation: there is no reproduced production bug claim. A one-edit isolated production mutant skips unique-posting enumeration and fails the exact pre-correction raw p12 assertion (`[]` versus `[17]`); it proves the test starts at the index, not current-state substitution. The unchanged native companion passes with the authentic services. Compiler-only failures and the original nine-selector cumulative1,024/1,024 refusal remain qualified fixture evidence, not semantic REDs. The corrected fixture uses explicit independent At operations, never resetting a live reader ledger.

Raw logs, profiles, watchdogs and the mutant diff are in `E18_RAW_EVIDENCE.tar.gz`. Its sorted USTAR entries have zero timestamps/owners and mode0644; gzip has timestamp0 and no filename. `ARCHIVE_PATH_HASHES.json` is directly readable and also inside the archive, with each original path, size and SHA-256.

Extract and verify from the repository root:

```sh
evidence=tasks/evidence/v2-index-e18
extracted=$(mktemp -d)
tar -xzf "$evidence/E18_RAW_EVIDENCE.tar.gz" -C "$extracted"
python3 - "$evidence" "$extracted" <<'PYVERIFY'
import hashlib, json, pathlib, sys
source, output = map(pathlib.Path, sys.argv[1:])
receipt = json.loads((source / 'FINAL_RECEIPT.json').read_text())
assert hashlib.sha256((source / 'E18_RAW_EVIDENCE.tar.gz').read_bytes()).hexdigest() == receipt['raw_archive']['sha256']
assert (source / 'ARCHIVE_PATH_HASHES.json').read_bytes() == (output / 'ARCHIVE_PATH_HASHES.json').read_bytes()
manifest = json.loads((source / 'ARCHIVE_PATH_HASHES.json').read_text())
for name, expected in manifest.items():
    data = (output / name).read_bytes()
    assert len(data) == expected['bytes'], name
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name
print(f'Verified {len(manifest)} original raw artifacts in {output}')
PYVERIFY
```

This is the finite existing-schema E18 slice on one host with six native OS processes. Relationship unique properties are not declared in this fixture; endpoint/incidence mirrors are covered. Broader selector/scan parity, larger/multi-host workloads, remaining V2 comparison and later V0–V7 work are outside this acceptance. No full module gate, dependency vulnerability rescan, or production coverage threshold is claimed anew; the accepted unchanged base retains those prior qualifications. No push is performed by this author.
