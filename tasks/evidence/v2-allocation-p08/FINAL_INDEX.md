# Native graph P08 author evidence

Test-only proof using real child-owned Acquire/Next, six native OS processes, quorum reservation crash boundaries, partial issuance, same-directory restarts, allocator/recipient fences, stale fresh Entity/Life/Value claims and exact original graph retries. Production bytes are unchanged.

Read `FINAL_RECEIPT.json`, `WITNESS_SUMMARY.json`, `FINAL_GATE_RESULTS.json`, `PROCESS_CLEANUP.json`, `SCOPED_COVERAGE.json` and `QUALIFICATIONS.json`. `final-inventory.json` pins selected parents; `final-go-source.json` pins all 522 Go files. Root-owned native range-move companion is separate.

52 test parents plus one fuzz seed parent pass in each normal/race mode. There are no selected skips. Each actual process schedule reaps all six live children successfully; four intentionally killed old incarnations are separately reaped per mode. The one-edit recipient-session omission fails at stale unused Node23/Life24 admission, proving sensitivity. This is new acceptance evidence, not a production regression correction.

Raw logs/profiles/watchdogs, including failed compiler/setup probes, are stored byte-for-byte in deterministic `raw-evidence.tar.gz`. Sorted tar entries use fixed zero timestamps/UID/GID and mode0644; gzip timestamp is zero. `RAW_MANIFEST.json` records original paths, sizes and SHA256 hashes. Extract outside the repository:

```sh
mkdir -p /var/tmp/p08-review
tar -xzf tasks/evidence/v2-allocation-p08/raw-evidence.tar.gz -C /var/tmp/p08-review
```

Verify extracted bytes from the repository root:

```sh
python3 - <<'PYVERIFY'
import hashlib,json,pathlib
m=json.loads(pathlib.Path('tasks/evidence/v2-allocation-p08/RAW_MANIFEST.json').read_text())
for a in m['artifacts']:
    b=(pathlib.Path('/var/tmp/p08-review')/a['path']).read_bytes()
    assert len(b)==a['size'] and hashlib.sha256(b).hexdigest()==a['sha256'], a['path']
print('verified',len(m['artifacts']))
PYVERIFY
```

No global same-identity recreation admission, power-loss, multi-host, native MaxUint64 exhaustion or whole-module validation claim is made. Root owns final acceptance/integration; external handoff candidates are not applied. Prior accepted production evidence carries by exact byte identity, with fresh scoped normal/race and pinned static gates recorded here.
