# René takeover — 2026-10-16

Pull branch `v5`, then read root `HANDOVER.md`, the lessons digest and the
weighted packages / V2 A–F queue in `tasks/backlog.md`. Markus requested one
final push, then takeover. All three implementation agents are stopped and all
owned native cohorts exited without forced cleanup. This is unfinished V2,
not a release or consumer migration. No remaining candidate patch needs applying.

## Integrated changes

Implementation pin: `79b1cff99b3d61f3b6f4d94d56e98850481f7725`.
Latest main documentation through `97efa1c0` is merged at `e76b8b9b`.
This merge changes no v5 implementation source. No main push/tag/release.

- `5a9c2cc1`: retained history reads fund lookup plus actual payload work before
  hashing/copying; residual budgets remain bounded, failures charge conservatively.
- `d672a641`: RecoveryStepBudget charges actual RS4 size before decode rather than
  the maximum 64 KiB per read; original quotas remain unchanged.
- `2bef4b58`, `002b2193`: P03 keeps its surviving original caller/session and joins
  replacement observations to authenticated raw originals. Full normalized
  semantic checking still runs; exact equality is not weakened.
- `0c2b5a5b`: removes the temporary native feed observer.
- `6cd796ab`: malformed-identity probes preserve GFP framing so they reach the
  intended validation rather than failing their fixture preconditions.
- `23efb53c`: strict native and Python classification of pinned Pebble's actual
  short-write WAL Close panic. Arbitrary panic/exit2 remains a failure.
- `87f37a22`: startup directory-sync injection selects the mandatory owned-root
  Sync after actual WAL creation, rather than recoverable background flushes.
- `7fdacc95`, `79b1cff9`: P01 final current reads retain the surviving leader route;
  replacement old-cut reads use the existing source-safe helper.

## Final focused results

| Case | Result at implementation pin |
|---|---|
| P01 original/literal first before-phase1 leaf | PASS, 7.90 s; independent complete-artifact replay PASS |
| Directory-sync genuine same-directory reopen | PASS, 4.20 s; independent complete-artifact replay PASS |
| WAL short-write | PASS, 4.97 s; independent complete-artifact replay PASS |
| WAL parent | FAIL at write-eio: actual exit2 rejected by current classifier; ENOSPC/fsync unrun after failfast |
| State rotated/reorder move-phase1-p3-page0-kind1-before | FAIL, 32.98 s, retained frame part3/index104: remote graphapply corrupt (wire cause0004) |

Two fresh P03 coordinator3/coordinator8 captures passed at binary `0c2b5a5b`
with checker `002b2193` (18.31/18.19 s). Both passed independent full-artifact
replay again with the final checker. These are selected cases, not full families.
Earlier P05 late Resolve and seven D packet passes retain their prior pins.

`manifest.json` pins the binary, exact commands, monitor reports, author
RED/GREEN/race/coverage receipts, copied logs and hashes. Author receipts retain
original local paths; the manifest maps them to committed copies. The trace
archive includes required sibling history JSON. All seven members were hashed
after extraction and all five complete selected traces replayed from that copy.
The parent directory's `protocol-traces-with-sidecars-0c2b5a5b.*` additionally
repairs portability of nine older P03/P05 captures; some older JSONL-only archives
omitted required sidecars. Original archives and failed receipts remain unchanged.

## Resume with these failures

1. State retained-feed frame failure above. Earlier history/RS4 budget refusals
   are fixed, but this later corrupt refusal is unresolved; do not label it
   aggregate exhaustion from post-error conservatively charged counters.
2. WAL write-eio fail-stop classification. Preserve actual IO/cookie/PID/path
   and pinned failure-stack requirements; inspect the saved full stderr. Then
   qualify still-unrun ENOSPC/fsync leaves. No blanket exit2 acceptance exists.
3. `TestRetainedFeedNativeCompletePagesOriginalACKAndSnapshot/pages_and_ACK`
   reaches all seven intended malformed-identity checks after the framing fix,
   then remains RED after64 pages: done=false, cross=1, local=0 (line534).
   Possible protocol-progress starvation is only a hypothesis. This was
   reproduced before the RS4 fix and has not been rerun since.
4. `TestApplicationRecoveryBytePoolNativeReopenPublishedAS9AndReady` fails at
   final spanRecoveryInstall(done), application_recovery_pool_snapshot_test.go:243,
   with raftlog invalid input. Reproduced on unchanged pre-RS4-fix source;
   not demonstrated to be introduced by that change. Saved baseline log included.

Reproduce the two direct parents from `v5/` using Go1.26.9, GOWORK=off,
GOTOOLCHAIN=local and GOMAXPROCS=2:

```sh
go test -p=1 -count=1 -v -run '^TestRetainedFeedNativeCompletePagesOriginalACKAndSnapshot$/^pages_and_ACK$' ./internal/graphapply
go test -p=1 -count=1 -run '^TestApplicationRecoveryBytePoolNativeReopenPublishedAS9AndReady$' ./internal/raftlog
```

The four final native selectors, original deadlines, environment and watchdog
limits are in the committed `handover-*.json` monitor reports. They ran from
`v5/internal/graphapply`; retain that cwd for Python checker lookup. Monitor
wrapper124 here means the saved test exit1, not a timeout; inspect reason and
exit_code. Do not overwrite a running test binary, as native children exec it.

## Acceptance still open

V0/V1 accepted; V2 has42 accepted planning points, not42% of coding hours.
B transaction families, C ownership/identity/GC, D composed faults/exploration/
soak, E equivalent FoundationDB fault/workload qualification and F measurement/
engine choice/final gates remain open. The complete cost matrix is11,520 paired
cells /23,040 engine runs; three tiny paired cells do not qualify it. V3–V7 keep
their original gates and dependencies. No engine selection has been made.

No fresh full CI, full normal/race/coverage, lint or security gate was run for this
handover. Focused author results are source-scoped, not whole-phase acceptance.
Follow implementation → correctness → measurement → improvement; do not reopen
an already qualified component without changed source or an uncovered requirement.
The full original plan remains `docs/v5/PLAN.md`; `docs/v5/HANDOVER.md` is unchanged
external review. Rho owns storage; Sigma owns reasoning.
