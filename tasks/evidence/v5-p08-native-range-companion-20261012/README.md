# P08 native completed-range composition guard

Base commit: `92e650c80a152897e5650643b6832b5fa0e4fabf`.
Only the companion test and this evidence directory are changed. Production,
public API/schema, PLAN, backlog and process hooks remain unchanged.

## Source audit and gap

PLAN 5.1 separates immutable grant provenance from current range ownership:
allocation cannot reactivate a former owner. PLAN 5.4 preserves original IDs,
request outcomes and old cuts through handoff. Substrate P07 specifies exact
request recovery; P08 combines unused cached IDs with owner transfer and delayed
allocation acknowledgement.

Existing direct evidence at the base commit:

| Test source | Directly established | Missing composition |
|---|---|---|
| `range_move_mutator_fences_test.go:14` | Queued recipient2 sequence2 grant and AddLabel refuse while target is STAGED; no CDC; resumed identities survive | Completed ownership transfer of the same grant range with a still-current recipient |
| `range_move_coordinator_test.go:146` | Native completed handoff, stable existing IDs, target mutation and retained old view | Fresh Entity/Life/Value claims and delayed grant install |
| `range_move_transfer_guards_test.go:156` | Source-qualified graph request, receipt and cross-decision originals survive with keeper offline | Grant/request recovery after moved-range activation |
| `allocation_host_test.go:445` | Original reserve/install recovery, no duplicate CDC, retained grant/outcome after service-epoch rotation/reopen | Range ownership transfer; service rotation is a different schedule |
| `range_move_six_process_test.go:242` | Existing-ID delayed write refusal and retained/current public graph sets, with six native voter probes | New identity claims from unused cached IDs and delayed grant install |

The companion uses the existing six-native-voter fixture (three voters in each
of two groups). These are in-process voters, not OS processes or physical hosts.

## Exact schedule

1. Preserve real source recipient1 and its original still-owned grant `[1,16]`.
   Create endpoint3/life4 using that retained grant, alongside endpoint1/life2.
2. Acquire an actual cursor for recipient1 sequence2 `[33,48]`, mint and commit
   node33/life34, and preserve exact committed creation CDC and request bytes.
   Capture a completed original grant observation and retain an old graph cut.
3. Complete transfer of the observed publication `[33,48]` from partition3 to8.
   Service authority/high-water and recipient session/phases are unchanged.
4. Exact original reservation replay returns the same result/index/hash. A new
   request for the same grant is `grantRecovery`. New-key delayed InstallGrant
   using the old observation is `controlRecovery`; all six voters retain owner8
   and the incremented range epoch. Original node33 creation replay returns the
   same identities/result/index/hash; target lookup preserves source-qualified
   original bytes. Every replay/control preserves both groups' logical root,
   floor and zero new CDC at the evaluated application record.
5. Continue the actual cached cursor after transfer. Relationship35/life36 with
   live endpoints1→3 refuses `reasonRemoteParticipant`. Reopen owner1 with Life37
   refuses `reasonStale`; owner1's range is still local, since Life routing follows
   its owner. Set node3/life4 to new typed I64(181) using Value38 refuses
   `reasonRemoteParticipant`. Each refusal leaves all six graph roots/floors
   unchanged and emits no CDC. Entity/Value routing refusals are complementary;
   they do not establish that allocation admission's ownership check ran.
6. Otherwise identical positive operations use IDs5/6,7,8 from the real still-owned
   grant. Cursor1 was discarded by the existing fixture, so these are qualified
   retained-grant claims, not reconstructed cursor issuance. Exact current entity
   set is `{1,3,5,17,33}` over the complete published ID universe `[1,48]`; Life37
   and Value38 are absent; endpoint posting sets are exactly `{5}`. Owner1 uses
   Life7, node3 property `p` is I64(181) with component reference8 and canonical
   ValueIdentity8. The new owner can label existing node33/life34. The old cut
   retains original active unlabeled identities and lacks post-move life7,
   value8, typed value identity and relationship5.

## Qualification

All compiled runs use Go1.26.9, GOMAXPROCS2, GOMEMLIMIT2GiB, GOFLAGS `-p=1`,
GOWORKoff, and the supplied aggregate4GiB RSS/180s wrapper. Its sampled process
tree is an observed cap, not a kernel hard memory bound. The final Go file SHA256
is in `source-hashes.json`.

| Evidence | Result | Observed wall / peak aggregate RSS |
|---|---|---|
| `final-normal-coverage.log`, matching limit JSON | Companion plus existing staged/completed native guards PASS | 20.243s / 898.516MiB |
| `final-race.log`, matching limit JSON | Final companion PASS under race detector | 17.539s / 830.953MiB |
| `ownership-omission.log`, matching limit JSON | Compiled omission mutant fails at Life37 assertion, expected | 11.810s / 825.609MiB |

Scoped package coverage is37.1%; `coverage-summary.txt` records selected exercised
methods. This is scoped test coverage, not the repository's aggregate `make cover`
or `make test-race` gates. No production statements/public methods were added.
Root integration owns the full gates.

The first positive-property component query used64KiB and refused with a resource
limit; it was corrected to the existing certified helper's bounded100-row/1MiB
page shape before the final qualified runs. This was test admission, not a
production semantic failure.

## Isolated ownership omission control

`ownership-omission.patch` changes exactly one condition in an overlay copy of
`partition_writes.go`: ignore current range-ownership failure for `lifeBinding`.
The native Host/Driver schedule then commits Life37 with reasonNone/applied where
the test requires reasonStale (`allocation_p08_native_test.go:214`). This directly
shows the Life37 assertion detects the missing allocation fence. Original and
overlay SHA256 values are recorded in `source-hashes.json`; apply the patch to the
hash-matched original to reproduce the overlay bytes.

The production worktree file was never edited. The overlay used the canonical
`/private/var/tmp` source path; an initial `/var/tmp` alias overlay did not match
Go's source path and is excluded from mutant evidence. Only the same one-condition
control was run after correcting that path. Generated full overlay/profile files
are local scratch and are not committed.

Baseline production passed before any omission. This is mutant sensitivity RED,
not a real production semantic RED, bug fix, OS-process/physical-host durability
claim, fixed allocator HOME relocation, or complete P08 acceptance.

## Integration review

Root reviewed the complete final test and verified both source hashes, the
reconstructed omission hash, all three final watchdog receipts and raw test
results. The normal run has exactly three passing parents; the race run has
one; neither has failures or skips. The compiled omission fails at the Life37
refusal. The integrated diff contains no production changes. This closes this
focused range-ownership regression guard only; process-crash P08 and full V2
acceptance remain open.
