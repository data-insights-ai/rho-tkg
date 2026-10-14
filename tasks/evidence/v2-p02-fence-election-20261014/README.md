# Finite native P02 prepare/fence/election receipt

The exact parent `TestPrepareFenceP02ActualOrderingAcrossElection` passes normal,
race and scoped coverage in all three cases, plus affected vet, configured lint
and security. Nine actual traces pass the existing independent serial/cut oracle.
Normal/race/coverage package times are 23.302/80.045/32.312 seconds; each mode
reaps 37 incarnations (36 clean exits and one planned A1 SIGKILL).

The schedules retain queued pre-fence Prepare, actual A-before/B-after YESes,
and a genuine fence persisted before Stage followed by leader loss/election.
Actual vote floors are [106,6] or [6,106], COMMIT is107; captured installed floors
remain6. The third case also installs and replays an original local Close prepared
before the fence. Complete current/old graph, postings, census, CDC and originals
are checked through same-directory rejoin/reopen. Register query projection and
local replay disposition are bound to their actual native API representations.

`receipt.json` binds final source, exact commands, raw traces and all nine prior
RED attempts. `raw-manifest.json` hashes every member of `raw.tar.gz`; external
original process directories remain preserved. Failed cleanup kills are not fault
acceptance. Scoped6.6% parent coverage excludes test helpers and unmerged child
profiles and makes no full-module/release coverage claim.

This finite receipt does not accept the complete P02 family, full V2, power loss,
or the old four-fault wrapper. The heavy lease was released after terminal gates.
