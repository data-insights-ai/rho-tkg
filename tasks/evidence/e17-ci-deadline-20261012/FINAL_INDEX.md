E17 caller cancellation / CI deadline correction

The private process helper now carries a parent-owned interruption only after the correlated child worker has returned a structurally valid original Pending. It resumes those same bytes explicitly and still requires actual COMMIT and both Finish statuses. No deadline is treated as success. The 10-second invocation and 3-second cancellation-cleanup bounds remain; no production file changed.

GMR2 is a private test-result envelope. It preserves the actual 16-bit graph error mask instead of losing context causes in the generic IPC error. Cancellation joined with corrupt/invalid/closed/poisoned/unknown errors refuses. The core cancellation test reaches durable Import2, cancels before delivering Import3, completes the original move, validates exact placement CDC once/group and voter outcome bytes, original graph property/life, immutable controls and zero read owners.

[FINAL_RECEIPT.json](FINAL_RECEIPT.json) reports all 7 selected parents PASS in normal and race, no SKIP; fmt/vet and pinned lint pass. [HOSTED_FAILURE.json](HOSTED_FAILURE.json) retains the hosted failure excerpt/hash and its qualification. The 527 Go source files are sealed in [final-go-source.json](final-go-source.json); all 218 production files equal the base in [production-unchanged.json](production-unchanged.json).

Raw logs, bounded watchdogs, compiler/oracle qualifications, exact pre-correction helper/test snapshots and final patch are in deterministic[raw-evidence.tar.gz](raw-evidence.tar.gz). [RAW_MANIFEST.json](RAW_MANIFEST.json) records every member path, byte size and SHA256 plus archive hash. Extract into an empty directory with `tar -xzf raw-evidence.tar.gz -C /path/to/empty-dir`; verify the member hashes against that manifest. Raw bytes are unchanged; tar timestamps/ownership and gzip timestamp are deterministic.

This is a focused test-helper correction, not a new E17 production regression or complete repository/hosted acceptance claim. Parent integration owns fresh hosted validation.
