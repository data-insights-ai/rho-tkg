# Reviewed native-v4 functional prerequisite

The [tool reproduction contract](../../../../../tools/graph-compare/README.md)
isolates shipped v4.43.0 commit `4126bb1` and the unchanged 114-file reference
pin. The [portable review](review.json) retains the historical external receipt
and appends fresh parent validation of current files. Historical source/tar
hashes are evidence; the runtime pin binds the actual commit/tree and all 1,438
file contents.

The scope is 92 Memory, 92 synchronous Badger and 23 Badger current reopen
answers, with one history query pending per backend. Go 1.26.9, separate module
gates and exact dependency/source/reference checks make this reproducible.
Callback limits do not bound native precomputation; heap/file snapshots do not
prove physical resource budgets. Vendor adapters, native-v5, complete profiles
and phase acceptance remain open; Backend seam work is separate.

Fresh parent validation on 2026-10-10 checks the current 14 tool files: 13 match
the historical receipt; `reproduce.py` is now `7994c38b…` rather than the older
`42545b62…`. Both fingerprints and the old review remain in `review.json`.
Ten adversarial Python tests, Go race/83.0% coverage, build/vet/fmt and pinned
lint/security/vuln/export checks pass with the same source and input pins.
Actionlint 1.7.12 passes the exact workflows with shellcheck and pyflakes
disabled; this is local static evidence, not remote CI. The fresh receipt
accepts only the basic-current native-v4 prerequisite.
