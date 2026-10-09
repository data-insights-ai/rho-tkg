"""Apply one mutant at a time, run the named tests, keep the output, restore."""
import subprocess
import sys

ROOT = "/home/renework2023/Work/2026/datainsights/rho-tkg/.claude/worktrees/agent-a89f184f285eccfe2/"
EVID = ROOT + "tasks/evidence/has-history/"
BADGER = "pkg/graph/store/badger/badgerstore_history_presence.go"
BSTORE = "pkg/graph/store/badger/badgerstore.go"
TIERED = "pkg/graph/store/tiered/tieredstore_history_presence.go"
TCFG = "pkg/graph/store/tiered/tieredstore.go"
HIST = "pkg/graph/store/badger/badgerstore_history.go"

BADGER_TESTS = ("./pkg/graph/store/badger/", "TestHistoryPresence")
GRAPH_TESTS = ("./pkg/graph/", "TestHasHistoryDifferential")
TIERED_TESTS = ("./pkg/graph/store/tiered/", "TestTieredHistoryPresence")

MUTANTS = [
    ("m1-no-delete-maintenance", BADGER, [(
        """	if isDelete {
		delete(p.has, id)
		p.stamp++
		p.unknown[id] = p.stamp
	} else {""",
        """	if isDelete {
		// MUTANT: a delete leaves the set unchanged
	} else {""")], [BADGER_TESTS, GRAPH_TESTS]),
    ("m2a-probe-skips-pending-buffer", BADGER, [(
        """	overlay, deletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	if len(overlay) > 0 {""",
        """	overlay, deletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	overlay, deletes = nil, nil // MUTANT: committed keys only
	if len(overlay) > 0 {""")], [BADGER_TESTS, GRAPH_TESTS]),
    ("m2b-build-skips-pending-buffer", BADGER, [(
        """	var ids []snowflake.ID
	var err error
	if kind == storepkg.KeyHistNode {""",
        """	var ids []snowflake.ID
	var err error
	// MUTANT: the build scans committed keys only.
	err = bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{kind}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) != storepkg.SizeHistKey {
				continue
			}
			id := storepkg.ParseIDFromKey(k, 1)
			if len(ids) == 0 || ids[len(ids)-1] != id {
				ids = append(ids, id)
			}
		}
		return nil
	})
	if false {""")], [BADGER_TESTS, GRAPH_TESTS]),
    ("m3a-build-scan-overrides-notes", BADGER, [(
        """	for _, id := range ids {
		if _, noted := p.has[id]; noted {
			continue
		}
		if _, noted := p.unknown[id]; noted {
			continue
		}
		p.has[id] = struct{}{}
	}""",
        """	// MUTANT: the scan replaces whatever the notes recorded during it.
	p.has = make(map[snowflake.ID]struct{}, len(ids))
	p.unknown = make(map[snowflake.ID]uint64)
	for _, id := range ids {
		p.has[id] = struct{}{}
	}""")], [BADGER_TESTS]),
    ("m3b-tracking-starts-after-scan", BADGER, [(
        """	if !p.tracking {
		p.has = make(map[snowflake.ID]struct{})
		p.unknown = make(map[snowflake.ID]uint64)
		p.tracking = true
	}
	p.mu.Unlock()
	bs.wbMu.Unlock()
""",
        """	if !p.tracking {
		p.has = make(map[snowflake.ID]struct{})
		p.unknown = make(map[snowflake.ID]uint64)
	}
	p.mu.Unlock()
	bs.wbMu.Unlock()
	defer func() { bs.wbMu.Lock(); p.mu.Lock(); p.tracking = true; p.mu.Unlock(); bs.wbMu.Unlock() }() // MUTANT
"""), (
        """	p.built.Store(true)
	p.mu.Unlock()
	return nil
}""",
        """	p.built.Store(true)
	p.mu.Unlock()
	return nil
} // MUTANT end""")], [BADGER_TESTS]),
    ("m4-probe-installs-without-stamp-check", BADGER, [(
        """	if cur, ok := p.unknown[id]; ok && cur == stamp {""",
        """	if _, ok := p.unknown[id]; ok || true { // MUTANT: no per-ID generation check
		_ = stamp""")], [BADGER_TESTS]),
    ("m5-clear-does-not-exclude-builds", BSTORE, [(
        """	bs.histNodePresence.buildMu.Lock()
	defer bs.histNodePresence.buildMu.Unlock()
	bs.histRelPresence.buildMu.Lock()
	defer bs.histRelPresence.buildMu.Unlock()
""", "	// MUTANT: no build exclusion\n")], [BADGER_TESTS]),
    ("m6-clear-keeps-the-set", BSTORE, [(
        """	bs.histNodePresence.resetLocked()
	bs.histRelPresence.resetLocked()
""", "	// MUTANT: set survives Clear\n")], [BADGER_TESTS, GRAPH_TESTS]),
    ("m7-tiered-owner-shard-only", TIERED, [(
        """func (ts *Store) historyPresentWithArchive(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {""",
        """func (ts *Store) historyPresentWithArchive(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	if true {
		return has, nil // MUTANT
	}"""), (
        """func (ts *Store) historyPresentWithReference(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {""",
        """func (ts *Store) historyPresentWithReference(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	if true {
		return has, nil // MUTANT
	}"""), (
        """func (ts *Store) historyPresentAnywhere(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {""",
        """func (ts *Store) historyPresentAnywhere(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	if true {
		return has, nil // MUTANT
	}""")], [TIERED_TESTS]),
    ("m9-id-overlay-resolves-per-id", HIST, [(
        """		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			delete(setKeys, k)
			return
		}""",
        """		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			for sk := range setKeys { // MUTANT: a delete drops every SET of the same ID
				if sk[:9] == k[:9] {
					delete(setKeys, sk)
				}
			}
			return
		}""")], [("./pkg/graph/store/badger/", "TestHistoryPresence_RandomizedDifferential"),
             ("./pkg/graph/store/badger/", "TestHistoryIDOverlay"), GRAPH_TESTS]),
    ("m10-failed-clear-keeps-built-empty-set", BADGER, [(
        """	p.built.Store(false)
	p.tracking = false
	p.has, p.unknown = nil, nil
""", """	if p.tracking { // MUTANT: reset in place, set stays built
		p.has = make(map[snowflake.ID]struct{})
		p.unknown = make(map[snowflake.ID]uint64)
	}
""")], [BADGER_TESTS]),
    ("m11-tiered-reference-error-swallowed", TIERED, [(
        """	refHas, err := ask(ref)
	if err != nil {
		return false, err
	}""", """	refHas, err := ask(ref)
	if err != nil {
		return has, nil // MUTANT: a failing reference shard reads as no history
	}""")], [TIERED_TESTS]),
    ("m8-tiered-cold-shards-build-the-set", TCFG, [(
        "	cfg.HistoryPresenceProbeOnly = cold\n", "	cfg.HistoryPresenceProbeOnly = false // MUTANT\n")], [TIERED_TESTS]),
]


def main(only):
    summary = []
    for name, path, edits, suites in MUTANTS:
        if only and name not in only:
            continue
        full = ROOT + path
        orig = open(full).read()
        mutated = orig
        for old, new in edits:
            assert mutated.count(old) == 1, (name, old[:60])
            mutated = mutated.replace(old, new)
        open(full, "w").write(mutated)
        try:
            out_lines = []
            red = False
            for pkg, run in suites:
                proc = subprocess.run(["go", "test", pkg, "-run", run, "-count=1", "-timeout", "300s"],
                                      cwd=ROOT, capture_output=True, text=True)
                text = proc.stdout + proc.stderr
                text = "\n".join(l for l in text.splitlines() if "ERROR graph: flush failed" not in l)
                out_lines.append(f"$ go test {pkg} -run {run} -count=1\n{text}\nexit={proc.returncode}\n")
                red = red or proc.returncode != 0
            open(EVID + f"mutant-{name}.txt", "w").write("\n".join(out_lines))
            summary.append(f"{name}: {'RED' if red else 'GREEN (survived)'}")
        finally:
            open(full, "w").write(orig)
    print("\n".join(summary))


if __name__ == "__main__":
    main(set(sys.argv[1:]))
