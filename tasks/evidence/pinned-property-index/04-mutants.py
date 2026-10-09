import subprocess
import sys

CORE = ["go", "test", "./pkg/graph/internal/core/", "-run", "TestPinned|TestBitemporalOracleHarness", "-count=1", "-short"]
STORE = ["go", "test", "./pkg/graph/store/", "./pkg/graph/store/badger/", "./pkg/graph/store/sharded/", "-run", "PropertyTxMembers|PropTxBuild|ShardedPropertyTx", "-count=1"]

M = [
    ("record only the current value (memory build skips history rows)",
     "pkg/graph/store/memory/memorystore_propertytxmembers.go",
     """	for _, versions := range ms.relHistory {
		for _, r := range versions {
			record(r)
		}
	}""", """	for _, versions := range ms.relHistory {
		for _, r := range versions {
			_ = r
		}
	}""", [CORE, STORE]),
    ("record only the current value (memory history rows not recorded at write)",
     "pkg/graph/store/memory/memorystore_ordinal.go",
     """func (ms *Store) historyRel(r *types.Relationship) *types.Relationship {
	ms.recordRelPropTxLocked(r)""", """func (ms *Store) historyRel(r *types.Relationship) *types.Relationship {""", [CORE, STORE]),
    ("record only the current value (badger demoted row not recorded)",
     "pkg/graph/store/badger/badgerstore_history_rel.go",
     """	bs.recordRelPropTxLocked(prevState)                        // backlog 8 — and its property values
""", "", [CORE, STORE]),
    ("drop deleted rels (memory build skips rels without a current row)",
     "pkg/graph/store/memory/memorystore_propertytxmembers.go",
     """	for _, versions := range ms.relHistory {
		for _, r := range versions {
			record(r)""", """	for id, versions := range ms.relHistory {
		if _, live := ms.rels[id]; !live {
			continue
		}
		for _, r := range versions {
			record(r)""", [CORE, STORE]),
    ("drop deleted rels (badger tombstone not recorded)",
     "pkg/graph/store/badger/badgerstore_history_rel.go",
     """	bs.recordRelPropTxLocked(tombstone)                        // backlog 8
""", "", [STORE]),
    ("prune off by one (firstTx == pin pruned)",
     "pkg/graph/internal/core/temporal.go",
     """		if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
			return true // first carried the value after the pin: cannot match""", """		if pinTx != 0 && firstTx != 0 && firstTx >= pinTx {
			return true // first carried the value after the pin: cannot match""", [CORE]),
    ("prune off by one, node twin",
     "pkg/graph/internal/core/temporal.go",
     """			if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
				return true
			}
			if cands != nil {""", """			if pinTx != 0 && firstTx != 0 && firstTx >= pinTx {
				return true
			}
			if cands != nil {""", [CORE]),
    ("skip the pending buffer in the build",
     "pkg/graph/store/badger/badgerstore_propertytxmembers.go",
     """	bs.rangePending(func(k string, op writeOp) {
		if len(k) == 0 {
			return
		}
		switch {
		case k[0] == curKind""", """	bs.rangePending(func(k string, op writeOp) {
		if len(k) >= 0 {
			return
		}
		switch {
		case k[0] == curKind""", [CORE, STORE]),
    ("skip the generation guard",
     "pkg/graph/store/badger/badgerstore_propertytxmembers.go",
     """		if bs.propTxGen != gen || bs.relPropTx[key] != sc {""", """		if bs.relPropTx[key] == nil {""", [STORE]),
    ("turn tracking on only at install (writes during the build lost)",
     "pkg/graph/store/badger/badgerstore_propertytxmembers.go",
     """			bs.relPropTx[key] = sc // tracking from here on
		}
		gen := bs.propTxGen
		bs.idxMu.Unlock()
""", """		}
		gen := bs.propTxGen
		bs.idxMu.Unlock()
		defer func() { bs.idxMu.Lock(); bs.relPropTx[key] = sc; bs.idxMu.Unlock() }()
""", [STORE]),
    ("ignore the capability absence (no sidecar: current matches only, no fold)",
     "pkg/graph/internal/core/temporal.go",
     """	if c.relPropTxMembers == nil || !c.propertyCandidateSidecarUsable(opts.Depth) {
		return c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, fn)
	}""", """	if c.relPropTxMembers == nil || !c.propertyCandidateSidecarUsable(opts.Depth) {
		for _, id := range currentIDs {
			if err := fn(id); err != nil {
				return err
			}
		}
		return nil
	}""", [CORE]),
    ("ignore an undeclared index (ErrIndexNotFound treated as no members)",
     "pkg/graph/internal/core/temporal.go",
     """	if errors.Is(err, storepkg.ErrIndexNotFound) {
		return c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, fn)
	}""", """	if errors.Is(err, storepkg.ErrIndexNotFound) {
		err = nil
	}""", [CORE]),
    ("composite: a key without an index constrains to nothing",
     "pkg/graph/internal/core/temporal.go",
     """		if errors.Is(err, storepkg.ErrIndexNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		cands = set""", """		if errors.Is(err, storepkg.ErrIndexNotFound) {
			cands = map[types.NodeID]struct{}{}
			continue
		}
		if err != nil {
			return err
		}
		cands = set""", [CORE]),
    ("do not invalidate on Clear (badger)",
     "pkg/graph/store/badger/badgerstore.go",
     """	bs.dropPropertyTxMembersLocked() // property sidecars; an in-flight build discards its scan
""", "", [CORE, STORE]),
    ("do not invalidate on Clear (memory)",
     "pkg/graph/store/memory/memorystore.go",
     """	ms.dropPropertyTxMembersLocked()   // property sidecars; rebuilt on next temporal lookup
""", "", [CORE, STORE]),
    ("do not invalidate on exact erasure (memory)",
     "pkg/graph/store/memory/memorystore_exact_erasure.go",
     """	ms.dropPropertyTxMembersLocked()
	ms.docColumns = make""", """	ms.docColumns = make""", [STORE]),
    ("do not invalidate on retention purge (badger)",
     "pkg/graph/store/badger/badgerstore_retention_purge.go",
     """	if nodesPurged > 0 {
		// The purged rows' values""", """	if nodesPurged < 0 {
		// The purged rows' values""", [CORE]),
    ("node: record a row under a label it does not carry (memory build ignores the label)",
     "pkg/graph/store/memory/memorystore_propertytxmembers.go",
     """	record := func(n *types.Node) {
		if !n.HasLabelTokenRaw(key.LabelToken) {
			return
		}""", """	record := func(n *types.Node) {""", [CORE, STORE]),
]


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    lines = [l for l in (p.stdout + p.stderr).splitlines() if l.startswith(("--- FAIL", "FAIL", "ok", "    --- FAIL")) or "_test.go:" in l]
    return p.returncode, lines


only = sys.argv[1:]
for i, (name, path, old, new, cmds) in enumerate(M, 1):
    if only and str(i) not in only:
        continue
    src = open(path).read()
    assert src.count(old) == 1, (name, path, src.count(old))
    open(path, "w").write(src.replace(old, new))
    try:
        print(f"## M{i}: {name}  ({path})", flush=True)
        red = False
        for cmd in cmds:
            rc, lines = run(cmd)
            red = red or rc != 0
            for l in lines[:8]:
                print("   " + l[:220])
        print(f"   => {'RED (caught)' if red else 'GREEN (NOT caught)'}\n", flush=True)
    finally:
        open(path, "w").write(src)
