package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The lifecycle doors over a re-imported ID (backlog 38): replica apply,
// export → import, history compaction and retention purge see a chain whose
// versions continue across lives, and an old chain (written before the fix:
// the re-import restarted at version 0 and overwrote) reads as it did.

// rlTwoLives drives e through two lives of one entity and returns its ID, the
// pins taken after every write and the valid instants to probe:
// create, update, delete, re-import, update, bounded cascade, close.
func rlTwoLives(e *ccEnt) (int64, []types.Instant, []types.Instant) {
	e.t.Helper()
	e.add("B", 1000, nil)
	id := e.add("T", 1000, nil)
	pins := []types.Instant{e.pin()}
	e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(1)})
	pins = append(pins, e.pin())
	e.mustDel(id)
	d := e.tombstoneAt(id)
	pins = append(pins, e.pin())
	e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)})
	pins = append(pins, e.pin())
	e.mustUpdate(id, map[string]any{"x": int64(5)})
	pins = append(pins, e.pin())
	e.mustCascade(id, 5500, 6000, map[string]any{"x": int64(4)})
	pins = append(pins, e.pin())
	closeV := e.pin() + 1000 // after the update's start (its UpdatedAt)
	if err := e.closeAt(id, closeV); err != nil {
		e.t.Fatalf("close: %v", err)
	}
	pins = append(pins, e.pin())
	return id, pins, rlWith(rlValids, d, closeV)
}

// rlTwin is the adapter over another graph holding the same entities.
func rlTwin(e *ccEnt, g *Core) *ccEnt {
	return &ccEnt{t: e.t, g: g, ctx: e.ctx, rel: e.rel, start: e.start, end: e.end, names: e.names}
}

// rlSameChains fails unless every tracked entity has the same stored chain
// (versions, x, every stamp, both hashes) on e and twin, the twin's chains
// verify, and the twin answers every pin and valid instant as e does.
func rlSameChains(phase string, e, twin *ccEnt, pins, valids []types.Instant) {
	e.t.Helper()
	for _, id := range e.tracked() {
		if got, want := twin.chainString(id), e.chainString(id); got != want {
			e.t.Fatalf("[%s %s] %s chain:%s\n want:%s", e.kind(), phase, e.names[id], got, want)
		}
		if got, want := fmt.Sprint(twin.historyRows(id)), fmt.Sprint(e.historyRows(id)); got != want {
			e.t.Fatalf("[%s %s] %s history rows:\n %s\n want\n %s", e.kind(), phase, e.names[id], got, want)
		}
		twin.verifies(phase, id)
	}
	e.unchanged(phase, e.record(pins, valids), twin.record(pins, valids), e.tracked()...)
}

// TestReImportReplicaApply: a change-log primary records two lives of an
// entity; a read-only replica applies the feed and holds the same chain
// (continued versions, linked hashes, every stamp), verifies it and answers
// every pin as the primary.
//
// Catches: a replica apply that renumbers a re-import record, and (red before
// the fix) the overwritten first-life row on both sides.
func TestReImportReplicaApply(t *testing.T) {
	t.Parallel()
	for _, be := range pastDatedBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				primary := be.open(t, Config{SnowflakeNodeID: 0}, true)
				replica := be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: primary.Repl}, false)
				useTestClock(t, primary)
				e := newCCEnt(t, primary, rel)
				id, pins, valids := rlTwoLives(e)
				rows := e.historyRows(id)
				if len(rows) != int(e.maxVersion(id)) {
					t.Fatalf("[%s] primary holds %d history rows below top v%d: a row was overwritten:%s", e.kind(), len(rows), e.maxVersion(id), e.chainString(id))
				}
				if _, err := replica.Repl.ApplyChanges(changeFeed(t, primary)); err != nil {
					t.Fatalf("ApplyChanges: %v", err)
				}
				rlSameChains("replica", e, rlTwin(e, replica), pins, valids)
			})
		}
	}
}

// TestReImportExportImportRoundTrip: two lives exported and imported into a
// fresh graph of the same backend keep every row, verify, and answer every
// pin as the source.
func TestReImportExportImportRoundTrip(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				src := be.open(t, true)
				useTestClock(t, src)
				e := newCCEnt(t, src, rel)
				id, pins, valids := rlTwoLives(e)
				if n := len(e.historyRows(id)); n != int(e.maxVersion(id)) {
					t.Fatalf("[%s] source holds %d history rows below top v%d:%s", e.kind(), n, e.maxVersion(id), e.chainString(id))
				}
				var buf bytes.Buffer
				if err := src.IO.Export(&buf); err != nil {
					t.Fatalf("Export: %v", err)
				}
				dst := be.open(t, true)
				if err := dst.IO.Import(&buf, tkgio.ImportOptions{}); err != nil {
					t.Fatalf("Import: %v", err)
				}
				rlSameChains("export → import", e, rlTwin(e, dst), pins, valids)
			})
		}
	}
}

// TestReImportCompactionAcrossLives: KeepVersions compaction over two lives
// trims the oldest rows (the earlier life first), keeps the newest ones
// byte-identical, leaves a chain that verifies against its stub, and every
// door agrees with the oracle at a pin after the compaction. Sharded declines
// compaction (ErrCapabilityNotSupported).
func TestReImportCompactionAcrossLives(t *testing.T) {
	t.Parallel()
	for _, keep := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("keep=%d", keep), func(t *testing.T) {
			t.Parallel()
			ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
				e := mk(t)
				id, _, valids := rlTwoLives(e)
				before := e.historyRows(id)
				var err error
				if e.rel {
					_, err = e.g.Admin.CompactHistoryRels(context.Background(), RetentionPolicy{KeepVersions: keep})
				} else {
					_, err = e.g.Admin.CompactHistoryNodes(context.Background(), RetentionPolicy{KeepVersions: keep})
				}
				if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
					return
				}
				if err != nil {
					t.Fatalf("compact: %v", err)
				}
				after := e.historyRows(id)
				for v, fp := range after {
					if before[v] != fp {
						t.Fatalf("[%s] compaction changed kept row v%d: %s -> %s", e.kind(), v, before[v], fp)
					}
				}
				if len(after) < keep || len(after) >= len(before) {
					t.Fatalf("[%s] compaction kept %d of %d rows with KeepVersions %d:%s", e.kind(), len(after), len(before), keep, e.chainString(id))
				}
				e.agree("after compaction", []types.Instant{e.pin()}, valids)
			})
		})
	}
}

// TestReImportRetentionPurge: a retention purge removes a re-imported node
// with both lives' history (and a relationship to it, both lives); the ID can
// then be imported again. Tiered and sharded decline the purge.
func TestReImportRetentionPurge(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				g := be.open(t, true)
				g.allowRetentionPurge = true
				useTestClock(t, g)
				e := newCCEnt(t, g, rel)
				id, _, _ := rlTwoLives(e)
				label := ccLabel
				if rel {
					label = "Ev" // purging the end node removes the relationship
				}
				_, err := g.Admin.PurgeExpiredNodes(context.Background(), PurgePolicy{Label: label, Mode: PurgeByAge, Before: e.pin() + 1})
				if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
					return
				}
				if err != nil {
					t.Fatalf("PurgeExpiredNodes: %v", err)
				}
				if rows := e.historyRows(id); len(rows) != 0 {
					t.Fatalf("[%s] purge left %d history rows:%s", e.kind(), len(rows), e.chainString(id))
				}
				if _, err := e.getCurrent(id); !e.isAbsent(err) {
					t.Fatalf("[%s] purge left the current row: %v", e.kind(), err)
				}
			})
		}
	}
}

// TestReImportOldChainReadsAsBefore — GUARD (passes before and after the
// fix): a chain stored by v4.43–v4.47, where the re-import restarted at
// version 0 and its first Update overwrote the earlier life's version 0, is
// built with the store write the old import made. It must read as it did:
// the read side keeps the life rule (a row's life is the number of deletes
// recorded before its TxFrom), applied to chains whose versions do not
// continue. Every door equals the oracle, and three answers are pinned by
// hand. A resolver that orders the chain by version alone (ignoring the
// tombstone) answers the overwritten re-import row from the earlier life's
// slot.
func TestReImportOldChainReadsAsBefore(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		e.add("B", 1000, nil)
		id := e.add("T", 1000, nil)
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(1)})
		e.mustDel(id)
		d := e.tombstoneAt(id)
		// The old import: version 0, no predecessor hash, TxFrom = now.
		rlOldImport(e, id, map[string]any{"x": int64(9)}, 5000, 0)
		e.mustUpdate(id, map[string]any{"x": int64(5)})
		chain := e.chain(id)
		if len(chain) != 3 || chain[0].x != int64(9) || chain[1].tm.DeletedAt == 0 || !chain[2].current {
			t.Fatalf("fixture: not the old overwritten shape:%s", e.chainString(id))
		}
		now := e.pin()
		pins := []types.Instant{d, now}
		valids := rlWith(rlValids, d)
		// Verify*Chain is false on this chain before and after the fix: the
		// overwritten v0 was the predecessor v1's PrevHash names (data the old
		// release already lost; no migration repairs it).
		e.agreeReads("old chain", pins, valids)
		e.expect("old chain valid 2500", e.atTxDoors(2500, now), ccSet(ccVer("B", 0), ccVer("T", 1)), id)
		e.expect("old chain valid 5200", e.atTxDoors(5200, now), ccSet(ccVer("B", 0), ccVer("T", 0)), id)
		e.expect("old chain as-of now", e.asOfDoors(now), ccSet(ccVer("B", 0), ccVer("T", 2)), id)

		// The overlap shape (TestAtTxReImportOverlapsEarlierLife as v4.43–v4.47
		// stored it): the first life's update starts at its write time, the old
		// re-import (version 0, valid from 5000) is the newer belief from 5000
		// on. Ordered by version alone the first life's v1 tiles after the
		// re-imported v0 and answers between its start and the delete.
		f := mk(t)
		o := f.add("O", 1000, nil)
		f.mustUpdate(o, map[string]any{"x": int64(1)})
		var upd types.Instant
		for _, r := range f.chain(o) {
			if r.current {
				upd = r.tm.UpdatedAt
			}
		}
		f.mustDel(o)
		od := f.tombstoneAt(o)
		rlOldImport(f, o, map[string]any{"x": int64(9)}, 5000, 0)
		now = f.pin()
		f.agreeReads("old overlap chain", []types.Instant{od, now}, rlWith(rlValids, upd, od))
		for _, va := range []types.Instant{5000, upd - 1, upd, od - 1, od, od + 1} {
			for _, p := range []types.Instant{now, 0} {
				f.expect(fmt.Sprintf("old overlap chain valid %d pin %d", va, p), f.atTxDoorsOr(va, p), ccVer("O", 0), o)
			}
		}
	})
}

// atTxDoorsOr is atTxDoors at pin p, or validDoors when p is 0.
func (e *ccEnt) atTxDoorsOr(va, p types.Instant) map[string]string {
	if p == 0 {
		return e.validDoors(va)
	}
	return e.atTxDoors(va, p)
}
