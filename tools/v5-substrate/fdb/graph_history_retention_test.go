package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func TestGraphHistoryRetirementAdmissionAndOriginalAuthorityGuards(t *testing.T) {
	p := graphHistoryPolicy{MaxOwners: 16, MaxControlBytes: 128 << 10, PageRows: 2, PageBytes: 128 << 10, MaxWorkRows: 1024, MaxWorkBytes: 8 << 20, MaxChainSteps: 8, MaxOutputBytes: 4096}
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	bad := p
	bad.PageRows = 129
	if err := bad.validate(); !errors.Is(err, errInvalid) {
		t.Fatal("unbounded native page admitted", err)
	}
	if _, err := newGraphHistoryRetention(nil, cut{}, p); !errors.Is(err, errInvalid) {
		t.Fatal("nil source admitted", err)
	}
	if retryable, maybe := graphCostParallelError(errHistoryBusy); retryable || maybe {
		t.Fatal("source retirement guard was relabelled as native SDK authority")
	}
	if graphHistoryDeletionAllowed(value{Round: 5, Version: 1}, value{Round: 5, Version: 1}, 6) {
		t.Fatal("certified-floor predecessor anchor was deleted")
	}
	if !graphHistoryDeletionAllowed(value{Round: 5, Version: 1}, value{Round: 6, Version: 2, Deleted: true}, 6) {
		t.Fatal("authentic successor tombstone did not protect its anchor")
	}
	// The genuine SDK case below is authored, not executed at this source stage.
	t.Run("HeldOldCutBlocksRetirementThenResumesExactCollection", func(t *testing.T) {
		profile := graphCostProfile{Version: 1, Facts: 2, CrossPercent: 100, Writers: 1}
		h := fixture(t, graphCostConfig("history-retirement-direct", profile))
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := newGraphHistoryRetention(h, initial, p)
		if err != nil {
			t.Fatal(err)
		}
		apply := func(phase string, fact uint64, command graphCommand, provenance uint64) {
			before, _, err := h.graphCostPointSnapshot(command)
			if err != nil {
				t.Fatal(err)
			}
			coordinator := uint8(0)
			if phase == "setup" {
				coordinator = command.Node.Owner
			}
			x := h.graphTransaction(fmt.Sprintf("retirement-%s-%d", phase, fact), coordinator, command, before)
			x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
			if phase != "setup" {
				id := uint64(5)
				if phase == "update" {
					id = 6
				}
				x.Lifecycle.EdgeLife = command.Edge.ID + 1
				x.Lifecycle.Definition = &lifecycleDefinition{ID: id, Kind: "i64", I64: command.Edge.Value}
			}
			o, _, err := h.graphCostSubmit(x)
			if err != nil || !o.Commit {
				t.Fatal("genuine operation", o, err)
			}
		}
		for i, n := range profile.nodes() {
			apply("setup", uint64(i), graphCommand{Kind: "node-create", Node: n}, uint64(i+1))
		}
		for i := range profile.Facts {
			apply("create", i, graphCommand{Kind: "edge-create", Edge: profile.edge(i, 5)}, 4+i)
		}
		old, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var secret [32]byte
		secret[0] = 1
		token, err := s.pin("old", old, secret)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := s.open(t.Context(), token)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
		for i := range profile.Facts {
			apply("update", i, graphCommand{Kind: "edge-update", Edge: profile.edge(i, 6)}, 4+profile.Facts+i)
		}
		final, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.expire("expire-old", token); err != nil {
			t.Fatal(err)
		}
		if _, err := s.open(t.Context(), token); !errors.Is(err, errHistoryExpired) {
			t.Fatal("expired token granted a new owner", err)
		}
		if err := s.begin("retire", final); !errors.Is(err, errHistoryInUse) {
			t.Fatal("retirement lost held owner", err)
		}
		view := snapshot{Values: [2]map[string]value{{}, {}}}
		if _, err := owner.walk(t.Context(), func(row graphCostRawRow) error {
			key, v, err := h.graphHistoryRow(row, old.record.Round)
			if err != nil {
				return err
			}
			if v.Deleted {
				delete(view.Values[row.Group], key)
			} else {
				view.Values[row.Group][key] = v
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		projected, err := h.projectGraph(view)
		if err != nil || graphCostCheckView(projected, profile, 5) != nil {
			t.Fatal("held old cut substituted current6", projected, err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.begin("retire", final); err != nil {
			t.Fatal(err)
		}
		if _, err := h.at(old, 2, nil); !errors.Is(err, errHistoryExpired) {
			t.Fatal("below-floor new At succeeded during partial retirement", err)
		}
		var page graphHistoryPage
		for step := 0; step < 64; step++ {
			page, err = s.collect(t.Context(), "retire", graphHistoryBudget{Rows: 2, Bytes: 128 << 10, OutputBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			if page.Done {
				break
			}
		}
		if !page.Done || page.Seal.Deleted[0] != 4 || page.Seal.Deleted[1] != 2 || page.Seal.Anchors[0] != 6 || page.Seal.Anchors[1] != 3 {
			t.Fatal("exact source collection census", page)
		}
		current, err := h.at(final, 2, nil)
		if err != nil {
			t.Fatal(err)
		}
		projected, err = h.projectGraph(current)
		if err != nil || graphCostCheckView(projected, profile, 6) != nil {
			t.Fatal("floor anchors changed complete current", projected, err)
		}
		groups, err := h.changes(initial, final, 2)
		if err != nil || len(groups) != 7 {
			t.Fatal("collection lost raw original CDC", groups, err)
		}
		for _, group := range groups {
			if actual, err := h.graphCostOriginal(group.Tx, group.Outcome); err != nil || !graphColumnSame(actual, group) {
				t.Fatal("collection rewrote original request/body", err)
			}
		}
		// Preserve the retained row count with two authentic revisions: remove
		// fact0's current anchor and reinsert fact1's already-retired old revision.
		// Original authentication and cardinality alone would accept this corruption.
		var anchor, retired changedValue
		for _, group := range groups {
			for _, write := range group.Writes {
				if write.Group == 0 && write.Key == edgeKey("edge", profile.edge(0, 6)) && write.After.Value == 6 {
					anchor = write
				}
				if write.Group == 0 && write.Key == edgeKey("edge", profile.edge(1, 5)) && write.After.Value == 5 {
					retired = write
				}
			}
		}
		if anchor.After.Version == 0 || retired.After.Version == 0 {
			t.Fatal("missing authentic adversarial originals")
		}
		anchorKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(0), anchor.After.Round, anchor.Key))
		retiredKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(0), retired.After.Round, retired.Key))
		anchorBody, err := wire(anchor.After)
		if err != nil {
			t.Fatal(err)
		}
		retiredBody, err := wire(retired.After)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
			actual, err := tr.Get(anchorKey).Get()
			if err != nil || !bytes.Equal(actual, anchorBody) {
				return nil, errors.Join(errMismatch, err)
			}
			absent, err := tr.Get(retiredKey).Get()
			if err != nil || len(absent) != 0 {
				return nil, errors.Join(errMismatch, err)
			}
			tr.Clear(anchorKey)
			tr.Set(retiredKey, retiredBody)
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
				tr.Set(anchorKey, anchorBody)
				tr.Clear(retiredKey)
				return nil, nil
			})
			if err != nil {
				t.Error(err)
			}
		})
		if view, err := h.at(final, 2, nil); !errors.Is(err, errUnavailable) || view.Values[0] != nil || view.Values[1] != nil {
			t.Fatal("count-preserving missing anchor became exact-empty success", view, err)
		}
		if _, err := s.collect(context.Background(), "foreign", graphHistoryBudget{Rows: 2, Bytes: 128 << 10, OutputBytes: 4096}); !errors.Is(err, errMismatch) {
			t.Fatal("foreign collector resumed original identity", err)
		}
	})
}
