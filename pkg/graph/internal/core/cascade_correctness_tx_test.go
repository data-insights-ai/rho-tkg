package core

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTxRefusedUpdateWithTxTakesNoSnapshot: a GraphTx caller-instant update
// that is refused (t not after the chain's stamps, an update that changes
// nothing, a closed entity) is refused BEFORE the transaction snapshots the
// entity, so a refusal never schedules a history rewrite for Rollback
// (backlog 19). White-box: the snapshot set stays empty. Counterpart: an
// accepted update does snapshot.
func TestTxRefusedUpdateWithTxTakesNoSnapshot(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		closed := e.add("C", 1000, nil)
		if err := e.closeAt(closed, 2000); err != nil {
			t.Fatalf("CloseVersion: %v", err)
		}
		update := func(tx *GraphTx, id int64, m map[string]any, at types.Instant) error {
			if e.rel {
				_, err := tx.UpdateRelationshipWithTx(types.RelID(id), m, at)
				return err
			}
			_, err := tx.UpdateNodeWithTx(types.NodeID(id), m, at)
			return err
		}
		cases := []struct {
			name string
			id   int64
			m    map[string]any
			at   func() types.Instant
			want error
		}{
			{"t below the recorded stamps", id, map[string]any{"x": int64(1)}, func() types.Instant { return 1 }, ErrTxOrder},
			{"no change", id, map[string]any{"x": int64(0)}, e.pin, ErrTxOrder},
			{"closed entity", closed, map[string]any{"x": int64(1)}, e.pin, ErrAlreadyClosed},
		}
		for _, c := range cases {
			at := c.at()
			tx, err := e.g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			err = update(tx, c.id, c.m, at)
			snapshots := len(tx.snapshotSet)
			_ = tx.Rollback()
			if !errors.Is(err, c.want) {
				t.Fatalf("%s: err = %v; want %v", c.name, err, c.want)
			}
			if snapshots != 0 {
				t.Fatalf("%s: a refused update left %d snapshots", c.name, snapshots)
			}
		}
		at := e.pin()
		tx, err := e.g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if err := update(tx, id, map[string]any{"x": int64(1)}, at); err != nil {
			_ = tx.Rollback()
			t.Fatalf("accepted update: %v", err)
		}
		if len(tx.snapshotSet) != 1 {
			t.Fatalf("an accepted update left %d snapshots; want 1", len(tx.snapshotSet))
		}
		_ = tx.Rollback()
	})
}
