package core

import (
	"context"
	"sync/atomic"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// walkCountingStore counts the store's equality lookups and scans (each one
// an index walk or a scan; on tiered and sharded a walk of every shard). It
// declares the keys door itself, so nativeCapability admits it.
type walkCountingStore struct {
	*memory.Store
	walks   atomic.Int64
	sidecar atomic.Int64 // property tx-membership enumerations
}

// The sidecar doors, one key and several keys (the latter folded from the
// former here, as a store without a fan-out would).
func (s *walkCountingStore) ForEachNodePropertyTxMember(tok uint16, key, vk string, fn func(types.NodeID, types.Instant) bool) error {
	s.sidecar.Add(1)
	return s.Store.ForEachNodePropertyTxMember(tok, key, vk, fn)
}

func (s *walkCountingStore) ForEachNodePropertyTxMemberDuring(tok uint16, key, vk string, a, b, tx types.Instant, fn func(types.NodeID, types.Instant) bool) error {
	s.sidecar.Add(1)
	return s.Store.ForEachNodePropertyTxMemberDuring(tok, key, vk, a, b, tx, fn)
}

func (s *walkCountingStore) ForEachNodePropertyTxMemberKeys(tok uint16, key string, vks []string, fn func(types.NodeID, types.Instant) bool) error {
	s.sidecar.Add(1)
	for _, vk := range vks {
		if err := s.Store.ForEachNodePropertyTxMember(tok, key, vk, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *walkCountingStore) ForEachNodePropertyTxMemberKeysDuring(tok uint16, key string, vks []string, a, b, tx types.Instant, fn func(types.NodeID, types.Instant) bool) error {
	s.sidecar.Add(1)
	for _, vk := range vks {
		if err := s.Store.ForEachNodePropertyTxMemberDuring(tok, key, vk, a, b, tx, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *walkCountingStore) ForEachRelPropertyTxMember(tok uint16, key, vk string, fn func(types.RelID, types.Instant) bool) error {
	s.sidecar.Add(1)
	return s.Store.ForEachRelPropertyTxMember(tok, key, vk, fn)
}

func (s *walkCountingStore) ForEachRelPropertyTxMemberDuring(tok uint16, key, vk string, a, b, tx types.Instant, fn func(types.RelID, types.Instant) bool) error {
	s.sidecar.Add(1)
	return s.Store.ForEachRelPropertyTxMemberDuring(tok, key, vk, a, b, tx, fn)
}

func (s *walkCountingStore) ForEachRelPropertyTxMemberKeys(tok uint16, key string, vks []string, fn func(types.RelID, types.Instant) bool) error {
	s.sidecar.Add(1)
	for _, vk := range vks {
		if err := s.Store.ForEachRelPropertyTxMember(tok, key, vk, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *walkCountingStore) ForEachRelPropertyTxMemberKeysDuring(tok uint16, key string, vks []string, a, b, tx types.Instant, fn func(types.RelID, types.Instant) bool) error {
	s.sidecar.Add(1)
	for _, vk := range vks {
		if err := s.Store.ForEachRelPropertyTxMemberDuring(tok, key, vk, a, b, tx, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *walkCountingStore) NodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.walks.Add(1)
	return s.Store.NodesByLabelAndProperty(tok, key, value, opts)
}

func (s *walkCountingStore) NodesByLabelAndPropertyKeys(tok uint16, key string, valueKeys []string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.walks.Add(1)
	return s.Store.NodesByLabelAndPropertyKeys(tok, key, valueKeys, opts)
}

func (s *walkCountingStore) NodesByLabel(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.walks.Add(1)
	return s.Store.NodesByLabel(tok, opts)
}

func (s *walkCountingStore) RelationshipsByTypeAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.walks.Add(1)
	return s.Store.RelationshipsByTypeAndProperty(tok, key, value, opts)
}

func (s *walkCountingStore) RelationshipsByTypeAndPropertyKeys(tok uint16, key string, valueKeys []string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.walks.Add(1)
	return s.Store.RelationshipsByTypeAndPropertyKeys(tok, key, valueKeys, opts)
}

func (s *walkCountingStore) RelationshipsByType(tok uint16, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.walks.Add(1)
	return s.Store.RelationshipsByType(tok, opts)
}

// TestRound11PointSeekAsksTheStoreOnce (round 11 item 1): an equality seek of
// the number 1000 (ten forms) asks the store at most once per kind of read,
// for nodes and relationships: one index lookup (the plain door, the current
// rows of a pinned generic door) and one membership-sidecar enumeration (the
// pinned doors); the rows are every stored width. Before round 11 the seam
// asked once per form (ten lookups and ten enumerations; on tiered each over
// every shard).
func TestRound11PointSeekAsksTheStoreOnce(t *testing.T) {
	t.Parallel()
	st := &walkCountingStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	ctx := context.Background()
	if err := g.Index.CreateProperty("P", "k"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index.CreateRelProperty("E", "k"); err != nil {
		t.Fatal(err)
	}
	hub, err := g.Nodes.Add(ctx, []string{"Hub"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{int64(1000), 1000.0, int16(1000), uint32(1000), float32(1000), int64(7)} {
		n, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"k": v})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Add(ctx, "E", hub, n, map[string]any{"k": v}); err != nil {
			t.Fatal(err)
		}
	}
	now, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	pinned := storepkg.QueryOpts{TxPin: now, TxPinSet: true}
	for _, c := range []struct {
		name           string
		walks, sidecar int64
		seek           func() (int, error)
	}{
		{"ByLabelAndProperty", 1, 0, func() (int, error) {
			rows, err := g.Nodes.ByLabelAndProperty("P", "k", 1000, storepkg.QueryOpts{})
			return len(rows), err
		}},
		{"ByTypeAndProperty", 1, 0, func() (int, error) {
			rows, err := g.Rels.ByTypeAndProperty("E", "k", 1000, storepkg.QueryOpts{})
			return len(rows), err
		}},
		{"ByLabelAndProperty TxPin", 1, 1, func() (int, error) {
			rows, err := g.Nodes.ByLabelAndProperty("P", "k", 1000, pinned)
			return len(rows), err
		}},
		{"ByTypeAndProperty TxPin", 1, 1, func() (int, error) {
			rows, err := g.Rels.ByTypeAndProperty("E", "k", 1000, pinned)
			return len(rows), err
		}},
		{"NodesByLabelPropertyAt", 0, 1, func() (int, error) {
			rows, err := g.Temporal.NodesByLabelPropertyAt("P", "k", 1000, now)
			return len(rows), err
		}},
		{"RelsByTypePropertyAt", 0, 1, func() (int, error) {
			rows, err := g.Temporal.RelsByTypePropertyAt("E", "k", 1000, now)
			return len(rows), err
		}},
	} {
		st.walks.Store(0)
		st.sidecar.Store(0)
		n, err := c.seek()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if n != 5 {
			t.Errorf("%s: %d rows, want the 5 stored widths of 1000", c.name, n)
		}
		if walks, side := st.walks.Load(), st.sidecar.Load(); walks != c.walks || side != c.sidecar {
			t.Errorf("%s: %d store lookups and %d sidecar enumerations for one seek of 1000, want %d and %d", c.name, walks, side, c.walks, c.sidecar)
		}
	}
}
