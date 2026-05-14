package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	lockspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

var errRelationshipUpdateMissingCurrentEndpointLock = errors.New("relationship update did not hold current endpoint lock")

type reusedRelEndpointProbeStore struct {
	storepkg.MandatoryStore
	targetID   types.RelID
	oldRel     *types.Relationship
	currentID  types.NodeID
	locks      *lockspkg.Manager
	gets       atomic.Uint32
	probeDone  chan struct{}
	probeError atomic.Bool
}

func (s *reusedRelEndpointProbeStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if id == s.targetID && s.gets.Add(1) == 1 {
		return s.oldRel.DeepCopy(), nil
	}
	return s.MandatoryStore.GetRelationship(id)
}

func (s *reusedRelEndpointProbeStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prev *types.Relationship) error {
	if current.ID() == s.targetID {
		if err := s.requireCurrentEndpointLock(); err != nil {
			return err
		}
	}
	return s.MandatoryStore.ReplaceRelWithHistory(current, prevVersion, prev)
}

func (s *reusedRelEndpointProbeStore) ReplaceRelationship(current *types.Relationship) error {
	if current.ID() == s.targetID {
		if err := s.requireCurrentEndpointLock(); err != nil {
			return err
		}
	}
	return s.MandatoryStore.ReplaceRelationship(current)
}

func (s *reusedRelEndpointProbeStore) requireCurrentEndpointLock() error {
	acquired := make(chan struct{})
	go func() {
		s.locks.LockEntity(s.currentID.SnowflakeID())
		s.locks.UnlockEntity(s.currentID.SnowflakeID())
		close(acquired)
	}()
	select {
	case <-acquired:
		s.probeError.Store(true)
		close(s.probeDone)
		return errRelationshipUpdateMissingCurrentEndpointLock
	case <-time.After(50 * time.Millisecond):
	}
	defer func() {
		go func() {
			<-acquired
			close(s.probeDone)
		}()
	}()
	return nil
}

func TestUpdateRelationship_RetriesWhenReusedIDHasDifferentEndpoints(t *testing.T) {
	t.Parallel()

	g, probe, relID := newReusedRelEndpointProbeFixture(t)

	_, err := g.Rels.Update(relID, map[string]any{"weight": int64(7)})
	if err != nil {
		t.Fatalf("Update relationship after endpoint reuse: %v", err)
	}
	assertReusedRelEndpointProbe(t, probe)
}

func TestUpdateRelationshipInPlace_RetriesWhenReusedIDHasDifferentEndpoints(t *testing.T) {
	t.Parallel()

	g, probe, relID := newReusedRelEndpointProbeFixture(t)

	_, err := g.Rels.UpdateInPlace(relID, map[string]any{"weight": int64(7)})
	if err != nil {
		t.Fatalf("Update relationship in place after endpoint reuse: %v", err)
	}
	assertReusedRelEndpointProbe(t, probe)
}

func TestRelationshipCAS_RetriesWhenReusedIDHasDifferentEndpoints(t *testing.T) {
	t.Parallel()

	g, probe, relID := newReusedRelEndpointProbeFixture(t)

	ok, err := g.Rels.CompareAndSetProperty(relID, "weight", nil, int64(7))
	if err != nil {
		t.Fatalf("CompareAndSetProperty after endpoint reuse: %v", err)
	}
	if !ok {
		t.Fatal("CompareAndSetProperty ok = false, want true")
	}
	assertReusedRelEndpointProbe(t, probe)
}

func newReusedRelEndpointProbeFixture(t *testing.T) (*Core, *reusedRelEndpointProbeStore, types.RelID) {
	t.Helper()

	probe := &reusedRelEndpointProbeStore{
		MandatoryStore: memory.New(),
		probeDone:      make(chan struct{}),
	}
	g, err := New(Config{Store: probe})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	probe.locks = g.entityLocks

	relID := types.RelID(987654321)
	oldStart := addUpdateLockNode(t, g, "OldStart")
	oldEnd := addUpdateLockNode(t, g, "OldEnd")
	currentStart := addNodeOnShardDistinctFrom(t, g, "CurrentStart", relID.SnowflakeID(), oldStart.ID().SnowflakeID(), oldEnd.ID().SnowflakeID())
	currentEnd := addUpdateLockNode(t, g, "CurrentEnd")

	current, err := g.Rels.Import(context.Background(), relID, "KNOWS", currentStart, currentEnd, nil)
	if err != nil {
		t.Fatalf("Import current relationship: %v", err)
	}
	oldRel := types.NewRelationship(relID, uint16(current.TypeToken()), oldStart.ID(), oldEnd.ID())
	probe.targetID = relID
	probe.oldRel = oldRel
	probe.currentID = currentStart.ID()

	return g, probe, relID
}

func assertReusedRelEndpointProbe(t *testing.T, probe *reusedRelEndpointProbeStore) {
	t.Helper()
	if probe.probeError.Load() {
		t.Fatal(errRelationshipUpdateMissingCurrentEndpointLock)
	}
	select {
	case <-probe.probeDone:
	case <-time.After(time.Second):
		t.Fatal("endpoint-lock probe did not finish")
	}
	if got := probe.gets.Load(); got < 3 {
		t.Fatalf("GetRelationship calls = %d, want retry after stale endpoint peek", got)
	}
}

func addUpdateLockNode(t *testing.T, g *Core, label string) *types.Node {
	t.Helper()
	n, err := g.Nodes.Add([]string{label}, nil)
	if err != nil {
		t.Fatalf("Add node %s: %v", label, err)
	}
	return n
}

func addNodeOnShardDistinctFrom(t *testing.T, g *Core, label string, ids ...snowflake.ID) *types.Node {
	t.Helper()
	blocked := make(map[uint8]struct{}, len(ids))
	for _, id := range ids {
		blocked[lockspkg.ShardIndex(id)] = struct{}{}
	}
	for i := 0; i < 2048; i++ {
		n := addUpdateLockNode(t, g, fmt.Sprintf("%s%d", label, i))
		if _, exists := blocked[lockspkg.ShardIndex(n.ID().SnowflakeID())]; !exists {
			return n
		}
	}
	t.Fatalf("could not allocate node on a shard distinct from %d blocked shards", len(blocked))
	return nil
}
