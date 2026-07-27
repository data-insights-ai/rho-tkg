package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var coreExactErasureBounds = ExactErasureBounds{
	MaxRelationshipIdentities: 32,
	MaxRelationshipVersions:   128,
	MaxEndpointNodeIdentities: 64,
}

func TestAdminOpsExactEraseGateDigestMetadataAndRetry(t *testing.T) {
	ctx := context.Background()
	disabled, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	n, err := disabled.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := disabled.Admin.ExactErase(ctx, ExactErasureRequest{NodeIDs: []types.NodeID{n.ID()}}); !errors.Is(err, ErrExactErasureDisabled) {
		t.Fatalf("disabled ExactErase = %v, want ErrExactErasureDisabled", err)
	}
	if _, err := disabled.Nodes.Get(ctx, n.ID()); err != nil {
		t.Fatalf("disabled refusal mutated node: %v", err)
	}
	_ = disabled.Close()

	ms := memory.New()
	g, err := New(Config{Store: ms, AllowExactErasure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	a, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"email": "erase@example.test"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"note": "erase relationship"})
	if err != nil {
		t.Fatal(err)
	}

	// Seed the exact Core metadata shape the operation must remove atomically.
	labelTok, ok := g.labels.Lookup("Person")
	if !ok {
		t.Fatal("Person label not registered")
	}
	ownerKey := foreverOwnerKey(labelTok, "email", "s:erase@example.test")
	g.uniqueMu.Lock()
	g.uniqueOwners[ownerKey] = a.ID()
	if err := g.storeForeverOwnersLocked(ms); err != nil {
		g.uniqueMu.Unlock()
		t.Fatal(err)
	}
	g.uniqueMu.Unlock()
	if err := ms.MetaSet(compactStubNodeKey(a.ID()), []byte("stub-with-erased-id")); err != nil {
		t.Fatal(err)
	}

	first, err := g.Admin.ExactErase(ctx, ExactErasureRequest{
		NodeIDs:         []types.NodeID{a.ID(), a.ID()},
		RelationshipIDs: []types.RelID{r.ID(), r.ID()},
		Bounds:          coreExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if first.Digest == "" || first.NodeCount != 1 || first.RelationshipCount != 1 {
		t.Fatalf("receipt = %+v", first)
	}
	if _, err := g.Nodes.Get(ctx, a.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("erased node read = %v", err)
	}
	if _, err := g.Nodes.Get(ctx, b.ID()); err != nil {
		t.Fatalf("survivor read = %v", err)
	}

	second, err := g.Admin.ExactErase(ctx, ExactErasureRequest{
		RelationshipIDs: []types.RelID{r.ID()},
		NodeIDs:         []types.NodeID{a.ID()},
		Bounds:          coreExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("idempotent ExactErase: %v", err)
	}
	if second != first {
		t.Fatalf("retry receipt = %+v, want %+v", second, first)
	}
	g.uniqueMu.RLock()
	_, ownerSurvived := g.uniqueOwners[ownerKey]
	g.uniqueMu.RUnlock()
	if ownerSurvived {
		t.Fatal("UniqueForever owner survived exact erasure")
	}
	ownersBlob, err := ms.MetaGet(uniqueForeverOwnersMeta)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ownersBlob, []byte("erase@example.test")) {
		t.Fatalf("UniqueForever metadata retained erased value: %q", ownersBlob)
	}
	if stub, err := ms.MetaGet(compactStubNodeKey(a.ID())); err != nil || len(stub) != 0 {
		t.Fatalf("compaction stub survived = (%q, %v)", stub, err)
	}
}

type exactErasureMissingStore struct {
	storepkg.MandatoryStore
}

func TestAdminOpsExactEraseCapabilityAndValidation(t *testing.T) {
	g, err := New(Config{
		Store:             &exactErasureMissingStore{MandatoryStore: memory.New()},
		AllowExactErasure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.Admin.ExactErase(nil, ExactErasureRequest{NodeIDs: []types.NodeID{1}}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context = %v, want ErrNilContext", err)
	}
	if _, err := g.Admin.ExactErase(context.Background(), ExactErasureRequest{}); !errors.Is(err, ErrInvalidExactErasureRequest) {
		t.Fatalf("empty request = %v, want ErrInvalidExactErasureRequest", err)
	}
	if _, err := g.Admin.ExactErase(context.Background(), ExactErasureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds:  coreExactErasureBounds,
	}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("missing capability = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Admin.ResolveExactErasure(
		context.Background(),
		ExactErasureRequest{NodeIDs: []types.NodeID{1}, Bounds: coreExactErasureBounds},
	); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("missing resolve capability = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestAdminOpsResolveExactErasureValidationLifecycleAndClosure(t *testing.T) {
	ctx := context.Background()
	g, err := New(Config{Store: memory.New(), AllowExactErasure: true})
	if err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}

	resolved, err := g.Admin.ResolveExactErasure(ctx, ExactErasureRequest{
		NodeIDs: []types.NodeID{a.ID(), a.ID()},
		Bounds:  coreExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("ResolveExactErasure: %v", err)
	}
	if len(resolved.Request.NodeIDs) != 1 ||
		len(resolved.Request.RelationshipIDs) != 1 ||
		resolved.Request.RelationshipIDs[0] != r.ID() ||
		len(resolved.EndpointNodeIDs) != 2 ||
		len(resolved.RelationshipBindings) != 1 ||
		resolved.RelationshipBindings[0].Type != "KNOWS" {
		t.Fatalf("resolved = %+v", resolved)
	}
	if _, err = g.Admin.ResolveExactErasure(nil, resolved.Request); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context = %v, want ErrNilContext", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = g.Admin.ResolveExactErasure(cancelled, resolved.Request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context = %v, want context.Canceled", err)
	}
	if _, err = g.Admin.ResolveExactErasure(ctx, ExactErasureRequest{}); !errors.Is(err, ErrInvalidExactErasureRequest) {
		t.Fatalf("empty resolve = %v, want ErrInvalidExactErasureRequest", err)
	}
	if err = g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = g.Admin.ResolveExactErasure(ctx, resolved.Request); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed resolve = %v, want ErrGraphClosed", err)
	}
}
