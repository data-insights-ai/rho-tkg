package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

// Authored only: these pure guards deliberately require no live SDK fixture.
func TestGraphOwnerCurrentAuthorityAndImmutableCaptureGuards(t *testing.T) {
	state := graphOwnerState{Version: 2, Routes: [2]graphOwnerRoute{{"target", 2}, {"target", 1}}}
	old := graphOwnerIntent{Routes: [2]graphOwnerRoute{{"source", 1}, {"target", 1}}}
	x := transaction{Coordinator: 0, Participants: []participant{{Group: 0}, {Group: 1}}}
	if err := graphOwnerCurrent(state, graphOwnerAuthority{ID: "source"}, 0, graphOwnerRoute{"source", 1}); !errors.Is(err, errStale) {
		t.Fatal("old owner may acknowledge original", err)
	}
	if err := graphOwnerCurrent(state, graphOwnerAuthority{ID: "target"}, 0, graphOwnerRoute{"target", 2}); err != nil {
		t.Fatal("successor denied unchanged original recovery", err)
	}
	aba := graphOwnerState{Version: 3, Routes: [2]graphOwnerRoute{{"source", 3}, {"target", 1}}}
	if err := graphOwnerCurrent(aba, graphOwnerAuthority{ID: "source"}, 0, graphOwnerRoute{"source", 1}); !errors.Is(err, errStale) {
		t.Fatal("return to same owner ID revived epoch1 authority", err)
	}
	if err := graphOwnerNew(state, x, old); !errors.Is(err, errStale) {
		t.Fatal("absent old original became new execution", err)
	}
	current := old
	current.Routes = state.Routes
	if err := graphOwnerNew(state, x, current); err != nil {
		t.Fatal(err)
	}
	state.Moving = "move"
	if err := graphOwnerNew(state, x, current); !errors.Is(err, errOwnerBusy) {
		t.Fatal(err)
	}
	if retryable, maybe := graphCostParallelError(errOwnerBusy); retryable || maybe {
		t.Fatal("handoff guard invented SDK decision authority")
	}
	// Native request validation remains historical: immutable config epochs are
	// not owner epochs and must never be rewritten to admit an old CDC group.
	h := &harness{config: graphCostConfig(graphOwnerNamespace+"guards", graphCostProfile{1, 2, 100, 1})}
	if _, _, err := h.ownerGateIn(nil, transaction{}, [32]byte{}, nil); !errors.Is(err, errStale) {
		t.Fatal("unowned execute bypassed fresh namespace gate", err)
	}
	if _, err := h.recover(0, "request"); !errors.Is(err, errStale) {
		t.Fatal("unowned recover", err)
	}
	if _, _, err := h.graphCostReadParallelOriginal(t.Context(), transaction{}, nil); !errors.Is(err, errStale) {
		t.Fatal("unowned paired-original recovery", err)
	}
	legacy := &harness{config: defaults()}
	if catalog, _, err := legacy.ownerGateIn(nil, transaction{}, [32]byte{}, nil); err != nil || catalog != nil {
		t.Fatal("legacy execute gained catalog IO", err)
	}
	if err := legacy.ownerPublicationIn(nil); err != nil {
		t.Fatal("legacy cut gained catalog IO", err)
	}

	a := graphOwnerAuthority{ID: "source", Secret: [32]byte{1}}
	catalog := &graphOwnerCatalog{binding: graphOwnerBinding{Authorities: [2]graphOwnerPrincipal{{a.ID, sha256.Sum256(a.Secret[:])}}}}
	if err := catalog.authenticate(a); err != nil {
		t.Fatal(err)
	}
	a.Secret[0]++
	if err := catalog.authenticate(a); !errors.Is(err, errStale) {
		t.Fatal("forged authority", err)
	}
	var client *graphOwnerClient
	if _, _, err := client.input(nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err := client.input(make([]byte, 32<<10+1)); !errors.Is(err, errInvalid) {
		t.Fatal("unadmitted envelope", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := client.Submit(ctx, graphOwnerIntent{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled submit entered SDK", err)
	}
	if _, _, err := client.Original(ctx, graphOwnerIntent{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled recovery entered SDK", err)
	}
	if _, err := client.Prepare(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled reservation entered SDK", err)
	}
	if _, err := client.PrepareAllocated(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled allocated factory", err)
	}
	if _, _, err := client.ResolveFenced(ctx, graphOwnerIntent{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled successor recovery", err)
	}
	if _, err := client.CloseAllocationSession(t.Context(), graphOwnerAllocationSession{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil cleanup receiver", err)
	}
	if _, err := client.CloseReservedIDs(t.Context(), "grant", [32]byte{1}); !errors.Is(err, errInvalid) {
		t.Fatal("nil grant cleanup receiver", err)
	}
}
