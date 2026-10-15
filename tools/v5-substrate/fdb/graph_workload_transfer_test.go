package main

import (
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored guards only. No helper reconstruction claims an SDK process kill,
// actual Linux transport, matrix acceptance or matched native retention.
func TestGraphTransferImmutableProcessAndCanceledGCGuards(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (*graphHistoryRetention)(nil).beginContext(ctx, "gc", cut{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled admission entered native SDK", err)
	}
	if err := (*graphHistoryRetention)(nil).beginContext(nil, "gc", cut{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil context", err)
	}
	if err := (&graphHistoryRetention{}).beginContext(ctx, "gc", cut{}); !errors.Is(err, context.Canceled) {
		t.Fatal("zero database entered despite canceled context", err)
	}
	if _, err := (*graphHistoryRetention)(nil).collectContext(ctx, "gc", graphHistoryBudget{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled collector entered native SDK", err)
	}
	if _, err := (*graphHistoryRetention)(nil).collectContext(nil, "gc", graphHistoryBudget{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil collector context", err)
	}
	if _, err := (&graphHistoryRetention{}).collectContext(t.Context(), "gc", graphHistoryBudget{}); !errors.Is(err, errInvalid) {
		t.Fatal("refused budget entered zero native database", err)
	}
	unknown := graphHistoryCommitError(fdb.Error{Code: 1021})
	native, found := errors.AsType[fdb.Error](unknown)
	if !errors.Is(unknown, errUnknown) || !found || native.Code != 1021 {
		t.Fatal("bounded native uncertainty lost classification/cause", unknown)
	}
	call := graphTransferCall{ID: 1, Method: "submit", Intent: &graphOwnerIntent{Body: []byte("immutable")}}
	if err := call.validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []graphTransferCall{{Method: "submit"}, {ID: 1, Method: "current-as-snapshot"}, {ID: 1, Method: "submit"}, {ID: 1, Method: "prepare", Intent: call.Intent}} {
		if err := bad.validate(); !errors.Is(err, errInvalid) {
			t.Fatal("unowned/mixed RPC accepted", bad, err)
		}
	}
	for _, failure := range []error{errStale, errOwnerBusy, errMismatch, errAllocationConsumed, errHistoryExpired, errLimit, context.DeadlineExceeded} {
		r := graphTransferReply{}
		r.failure(failure)
		if r.NativeCode != nil || r.Error == "" || !errors.Is(r.cause(), failure) {
			t.Fatal("domain refusal invented SDK authority/lost sentinel", r, failure)
		}
	}
	catalog := &graphOwnerCatalog{h: &harness{config: config{Graph: "guard"}}, binding: graphOwnerBinding{Transfer: &graphOwnerTransferPolicy{}}}
	move := graphOwnerMove{Request: "storage", Before: graphOwnerState{Version: 2, Routes: [2]graphOwnerRoute{{"B", 2}, {"B", 1}}}}
	frozen := move.Before
	frozen.Moving = move.Request
	after := move.Before
	after.Version++
	beforeBody, err := wire(frozen)
	if err != nil {
		t.Fatal(err)
	}
	afterBody, err := wire(after)
	if err != nil {
		t.Fatal(err)
	}
	before := graphCostStoredRecord{catalog.key("state", "current"), beforeBody}
	observed := graphCostStoredRecord{before.Key, afterBody}
	if err := graphTransferCanceledHead(catalog, move, before, observed); err != nil {
		t.Fatal("real Cancel control fence", err)
	}
	if err := graphTransferCanceledHead(catalog, move, before, before); !errors.Is(err, errMismatch) {
		t.Fatal("Cancel did not advance control version", err)
	}
	meta := catalog.h.key(0, "meta", "state")
	if err := graphTransferCanceledHead(catalog, move, graphCostStoredRecord{meta, []byte("old")}, graphCostStoredRecord{meta, []byte("unrelated-new")}); !errors.Is(err, errMismatch) {
		t.Fatal("post-census unrelated mutation accepted", err)
	}
}
