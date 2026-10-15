package main

import (
	"errors"
	"testing"
)

// Explicit small direct fixture grant. These limits are not matrix acceptance.
func ownerDirectCensusPolicy() graphOwnerCensusPolicy {
	return graphOwnerCensusPolicy{PageRows: 16, PageBytes: 128 << 10, HeadRows: 256, HeadBytes: 48 << 10, MaxRows: 1 << 20, MaxBytes: 1 << 30, MaxCalls: 4096, MaxWorkRows: 1 << 28, MaxWorkBytes: 1 << 42}
}

func TestGraphOwnerCensusAdmissionAndClosedClasses(t *testing.T) {
	p := ownerDirectCensusPolicy()
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*graphOwnerCensusPolicy){
		func(p *graphOwnerCensusPolicy) { p.HeadRows = 513 },
		func(p *graphOwnerCensusPolicy) { p.HeadBytes = 64 << 10 },
		func(p *graphOwnerCensusPolicy) { p.PageRows = 129 },
		func(p *graphOwnerCensusPolicy) { p.PageBytes = 32 << 10 },
		func(p *graphOwnerCensusPolicy) { p.MaxWorkRows = p.MaxRows - 1 },
	} {
		bad := p
		change(&bad)
		if err := bad.validate(); !errors.Is(err, errInvalid) {
			t.Fatal("unbounded/insufficient native census", bad, err)
		}
	}
	h := &harness{config: graphCostConfig("census-closed-class-guard", graphCostProfile{1, 2, 100, 1})}
	c := &graphOwnerCatalog{h: h}
	for _, key := range [][]byte{h.key(0, "current", "key"), c.key("allocation-slot", "grant"), c.key("cleanup", "grant/grant"), h.historyKey("state"), h.key(0, "feed-v1/cursor", "feed"), h.key(0, "workload-transport-v2", "binding")} {
		if ok, err := c.censusClass(key); err != nil || !ok {
			t.Fatal("known native class", string(key), err)
		}
	}
	for _, key := range [][]byte{h.key(0, "future-mutable", "head"), h.key(0, "feed-v1/future-mutable", "head"), c.key("state", "future-head"), h.key(0, "meta", "future-head"), h.historyKey("future-head")} {
		if ok, err := c.censusClass(key); ok || !errors.Is(err, errUnavailable) {
			t.Fatal("unbound mutable class sealed", string(key), err)
		}
	}
	for _, suffix := range []string{"retention-v1/binding/extra", "retention-v1/reference/", "generation/67/progress/extra", "generation/67/chunk/unknown"} {
		if ok, err := c.censusClass([]byte(h.prefix(0) + "columns-v1/" + suffix)); ok || !errors.Is(err, errUnavailable) {
			t.Fatal("unbound column subtype", suffix, err)
		}
	}
	if !censusProgressValid(graphOwnerCensusProgress{}, p) {
		t.Fatal("empty admitted census refused")
	}
	for _, bad := range []graphOwnerCensusProgress{{EOF: true}, {Pages: 1, Calls: 1}, {Pending: "page-0000000000000002"}, {Position: graphOwnerCensusPosition{Group: 2, Cursor: []byte("foreign")}, EOF: true}} {
		if censusProgressValid(bad, p) {
			t.Fatal("partial/foreign progress authorized EOF", bad)
		}
	}
	var absent *graphOwnerCatalog
	if _, err := absent.PrepareCensus(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}, "c"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := absent.CensusPage(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}, "c", 1); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := absent.ObserveCensus(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}, "c"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

// Authored real SDK guard, not executed. A genuine SAME terminal call changes a
// captured cleanup head although graph counts/values remain equal. No footer or
// activation may bless that mixed-time prefix. Positive complete EOF/seal and
// native all-control byte counts are direct calls in the allocation parent.
func TestGraphOwnerCensusActualChangedNativeHeadRefusesActivation(t *testing.T) {
	h, _, catalog, ca, cb, a, b := ownerActualTransferFixtureAdmission(t, true)
	if _, err := ca.ReserveIDs(t.Context(), "remote-closed", [32]byte{7}, 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cb.CloseReservedIDs(t.Context(), "remote-closed", [32]byte{7}); err != nil {
		t.Fatal(err)
	}
	cut := mustCut(t, h, []uint8{0, 1}, nil)
	move, err := catalog.ReserveMove(t.Context(), a, ca.routes[0], "move", [32]byte{8}, 0, b.ID, cut)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Freeze(t.Context(), a, move); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.PrepareCensus(t.Context(), a, move, "census"); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Activate(t.Context(), b, move); !errors.Is(err, errUnavailable) {
		t.Fatal("partial census activated", err)
	}
	if _, err := cb.CloseReservedIDs(t.Context(), "remote-closed", [32]byte{7}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CensusPage(t.Context(), a, move, "census", 1); !errors.Is(err, errOwnerBusy) {
		t.Fatal("equal graph counts hid native control change", err)
	}
	if _, err := catalog.Activate(t.Context(), b, move); !errors.Is(err, errOwnerBusy) {
		t.Fatal("mixed native prefix activated", err)
	}
	if _, err := catalog.ObserveCensus(t.Context(), a, move, "census"); !errors.Is(err, errUnavailable) {
		t.Fatal("partial successful footer", err)
	}
	if err := catalog.Cancel(t.Context(), a, move); err != nil {
		t.Fatal("already funded frozen-owner cleanup stranded", err)
	}
}
