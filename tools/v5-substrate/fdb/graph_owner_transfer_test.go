package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"testing"
	"time"
)

func ownerDirectTransferPolicy(c config, run [16]byte) graphOwnerTransferPolicy {
	now := time.Now().UnixNano()
	return graphOwnerTransferPolicy{Version: 1, Config: c, Run: run, BudgetSeconds: 180, AdmittedUnixNanos: now, DeadlineUnixNanos: now + int64(180*time.Second), MaxRecords: 4096, MaxControlBytes: 16 << 20, MaxProposalBytes: 8192, MaxGrants: 8, MaxBlock: 32, MaxAttempts: 64, MaxSubmissions: 8, MaxCallbacks: 16, CleanupMillis: 30_000, MaxCleanupCalls: 8}
}

// Authored only: ordinary catalog1 grants remain unchanged, and a caller must
// explicitly fund a fresh matrix policy rather than inheriting larger caps.
func TestGraphOwnerTransferAdmissionAndFencedAbortGuards(t *testing.T) {
	run := [16]byte{1}
	spec := graphFeedWorkloadSpec{Version: 1, Family: graphOwnerTransferFamily, Facts: 20, CrossPercent: 100, Writers: 1, Layout: "kv"}
	c := graphOwnerTransferConfig(spec, hex.EncodeToString(run[:]))
	p := ownerDirectTransferPolicy(c, run)
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*graphOwnerTransferPolicy){
		func(p *graphOwnerTransferPolicy) { p.MaxBlock = graphOwnerMaxBlock + 1 },
		func(p *graphOwnerTransferPolicy) { p.MaxGrants = 3 },
		func(p *graphOwnerTransferPolicy) { p.MaxRecords = 10 },
		func(p *graphOwnerTransferPolicy) { p.MaxControlBytes = math.MaxUint64 },
		func(p *graphOwnerTransferPolicy) { p.DeadlineUnixNanos++ },
		func(p *graphOwnerTransferPolicy) { p.MaxProposalBytes = 32<<10 + 1 },
		func(p *graphOwnerTransferPolicy) { p.CleanupMillis++ },
		func(p *graphOwnerTransferPolicy) { p.MaxCleanupCalls++ },
	} {
		bad := p
		change(&bad)
		if err := bad.validate(); !errors.Is(err, errInvalid) {
			t.Fatal("unfunded/foreign admission", bad, err)
		}
	}
	if err := (graphOwnerPolicy{4097, 16 << 20}).validate(); !errors.Is(err, errInvalid) {
		t.Fatal("ordinary core1 cap widened", err)
	}
	for _, writers := range []int{1, 8, 32} {
		large := spec
		large.Facts = 1_000_000
		large.Writers = writers
		cfg := graphOwnerTransferConfig(large, hex.EncodeToString(run[:]))
		grant := p
		grant.Config = cfg
		grant.MaxBlock = graphOwnerMaxBlock
		grant.MaxGrants = 8
		grant.MaxRecords = 10_000_000
		grant.MaxControlBytes = 1 << 40
		if err := grant.validate(); err != nil {
			t.Fatal("explicit finite source admission", writers, err)
		}
	}
	state := graphOwnerState{Version: 2, Routes: [2]graphOwnerRoute{{"B", 2}, {"B", 1}}}
	intent := graphOwnerIntent{Routes: [2]graphOwnerRoute{{"A", 1}, {"B", 1}}}
	x := transaction{Participants: []participant{{Group: 0}, {Group: 1}}}
	catalog := &graphOwnerCatalog{h: &harness{config: c}, binding: graphOwnerBinding{Transfer: &p}}
	if err := graphOwnerFenced(catalog, state, x, intent); err != nil {
		t.Fatal(err)
	}
	current := intent
	current.Routes = state.Routes
	if err := graphOwnerFenced(catalog, state, x, current); !errors.Is(err, errStale) {
		t.Fatal("absence/current capture fabricated ABORT", err)
	}
	state.Moving = "handoff"
	if err := graphOwnerFenced(catalog, state, x, intent); !errors.Is(err, errOwnerBusy) {
		t.Fatal("unactivated authority ABORT", err)
	}
	state.Moving = ""
	state.Routes[0] = graphOwnerRoute{"B", 1}
	if err := graphOwnerFenced(catalog, state, x, intent); !errors.Is(err, errMismatch) {
		t.Fatal("owner changed without epoch", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := catalog.operationContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("dead caller entered SDK", err)
	}
	if bytes := catalog.recordCharge([]byte("key"), []byte("body"), graphOwnerIntent{}); bytes != uint64(3+graphOwnerRecordBytes) {
		t.Fatal("catalog1 reservation changed", bytes)
	}
	catalog.binding.Version = 2
	if bytes := catalog.recordCharge([]byte("key"), []byte("body"), graphOwnerIntent{}); bytes != 7 {
		t.Fatal("fresh immutable accounting", bytes)
	}
	if bytes := catalog.recordCharge([]byte("key"), []byte("body"), graphOwnerAllocationSlot{}); bytes != uint64(3+graphOwnerRecordBytes) {
		t.Fatal("terminal reservation stolen", bytes)
	}
}

func TestGraphOwnerFundedCleanupDeadlineAndNoResetGuards(t *testing.T) {
	run := [16]byte{1}
	cfg := graphOwnerTransferConfig(graphFeedWorkloadSpec{Version: 1, Family: graphOwnerTransferFamily, Facts: 20, CrossPercent: 100, Writers: 1, Layout: "kv"}, hex.EncodeToString(run[:]))
	p := ownerDirectTransferPolicy(cfg, run)
	p.AdmittedUnixNanos -= int64(181 * time.Second)
	p.DeadlineUnixNanos = p.AdmittedUnixNanos + int64(180*time.Second)
	c := &graphOwnerCatalog{binding: graphOwnerBinding{Transfer: &p}}
	if _, _, err := c.operationContext(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("new work after run deadline", err)
	}
	now := p.DeadlineUnixNanos + int64(time.Second)
	ticket, err := p.cleanupDebit(graphOwnerCleanup{Request: "grant/original"}, now)
	if err != nil || ticket.Calls != 1 || ticket.DeadlineUnixNanos != p.DeadlineUnixNanos+int64(30*time.Second) {
		t.Fatal("funded cleanup lost after normal deadline", ticket, err)
	}
	deadline := ticket.DeadlineUnixNanos
	for ticket.Calls < p.MaxCleanupCalls {
		ticket, err = p.cleanupDebit(ticket, now+int64(time.Second))
		if err != nil || ticket.DeadlineUnixNanos != deadline {
			t.Fatal("replacement reset credit/deadline", ticket, err)
		}
	}
	if _, err := p.cleanupDebit(ticket, now); !errors.Is(err, errLimit) {
		t.Fatal("unbounded cleanup retries", err)
	}
	ticket.Calls = 1
	if _, err := p.cleanupDebit(ticket, deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired ticket renewed", err)
	}
	ticket.Calls = 0
	if _, err := p.cleanupDebit(ticket, now); !errors.Is(err, errMismatch) {
		t.Fatal("spent/deadline state reset", err)
	}
}

func TestGraphOwnerOwnedTransportAndCensusBindingGuards(t *testing.T) {
	a := graphTransportGuardPolicy()
	a.Spec.Family = graphOwnerTransferFamily
	a.CausalMinimumMillis, _ = graphTransportCausalMinimum(a.Spec, a.Transport)
	cfg := graphTransportConfig(a.Spec, hex.EncodeToString(a.Run[:]))
	p := ownerDirectTransferPolicy(cfg, a.Run)
	p.Version, p.Admission, p.Census = 2, &a, new(ownerDirectCensusPolicy())
	p.BudgetSeconds, p.AdmittedUnixNanos, p.DeadlineUnixNanos = a.BudgetSeconds, a.AdmittedUnixNanos, a.DeadlineUnixNanos
	p.MaxGrants, p.MaxBlock, p.MaxRecords, p.MaxControlBytes = 8, graphOwnerMaxBlock, 10_000_000, 1<<40
	*p.Census = graphOwnerCensusPolicy{PageRows: 128, PageBytes: 4 << 20, HeadRows: 512, HeadBytes: 48 << 10, MaxRows: 1 << 31, MaxBytes: 1 << 45, MaxCalls: 1 << 28, MaxWorkRows: 1 << 48, MaxWorkBytes: 1 << 62}
	if err := p.validate(); err != nil {
		t.Fatal("explicit owned transfer binding", err)
	}
	if !graphOwnerSelected(cfg) || !graphTransported(cfg) || cfg.Cost.Version != 8 {
		t.Fatal("fresh route bypassed original/owner gate", cfg)
	}
	for _, change := range []func(*graphOwnerTransferPolicy){
		func(p *graphOwnerTransferPolicy) { p.Admission = nil },
		func(p *graphOwnerTransferPolicy) { p.Census = nil },
		func(p *graphOwnerTransferPolicy) { p.Version = 1 },
		func(p *graphOwnerTransferPolicy) { p.DeadlineUnixNanos-- },
	} {
		bad := p
		change(&bad)
		if err := bad.validate(); err == nil {
			t.Fatal("missing/reset owned transport admitted", bad)
		}
	}
	legacy := ownerDirectTransferPolicy(graphOwnerTransferConfig(graphFeedWorkloadSpec{Version: 1, Family: graphOwnerTransferFamily, Facts: 20, CrossPercent: 100, Writers: 1, Layout: "kv"}, hex.EncodeToString(a.Run[:])), a.Run)
	body, err := wire(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("Admission")) || bytes.Contains(body, []byte("Census")) {
		t.Fatal("direct policy1 bytes widened", string(body))
	}
}
