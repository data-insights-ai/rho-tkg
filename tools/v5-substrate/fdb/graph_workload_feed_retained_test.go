package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

// Authored source guards, not executed Linux/SDK/capacity evidence.
func TestGraphFeedRetainedWorkloadAdmissionAndIntervalGuards(t *testing.T) {
	for _, writers := range []int{1, 8, 32} {
		for _, facts := range []uint64{40, 10_000, 100_000, 1_000_000} {
			for _, layout := range []string{"kv", "columns"} {
				a := graphTransportGuardPolicy()
				a.Version, a.Spec.Version = 3, 3
				a.Spec.Facts, a.Spec.Writers, a.Spec.Layout = facts, writers, layout
				a.CausalMinimumMillis, _ = graphTransportCausalMinimum(a.Spec, a.Transport)
				c := graphFeedRetainedConfig(a.Spec, hex.EncodeToString(a.Run[:]))
				if !validGraphFeedRetainedConfig(c) || !validGraphCostProfile(c) || a.validate() != nil {
					t.Fatal("fresh dimensions", c, a.validate())
				}
				g := graphTransferGrant{MaxRecords: 1 << 30, MaxControlBytes: 1 << 45, MaxGrants: 64, MaxBlock: graphOwnerMaxBlock, Census: graphOwnerCensusPolicy{PageRows: 128, PageBytes: 4 << 20, HeadRows: 512, HeadBytes: 48 << 10, MaxRows: 1 << 31, MaxBytes: 1 << 45, MaxCalls: 1 << 28, MaxWorkRows: 1 << 48, MaxWorkBytes: 1 << 62}}
				p, err := graphFeedRetainedPolicy(g, a)
				if err != nil {
					t.Fatal(err)
				}
				if !graphOwnerTransferCensus(&p) || graphOwnerTransferCensus(nil) || !graphOwnerTransferCensus(&graphOwnerTransferPolicy{Version: 2}) {
					t.Fatal("fresh census admission or old exact Version2 changed")
				}
				for _, change := range []func(*graphOwnerTransferPolicy){func(p *graphOwnerTransferPolicy) { p.Version = 2 }, func(p *graphOwnerTransferPolicy) { p.MaxRecords = 3 }, func(p *graphOwnerTransferPolicy) { p.DeadlineUnixNanos++ }, func(p *graphOwnerTransferPolicy) { p.MaxCleanupCalls = 0 }} {
					bad := p
					change(&bad)
					if bad.validate() == nil || bad.Version == 3 && graphOwnerTransferCensus(&bad) {
						t.Fatal("unfunded/changed fresh agreement")
					}
				}
				h := &harness{config: c}
				policy := graphFeedRetainedColumnPolicy(c.Cost)
				if !graphColumnProfile(h, policy) || graphColumnMagic(policy) != "RHOCLM03" {
					t.Fatal("fresh interval codec")
				}
				old := policy
				old.Version = 0
				if graphColumnProfile(h, old) {
					t.Fatal("ordinary policy accepted under fresh config")
				}
				foreign := c
				foreign.Cost.Version = 5
				if graphColumnProfile(&harness{config: foreign}, policy) {
					t.Fatal("interval policy broadened old agreement")
				}
			}
		}
	}
	p := graphColumnProgress{Generation: graphColumnGeneration{Version: 2, Initial: certificate{Round: 10, Groups: [2]uint64{7, 3}}, Cut: certificate{Round: 12, Groups: [2]uint64{9, 4}}}, Rows: 3, Sequences: [2]uint64{9, 4}, History: [2]uint64{6, 1}, Metadata: [2]metadata{{Floor: 12, Groups: 9, Versions: 100}, {Floor: 12, Groups: 4, Versions: 100}}}
	if !graphColumnComplete(p) {
		t.Fatal("authentic interval incorrectly treated as full prefix")
	}
	p.Rows = 13
	if graphColumnComplete(p) {
		t.Fatal("old full-prefix count blessed interval omission")
	}
	p.Rows = 3
	p.Sequences[1]--
	if graphColumnComplete(p) {
		t.Fatal("half interval accepted")
	}
	if _, ok := graphColumnIntervalRows(graphColumnGeneration{Initial: certificate{Groups: [2]uint64{8, 4}}}, [2]uint64{7, 5}); ok {
		t.Fatal("unsigned initial underflow")
	}
	legacy := graphColumnTestPolicy()
	before, _ := wire(legacy)
	if bytes.Contains(before, []byte("Version")) || graphColumnMagic(legacy) != "RHOCLM01" {
		t.Fatal("zero-version legacy bytes changed")
	}
	var store *graphColumnStore
	if _, err := store.publish(t.Context(), "missing", cut{}, cut{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newGraphColumnStoreContext(ctx, nil, graphColumnPolicy{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled call reached native", err)
	}
}

func TestGraphFeedRetainedIntervalChunkBindsInitialAndOriginal(t *testing.T) {
	a := graphTransportGuardPolicy()
	a.Version, a.Spec.Version = 3, 3
	a.Spec.Facts, a.Spec.Writers = 40, 32
	a.CausalMinimumMillis, _ = graphTransportCausalMinimum(a.Spec, a.Transport)
	h := &harness{config: graphFeedRetainedConfig(a.Spec, hex.EncodeToString(a.Run[:]))}
	node := h.config.Cost.nodes()[0]
	command := graphCommand{Kind: "node-create", Node: node}
	x := h.graphTransaction("interval-node", 0, command, snapshot{Values: [2]map[string]value{{}, {}}})
	x.Lifecycle = &lifecycleMetadata{Provenance: 1}
	body, err := wire(x)
	if err != nil {
		t.Fatal(err)
	}
	o := outcome{Commit: true, Round: 11, Sequence: 8, Digest: sha256.Sum256(body), ID: x.ID, Request: x.Request}
	group := changeGroup{Tx: x, Outcome: o, Writes: []changedValue{{Group: 0, Key: lifeKey(node), After: value{Value: 1, Version: 1, Round: 11, TxID: x.ID}}}}
	original, err := wire(group)
	if err != nil {
		t.Fatal(err)
	}
	row, err := h.graphColumnRow(original)
	if err != nil {
		t.Fatal(err)
	}
	g := graphColumnGeneration{Version: 2, ID: "interval", Binding: sha256.Sum256([]byte("fresh-binding")), Initial: certificate{Round: 10, Groups: [2]uint64{7, 3}}, Cut: certificate{Round: 11, Groups: [2]uint64{8, 3}}}
	policy := graphFeedRetainedColumnPolicy(h.config.Cost)
	encoded, err := encodeGraphColumnChunk(h, g, 0, [32]byte{}, []graphColumnRow{row}, policy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphColumnChunk(h, g, 0, [32]byte{}, encoded, policy)
	if err != nil || len(decoded) != 1 || !graphColumnSameRow(row, decoded[0]) {
		t.Fatal(err)
	}
	for _, change := range []func(*graphColumnGeneration){func(g *graphColumnGeneration) { g.Initial.Groups[0]-- }, func(g *graphColumnGeneration) { g.Initial.Round-- }, func(g *graphColumnGeneration) { g.Version = 1 }} {
		bad := g
		change(&bad)
		if _, err := decodeGraphColumnChunk(h, bad, 0, [32]byte{}, encoded, policy); !errors.Is(err, errMismatch) {
			t.Fatal("changed initial/format accepted", err)
		}
	}
	bad := g
	bad.Version = 1
	if _, err := encodeGraphColumnChunk(h, bad, 0, [32]byte{}, []graphColumnRow{row}, policy); !errors.Is(err, errInvalid) {
		t.Fatal("fresh format accepted old generation", err)
	}
	encoded[len(encoded)-1] ^= 1
	if bytes.Equal(decoded[0].Original, encoded[len(encoded)-len(original):]) {
		t.Fatal("owned original aliases caller bytes")
	}
}
