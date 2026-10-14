package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Input/codec guards only. These never claim an actual FDB page was read.
func TestGraphCostStreamRejectsInvalidBoundsAndRawKeys(t *testing.T) {
	for _, policy := range []graphCostStreamPolicy{{}, {129, 1024}, {1, 16<<20 + 1}} {
		if !errors.Is(policy.validate(), errInvalid) {
			t.Fatal("invalid page policy", policy)
		}
	}
	var absent *harness
	if end, err := absent.graphCostWalkHistory(cut{}, cut{}, graphCostStreamPolicy{1, 4096}, nil); !errors.Is(err, errInvalid) || end != (graphCostStreamEnd{}) {
		t.Fatal("invalid owner/callback published EOF", end, err)
	}
	h := &harness{config: graphCostConfig("raw-guard", graphCostProfile{Version: 1, Facts: 1, Writers: 1})}
	v := value{Value: 1, Version: 1, Round: math.MaxUint64, TxID: "cost-setup-0"}
	body, err := wire(v)
	if err != nil {
		t.Fatal(err)
	}
	key := lifeKey(h.config.Cost.nodes()[0])
	row := graphCostRawRow{Key: fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(0), v.Round, key)), Value: body}
	if err := h.graphCostValidateRaw(row, true, math.MaxUint64); err != nil {
		t.Fatal("uint64 maximum raw cutoff", err)
	}
	for _, change := range []func(*graphCostRawRow){
		func(r *graphCostRawRow) { r.Key = append(bytes.Clone(r.Key), '0') },
		func(r *graphCostRawRow) { r.Group = 1 },
		func(r *graphCostRawRow) { r.Value = append(bytes.Clone(r.Value), '\n') },
	} {
		bad := row
		change(&bad)
		if err := h.graphCostValidateRaw(bad, true, math.MaxUint64); !errors.Is(err, errInvalid) {
			t.Fatal("noncanonical raw key/value classification", err)
		}
	}
}

// Actual native SDK transactions are required by fixture; no fallback/model.
// The source parent is not executed until a separately approved FDB lease.
func TestGraphCostStreamActualPagesOldCutOriginalsAndQuiescence(t *testing.T) {
	h := fixture(t, graphCostConfig(t.Name(), graphCostProfile{Version: 1, Facts: 2, CrossPercent: 50, Writers: 1}))
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	requestPrefix := "cost"
	apply := func(phase string, i uint64, command graphCommand, provenance uint64) {
		t.Helper()
		s, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := uint8(0)
		if phase == "setup" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(fmt.Sprintf("%s-%s-%d", requestPrefix, phase, i), coordinator, command, s)
		x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
		if phase != "setup" {
			x.Lifecycle.EdgeLife = command.Edge.ID + 1
			x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
		}
		mustSubmit(t, h, x)
	}
	p := h.config.Cost
	for i, node := range p.nodes() {
		apply("setup", uint64(i), graphCommand{Kind: "node-create", Node: node}, uint64(i+1))
	}
	for i := uint64(0); i < p.Facts; i++ {
		apply("create", i, graphCommand{Kind: "edge-create", Edge: p.edge(i, 5)}, 4+i)
	}
	old := mustCut(t, h, []uint8{0, 1}, nil)
	for i := uint64(0); i < p.Facts; i++ {
		apply("update", i, graphCommand{Kind: "edge-update", Edge: p.edge(i, 6)}, 4+p.Facts+i)
	}
	final := mustCut(t, h, []uint8{0, 1}, nil)
	policy := graphCostStreamPolicy{PageRows: 1, PageBytes: 64 << 10}
	var history, current, cdc []graphCostRawRow // Tiny test witnesses, never runner state.
	collect := func(out *[]graphCostRawRow) func(graphCostRawRow) error {
		return func(row graphCostRawRow) error { *out = append(*out, row); return nil }
	}
	end, err := h.graphCostWalkHistory(old, final, policy, collect(&history))
	if err != nil || end.Rows != [2]uint64{12, 3} || len(history) != 15 {
		t.Fatal("complete native retained-history census", end, len(history), err)
	}
	end, err = h.graphCostWalkCurrent(final, policy, collect(&current))
	if err != nil || end.Rows != [2]uint64{7, 2} || len(current) != 9 {
		t.Fatal("complete native current census", end, len(current), err)
	}
	end, err = h.graphCostWalkChanges(initial, final, policy, collect(&cdc))
	if err != nil || end.Rows != [2]uint64{6, 1} || len(cdc) != 7 || len(cdc[0].IDValue) == 0 || len(cdc[0].RequestValue) == 0 {
		t.Fatal("complete native original groups", end, err)
	}
	// Match every raw stored revision/current row to the complete independent
	// original change groups. Equal counts cannot conceal a substituted value,
	// omitted posting, duplicate revision or current-state historical answer.
	expectedHistory, expectedCurrent := map[string][]byte{}, map[string][]byte{}
	oldRows := snapshot{Values: [2]map[string]value{{}, {}}}
	for _, raw := range cdc {
		group, err := decode[changeGroup](raw.Value)
		if err != nil {
			t.Fatal(err)
		}
		for _, write := range group.Writes {
			body, err := wire(write.After)
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%shistory/%016x/%x", h.prefix(write.Group), write.After.Round, write.Key)
			if _, duplicate := expectedHistory[key]; duplicate {
				t.Fatal("duplicate original physical revision")
			}
			expectedHistory[key] = body
			expectedCurrent[string(h.key(write.Group, "current", write.Key))] = body
		}
	}
	checkRaw := func(rows []graphCostRawRow, expected map[string][]byte) {
		t.Helper()
		for _, raw := range rows {
			body, found := expected[string(raw.Key)]
			if !found || !bytes.Equal(body, raw.Value) {
				t.Fatal("raw physical revision differs from original", string(raw.Key))
			}
			delete(expected, string(raw.Key))
		}
		if len(expected) != 0 {
			t.Fatal("missing original physical revisions", len(expected))
		}
	}
	checkRaw(history, expectedHistory)
	checkRaw(current, expectedCurrent)
	for _, raw := range history {
		v, err := decode[value](raw.Value)
		key, decodeErr := hex.DecodeString(string(raw.Key)[len(h.prefix(raw.Group)+"history/")+17:])
		if err != nil || decodeErr != nil {
			t.Fatal(err, decodeErr)
		}
		if v.Round <= old.record.Round {
			oldRows.Values[raw.Group][string(key)] = v
		}
	}
	view, err := h.projectGraph(oldRows)
	if err != nil || graphCostCheckView(view, p, 5) != nil {
		t.Fatal("raw history substituted current graph at old cut", err)
	}
	end, err = h.graphCostWalkChanges(final, final, policy, func(graphCostRawRow) error { t.Fatal("extra empty-range row"); return nil })
	if err != nil || end.Rows != [2]uint64{} {
		t.Fatal("empty native range", end, err)
	}
	stop := errors.New("callback refused")
	if end, err := h.graphCostWalkHistory(old, final, policy, func(graphCostRawRow) error { return stop }); !errors.Is(err, stop) || end != (graphCostStreamEnd{}) {
		t.Fatal("callback failure published EOF", end, err)
	}
	if end, err := h.graphCostWalkCurrent(final, graphCostStreamPolicy{1, 1}, collect(&current)); !errors.Is(err, errLimit) || end != (graphCostStreamEnd{}) {
		t.Fatal("byte admission published EOF", end, err)
	}
	// Two native missing-row refusals: a durable original cannot be borrowed
	// from its group body, and a truncated sequence cannot publish its watermark.
	for _, missing := range []struct {
		key, value []byte
		want       error
	}{{cdc[0].IDKey, cdc[0].IDValue, errMismatch}, {cdc[len(cdc)-1].Key, cdc[len(cdc)-1].Value, errUnavailable}} {
		_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Clear(fdb.Key(missing.key)); return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		end, err := h.graphCostWalkChanges(initial, final, policy, func(graphCostRawRow) error { return nil })
		if !errors.Is(err, missing.want) || end != (graphCostStreamEnd{}) {
			t.Fatal("missing native evidence published EOF", end, err)
		}
		_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Set(fdb.Key(missing.key), missing.value); return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	changed := false
	if end, err := h.graphCostWalkCurrent(final, policy, func(graphCostRawRow) error {
		if !changed {
			changed = true
			requestPrefix = "after-cut"
			// A fresh actual native request invalidates quiescence. This is not
			// a replay under an already committed request/payload binding.
			apply("update", 0, graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}, 4+p.Facts)
		}
		return nil
	}); !errors.Is(err, errMismatch) || end != (graphCostStreamEnd{}) {
		t.Fatal("quiescence violation published EOF", end, err)
	}
}
