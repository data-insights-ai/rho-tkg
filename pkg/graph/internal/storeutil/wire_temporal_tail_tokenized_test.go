package storeutil

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCrownEquivalenceNodeTokenized proves the END-TO-END byte-identity the
// ingest apply-side consumption relies on: for a finalized node and an applier
// timestamp T,
//
//	Patch(PreEncodeNodeWireV2WithKeys(node, reg), T, 0) == MarshalNodeWireWithKeys(nodeₜ, reg)
//
// where nodeₜ is the SAME node with TxFrom stamped to T — the exact bytes the
// store's marshalNodeBytes produces at flush. This is the crown property of
// wire_temporal_tail.go extended through the property-key TOKENIZATION the
// persisted entity row carries (unlike the untokenized change-log put body).
func TestCrownEquivalenceNodeTokenized(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xD1CE))
	reg := registrypkg.NewPropertyKeyRegistry()

	for i := 0; i < 400; i++ {
		n := randomTokenizedNode(t, rng)
		txFrom := randomTimestamp(rng)
		txTo := randomTimestamp(rng)

		pre, err := PreEncodeNodeWireV2WithKeys(n, reg)
		if err != nil {
			t.Fatalf("pre-encode[%d]: %v", i, err)
		}
		if !HasWireTemporalTail(pre) {
			t.Fatalf("pre-encode[%d] has no fixed tail", i)
		}
		patched := append([]byte(nil), pre...)
		if err := PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
			t.Fatalf("patch[%d]: %v", i, err)
		}

		// The store's own encode of the finalized node (TxFrom=T, TxTo stamped).
		nt := n.DeepCopy()
		tm := nt.Temporal()
		if tm == nil {
			tm = &types.TemporalMetadata{}
		}
		tm.TxFrom = types.Instant(txFrom)
		tm.TxTo = types.Instant(txTo)
		nt.SetTemporal(tm)
		direct, err := MarshalNodeWireWithKeys(nt, reg)
		if err != nil {
			t.Fatalf("direct[%d]: %v", i, err)
		}
		if !bytes.Equal(patched, direct) {
			t.Fatalf("tokenized crown mismatch node[%d]:\n patched=%s\n direct =%s",
				i, hex.EncodeToString(patched), hex.EncodeToString(direct))
		}
		// And it decodes back through the registry to the finalized state.
		back, err := UnmarshalNodeWireWithKeys(patched, reg)
		if err != nil {
			t.Fatalf("decode patched[%d]: %v", i, err)
		}
		if back.TxFrom != txFrom || back.TxTo != txTo {
			t.Fatalf("patched decode[%d]: tf=%d tt=%d want %d %d", i, back.TxFrom, back.TxTo, txFrom, txTo)
		}
	}
}

// TestCrownEquivalenceRelTokenized is the relationship mirror.
func TestCrownEquivalenceRelTokenized(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xF00D))
	reg := registrypkg.NewPropertyKeyRegistry()

	for i := 0; i < 400; i++ {
		r := randomTokenizedRel(t, rng)
		txFrom := randomTimestamp(rng)
		txTo := randomTimestamp(rng)

		pre, err := PreEncodeRelWireV2WithKeys(r, reg)
		if err != nil {
			t.Fatalf("pre-encode rel[%d]: %v", i, err)
		}
		patched := append([]byte(nil), pre...)
		if err := PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
			t.Fatalf("patch rel[%d]: %v", i, err)
		}

		rt := r.DeepCopy()
		tm := rt.Temporal()
		if tm == nil {
			tm = &types.TemporalMetadata{}
		}
		tm.TxFrom = types.Instant(txFrom)
		tm.TxTo = types.Instant(txTo)
		rt.SetTemporal(tm)
		direct, err := MarshalRelWireWithKeys(rt, reg)
		if err != nil {
			t.Fatalf("direct rel[%d]: %v", i, err)
		}
		if !bytes.Equal(patched, direct) {
			t.Fatalf("tokenized crown mismatch rel[%d]:\n patched=%s\n direct =%s",
				i, hex.EncodeToString(patched), hex.EncodeToString(direct))
		}
	}
}

// randomTokenizedNode builds a real Node with randomized properties/temporal so
// property-key tokenization is genuinely exercised (empty and multi-key nodes).
func randomTokenizedNode(t *testing.T, rng *rand.Rand) *types.Node {
	t.Helper()
	extras := make([]uint16, 0, 2)
	for j := 0; j < rng.Intn(3); j++ {
		extras = append(extras, uint16(rng.Intn(60000)+2))
	}
	n := types.NewNode(types.NodeID(snowflake.ID(rng.Int63n(1<<40)+1)), uint16(rng.Intn(60000)+1), extras)
	n.SetVersion(uint32(rng.Intn(1 << 10)))
	if props := randomTokenizedProps(rng); len(props) > 0 {
		n.SetProperties(mustPropertySlice(t, props))
	}
	// Temporal with a ZERO transaction-time tail (TxFrom/TxTo stamped by apply).
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: types.Instant(rng.Intn(1000)),
		CreatedAt: types.Instant(rng.Intn(1 << 30)),
		CreatedBy: unicodeSamples[rng.Intn(len(unicodeSamples))],
	})
	n.SetIntegrity(&types.NodeIntegrity{Hash: unicodeSamples[rng.Intn(len(unicodeSamples))]})
	return n
}

func randomTokenizedRel(t *testing.T, rng *rand.Rand) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(
		types.RelID(snowflake.ID(rng.Int63n(1<<40)+1)),
		uint16(rng.Intn(60000)+1),
		types.NodeID(snowflake.ID(rng.Int63n(1<<40)+1)),
		types.NodeID(snowflake.ID(rng.Int63n(1<<40)+1)),
	)
	r.SetVersion(uint32(rng.Intn(1 << 10)))
	if props := randomTokenizedProps(rng); len(props) > 0 {
		r.SetProperties(mustPropertySlice(t, props))
	}
	r.SetTemporal(&types.TemporalMetadata{
		ValidFrom: types.Instant(rng.Intn(1000)),
		CreatedAt: types.Instant(rng.Intn(1 << 30)),
	})
	r.SetIntegrity(&types.RelIntegrity{Hash: unicodeSamples[rng.Intn(len(unicodeSamples))]})
	return r
}

func randomTokenizedProps(rng *rand.Rand) map[string]any {
	n := rng.Intn(4)
	if n == 0 {
		return nil
	}
	out := make(map[string]any, n)
	for j := 0; j < n; j++ {
		out[propKeys[rng.Intn(len(propKeys))]] = int64(rng.Intn(1000))
	}
	return out
}
