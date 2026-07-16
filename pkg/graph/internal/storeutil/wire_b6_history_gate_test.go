package storeutil

// Phase 0.9b — B6 anchor+delta history on-disk gate (measurement-only; nothing
// here ships).
//
// B6 proposes storing version-history rows (badger 0x07/0x08) as anchor+delta
// instead of full entity snapshots: a full ANCHOR every ANCHOR_INTERVAL versions,
// and between anchors a DELTA carrying only the properties that changed (plus
// both hashes + temporal, verbatim). The RAW estimate was 40% (5 props) .. 94%
// (20 props / large unchanged blobs) less history storage.
//
// The B3 gate taught the lesson: a RAW estimate need not survive block-Snappy.
// History rows for ONE entity are keyed 0x07/<id>/<version> — CONTIGUOUS in the
// keyspace — so an unchanged blob repeated across versions lands in the same 4KB
// Snappy block and may already compress away. This test measures B6 the same way
// the B3 gate did: full-snapshot v2 vs anchor+delta B6, both block-Snappy'd at
// Badger's 4KB block size in keyspace (per-entity-contiguous) order.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/vmihailenco/msgpack/v5"
)

const b6AnchorInterval = 16

// b6Entity is one entity's version chain for the measurement.
type b6Entity struct {
	id        int64
	base      int64
	versions  int
	baseProps []PropertyWire // full property set at v0
}

// buildB6Corpus produces entities with realistic version depth and a large
// unchanged blob, where each version bumps only 1-2 scalars.
func buildB6Corpus(t *testing.T, entities int) []b6Entity {
	t.Helper()
	node, err := snowflake.NewNode(4,
		snowflake.WithEpoch(snowflakepkg.Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	sharedNotes := []string{
		"Enterprise customer onboarded via partner channel; contract renewed 2026, " +
			"net-90 terms, primary contact prefers email, escalation path documented in CRM.",
		"SIEM correlation: repeated failed auth from egress range, geo-velocity flag, " +
			"tied to service account rotation window; analyst triaged as benign misconfig.",
	}
	rng := &gateRNG{s: 0xD1B54A32D192ED03}
	out := make([]b6Entity, entities)
	for i := range out {
		id := int64(node.Generate())
		base := snowflakepkg.DecomposeID(snowflake.ID(id)).CreatedAt.UnixMilli()
		// Version depth distribution: many short chains, some long ones.
		var v int
		switch rng.intn(10) {
		case 0, 1, 2, 3:
			v = 2 + rng.intn(3) // 2..4
		case 4, 5, 6:
			v = 5 + rng.intn(6) // 5..10
		default:
			v = 12 + rng.intn(20) // 12..31 (crosses anchor boundary)
		}
		propCount := 8 + rng.intn(13) // 8..20 — the wide-entity case
		props := make([]PropertyWire, 0, propCount)
		props = append(props,
			PropertyWire{Key: "id", Value: int64(i), Type: 2},
			PropertyWire{Key: "name", Value: "Entity " + hexHash(rng)[:10], Type: 6},
			PropertyWire{Key: "status", Value: "active", Type: 6}, // the churning scalar
			PropertyWire{Key: "counter", Value: int64(0), Type: 2},
			PropertyWire{Key: "notes", Value: sharedNotes[rng.intn(len(sharedNotes))], Type: 6}, // large, unchanged
		)
		for p := len(props); p < propCount; p++ {
			props = append(props, PropertyWire{Key: "attr" + string(rune('a'+p)), Value: "val-" + hexHash(rng)[:12], Type: 6})
		}
		out[i] = b6Entity{id: id, base: base, versions: v, baseProps: props}
	}
	return out
}

// versionRow builds the v2 FULL snapshot NodeWire for version ver: the churning
// scalars ("status"/"counter") advance, everything else stays identical.
func (e b6Entity) versionRow(ver int) NodeWire {
	props := make([]PropertyWire, len(e.baseProps))
	copy(props, e.baseProps)
	for i := range props {
		switch props[i].Key {
		case "status":
			props[i].Value = []string{"active", "pending", "suspended", "active"}[ver%4]
		case "counter":
			props[i].Value = int64(ver)
		}
	}
	return NodeWire{
		FormatVersion: 2,
		ID:            e.id,
		PrimaryLabel:  2,
		Version:       ver,
		CreatedAt:     e.base,
		UpdatedAt:     e.base + int64(ver)*3600_000,
		Hash:          hexHashVer(e.id, ver),
		PrevHash:      hexHashVer(e.id, ver-1),
		Properties:    props,
	}
}

func hexHashVer(id int64, ver int) string {
	r := &gateRNG{s: uint64(id)*2654435761 + uint64(ver+1)*40503}
	return hexHash(r)
}

// deltaRow builds the B6 DELTA payload for version ver vs ver-1: a 1-byte 'D'
// tag + a NodeWire carrying ONLY the changed properties (status/counter) plus id,
// version, temporal, and both hashes verbatim. Unchanged props (incl. the blob)
// are elided.
func (e b6Entity) deltaRow(ver int) ([]byte, error) {
	changed := []PropertyWire{
		{Key: "status", Value: []string{"active", "pending", "suspended", "active"}[ver%4], Type: 6},
		{Key: "counter", Value: int64(ver), Type: 2},
	}
	w := NodeWire{
		FormatVersion: 2,
		ID:            e.id,
		Version:       ver,
		CreatedAt:     e.base,
		UpdatedAt:     e.base + int64(ver)*3600_000,
		Hash:          hexHashVer(e.id, ver),
		PrevHash:      hexHashVer(e.id, ver-1),
		Properties:    changed,
	}
	b, err := msgpack.Marshal(w)
	if err != nil {
		return nil, err
	}
	return append([]byte{'D'}, b...), nil
}

// anchorRow is the full snapshot with a 1-byte 'A' tag.
func (e b6Entity) anchorRow(ver int) ([]byte, error) {
	b, err := msgpack.Marshal(e.versionRow(ver))
	if err != nil {
		return nil, err
	}
	return append([]byte{'A'}, b...), nil
}

func TestB6HistorySnappyGate(t *testing.T) {
	const entities = 3000
	corpus := buildB6Corpus(t, entities)

	// Rows in keyspace order: all versions of entity 0, then entity 1, ...
	// (matches 0x07/<id>/<version> contiguity, which is what Snappy sees).
	var v2rows, b6rows [][]byte
	var v2rawTotal, b6rawTotal int
	var totalVersions, anchors, deltas int
	for _, e := range corpus {
		for ver := 0; ver < e.versions; ver++ {
			totalVersions++
			// v2: every version is a full snapshot.
			b2, err := msgpack.Marshal(e.versionRow(ver))
			if err != nil {
				t.Fatalf("marshal v2: %v", err)
			}
			v2rows = append(v2rows, b2)
			v2rawTotal += len(b2)

			// B6: anchor at ver%16==0, else delta.
			var b6 []byte
			if ver%b6AnchorInterval == 0 {
				b6, err = e.anchorRow(ver)
				anchors++
			} else {
				b6, err = e.deltaRow(ver)
				deltas++
			}
			if err != nil {
				t.Fatalf("marshal b6: %v", err)
			}
			b6rows = append(b6rows, b6)
			b6rawTotal += len(b6)
		}
	}

	v2snap := blockSnappyBytes(v2rows)
	b6snap := blockSnappyBytes(b6rows)

	rawPct := 100.0 * float64(v2rawTotal-b6rawTotal) / float64(v2rawTotal)
	snapPct := 100.0 * float64(v2snap-b6snap) / float64(v2snap)

	t.Logf("B6 history gate: %d entities, %d version rows (%d anchors + %d deltas), 8..20 props w/ large unchanged blob:",
		entities, totalVersions, anchors, deltas)
	t.Logf("  RAW history:     v2=%d B  b6=%d B  saved=%d B (%.2f%%)", v2rawTotal, b6rawTotal, v2rawTotal-b6rawTotal, rawPct)
	t.Logf("  BLOCK-Snappy 4K: v2=%d B  b6=%d B  saved=%d B (%.2f%%)   <-- the disk gate", v2snap, b6snap, v2snap-b6snap, snapPct)
	t.Logf("  Snappy ratio:    v2 %.2fx  b6 %.2fx", float64(v2rawTotal)/float64(v2snap), float64(b6rawTotal)/float64(b6snap))
	t.Logf("  GATE: B6 proceeds only if BLOCK-Snappy saving >= ~10%%. Measured: %.2f%%", snapPct)
}
