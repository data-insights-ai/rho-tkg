package storeutil

// Phase 0.9 — B3 on-disk measurement gate (measurement-only; nothing here ships).
//
// B3 proposes delta-encoding the five mid-map timestamps (vf/vt/ca/ua/da)
// against the entity's snowflake creation instant (recoverable from the stored
// id at zero extra bytes) instead of the current forced 9-byte int64
// (EncodeInt64 / 0xd3). The plan gates the whole B3 build on this: proceed only
// if the delta form saves >= ~10% on DISK after Snappy on realistic data.
//
// Methodological care (why per-row Snappy would lie): Badger compresses SSTable
// *blocks* (default 4KB, s2.EncodeSnappy — see badger/table/builder.go), NOT
// individual values. Cross-row timestamp redundancy (many rows sharing ca/ua)
// is exactly what block-Snappy already captures, so a per-row measurement would
// overstate B3's win. This test concatenates rows into 4KB blocks and compresses
// each block, mirroring Badger's real behavior.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/msgpack/v5"
)

// deterministic LCG — Date.now()/rand are undesirable in a repeatable gate.
type gateRNG struct{ s uint64 }

func (r *gateRNG) next() uint64 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return r.s >> 11
}
func (r *gateRNG) intn(n int) int    { return int(r.next() % uint64(n)) }
func (r *gateRNG) i64(n int64) int64 { return int64(r.next() % uint64(n)) }

func hexHash(r *gateRNG) string {
	const hexdig = "0123456789abcdef"
	b := make([]byte, 64) // SHA-256 hex = 64 chars, high entropy
	for i := range b {
		b[i] = hexdig[r.intn(16)]
	}
	return string(b)
}

// buildGateCorpus returns n realistic v2 NodeWire rows plus each row's
// snowflake-derived base millis (for the v3 delta encode). Rows model the shapes
// the user named: customer / product / contact / SIEM-event — 5..20 properties,
// some values large and shared across rows, hashes on every row.
func buildGateCorpus(t *testing.T, n int) ([]NodeWire, []int64) {
	t.Helper()
	node, err := snowflake.NewNode(3,
		snowflake.WithEpoch(snowflakepkg.Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// A few large, shared free-text blobs — the "unchanged huge value" case.
	sharedNotes := []string{
		"Enterprise customer onboarded via partner channel; contract renewed 2026, " +
			"net-90 terms, primary contact prefers email, escalation path documented in CRM.",
		"SIEM correlation: repeated failed auth from egress range, geo-velocity flag, " +
			"tied to service account rotation window; analyst triaged as benign misconfig.",
		"Product SKU discontinued in EU, replacement mapped, inventory drained to zero, " +
			"support articles updated, warranty honored through end of fiscal year.",
	}
	regions := []string{"eu-west", "us-east", "ap-south", "sa-east"}
	tiers := []string{"gold", "silver", "bronze", "platinum"}

	rng := &gateRNG{s: 0x9E3779B97F4A7C15}
	rows := make([]NodeWire, n)
	bases := make([]int64, n)
	for i := 0; i < n; i++ {
		id := int64(node.Generate())
		base := snowflakepkg.DecomposeID(snowflake.ID(id)).CreatedAt.UnixMilli()
		bases[i] = base

		// Timestamps clustered near the creation instant (the realistic case B3
		// targets), with a minority of backdated valid-from (negative delta) and
		// updated rows (ua > ca).
		ca := base
		ua := base
		if rng.intn(100) < 40 { // 40% updated after creation
			ua = base + rng.i64(30*24*3600*1000) // up to 30 days later
		}
		var vf int64 = base
		switch rng.intn(10) {
		case 0, 1: // backdated valid-from (domain knowledge earlier than record)
			vf = base - rng.i64(365*24*3600*1000) // up to a year earlier
		case 2: // no valid-from claim
			vf = 0
		}
		var vt int64
		if rng.intn(100) < 15 { // 15% closed interval
			vt = ua + rng.i64(90*24*3600*1000)
		}
		var da int64 // deletions rare in a live store; leave 0

		propCount := 5 + rng.intn(16) // 5..20
		props := make([]PropertyWire, 0, propCount)
		props = append(props,
			PropertyWire{Key: "id", Value: int64(i), Type: 2},
			PropertyWire{Key: "name", Value: "Entity " + hexHash(rng)[:8], Type: 6},
			PropertyWire{Key: "region", Value: regions[rng.intn(len(regions))], Type: 6},
			PropertyWire{Key: "tier", Value: tiers[rng.intn(len(tiers))], Type: 6},
			PropertyWire{Key: "score", Value: float64(rng.intn(10000)) / 100.0, Type: 5},
		)
		if propCount > 5 {
			props = append(props, PropertyWire{Key: "notes", Value: sharedNotes[rng.intn(len(sharedNotes))], Type: 6})
		}
		for p := len(props); p < propCount; p++ {
			switch p % 4 {
			case 0:
				props = append(props, PropertyWire{Key: "attr" + string(rune('a'+p)), Value: int64(rng.intn(1_000_000)), Type: 2})
			case 1:
				props = append(props, PropertyWire{Key: "attr" + string(rune('a'+p)), Value: rng.intn(2) == 0, Type: 1})
			case 2:
				props = append(props, PropertyWire{Key: "attr" + string(rune('a'+p)), Value: "val-" + hexHash(rng)[:12], Type: 6})
			default:
				props = append(props, PropertyWire{Key: "attr" + string(rune('a'+p)), Value: float64(rng.intn(100000)) / 10.0, Type: 5})
			}
		}

		rows[i] = NodeWire{
			FormatVersion: 2,
			ID:            id,
			PrimaryLabel:  1 + rng.intn(6),
			Version:       rng.intn(4),
			HasTemporal:   vf != base || vt != 0,
			ValidFrom:     vf,
			ValidTo:       vt,
			TxFrom:        base + rng.i64(5000), // recording latency
			TxTo:          0,
			CreatedAt:     ca,
			UpdatedAt:     ua,
			DeletedAt:     da,
			CreatedBy:     "svc-ingest",
			Hash:          hexHash(rng),
			PrevHash:      hexHash(rng),
			Properties:    props,
		}
	}
	return rows, bases
}

// encodeNodeWireV3Proto is the MEASUREMENT-ONLY B3 prototype: byte-for-byte the
// v2 encoder EXCEPT the five mid-map timestamps vf/vt/ca/ua/da are emitted as
// native signed-int deltas from base (EncodeInt) instead of forced int64
// (EncodeInt64). id and the tf/tt trailing tail STAY EncodeInt64 (the tail-patch
// invariant). Field set, ordering, and map length are identical to v2 — only the
// integer WIDTH of those five values changes. Not wired into any store.
func encodeNodeWireV3Proto(w NodeWire, base int64) ([]byte, error) {
	var buf []byte
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)

	// Reuse the production field-count logic by encoding v2 first is not possible
	// here (widths differ), so recompute the map length exactly as EncodeMsgpack.
	fields := 4 // fv, id, pl, v  (fv present since FormatVersion=2)
	if len(w.ExtraLabels) > 0 {
		fields++
	}
	if len(w.Properties) > 0 {
		fields++
	}
	if w.HasTemporal {
		fields++
	}
	if w.ValidFrom != 0 {
		fields++
	}
	if w.ValidTo != 0 {
		fields++
	}
	fields += 2 // tf, tt fixed tail
	if w.CreatedAt != 0 {
		fields++
	}
	if w.UpdatedAt != 0 {
		fields++
	}
	if w.DeletedAt != 0 {
		fields++
	}
	if w.CreatedBy != "" {
		fields++
	}
	if w.UpdatedBy != "" {
		fields++
	}
	if w.BaseEntityID != 0 {
		fields++
	}
	if w.Hash != "" {
		fields++
	}
	if w.PrevHash != "" {
		fields++
	}
	if w.AuthorID != "" {
		fields++
	}
	if len(w.Signature) > 0 {
		fields++
	}
	if w.AuthorizedBy != "" {
		fields++
	}
	if w.AuthorizationLevel != 0 {
		fields++
	}

	b := &sliceWriter{}
	enc.Reset(b)
	deltaField := func(key string, v int64) error {
		if err := enc.EncodeString(key); err != nil {
			return err
		}
		return enc.EncodeInt(v - base) // B3: signed delta, variable width
	}

	if err := enc.EncodeMapLen(fields); err != nil {
		return nil, err
	}
	_ = encodeStringUint8Field(enc, "fv", w.FormatVersion)
	_ = encodeStringInt64Field(enc, "id", w.ID)
	_ = encodeStringIntField(enc, "pl", w.PrimaryLabel)
	if len(w.ExtraLabels) > 0 {
		_ = encodeStringAnyField(enc, "el", w.ExtraLabels)
	}
	if len(w.Properties) > 0 {
		_ = encodePropertyArray(enc, "p", w.Properties)
	}
	_ = encodeStringIntField(enc, "v", w.Version)
	if w.HasTemporal {
		_ = encodeStringBoolField(enc, "ht", w.HasTemporal)
	}
	if w.ValidFrom != 0 {
		_ = deltaField("vf", w.ValidFrom)
	}
	if w.ValidTo != 0 {
		_ = deltaField("vt", w.ValidTo)
	}
	if w.CreatedAt != 0 {
		_ = deltaField("ca", w.CreatedAt)
	}
	if w.UpdatedAt != 0 {
		_ = deltaField("ua", w.UpdatedAt)
	}
	if w.DeletedAt != 0 {
		_ = deltaField("da", w.DeletedAt)
	}
	if w.CreatedBy != "" {
		_ = encodeStringStringField(enc, "cb", w.CreatedBy)
	}
	if w.UpdatedBy != "" {
		_ = encodeStringStringField(enc, "ub", w.UpdatedBy)
	}
	if w.BaseEntityID != 0 {
		_ = encodeStringInt64Field(enc, "be", w.BaseEntityID)
	}
	if w.Hash != "" {
		_ = encodeStringStringField(enc, "h", w.Hash)
	}
	if w.PrevHash != "" {
		_ = encodeStringStringField(enc, "ph", w.PrevHash)
	}
	if w.AuthorID != "" {
		_ = encodeStringStringField(enc, "aid", w.AuthorID)
	}
	if len(w.Signature) > 0 {
		_ = encodeStringBytesField(enc, "sig", w.Signature)
	}
	if w.AuthorizedBy != "" {
		_ = encodeStringStringField(enc, "aby", w.AuthorizedBy)
	}
	if w.AuthorizationLevel != 0 {
		_ = encodeStringUint8Field(enc, "al", w.AuthorizationLevel)
	}
	// Fixed trailing tail — stays forced int64.
	_ = encodeStringInt64Field(enc, "tf", w.TxFrom)
	if err := encodeStringInt64Field(enc, "tt", w.TxTo); err != nil {
		return nil, err
	}
	buf = b.b
	return buf, nil
}

// sliceWriter is a trivial io.Writer accumulating into a byte slice.
type sliceWriter struct{ b []byte }

func (w *sliceWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }

// blockSnappyBytes concatenates rows into ~4KB blocks (Badger's default
// BlockSize) and s2.EncodeSnappy-compresses each block, returning the summed
// compressed size — the realistic on-disk value-byte model.
func blockSnappyBytes(rows [][]byte) int {
	const blockSize = 4 * 1024
	total := 0
	var block []byte
	flush := func() {
		if len(block) == 0 {
			return
		}
		total += len(s2.EncodeSnappy(nil, block))
		block = block[:0]
	}
	for _, r := range rows {
		block = append(block, r...)
		if len(block) >= blockSize {
			flush()
		}
	}
	flush()
	return total
}

func TestB3OnDiskSnappyGate(t *testing.T) {
	const n = 4000
	rows, bases := buildGateCorpus(t, n)

	v2raw := make([][]byte, n)
	v3raw := make([][]byte, n)
	var v2rawTotal, v3rawTotal int
	for i := range rows {
		b2, err := msgpack.Marshal(rows[i]) // production v2 bytes (CustomEncoder)
		if err != nil {
			t.Fatalf("marshal v2 row %d: %v", i, err)
		}
		b3, err := encodeNodeWireV3Proto(rows[i], bases[i])
		if err != nil {
			t.Fatalf("encode v3 row %d: %v", i, err)
		}
		v2raw[i], v3raw[i] = b2, b3
		v2rawTotal += len(b2)
		v3rawTotal += len(b3)
	}

	v2snap := blockSnappyBytes(v2raw)
	v3snap := blockSnappyBytes(v3raw)

	rawPct := 100.0 * float64(v2rawTotal-v3rawTotal) / float64(v2rawTotal)
	snapPct := 100.0 * float64(v2snap-v3snap) / float64(v2snap)
	perRowRaw := float64(v2rawTotal-v3rawTotal) / float64(n)

	t.Logf("B3 on-disk gate over %d realistic node rows (5..20 props, hashes, mixed timestamps):", n)
	t.Logf("  RAW wire:        v2=%d B  v3=%d B  saved=%d B (%.2f%%)  ~%.1f B/row",
		v2rawTotal, v3rawTotal, v2rawTotal-v3rawTotal, rawPct, perRowRaw)
	t.Logf("  BLOCK-Snappy 4K: v2=%d B  v3=%d B  saved=%d B (%.2f%%)   <-- the disk gate",
		v2snap, v3snap, v2snap-v3snap, snapPct)
	t.Logf("  Snappy ratio:    v2 %.2fx  v3 %.2fx", float64(v2rawTotal)/float64(v2snap), float64(v3rawTotal)/float64(v3snap))
	t.Logf("  GATE: B3 proceeds only if BLOCK-Snappy saving >= ~10%%. Measured: %.2f%%", snapPct)
}
