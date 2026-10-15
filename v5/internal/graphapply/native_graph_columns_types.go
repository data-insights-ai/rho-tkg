package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// This private V2 comparison layout is a derived retain-all view of the fixed
// node/life-bound-relationship/i64-p slice. It is not the V3 engine format.
const nativeColumnLayout = "native-component-i64-change-columns-v1"
const nativeColumnMetadataBytes = 16 << 10

var (
	errNativeColumnIncomplete = errors.New("native-column: incomplete generation")
	errNativeColumnChanged    = errors.New("native-column: publication source is not quiescent")
)

// All allowances are explicit embedding policy. Retained bytes include keys,
// chunks and reserved mutable metadata; cache/memtable targets are not RSS or
// filesystem/compaction ceilings. Work is conservative prepaid representation
// accounting; failed source attempts never regain their grant.
type nativeColumnPolicy struct {
	ChunkRecords, ChunkRows, ChunkBytes, FrameBytes              int
	MaxGenerations, MaxChunks, MaxRetainedBytes                  uint64
	MaxWorkRecords, MaxWorkBytes, MaxDecodedRows, MaxOutputBytes uint64
	MaxRPCs, MaxWireBytes                                        uint64
	CacheBytes                                                   int64
	MemTableBytes                                                uint64
	Lifetime                                                     time.Duration
	Version                                                      uint8    `json:",omitzero"` // fresh aggregate-row allowance; legacy zero bytes/recipe unchanged
	Admission                                                    [32]byte `json:",omitzero"` // fresh immutable workload/budget identity; no source authority
}

func (p nativeColumnPolicy) validate() error {
	if p.Version > 1 || p.Version == 0 && p.Admission != ([32]byte{}) {
		return errInvalid
	}
	if p.ChunkRecords < 1 || p.ChunkRecords > 16 || p.ChunkRows < 1 || p.ChunkRows > 32 || p.ChunkBytes < 1024 || p.ChunkBytes > 64<<10 || p.FrameBytes < 256 || p.FrameBytes > 64<<10 || p.MaxGenerations < 1 || p.MaxGenerations > 1024 || p.MaxChunks < 1 || p.MaxChunks > 1<<40 || p.MaxRetainedBytes < 4*nativeColumnMetadataBytes || p.MaxRetainedBytes > 1<<62 || p.MaxWorkRecords < 1 || p.MaxWorkBytes < uint64(p.ChunkBytes) || p.MaxDecodedRows < 1 || p.MaxOutputBytes < uint64(p.ChunkBytes) || p.CacheBytes < 64<<10 || p.CacheBytes > 1<<30 || p.MemTableBytes < 64<<10 || p.MemTableBytes > 64<<20 || p.Lifetime <= 0 || p.Lifetime > time.Minute {
		return errInvalid
	}
	return nil
}

type nativeColumnBinding struct {
	TerminalAttempts uint8 `json:",omitzero"` // explicit v2 finite terminal policy; v1 bytes unchanged
	Version          uint8
	Layout           string
	Agreement        [32]byte
	Coverage         byte      // Bit0/bit1 are explicit local-prefix coverage, never omitted-source EOF.
	Peers            [2][]byte // Complete actual authenticated scope and configured voter association.
	Configurations   [2][]byte // Complete actual graph genesis, schema and Q/ms axis.
	Policy           nativeColumnPolicy
}

type nativeColumnBoundary struct {
	Initial, Final []byte // Actual authenticated persisted certificates.
	Generation     uint64
}
type nativeColumnGeneration struct {
	ID       [16]byte
	Coverage byte
	Binding  [32]byte
	Sources  [2]nativeColumnBoundary
	Pin      [32]byte `json:",omitzero"` // v2 exact durable original; absent in every v1 encoding.
}
type nativeColumnCell struct {
	Present, Null        bool
	Handle               uint64
	Value                int64
	Revision, Provenance uint64
}
type nativeColumnRow struct {
	Owner, Life, Source, Target, SourceLife, TargetLife uint64
	Component, Kind, Mode, Interpretation, Role         byte
	Name, Type                                          string
	Axis                                                []byte // Complete exact native axis descriptor, not a guessed default.
	Scope                                               []byte // Exact native codec, including rational boundaries/axis identity.
	Before, After                                       nativeColumnCell
}
type nativeColumnFrame struct {
	Partition, Index             uint64
	Root                         [172]byte
	Outcome, CDC                 []byte // Genuine present frames; present-empty remains nonnil.
	Registration                 []byte // Genuine immutable cross manifest, including original request.
	OriginalOutcome, OriginalCDC []byte // Bound earlier native originals for a genuine replay entry.
	Rows                         []nativeColumnRow
}
type nativeColumnWork struct{ Records, Bytes, DecodedRows, OutputBytes, RPCs, WireBytes uint64 }
type nativeColumnProgress struct {
	Generation                nativeColumnGeneration
	Owner                     [16]byte // Last genuine in-process source capture charged to this generation.
	Next, Frames, Rows, Bytes uint64
	Positions                 [2]uint64
	Chain                     [32]byte
	Work                      nativeColumnWork
	EOF                       bool
}
type nativeColumnManifest struct {
	Generation                  nativeColumnGeneration
	Chunks, Frames, Rows, Bytes uint64
	Positions                   [2]uint64
	Chain                       [32]byte
}
type nativeColumnLedger struct{ Bytes, Chunks, Generations uint64 }

func nativeColumnAdd(a, b uint64) bool { return b <= ^uint64(0)-a }
func (w nativeColumnWork) admit(grant nativeColumnWork, p nativeColumnPolicy) bool {
	return w.Records <= p.MaxWorkRecords && grant.Records <= p.MaxWorkRecords-w.Records && w.Bytes <= p.MaxWorkBytes && grant.Bytes <= p.MaxWorkBytes-w.Bytes && w.DecodedRows <= p.MaxDecodedRows && grant.DecodedRows <= p.MaxDecodedRows-w.DecodedRows && w.OutputBytes <= p.MaxOutputBytes && grant.OutputBytes <= p.MaxOutputBytes-w.OutputBytes && w.RPCs <= p.MaxRPCs && grant.RPCs <= p.MaxRPCs-w.RPCs && w.WireBytes <= p.MaxWireBytes && grant.WireBytes <= p.MaxWireBytes-w.WireBytes
}
func (w nativeColumnWork) add(g nativeColumnWork) nativeColumnWork {
	return nativeColumnWork{Records: w.Records + g.Records, Bytes: w.Bytes + g.Bytes, DecodedRows: w.DecodedRows + g.DecodedRows, OutputBytes: w.OutputBytes + g.OutputBytes, RPCs: w.RPCs + g.RPCs, WireBytes: w.WireBytes + g.WireBytes}
}
func nativeColumnManifestFor(p nativeColumnProgress) nativeColumnManifest {
	return nativeColumnManifest{p.Generation, p.Next, p.Frames, p.Rows, p.Bytes, p.Positions, p.Chain}
}

// JSON is used only for small sealed metadata, never as the column data layout.
func nativeColumnMetadata(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b)+32 > nativeColumnMetadataBytes {
		return nil, errLimit
	}
	return seal(b), nil
}
func nativeColumnDecodeMetadata(b []byte, out any) error {
	if len(b) < 32 || len(b) > nativeColumnMetadataBytes {
		return errCorrupt
	}
	sum := sha256.Sum256(b[:len(b)-32])
	if !bytes.Equal(sum[:], b[len(b)-32:]) {
		return errCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(b[:len(b)-32]))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.Join(errCorrupt, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.Join(errCorrupt, err)
	}
	canonical, err := nativeColumnMetadata(out)
	if err != nil || !bytes.Equal(canonical, b) {
		return errors.Join(errCorrupt, err)
	}
	return nil
}
func nativeColumnSame(a, b any) bool {
	x, e1 := nativeColumnMetadata(a)
	y, e2 := nativeColumnMetadata(b)
	return e1 == nil && e2 == nil && bytes.Equal(x, y)
}
func nativeColumnGenerationHash(g nativeColumnGeneration) ([32]byte, error) {
	b, err := nativeColumnMetadata(g)
	return sha256.Sum256(b), err
}
func nativeColumnChain(previous [32]byte, body []byte) [32]byte {
	digest := sha256.Sum256(body)
	var joined [64]byte
	copy(joined[:32], previous[:])
	copy(joined[32:], digest[:])
	return sha256.Sum256(joined[:])
}

// Accounting quotas are finite uint64 counters, independent of actual owned
// bytes/RSS. All derived whole-phase products and capture sums are checked before
// effects. Per-chunk allocations still use the small shape limits above.
func (w nativeColumnWork) scaled(n uint64) (nativeColumnWork, error) {
	values := [6]uint64{w.Records, w.Bytes, w.DecodedRows, w.OutputBytes, w.RPCs, w.WireBytes}
	for i, v := range values {
		if n != 0 && v > ^uint64(0)/n {
			return nativeColumnWork{}, errLimit
		}
		values[i] = v * n
	}
	return nativeColumnWork{Records: values[0], Bytes: values[1], DecodedRows: values[2], OutputBytes: values[3], RPCs: values[4], WireBytes: values[5]}, nil
}
func (w nativeColumnWork) plus(g nativeColumnWork) (nativeColumnWork, error) {
	if !nativeColumnAdd(w.Records, g.Records) || !nativeColumnAdd(w.Bytes, g.Bytes) || !nativeColumnAdd(w.DecodedRows, g.DecodedRows) || !nativeColumnAdd(w.OutputBytes, g.OutputBytes) || !nativeColumnAdd(w.RPCs, g.RPCs) || !nativeColumnAdd(w.WireBytes, g.WireBytes) {
		return nativeColumnWork{}, errLimit
	}
	return w.add(g), nil
}

// Structural joining of two receipts is not new source authority. Callers must
// first obtain BOTH genuine sealed EOFs through their actual native publishers.
func nativeColumnJoinBoundaries(bindings [2]nativeColumnBinding, manifests [2]nativeColumnManifest) error {
	for g := range 2 {
		if bindings[g].Coverage != 1<<g || manifests[g].Generation.Coverage != 1<<g || manifests[g].Generation.ID == ([16]byte{}) {
			return errInvalid
		}
		body, err := nativeColumnMetadata(bindings[g])
		if err != nil || sha256.Sum256(body) != manifests[g].Generation.Binding {
			return errors.Join(errCorrupt, err)
		}
		if manifests[g].Frames > 0 && (manifests[g].Chunks == 0 || manifests[g].Chain == ([32]byte{})) {
			return errCorrupt
		}
		for n, boundary := range manifests[g].Generation.Sources {
			first, err := decodeGraphCertificate(boundary.Initial)
			if err != nil {
				return err
			}
			final, err := decodeGraphCertificate(boundary.Final)
			if err != nil || first.scope != final.scope || first.configuration != final.configuration || boundary.Generation == 0 || first.sourceIndex > final.sourceIndex {
				return errors.Join(errCorrupt, err)
			}
			expected := first.sourceIndex
			if n == g {
				expected = final.sourceIndex
			}
			if manifests[g].Positions[n] != expected {
				return errCorrupt
			}
			if n == g && manifests[g].Frames != final.sourceIndex-first.sourceIndex {
				return errCorrupt
			}
		}

		m, p := manifests[g], bindings[g].Policy
		if p.validate() != nil || m.Chunks > p.MaxChunks || m.Chunks > m.Frames || m.Frames > p.MaxChunks*uint64(p.ChunkRecords) || m.Rows > m.Frames*uint64(p.ChunkRows) || m.Bytes > p.MaxRetainedBytes || m.Frames == 0 && (m.Chunks != 0 || m.Rows != 0 || m.Bytes != 0 || m.Chain != ([32]byte{})) {
			return errCorrupt
		}
		bindings[g].Coverage = 3
	}
	if !nativeColumnSame(bindings[0], bindings[1]) || manifests[0].Generation.ID != manifests[1].Generation.ID || !nativeColumnSame(manifests[0].Generation.Sources, manifests[1].Generation.Sources) {
		return errInvalid
	}
	return nil
}
