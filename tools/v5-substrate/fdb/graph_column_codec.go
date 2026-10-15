package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// A fresh, explicit V2 comparison layout. It preserves the fixed graph slice;
// it is neither a generic graph planner nor the V3 engine format. Numeric
// columns are contiguous and unsigned identities are never converted to int64.
const graphColumnLayout = "scalar-cross-edge-i64-columns-v1"
const graphColumnEdgeUniqueLayout = "effective-unique-edge-life-i64-columns-v1"
const graphColumnHeader = 8 + 32 + 32 + 8 + 32 + 2
const graphColumnNumeric = 11

// Work counts admitted distinct logical rows, encoded chunks and authentic source KV
// bytes read. These explicit refusal policies are not hard SDK/RSS limits, a
// capacity qualification, or a bound on OS/filesystem/replica overhead.
type graphColumnPolicy struct {
	ChunkRows, ChunkBytes                       int
	MaxChunks, MaxGenerations, MaxRetainedBytes uint64
	MaxWorkRows, MaxWorkBytes                   uint64
	Version                                     uint8 `json:",omitzero"`
}

func (p graphColumnPolicy) validate() error {
	if p.Version == 1 || p.Version == 2 {
		if p.ChunkRows < 1 || p.ChunkRows > 32 || p.ChunkBytes < 1024 || p.ChunkBytes > 64<<10 || p.MaxChunks < 1 || p.MaxChunks > 1_000_000 || p.MaxGenerations < 1 || p.MaxGenerations > 1024 || p.MaxRetainedBytes < 8192 || p.MaxRetainedBytes > 1<<40 || p.MaxWorkRows < 1 || p.MaxWorkRows > 3_000_007 || p.MaxWorkBytes < uint64(p.ChunkBytes) || p.MaxWorkBytes > 1<<40 {
			return errInvalid
		}
		return nil
	}
	if p.Version != 0 {
		return errInvalid
	}
	if p.ChunkRows < 1 || p.ChunkRows > 32 || p.ChunkBytes < 1024 || p.ChunkBytes > 64<<10 || p.MaxChunks < 1 || p.MaxChunks > 1_000_000 || p.MaxGenerations < 1 || p.MaxGenerations > 1024 || p.MaxRetainedBytes < 8192 || p.MaxRetainedBytes > 1<<40 || p.MaxWorkRows < 1 || p.MaxWorkRows > 2_000_003 || p.MaxWorkBytes < uint64(p.ChunkBytes) || p.MaxWorkBytes > 1<<40 {
		return errInvalid
	}
	return nil
}

// A policy version cannot opt an ordinary agreement into fresh row semantics.
func graphColumnProfile(h *harness, p graphColumnPolicy) bool {
	return graphFeedRetainedColumnProfile(h, p) || h != nil && p.validate() == nil && validGraphCostProfile(h.config) && (h.config.Cost.Version == 4 && p.Version == 1 || h.config.Cost.Version != 4 && h.config.Cost.Version != 10 && p.Version == 0)
}
func graphColumnMagic(p graphColumnPolicy) string {
	if p.Version == 2 {
		return "RHOCLM03"
	}
	if p.Version == 1 {
		return "RHOCLM02"
	}
	return "RHOCLM01"
}

type graphColumnBinding struct {
	Version        uint8
	Layout, Schema string
	Config         config
	Scope          lifecycleScope
	Policy         graphColumnPolicy
}

type graphColumnGeneration struct {
	Version      uint8
	ID           string
	Binding      [32]byte
	Initial, Cut certificate
}

type graphColumnRow struct {
	Kind                                                        uint8 // 1=node presence, 2=edge create+property, 3=edge property update; fresh only:4=node close,5=edge close.
	Owner, Life, SourceNode, SourceLife, TargetNode, TargetLife uint64
	Value                                                       int64
	ValueID, Provenance, Round, Sequence                        uint64
	Coordinator                                                 uint8
	Original                                                    []byte // Canonical complete genuine transaction/change envelope.
}

func (h *harness) graphColumnRow(original []byte) (graphColumnRow, error) {
	var row graphColumnRow
	if h == nil || !validGraphCostProfile(h.config) || len(original) == 0 {
		return row, errInvalid
	}
	if len(original) > 64<<10 {
		return row, errLimit
	} // Before JSON decode or ownership copy.
	group, err := decode[changeGroup](original)
	if err != nil {
		return row, err
	}
	if err := h.validateChange(group); err != nil {
		return row, err
	}
	m, command, o := group.Tx.Lifecycle, group.Tx.GraphChange, group.Outcome
	if m == nil || command == nil {
		return row, errInvalid
	}
	row = graphColumnRow{Provenance: m.Provenance, Round: o.Round, Sequence: o.Sequence, Coordinator: o.Coordinator, Original: bytes.Clone(original)}
	switch command.Kind {
	case "node-create":
		row.Kind, row.Owner, row.Life, row.Value = 1, command.Node.Node, command.Node.Life, 1
	case "node-close":
		if h.config.Cost.Version != 4 {
			return graphColumnRow{}, errInvalid
		}
		row.Kind, row.Owner, row.Life = 4, command.Node.Node, command.Node.Life
	case "edge-close":
		if h.config.Cost.Version != 4 || m.Definition != nil {
			return graphColumnRow{}, errInvalid
		}
		e := command.Edge
		row.Kind, row.Owner, row.Life = 5, e.ID, m.EdgeLife
		row.SourceNode, row.SourceLife, row.TargetNode, row.TargetLife = e.Source.Node, e.Source.Life, e.Target.Node, e.Target.Life
	case "edge-create", "edge-update":
		if m.Definition == nil || m.Definition.Kind != "i64" {
			return graphColumnRow{}, errInvalid
		}
		row.Kind = 2
		if command.Kind == "edge-update" {
			row.Kind = 3
		}
		e := command.Edge
		row.Owner, row.Life, row.SourceNode, row.SourceLife, row.TargetNode, row.TargetLife = e.ID, m.EdgeLife, e.Source.Node, e.Source.Life, e.Target.Node, e.Target.Life
		row.Value, row.ValueID = e.Value, m.Definition.ID
	default:
		return graphColumnRow{}, errInvalid
	}
	return row, nil
}

func graphColumnNumbers(r graphColumnRow) [graphColumnNumeric]uint64 {
	// Value is a bit-preserving signed column; every other column is unsigned.
	return [graphColumnNumeric]uint64{r.Owner, r.Life, r.SourceNode, r.SourceLife, r.TargetNode, r.TargetLife, uint64(r.Value), r.ValueID, r.Provenance, r.Round, r.Sequence} // #nosec G115 -- exact signed bit representation.
}

func graphColumnSameRow(a, b graphColumnRow) bool {
	return a.Kind == b.Kind && a.Coordinator == b.Coordinator && graphColumnNumbers(a) == graphColumnNumbers(b) && bytes.Equal(a.Original, b.Original)
}

func graphColumnGenerationHash(g graphColumnGeneration) ([32]byte, error) {
	body, err := wire(g)
	return sha256.Sum256(body), err
}

// Original bytes live once per logical group in a bounded arena. Offsets and
// lengths are canonical consecutive spans; no alias survives this owned decode.
func encodeGraphColumnChunk(h *harness, g graphColumnGeneration, ordinal uint64, previous [32]byte, rows []graphColumnRow, p graphColumnPolicy) ([]byte, error) {
	if !graphColumnProfile(h, p) || p.Version == 2 && g.Version != 2 || len(rows) < 1 || len(rows) > p.ChunkRows {
		return nil, errInvalid
	}
	generation, err := graphColumnGenerationHash(g)
	if err != nil {
		return nil, err
	}
	size := graphColumnHeader + len(rows)*(2+8*graphColumnNumeric+8)
	if size > p.ChunkBytes {
		return nil, errLimit
	}
	for _, row := range rows {
		if len(row.Original) > p.ChunkBytes-size {
			return nil, errLimit
		} // Admit before decode/clone.
		actual, err := h.graphColumnRow(row.Original)
		if err != nil || !graphColumnSameRow(actual, row) {
			return nil, errors.Join(errMismatch, err)
		}
		size += len(row.Original)
	}
	if size > p.ChunkBytes {
		return nil, errLimit
	}
	body := make([]byte, size)
	copy(body, graphColumnMagic(p))
	copy(body[8:40], g.Binding[:])
	copy(body[40:72], generation[:])
	binary.BigEndian.PutUint64(body[72:80], ordinal)
	copy(body[80:112], previous[:])
	binary.BigEndian.PutUint16(body[112:114], uint16(len(rows))) // #nosec G115 -- rows<=32.
	n, base := len(rows), graphColumnHeader
	arena := base + n*(2+8*graphColumnNumeric+8)
	offset := 0
	for i, row := range rows {
		body[base+i], body[base+n+i] = row.Kind, row.Coordinator
		for column, v := range graphColumnNumbers(row) {
			binary.BigEndian.PutUint64(body[base+2*n+(column*n+i)*8:], v)
		}
		spans := base + 2*n + 8*graphColumnNumeric*n
		binary.BigEndian.PutUint32(body[spans+i*4:], uint32(offset))                // #nosec G115 -- chunk<=64KiB.
		binary.BigEndian.PutUint32(body[spans+n*4+i*4:], uint32(len(row.Original))) // #nosec G115 -- chunk<=64KiB.
		copy(body[arena+offset:], row.Original)
		offset += len(row.Original)
	}
	return body, nil
}

func decodeGraphColumnChunk(h *harness, g graphColumnGeneration, ordinal uint64, previous [32]byte, body []byte, p graphColumnPolicy) ([]graphColumnRow, error) {
	if !graphColumnProfile(h, p) || len(body) < graphColumnHeader || len(body) > p.ChunkBytes {
		return nil, errInvalid
	}
	generation, err := graphColumnGenerationHash(g)
	if err != nil {
		return nil, err
	}
	if p.Version == 2 && g.Version != 2 || string(body[:8]) != graphColumnMagic(p) || !bytes.Equal(body[8:40], g.Binding[:]) || !bytes.Equal(body[40:72], generation[:]) || binary.BigEndian.Uint64(body[72:80]) != ordinal || !bytes.Equal(body[80:112], previous[:]) {
		return nil, errMismatch
	}
	n := int(binary.BigEndian.Uint16(body[112:114]))
	if n < 1 || n > p.ChunkRows {
		return nil, errInvalid
	}
	base, arena := graphColumnHeader, graphColumnHeader+n*(2+8*graphColumnNumeric+8)
	if arena > len(body) {
		return nil, errInvalid
	}
	out, offset := make([]graphColumnRow, 0, n), 0
	for i := range n {
		var columns [graphColumnNumeric]uint64
		for column := range columns {
			columns[column] = binary.BigEndian.Uint64(body[base+2*n+(column*n+i)*8:])
		}
		spans := base + 2*n + 8*graphColumnNumeric*n
		start := uint64(binary.BigEndian.Uint32(body[spans+i*4:]))
		length := uint64(binary.BigEndian.Uint32(body[spans+n*4+i*4:]))
		if start != uint64(offset) || length == 0 || length > uint64(len(body)-arena-offset) {
			return nil, errInvalid
		}
		// length was admitted against delivered chunk bytes before slicing.
		original := body[arena+offset : arena+offset+int(length)]
		row := graphColumnRow{Kind: body[base+i], Coordinator: body[base+n+i], Owner: columns[0], Life: columns[1], SourceNode: columns[2], SourceLife: columns[3], TargetNode: columns[4], TargetLife: columns[5], Value: int64(columns[6]), ValueID: columns[7], Provenance: columns[8], Round: columns[9], Sequence: columns[10], Original: bytes.Clone(original)} // #nosec G115 -- Value preserves the original signed bit pattern.
		actual, err := h.graphColumnRow(original)
		if err != nil || !graphColumnSameRow(actual, row) {
			return nil, errors.Join(errMismatch, err)
		}
		out = append(out, row)
		offset += int(length)
	}
	if arena+offset != len(body) {
		return nil, errInvalid
	}
	return out, nil
}

func graphColumnChain(previous [32]byte, body []byte) [32]byte {
	digest := sha256.Sum256(body)
	var joined [64]byte
	copy(joined[:32], previous[:])
	copy(joined[32:], digest[:])
	return sha256.Sum256(joined[:])
}
