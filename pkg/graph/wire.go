package graph

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// nodeWire is the msgpack wire format for Node entities.
// All token values are stored as int (maps to msgpack integer).
// Temporal instants are stored as int64 (Unix milliseconds).
type nodeWire struct {
	ID           int64          `msgpack:"id"`
	PrimaryLabel int            `msgpack:"pl"`
	ExtraLabels  []int          `msgpack:"el,omitempty"`
	Properties   []propertyWire `msgpack:"p,omitempty"`
	Version      int            `msgpack:"v"`
	HasTemporal  bool           `msgpack:"ht,omitempty"`
	ValidFrom    int64          `msgpack:"vf,omitempty"`
	ValidTo      int64          `msgpack:"vt,omitempty"`
	TxFrom       int64          `msgpack:"tf,omitempty"`
	TxTo         int64          `msgpack:"tt,omitempty"`
	CreatedAt    int64          `msgpack:"ca,omitempty"`
	UpdatedAt    int64          `msgpack:"ua,omitempty"`
	DeletedAt    int64          `msgpack:"da,omitempty"`
	CreatedBy    string         `msgpack:"cb,omitempty"`
	UpdatedBy    string         `msgpack:"ub,omitempty"`
	BaseEntityID int64          `msgpack:"be,omitempty"`
	Hash         string         `msgpack:"h,omitempty"`
	PrevHash     string         `msgpack:"ph,omitempty"`
}

// relWire is the msgpack wire format for Relationship entities.
type relWire struct {
	ID           int64          `msgpack:"id"`
	RelType      int            `msgpack:"rt"`
	StartID      int64          `msgpack:"s"`
	EndID        int64          `msgpack:"e"`
	Properties   []propertyWire `msgpack:"p,omitempty"`
	Version      int            `msgpack:"v"`
	HasTemporal  bool           `msgpack:"ht,omitempty"`
	ValidFrom    int64          `msgpack:"vf,omitempty"`
	ValidTo      int64          `msgpack:"vt,omitempty"`
	TxFrom       int64          `msgpack:"tf,omitempty"`
	TxTo         int64          `msgpack:"tt,omitempty"`
	CreatedAt    int64          `msgpack:"ca,omitempty"`
	UpdatedAt    int64          `msgpack:"ua,omitempty"`
	DeletedAt    int64          `msgpack:"da,omitempty"`
	CreatedBy    string         `msgpack:"cb,omitempty"`
	UpdatedBy    string         `msgpack:"ub,omitempty"`
	BaseEntityID int64          `msgpack:"be,omitempty"`
	Hash         string         `msgpack:"h,omitempty"`
	PrevHash     string         `msgpack:"ph,omitempty"`
}

// propertyWire is the msgpack wire format for a single property key-value pair.
// Type carries the original Go type tag so exact types survive msgpack round-trips.
type propertyWire struct {
	Key   string `msgpack:"k"`
	Value any    `msgpack:"v"`
	Type  byte   `msgpack:"t"` // property type tag for faithful reconstruction
}

// --- Node conversion ---

// nodeToWire converts a Node to its wire format for serialization.
func nodeToWire(n *types.Node) nodeWire {
	w := nodeWire{
		ID:           int64(n.InternalID().SnowflakeID()),
		PrimaryLabel: int(n.PrimaryLabelToken().Value()),
		Version:      int(n.Version()),
	}

	extras := n.ExtraLabelTokens()
	if len(extras) > 0 {
		w.ExtraLabels = make([]int, len(extras))
		for i, t := range extras {
			w.ExtraLabels[i] = int(t.Value())
		}
	}

	w.Properties = propertiesToWire(n.Properties())

	if tm := n.Temporal(); tm != nil {
		w.HasTemporal = true
		w.ValidFrom = int64(tm.ValidFrom)
		w.ValidTo = int64(tm.ValidTo)
		w.TxFrom = int64(tm.TxFrom)
		w.TxTo = int64(tm.TxTo)
		w.CreatedAt = int64(tm.CreatedAt)
		w.UpdatedAt = int64(tm.UpdatedAt)
		w.DeletedAt = int64(tm.DeletedAt)
		w.CreatedBy = tm.CreatedBy
		w.UpdatedBy = tm.UpdatedBy
		w.BaseEntityID = int64(tm.BaseEntityID().SnowflakeID())
	}

	if ig := n.Integrity(); ig != nil {
		w.Hash = ig.Hash
		w.PrevHash = ig.PrevHash
	}

	return w
}

// wireToNode reconstructs a Node from its wire format.
func wireToNode(w nodeWire) *types.Node {
	var extras []uint16
	for _, e := range w.ExtraLabels {
		extras = append(extras, uint16(e)) // #nosec G115 — token values from our own serialization, always in uint16 range
	}

	n := types.NewNode(snowflake.ID(w.ID), uint16(w.PrimaryLabel), extras) // #nosec G115 — token from our own serialization
	n.SetProperties(wireToProperties(w.Properties))
	n.SetVersion(uint32(w.Version)) // #nosec G115 — version from our own serialization

	if w.HasTemporal {
		tm := &types.TemporalMetadata{
			ValidFrom: types.Instant(w.ValidFrom),
			ValidTo:   types.Instant(w.ValidTo),
			TxFrom:    types.Instant(w.TxFrom),
			TxTo:      types.Instant(w.TxTo),
			CreatedAt: types.Instant(w.CreatedAt),
			UpdatedAt: types.Instant(w.UpdatedAt),
			DeletedAt: types.Instant(w.DeletedAt),
			CreatedBy: w.CreatedBy,
			UpdatedBy: w.UpdatedBy,
		}
		if w.BaseEntityID != 0 {
			tm.SetBaseEntityID(snowflake.ID(w.BaseEntityID))
		}
		n.SetTemporal(tm)
	}

	if w.Hash != "" || w.PrevHash != "" {
		n.SetIntegrity(&types.NodeIntegrity{
			Hash:     w.Hash,
			PrevHash: w.PrevHash,
		})
	}

	return n
}

// --- Relationship conversion ---

// relToWire converts a Relationship to its wire format for serialization.
func relToWire(r *types.Relationship) relWire {
	w := relWire{
		ID:      int64(r.InternalID().SnowflakeID()),
		RelType: int(r.TypeToken().Value()),
		StartID: int64(r.StartNodeID().SnowflakeID()),
		EndID:   int64(r.EndNodeID().SnowflakeID()),
		Version: int(r.Version()),
	}

	w.Properties = propertiesToWire(r.Properties())

	if tm := r.Temporal(); tm != nil {
		w.HasTemporal = true
		w.ValidFrom = int64(tm.ValidFrom)
		w.ValidTo = int64(tm.ValidTo)
		w.TxFrom = int64(tm.TxFrom)
		w.TxTo = int64(tm.TxTo)
		w.CreatedAt = int64(tm.CreatedAt)
		w.UpdatedAt = int64(tm.UpdatedAt)
		w.DeletedAt = int64(tm.DeletedAt)
		w.CreatedBy = tm.CreatedBy
		w.UpdatedBy = tm.UpdatedBy
		w.BaseEntityID = int64(tm.BaseEntityID().SnowflakeID())
	}

	if ig := r.Integrity(); ig != nil {
		w.Hash = ig.Hash
		w.PrevHash = ig.PrevHash
	}

	return w
}

// wireToRel reconstructs a Relationship from its wire format.
func wireToRel(w relWire) *types.Relationship {
	r := types.NewRelationship(
		snowflake.ID(w.ID),
		uint16(w.RelType), // #nosec G115 — token from our own serialization
		snowflake.ID(w.StartID),
		snowflake.ID(w.EndID),
	)
	r.SetProperties(wireToProperties(w.Properties))
	r.SetVersion(uint32(w.Version)) // #nosec G115 — version from our own serialization

	if w.HasTemporal {
		tm := &types.TemporalMetadata{
			ValidFrom: types.Instant(w.ValidFrom),
			ValidTo:   types.Instant(w.ValidTo),
			TxFrom:    types.Instant(w.TxFrom),
			TxTo:      types.Instant(w.TxTo),
			CreatedAt: types.Instant(w.CreatedAt),
			UpdatedAt: types.Instant(w.UpdatedAt),
			DeletedAt: types.Instant(w.DeletedAt),
			CreatedBy: w.CreatedBy,
			UpdatedBy: w.UpdatedBy,
		}
		if w.BaseEntityID != 0 {
			tm.SetBaseEntityID(snowflake.ID(w.BaseEntityID))
		}
		r.SetTemporal(tm)
	}

	if w.Hash != "" || w.PrevHash != "" {
		r.SetIntegrity(&types.RelIntegrity{
			Hash:     w.Hash,
			PrevHash: w.PrevHash,
		})
	}

	return r
}

// --- Property conversion ---

// Property type tags — used to reconstruct exact Go types after msgpack decoding.
// Covers every type accepted by PropertySlice.Set() (deepCopyValue fast-paths).
const (
	ptUnknown    byte = 0 // fallback: normalize integers only
	ptBool       byte = 1
	ptInt        byte = 2
	ptInt8       byte = 3
	ptInt16      byte = 4
	ptInt32      byte = 5
	ptInt64      byte = 6
	ptUint       byte = 7
	ptUint8      byte = 8
	ptUint16     byte = 9
	ptUint32     byte = 10
	ptUint64     byte = 11
	ptFloat32    byte = 12
	ptFloat64    byte = 13
	ptString     byte = 14
	ptSliceStr   byte = 15
	ptSliceInt   byte = 16
	ptSliceInt64 byte = 17
	ptSliceF64   byte = 18
	ptSliceByte  byte = 19
	ptSliceBool  byte = 20
	ptSliceAny   byte = 21
	ptMapStrAny  byte = 22
	ptMapStrStr  byte = 23
	ptSliceF32   byte = 24
)

// propertyTypeTag returns the type tag for a property value.
func propertyTypeTag(v any) byte {
	switch v.(type) {
	case bool:
		return ptBool
	case int:
		return ptInt
	case int8:
		return ptInt8
	case int16:
		return ptInt16
	case int32:
		return ptInt32
	case int64:
		return ptInt64
	case uint:
		return ptUint
	case uint8:
		return ptUint8
	case uint16:
		return ptUint16
	case uint32:
		return ptUint32
	case uint64:
		return ptUint64
	case float32:
		return ptFloat32
	case float64:
		return ptFloat64
	case string:
		return ptString
	case []string:
		return ptSliceStr
	case []int:
		return ptSliceInt
	case []int64:
		return ptSliceInt64
	case []float32:
		return ptSliceF32
	case []float64:
		return ptSliceF64
	case []byte:
		return ptSliceByte
	case []bool:
		return ptSliceBool
	case []any:
		return ptSliceAny
	case map[string]any:
		return ptMapStrAny
	case map[string]string:
		return ptMapStrStr
	default:
		return ptUnknown
	}
}

// reconstructTypedValue reconstructs the exact Go type from a decoded msgpack
// value using the stored type tag. Msgpack destroys type fidelity: []string
// becomes []any, int64 becomes int8 for small values, etc. The type tag
// reverses this loss.
func reconstructTypedValue(v any, tag byte) any {
	if v == nil {
		return nil
	}
	switch tag {
	case ptBool:
		if b, ok := v.(bool); ok {
			return b
		}
	case ptInt:
		return int(toInt64(v))
	case ptInt8:
		return int8(toInt64(v)) // #nosec G115 — original value was int8
	case ptInt16:
		return int16(toInt64(v)) // #nosec G115 — original value was int16
	case ptInt32:
		return int32(toInt64(v)) // #nosec G115 — original value was int32
	case ptInt64:
		return toInt64(v)
	case ptUint:
		return uint(toUint64(v))
	case ptUint8:
		return uint8(toUint64(v)) // #nosec G115 — original value was uint8
	case ptUint16:
		return uint16(toUint64(v)) // #nosec G115 — original value was uint16
	case ptUint32:
		return uint32(toUint64(v)) // #nosec G115 — original value was uint32
	case ptUint64:
		return toUint64(v)
	case ptFloat32:
		if f, ok := v.(float64); ok {
			return float32(f)
		}
		if f, ok := v.(float32); ok {
			return f
		}
	case ptFloat64:
		if f, ok := v.(float64); ok {
			return f
		}
	case ptString:
		if s, ok := v.(string); ok {
			return s
		}
	case ptSliceStr:
		return toStringSlice(v)
	case ptSliceInt:
		return toIntSlice(v)
	case ptSliceInt64:
		return toInt64Slice(v)
	case ptSliceF32:
		return toFloat32SliceWire(v)
	case ptSliceF64:
		return toFloat64Slice(v)
	case ptSliceByte:
		// msgpack encodes []byte as binary — comes back as []byte.
		if b, ok := v.([]byte); ok {
			return b
		}
	case ptSliceBool:
		return toBoolSlice(v)
	case ptSliceAny:
		if s, ok := v.([]any); ok {
			return normalizeIntegersInSlice(s)
		}
	case ptMapStrAny:
		if m, ok := v.(map[string]any); ok {
			return normalizeIntegersInMap(m)
		}
	case ptMapStrStr:
		return toStringStringMap(v)
	}
	// ptUnknown or unmatched: best-effort integer normalization.
	return normalizeIntegersRecursive(v)
}

// --- Integer conversion helpers ---

// toInt64 converts any msgpack-decoded integer to int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n) // #nosec G115 — value fits, came from our serialization
	}
	return 0
}

// toUint64 converts any msgpack-decoded integer to uint64.
func toUint64(v any) uint64 {
	switch n := v.(type) {
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	case int8:
		return uint64(n) // #nosec G115 — value fits, came from our serialization
	case int16:
		return uint64(n) // #nosec G115 — value fits
	case int32:
		return uint64(n) // #nosec G115 — value fits
	case int64:
		return uint64(n) // #nosec G115 — value fits
	case int:
		return uint64(n) // #nosec G115 — value fits
	}
	return 0
}

// --- Slice reconstruction helpers ---

func toStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, len(s))
		for i, e := range s {
			out[i], _ = e.(string)
		}
		return out
	}
	return nil
}

func toIntSlice(v any) []int {
	switch s := v.(type) {
	case []int:
		return s
	case []any:
		out := make([]int, len(s))
		for i, e := range s {
			out[i] = int(toInt64(e))
		}
		return out
	}
	return nil
}

func toInt64Slice(v any) []int64 {
	switch s := v.(type) {
	case []int64:
		return s
	case []any:
		out := make([]int64, len(s))
		for i, e := range s {
			out[i] = toInt64(e)
		}
		return out
	}
	return nil
}

func toFloat64Slice(v any) []float64 {
	switch s := v.(type) {
	case []float64:
		return s
	case []any:
		out := make([]float64, len(s))
		for i, e := range s {
			if f, ok := e.(float64); ok {
				out[i] = f
			}
		}
		return out
	}
	return nil
}

// toFloat32SliceWire reconstructs a []float32 from a msgpack-decoded value.
// Msgpack encodes float32 elements as float32 in a typed slice.
func toFloat32SliceWire(v any) []float32 {
	switch s := v.(type) {
	case []float32:
		return s
	case []any:
		out := make([]float32, len(s))
		for i, e := range s {
			switch f := e.(type) {
			case float32:
				out[i] = f
			case float64:
				out[i] = float32(f)
			}
		}
		return out
	}
	return nil
}

func toBoolSlice(v any) []bool {
	switch s := v.(type) {
	case []bool:
		return s
	case []any:
		out := make([]bool, len(s))
		for i, e := range s {
			out[i], _ = e.(bool)
		}
		return out
	}
	return nil
}

func toStringStringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k], _ = val.(string)
		}
		return out
	}
	return nil
}

// --- Integer normalization ---

// normalizeIntegersRecursive normalizes msgpack-decoded integers:
// int8/int16/int32 → int64, uint8/uint16/uint32 → uint64.
// Recurses into []any and map[string]any containers.
func normalizeIntegersRecursive(v any) any {
	switch val := v.(type) {
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case []any:
		return normalizeIntegersInSlice(val)
	case map[string]any:
		return normalizeIntegersInMap(val)
	default:
		return v
	}
}

func normalizeIntegersInSlice(s []any) []any {
	out := make([]any, len(s))
	for i, e := range s {
		out[i] = normalizeIntegersRecursive(e)
	}
	return out
}

func normalizeIntegersInMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, val := range m {
		out[k] = normalizeIntegersRecursive(val)
	}
	return out
}

// propertiesToWire converts a PropertySlice to wire format.
// Each property's Go type is recorded in the Type tag for faithful reconstruction.
func propertiesToWire(ps types.PropertySlice) []propertyWire {
	if len(ps) == 0 {
		return nil
	}
	pw := make([]propertyWire, len(ps))
	for i, p := range ps {
		pw[i] = propertyWire{Key: p.Key, Value: p.Value, Type: propertyTypeTag(p.Value)}
	}
	return pw
}

// wireToProperties converts wire properties back to a PropertySlice.
// Wire data comes from our own serialization, so values are already validated
// and sorted — build the slice directly without re-validation.
//
// The Type tag drives faithful reconstruction of the original Go type.
// Old data without Type (decoded as 0/ptUnknown) falls through to
// normalizeIntegersRecursive — same behavior as before the type tag was added.
func wireToProperties(pw []propertyWire) types.PropertySlice {
	if len(pw) == 0 {
		return nil
	}
	ps := make(types.PropertySlice, len(pw))
	for i, p := range pw {
		ps[i] = types.Property{Key: p.Key, Value: reconstructTypedValue(p.Value, p.Type)}
	}
	return ps
}
