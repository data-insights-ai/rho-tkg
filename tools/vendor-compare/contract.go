package vendorcompare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ErrContract identifies malformed input or a mismatched trusted reference pin.
var ErrContract = errors.New("comparison: invalid contract or source pin")

// ErrLimit refuses an answer that exceeds a complete-output or callback cap.
var ErrLimit = errors.New("comparison: complete output/work limit")

// ErrUnsupported identifies an unsupported lane or native capability.
var ErrUnsupported = errors.New("comparison: unsupported lane/capability")

const referencePinSHA = "771f4854792d49fd2619327a2c1544196be62c91d18ea5d75a25a1b591e6b7f5"

// Cell preserves a common scalar type and its exact JSON value or binary64 bits.
type Cell struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value,omitempty"`
	Bits  string          `json:"bits,omitempty"`
}

// Entity is a complete external node or directed relationship row.
type Entity struct {
	Kind       string          `json:"kind"`
	ID         string          `json:"id"`
	Labels     []string        `json:"labels,omitempty"`
	Properties map[string]Cell `json:"properties"`
	Source     string          `json:"source,omitempty"`
	Target     string          `json:"target,omitempty"`
	Type       string          `json:"type,omitempty"`
}

// Event declares one ordered current-graph mutation stage.
type Event struct {
	Revision int             `json:"revision"`
	Op       string          `json:"op"`
	Row      Entity          `json:"row,omitzero"`
	Kind     string          `json:"kind,omitempty"`
	ID       string          `json:"id,omitempty"`
	Set      map[string]Cell `json:"set,omitempty"`
	Remove   []string        `json:"remove,omitempty"`
}

// Request selects one supported complete current answer.
type Request struct {
	Op        string   `json:"op"`
	Kind      string   `json:"kind,omitempty"`
	ID        string   `json:"id,omitempty"`
	Label     string   `json:"label,omitempty"`
	Type      string   `json:"type,omitempty"`
	Key       string   `json:"key,omitempty"`
	Value     Cell     `json:"value,omitzero"`
	Low       Cell     `json:"low,omitzero"`
	High      Cell     `json:"high,omitzero"`
	Keys      []string `json:"keys,omitempty"`
	Direction string   `json:"direction,omitempty"`
	MaxDepth  int      `json:"max_depth,omitzero"`
}

// Query binds a request to the pinned lane, revision and expected answer.
type Query struct {
	ID           string   `json:"id"`
	Revision     int      `json:"revision"`
	Lane         string   `json:"lane"`
	Request      Request  `json:"request"`
	Shape        string   `json:"shape"`
	ExpectedFile string   `json:"expected_file"`
	Expected     FileInfo `json:"expected"`
}

// FileInfo records the literal expected answer row count and file digest.
type FileInfo struct {
	Bytes int64  `json:"bytes"`
	SHA   string `json:"sha256"`
	Rows  int64  `json:"rows,omitzero"`
}

// Limits cap one complete answer and its delivered adapter callbacks.
type Limits struct{ MaxRows, MaxBytes, MaxVisited int }

func defaultLimits() Limits { return Limits{5_000_000, 64 << 20, 5_000_000} }
func (l Limits) valid() bool {
	return l.MaxRows > 0 && l.MaxBytes > 0 && l.MaxVisited > 0 && l.MaxRows <= 5_000_000 && l.MaxBytes <= 1<<30 && l.MaxVisited <= 10_000_000
}

// Walk distinguishes a directed walk by its complete edge identity sequence.
type Walk struct {
	Start   string   `json:"start"`
	Depth   int      `json:"depth"`
	EdgeIDs []string `json:"edge_ids"`
	NodeIDs []string `json:"node_ids"`
}

// Column preserves explicit property presence and its exact typed value.
type Column struct {
	Presence string `json:"presence"`
	Value    *Cell  `json:"value,omitempty"`
}

// Projection is a complete entity projection with declared column presence.
type Projection struct {
	Kind    string            `json:"kind"`
	ID      string            `json:"id"`
	Columns map[string]Column `json:"columns"`
}

// IDRow is an external entity identity.
type IDRow struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Adjacency preserves relationship and neighbor identity separately.
type Adjacency struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

func strictJSON(b []byte, dst any) error {
	if len(b) > 1<<20 || !utf8.Valid(b) {
		return ErrContract
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := walkJSON(d, 0); err != nil {
		return errors.Join(ErrContract, err)
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return ErrContract
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(dst); err != nil {
		return errors.Join(ErrContract, err)
	}
	return nil
}
func walkJSON(d *json.Decoder, depth int) error {
	if depth > 16 {
		return ErrContract
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := k.(string)
				if !ok || seen[key] || len(seen) >= 512 {
					return ErrContract
				}
				seen[key] = true
				if err := walkJSON(d, depth+1); err != nil {
					return err
				}
			}
		case '[':
			n := 0
			for d.More() {
				n++
				if n > 4096 {
					return ErrContract
				}
				if err := walkJSON(d, depth+1); err != nil {
					return err
				}
			}
		default:
			return ErrContract
		}
		_, err = d.Token()
	}
	return err
}

// Native converts the exact common scalar envelope without float-mediated integers.
func (c Cell) Native() (any, error) {
	switch c.Type {
	case "i64":
		if c.Bits != "" || len(c.Value) == 0 {
			return nil, ErrContract
		}
		s := string(c.Value)
		if c.Value[0] == '"' {
			if err := json.Unmarshal(c.Value, &s); err != nil {
				return nil, ErrContract
			}
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || strconv.FormatInt(n, 10) != s {
			return nil, ErrContract
		}
		return n, nil
	case "bool":
		if c.Bits != "" {
			return nil, ErrContract
		}
		if bytes.Equal(c.Value, []byte("true")) {
			return true, nil
		}
		if bytes.Equal(c.Value, []byte("false")) {
			return false, nil
		}
		return nil, ErrContract
	case "text":
		if c.Bits != "" || len(c.Value) == 0 || bytes.Equal(c.Value, []byte("null")) {
			return nil, ErrContract
		}
		var s string
		if err := json.Unmarshal(c.Value, &s); err != nil || !utf8.ValidString(s) {
			return nil, ErrContract
		}
		return s, nil
	case "f64":
		if len(c.Value) != 0 || len(c.Bits) != 16 {
			return nil, ErrContract
		}
		for _, b := range c.Bits {
			if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
				return nil, ErrContract
			}
		}
		bits, err := strconv.ParseUint(c.Bits, 16, 64)
		f := math.Float64frombits(bits)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, ErrContract
		}
		return f, nil
	default:
		return nil, ErrContract
	}
}
func cellFromNative(v any) (Cell, error) {
	switch n := v.(type) {
	case int64:
		b, _ := json.Marshal(strconv.FormatInt(n, 10))
		return Cell{Type: "i64", Value: b}, nil
	case bool:
		b, _ := json.Marshal(n)
		return Cell{Type: "bool", Value: b}, nil
	case string:
		if !utf8.ValidString(n) {
			return Cell{}, ErrContract
		}
		b, _ := json.Marshal(n)
		return Cell{Type: "text", Value: b}, nil
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return Cell{}, ErrContract
		}
		if n == 0 {
			n = 0
		}
		return Cell{Type: "f64", Bits: fmtHex(math.Float64bits(n))}, nil
	default:
		return Cell{}, ErrContract
	}
}
func fmtHex(n uint64) string { return fmt.Sprintf("%016x", n) }
func validateRequest(r Request) error {
	if r.Op == "lookup" {
		_, err := nativeID(r.ID, r.Kind)
		return err
	}
	if r.Op == "adjacency" || r.Op == "expand" {
		if _, err := nativeID(r.ID, "node"); err != nil {
			return err
		}
		if r.Op == "adjacency" && r.Direction != "in" && r.Direction != "out" {
			return ErrContract
		}
		if r.Op == "expand" && (r.MaxDepth < 1 || r.MaxDepth > 4) {
			return ErrContract
		}
		return nil
	}
	if r.Kind != "node" && r.Kind != "edge" {
		return ErrContract
	}
	switch r.Op {
	case "scan":
		return nil
	case "label":
		if r.Label == "" || len(r.Label) > 256 {
			return ErrContract
		}
		return nil
	case "type":
		if r.Type == "" || len(r.Type) > 256 {
			return ErrContract
		}
		return nil
	case "projection":
		if len(r.Keys) < 1 || len(r.Keys) > 64 {
			return ErrContract
		}
		seen := make(map[string]bool, len(r.Keys))
		for _, k := range r.Keys {
			if k == "" || len(k) > 256 || seen[k] {
				return ErrContract
			}
			seen[k] = true
		}
		return nil
	case "equality":
		if r.Key == "" || len(r.Key) > 256 {
			return ErrContract
		}
		_, err := r.Value.Native()
		return err
	case "range":
		if r.Key == "" || len(r.Key) > 256 || r.Low.Type != r.High.Type {
			return ErrContract
		}
		lo, err := r.Low.Native()
		if err != nil {
			return err
		}
		hi, err := r.High.Native()
		if err != nil {
			return err
		}
		switch l := lo.(type) {
		case int64:
			h, ok := hi.(int64)
			if !ok || l > h {
				return ErrContract
			}
		case float64:
			h, ok := hi.(float64)
			if !ok || l > h {
				return ErrContract
			}
		default:
			return ErrUnsupported
		}
		return nil
	default:
		return ErrUnsupported
	}
}
