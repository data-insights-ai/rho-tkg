package types

import (
	"errors"
	"math"
	"strconv"
)

// ErrInvalidID reports the zero ID or text that is not the canonical decimal
// form of a positive int64 ID.
var ErrInvalidID = errors.New("types: invalid ID")

// NodeID is an opaque, graph-qualified signed 64-bit node identity (1..MaxInt64;
// Bolt and Cypher integers and the v4 IDs are int64, so consumers convert at no cost). It carries no
// clock, placement or ownership: which partition currently owns a node is a
// separate, movable directory entry (PLAN identity row), and the value is never
// decoded for time or location. Zero is the unset value and negatives are invalid; neither is ever assigned. The
// engine allocator (internal/idalloc, uint64) must be capped at MaxInt64 for public IDs.
// Callers compare, store and hash it; they do not interpret its bits.
type NodeID int64

// RelID is the opaque 64-bit identity of a relationship, with the same rules as
// NodeID. The distinct type keeps node and relationship IDs from mixing.
type RelID int64

// NewNodeID wraps an allocated value, refusing zero with ErrInvalidID.
func NewNodeID(v int64) (NodeID, error) {
	if v <= 0 {
		return 0, ErrInvalidID
	}
	return NodeID(v), nil
}

// NewRelID wraps an allocated value, refusing zero with ErrInvalidID.
func NewRelID(v int64) (RelID, error) {
	if v <= 0 {
		return 0, ErrInvalidID
	}
	return RelID(v), nil
}

// ParseNodeID parses the canonical decimal form produced by String. Anything
// else, including zero, signs, spaces, leading zeros and overflow, is ErrInvalidID.
func ParseNodeID(s string) (NodeID, error) {
	v, err := parseID([]byte(s))
	return NodeID(v), err
}

// ParseRelID parses the canonical decimal form produced by String; see ParseNodeID.
func ParseRelID(s string) (RelID, error) {
	v, err := parseID([]byte(s))
	return RelID(v), err
}

// Int64 returns the raw value for codecs and keys.
func (id NodeID) Int64() int64 { return int64(id) }

// Int64 returns the raw value for codecs and keys.
func (id RelID) Int64() int64 { return int64(id) }

// IsZero reports the unset value.
func (id NodeID) IsZero() bool { return id == 0 }

// IsZero reports the unset value.
func (id RelID) IsZero() bool { return id == 0 }

// String is the canonical decimal form; the zero value prints as "0" for
// diagnostics only and does not parse back.
func (id NodeID) String() string { return strconv.FormatInt(int64(id), 10) }

// String is the canonical decimal form; see NodeID.String.
func (id RelID) String() string { return strconv.FormatInt(int64(id), 10) }

// MarshalText returns String, refusing the zero ID with ErrInvalidID.
func (id NodeID) MarshalText() ([]byte, error) { return marshalID(int64(id)) }

// MarshalText returns String, refusing the zero ID with ErrInvalidID.
func (id RelID) MarshalText() ([]byte, error) { return marshalID(int64(id)) }

// UnmarshalText parses the canonical decimal form and leaves the receiver
// unchanged on error.
func (id *NodeID) UnmarshalText(b []byte) error {
	v, err := parseID(b)
	if err != nil {
		return err
	}
	*id = NodeID(v)
	return nil
}

// UnmarshalText parses the canonical decimal form and leaves the receiver
// unchanged on error.
func (id *RelID) UnmarshalText(b []byte) error {
	v, err := parseID(b)
	if err != nil {
		return err
	}
	*id = RelID(v)
	return nil
}

func marshalID(v int64) ([]byte, error) {
	if v <= 0 {
		return nil, ErrInvalidID
	}
	return strconv.AppendInt(nil, v, 10), nil
}

// parseID accepts exactly the output of strconv.FormatUint for a nonzero value.
func parseID(b []byte) (int64, error) {
	if len(b) == 0 || len(b) > 19 || b[0] == '0' {
		return 0, ErrInvalidID
	}
	var v int64
	for _, c := range b {
		d := int64(c) - '0'
		if c < '0' || c > '9' || v > (math.MaxInt64-d)/10 {
			return 0, ErrInvalidID
		}
		v = v*10 + d
	}
	return v, nil
}
