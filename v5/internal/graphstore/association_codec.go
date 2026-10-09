package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Reserved independently of component-page families (8–11). Provisional only.
const (
	associationHeadRecord     recordKind = 0x80
	associationRevisionRecord recordKind = 0x81
	associationPostingRecord  recordKind = 0x82
	associationPrimaryRecord  recordKind = 0x83
)

func associationHeadKey(n Namespace, id assertion.ID) []byte {
	return binary.BigEndian.AppendUint64(keyPrefix(n, associationHeadRecord), uint64(id))
}
func associationRevisionKey(n Namespace, id assertion.ID, revision uint64) []byte {
	b := binary.BigEndian.AppendUint64(keyPrefix(n, associationRevisionRecord), uint64(id))
	return binary.BigEndian.AppendUint64(b, revision)
}
func appendAssociationTarget(dst []byte, t assertion.Target) []byte {
	dst = append(dst, byte(t.Kind))
	if t.Kind == assertion.EntityTarget {
		return binary.BigEndian.AppendUint64(dst, uint64(t.Entity))
	}
	k := t.Component
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Owner))
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Life))
	dst = append(dst, byte(k.Kind))
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Member))
	return appendField(dst, []byte(k.Name))
}
func associationTargetPrefix(n Namespace, t assertion.Target) []byte {
	return appendAssociationTarget(keyPrefix(n, associationPostingRecord), t)
}
func associationPostingKey(n Namespace, t assertion.Target, id assertion.ID) []byte {
	return binary.BigEndian.AppendUint64(associationTargetPrefix(n, t), uint64(id))
}
func associationPrimaryKey(n Namespace, id graphstate.EntityID) []byte {
	return binary.BigEndian.AppendUint64(keyPrefix(n, associationPrimaryRecord), uint64(id))
}

func associationCallerError(err error) error {
	if errors.Is(err, assertion.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, raftlog.ErrLimit) {
		return errors.Join(ErrResourceLimit, err)
	}
	return errors.Join(ErrInvalid, err)
}

// Persisted native revisions reference the immutable axis catalog, not a copied
// descriptor. The existing canonical child scope/knowledge commits its full
// definition hash. NoAssociation/symbolic revisions carry no axis ID payload.
func encodeAssociation(n Namespace, r assertion.Record, l AssociationLimits, catalog Limits) ([]byte, error) {
	wire, err := assertion.AppendRecord(nil, r, l.Record)
	if err != nil {
		return nil, associationCallerError(err)
	}
	b := recordHeader(n, associationRevisionRecord)
	s := r.Spec()
	if s.Placement.Kind == assertion.NativePlacement {
		b = append(b, 1)
		id := s.Placement.Native.Axis().Descriptor().ID
		b = append(b, id[:]...)
	} else {
		b = append(b, 0)
	}
	b = appendField(b, wire)
	return checkRecord(b, catalog)
}

func inspectAssociation(src []byte, n Namespace, l Limits) (temporal.AxisID, []byte, error) {
	c, err := inspectRecord(src, n, associationRevisionRecord, l)
	if err != nil {
		return temporal.AxisID{}, nil, err
	}
	flag, err := c.tag()
	if err != nil || flag > 1 {
		return temporal.AxisID{}, nil, ErrCorrupt
	}
	var axis temporal.AxisID
	if flag == 1 {
		id, err := c.take(16)
		if err != nil {
			return temporal.AxisID{}, nil, err
		}
		copy(axis[:], id)
		if axis == (temporal.AxisID{}) {
			return temporal.AxisID{}, nil, ErrCorrupt
		}
	}
	wire, err := c.field(l.MaxRecordBytes)
	if err != nil {
		return temporal.AxisID{}, nil, err
	}
	if err := c.done(); err != nil {
		return temporal.AxisID{}, nil, err
	}
	return axis, wire, nil
}

func associationRecordBytes(r assertion.Record, l AssociationLimits) (int, error) {
	b, err := assertion.AppendRecord(nil, r, l.Record)
	if err != nil {
		return 0, associationCallerError(err)
	}
	n := 64 + len(b)
	s := r.Spec()
	if s.Placement.Kind == assertion.NativePlacement {
		d := s.Placement.Native.Axis().Descriptor()
		n += 27 + len(d.Reference) + len(d.CanonicalUnit)
	}
	return n, nil
}

func validAssociationTarget(t assertion.Target, graph graphstate.GraphID, l AssociationLimits, maxName int) error {
	if len(t.Component.Name) > maxName {
		return ErrResourceLimit
	}
	rev, _ := state.NewRevision(1, 0)
	_, err := assertion.New(assertion.Spec{Ref: assertion.Ref{Graph: graph, ID: 1}, Target: t, Revision: rev, Interpretation: assertion.Constraint, Placement: assertion.Placement{Kind: assertion.NoAssociation}, Knowledge: assertion.Knowledge{Kind: assertion.NoKnowledge}}, l.Record)
	if err != nil {
		return associationCallerError(err)
	}
	return nil
}

func associationQueryBytes(q AssociationQuery, l AssociationLimits, maxName int) ([]byte, error) {
	if err := validAssociationTarget(q.Target, q.Graph, l, maxName); err != nil {
		return nil, err
	}
	if q.Interpretation > assertion.AssertedRelation || q.Placement > assertion.SymbolicPlacement {
		return nil, ErrInvalid
	}
	b := appendAssociationTarget(append([]byte{}, q.Graph[:]...), q.Target)
	b = append(b, byte(q.Interpretation), byte(q.Placement))
	if q.NativeWindow.Kind() == temporal.ScopeInvalid {
		return append(b, 0), nil
	}
	if q.Placement != assertion.NativePlacement || q.NativeWindow.Kind() == temporal.ScopeUnplaced {
		return nil, errors.Join(ErrInvalid, temporal.ErrUnsupportedPredicate)
	}
	wire, err := temporal.AppendScope(nil, q.NativeWindow, l.Record.Temporal)
	if err != nil {
		return nil, associationCallerError(err)
	}
	b = append(b, 1)
	return appendField(b, wire), nil
}

// Cursor identifies the complete canonical query and application index/hash,
// namespace and last visited key. It owns only bounded bytes, not a server map.
func associationContinuation(n Namespace, index uint64, imageHash, queryHash [32]byte, last []byte) []byte {
	b := append([]byte{'A', 'C', 1}, n.Graph[:]...)
	b = binary.BigEndian.AppendUint64(b, n.Partition)
	b = binary.BigEndian.AppendUint64(b, index)
	b = append(b, imageHash[:]...)
	b = append(b, queryHash[:]...)
	return exactCopy(appendField(b, last))
}

func parseAssociationContinuation(src []byte, n Namespace, index uint64, imageHash, queryHash [32]byte, lower, upper []byte, maxBytes int) ([]byte, error) {
	const fixed = 103
	if len(src) > maxBytes {
		return nil, ErrResourceLimit
	}
	if len(src) < fixed || !bytes.Equal(src[:3], []byte{'A', 'C', 1}) || !bytes.Equal(src[3:19], n.Graph[:]) || binary.BigEndian.Uint64(src[19:27]) != n.Partition || binary.BigEndian.Uint64(src[27:35]) != index || !bytes.Equal(src[35:67], imageHash[:]) || !bytes.Equal(src[67:99], queryHash[:]) {
		return nil, ErrInvalid
	}
	c := cursor{src[99:]}
	last, err := c.field(maxBytes)
	if err != nil || c.done() != nil || len(last) != len(lower)+8 || !bytes.HasPrefix(last, lower) || bytes.Compare(last, upper) >= 0 || binary.BigEndian.Uint64(last[len(lower):]) == 0 {
		return nil, ErrInvalid
	}
	return exactCopy(last), nil
}

func associationPrefixEnd(prefix []byte) []byte {
	b := exactCopy(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 255 {
			b[i]++
			return b[:i+1]
		}
	}
	return nil
}

func associationQueryHash(b []byte) [32]byte {
	return sha256.Sum256(append([]byte("rho-tkg:association-query:v1\x00"), b...))
}
