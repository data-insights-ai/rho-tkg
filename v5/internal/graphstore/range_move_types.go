package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

const rangeMoveMaxLogicalBytes = 16 << 20
const rangeMoveMaxCursorBytes = 128 << 10
const rangeMoveFixedBytes = 4096

const (
	rangeMoveValues byte = iota + 1
	rangeMoveEntities
	rangeMoveLives
	rangeMoveComponents
	rangeMoveCanonical
	rangeMoveDeclared
	rangeMovePresence
	rangeMoveDone
)

// RangeMoveSpec separates range placement epochs from partition root ownership.
// It is qualified inspection data, never a handoff, write or allocator grant.
type RangeMoveSpec struct {
	Publication RangePublication
	From, To    RangeOwnership
}

// RangeMoveCapture is a fixed actual source coordinate; index is store-local.
type RangeMoveCapture struct {
	Binding           raftlog.ApplicationBinding
	Generation, Index uint64
	Image             [172]byte
	ImageHash         [32]byte
}

// RangeMoveDescriptor binds immutable captures and a canonical logical stream.
// Plan returns an unsealed descriptor; only a complete portable manifest cursor
// can seal its totals. Decoding or sealing these values supplies no authority.
type RangeMoveDescriptor struct {
	Spec           RangeMoveSpec
	Source, Target RangeMoveCapture
	Declaration    [32]byte
	Configuration  [32]byte
	DefaultAxis    types.DefaultAxisBinding
	Rows, Bytes    uint64
	Digest         [32]byte
	Sealed         bool
}

// RangeMoveCursor owns only a bounded key and fixed frozen-source path. It is
// portable across source-handle restart at the exact captured root. Path IDs
// are internal physical continuation coordinates, never logical graph time.
type RangeMoveCursor struct {
	Binding               [32]byte
	Family                byte
	After, Key            []byte
	Path                  [32]uint64
	Slots                 [32]uint16
	Depth                 byte
	Record                uint64
	Cell                  uint32
	Offset                uint32
	Sequence, Rows, Bytes uint64
	Digest                [32]byte
}

// RangeMovePage owns one bounded logical record fragment. Before/Next are exact
// stream coordinates; page membership cannot replace native source provenance.
type RangeMovePage struct {
	Before, Next RangeMoveCursor
	Kind         byte
	Key, Payload []byte
	RecordBytes  uint32
	Offset       uint32
	RecordDone   bool
	Complete     bool
}

// RangeMoveImportState retains fixed future tree roots and bounded
// logical-record coordinates; fragments are private co-committed point rows. The outer materializer loads it from its actual
// durable control and co-commits every page's writes/progress under its fence.
type RangeMoveImportState struct {
	Cursor                RangeMoveCursor
	Sequence, Rows, Bytes uint64
	Digest                [32]byte
	Complete, Sealed      bool
	PreparationComplete   bool
	PreparationSteps      uint64
	component             []byte
	FutureRoot            Root
	index                 fullIndexDescriptor
	fragmentKind          byte
	fragmentKey           []byte
	fragmentStart         uint64
	fragmentOffset        uint32
	fragmentBytes         uint32
}

// RangeMovePageEffects owns only this page's computed target writes and next
// progress. It publishes no root, acknowledgment or business CDC by itself.
type RangeMovePageEffects struct {
	Base   raftlog.ApplicationRoot
	Writes []raftlog.KV
	Next   RangeMoveImportState
}

func (s RangeMoveSpec) valid() bool {
	p, from, to := s.Publication, s.From, s.To
	return p.Validate() == nil && from.Check(p, from.Partition, from.Epoch) == nil && to.Check(p, to.Partition, to.Epoch) == nil && !from.Fenced && !to.Fenced && from.Partition != to.Partition && from.Epoch < math.MaxUint64 && to.Epoch == from.Epoch+1
}

func (c RangeMoveCapture) valid(graph graphstate.GraphID, partition uint64) bool {
	r, err := DecodeRoot(c.Image[:])
	return err == nil && c.Binding.Validate() == nil && c.Binding.Identity.Graph == [16]byte(graph) && c.Binding.Identity.Partition == partition && c.Generation == 0 && c.Index > 0 && c.ImageHash == sha256.Sum256(c.Image[:]) && r.Namespace() == (Namespace{graph, partition}) && physicalFullTopology(r)
}

func (d RangeMoveDescriptor) valid() bool {
	return d.Spec.valid() && d.Declaration != ([32]byte{}) && d.Configuration == d.Spec.Publication.Configuration && d.Configuration != ([32]byte{}) && d.Source.valid(d.Spec.Publication.Graph, d.Spec.From.Partition) && d.Target.valid(d.Spec.Publication.Graph, d.Spec.To.Partition) && d.Source.Binding.SemanticContractID == d.Target.Binding.SemanticContractID && d.DefaultAxis.Check(types.GraphID(d.Spec.Publication.Graph), d.DefaultAxis.Axis(), DefaultLimits().Temporal) == nil && (d.Sealed && d.Digest != ([32]byte{}) || !d.Sealed && d.Rows == 0 && d.Bytes == 0 && d.Digest == ([32]byte{}))
}

func (d RangeMoveDescriptor) binding() [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:range-move-stream:v1\x00"))
	_, _ = h.Write(d.Spec.Publication.Graph[:])
	publication, _ := EncodeRangePublication(d.Spec.Publication)
	from, _ := EncodeRangeOwnership(d.Spec.From)
	to, _ := EncodeRangeOwnership(d.Spec.To)
	_, _ = h.Write(publication)
	_, _ = h.Write(from)
	_, _ = h.Write(to)
	for _, capture := range []RangeMoveCapture{d.Source, d.Target} {
		_, _ = h.Write(capture.Binding.Identity.Graph[:])
		_, _ = h.Write(capture.Binding.Identity.Group[:])
		_, _ = h.Write(capture.Binding.SemanticContractID[:])
	}
	axis := d.DefaultAxis.Axis()
	ax := axis.Descriptor()
	axisHash := axis.DefinitionHash()
	_, _ = h.Write(ax.ID[:])
	_, _ = h.Write(axisHash[:])
	var b [8]byte
	for _, v := range []uint64{d.Spec.Publication.RangeID, d.Spec.Publication.First, d.Spec.Publication.Last, d.Spec.From.Partition, d.Spec.From.Epoch, d.Spec.To.Partition, d.Spec.To.Epoch, d.Source.Index, d.Target.Index} {
		binary.BigEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	_, _ = h.Write(d.Source.ImageHash[:])
	_, _ = h.Write(d.Target.ImageHash[:])
	_, _ = h.Write(d.Declaration[:])
	_, _ = h.Write(d.Configuration[:])
	var result [32]byte
	h.Sum(result[:0])
	return result
}

func rangeMoveStart(d RangeMoveDescriptor) RangeMoveCursor {
	binding := d.binding()
	seed := sha256.Sum256(append([]byte("rho-tkg:range-move-rows:v1\x00"), binding[:]...))
	return RangeMoveCursor{Binding: binding, Family: rangeMoveValues, Digest: seed}
}

func rangeMoveCursorEqual(a, b RangeMoveCursor) bool {
	return a.Binding == b.Binding && a.Family == b.Family && bytes.Equal(a.After, b.After) && bytes.Equal(a.Key, b.Key) && a.Path == b.Path && a.Slots == b.Slots && a.Depth == b.Depth && a.Record == b.Record && a.Cell == b.Cell && a.Offset == b.Offset && a.Sequence == b.Sequence && a.Rows == b.Rows && a.Bytes == b.Bytes && a.Digest == b.Digest
}

func rangeMoveRowDigest(before [32]byte, kind byte, key, payload []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(before[:])
	_, _ = h.Write([]byte{kind})
	var b [8]byte
	for _, field := range [][]byte{key, payload} {
		binary.BigEndian.PutUint64(b[:], uint64(len(field)))
		_, _ = h.Write(b[:])
		_, _ = h.Write(field)
	}
	var result [32]byte
	h.Sum(result[:0])
	return result
}

func rangeMoveCloneCursor(c RangeMoveCursor) RangeMoveCursor {
	c.After = exactCopy(c.After)
	c.Key = exactCopy(c.Key)
	return c
}
