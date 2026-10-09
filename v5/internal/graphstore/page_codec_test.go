package graphstore

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func decoderReader(axis temporal.Axis, k graphstate.ComponentKey, kind recordKind, b []byte, l PageLimits) *pageReader {
	catalogLimits, _ := (Limits{}).resolve()
	root, _ := NewRoot(testNamespace(), 3)
	root.next = 16
	c := &Catalog{root: root, limits: catalogLimits}
	key := physicalKey(testNamespace(), kind, 1)
	return &pageReader{q: &reader{c: c, ctx: context.Background(), pending: map[string]raftlog.KV{string(key): {Key: key, Value: b}}}, limits: l}
}
func decodePhysical(q *pageReader, kind recordKind, k graphstate.ComponentKey, a temporal.Axis) error {
	switch kind {
	case directoryRecord:
		_, err := q.directory(1, k, a)
		return err
	case checkpointRecord:
		_, err := q.checkpoint(1, k, a)
		return err
	case patchRecord:
		_, _, err := q.patch(1, k, a)
		return err
	}
	return ErrInvalid
}
func pageCodecSeeds(t *testing.T) (temporal.Axis, graphstate.ComponentKey, map[recordKind][]byte) {
	t.Helper()
	a := testAxis(t, 1, temporal.ProfileIntegerZ)
	k := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
	c := decoderReader(a, k, directoryRecord, nil, DefaultPageLimits()).q.c
	all, _ := temporal.All(a)
	w := pagePoint(t, a, 5)
	patch := pagePatch(t, k, emptyPageState(t, a), w, presentLife(t), 1, false)
	leaf, err := encodeDirectory(testNamespace(), directoryPage{ID: 1, Key: k, Owned: all}, c, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := encodeCheckpoint(testNamespace(), 1, k, patch.State, c, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	tail, err := encodePatch(testNamespace(), patchPage{ID: 1, Key: k, Owned: all, Changes: patch.Changes}, c, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	return a, k, map[recordKind][]byte{directoryRecord: leaf, checkpointRecord: checkpoint, patchRecord: tail}
}
func TestPageDecodersCanonicalFramesAndEveryTruncation(t *testing.T) {
	a, k, seeds := pageCodecSeeds(t)
	for kind, wire := range seeds {
		t.Run(string(rune(kind)), func(t *testing.T) {
			q := decoderReader(a, k, kind, wire, DefaultPageLimits())
			if err := decodePhysical(q, kind, k, a); err != nil {
				t.Fatal(err)
			}
			for n := 0; n < len(wire); n++ {
				q := decoderReader(a, k, kind, wire[:n], DefaultPageLimits())
				if err := decodePhysical(q, kind, k, a); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("truncation %d kind%d %v", n, kind, err)
				}
			}
			trailer := append(exactCopy(wire), 1)
			if err := decodePhysical(decoderReader(a, k, kind, trailer, DefaultPageLimits()), kind, k, a); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			wrong := k
			wrong.Owner = 2
			if err := decodePhysical(decoderReader(a, k, kind, wire, DefaultPageLimits()), kind, wrong, a); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	q := decoderReader(a, k, checkpointRecord, seeds[checkpointRecord], DefaultPageLimits())
	q.limits.MaxCheckpointBytes = 1
	if err := decodePhysical(q, checkpointRecord, k, a); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	q = decoderReader(a, k, checkpointRecord, seeds[checkpointRecord], DefaultPageLimits())
	empty, err := q.checkpoint(0, k, a)
	if err != nil || len(empty.Pieces()) != 0 {
		t.Fatal(err)
	}
}
func TestPageExtentValidationRejectsGapsAndDuplicateHandles(t *testing.T) {
	a := testAxis(t, 1, temporal.ProfileRationalQ)
	all, _ := temporal.All(a)
	point := pagePoint(t, a, 0)
	lo, hi, _ := point.Bounds()
	p, _ := lo.Position()
	open, err := temporal.FiniteBound(p, false)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := temporal.Span(a, temporal.NegativeInfinity(), hi, temporal.Limits{})
	right, _ := temporal.Span(a, open, temporal.PositiveInfinity(), temporal.Limits{})
	valid := directoryPage{ID: 1, Owned: all, Level: 1, Children: []childPage{{2, left}, {3, right}}}
	if err := validateChildren(valid, temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	for _, children := range [][]childPage{{{2, left}, {2, right}}, {{2, left}, {3, left}}, {{2, point}, {3, right}}, {{3, right}, {2, left}}, {{1, left}, {3, right}}, {{0, left}, {3, right}}} {
		d := valid
		d.Children = children
		if err := validateChildren(d, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}
func FuzzPageDecoders(f *testing.F) {
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "clock", CanonicalUnit: "u"}, temporal.Limits{})
	if err != nil {
		f.Fatal(err)
	}
	k := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
	all, _ := temporal.All(a)
	s, _ := state.New(a, state.Limits{})
	p, _ := temporal.IntegerPosition(a, temporal.Int64(5))
	scope, _ := temporal.Point(p)
	v, _ := state.NewValueRef(11, 0)
	rev, _ := state.NewRevision(1, 7)
	result, err := s.Set(scope, v, rev, state.Limits{})
	if err != nil {
		f.Fatal(err)
	}
	c := decoderReader(a, k, directoryRecord, nil, DefaultPageLimits()).q.c
	leaf, err := encodeDirectory(testNamespace(), directoryPage{ID: 1, Key: k, Owned: all}, c, DefaultPageLimits())
	if err != nil {
		f.Fatal(err)
	}
	checkpoint, err := encodeCheckpoint(testNamespace(), 1, k, result.State(), c, DefaultPageLimits())
	if err != nil {
		f.Fatal(err)
	}
	patch, err := encodePatch(testNamespace(), patchPage{ID: 1, Key: k, Owned: all, Changes: result.Changes()}, c, DefaultPageLimits())
	if err != nil {
		f.Fatal(err)
	}
	for kind, b := range map[recordKind][]byte{directoryRecord: leaf, checkpointRecord: checkpoint, patchRecord: patch} {
		f.Add(byte(kind), b)
	}
	f.Add(byte(directoryRecord), []byte{})
	f.Fuzz(func(t *testing.T, tag byte, b []byte) {
		if len(b) > 4096 {
			return
		}
		kind := recordKind(tag)
		if kind < directoryRecord || kind > patchRecord {
			return
		}
		q := decoderReader(a, k, kind, b, DefaultPageLimits())
		if err := decodePhysical(q, kind, k, a); err != nil {
			return
		}
		var encoded []byte
		var err error
		switch kind {
		case directoryRecord:
			d, e := q.directory(1, k, a)
			if e != nil {
				t.Fatal(e)
			}
			encoded, err = encodeDirectory(testNamespace(), d, q.q.c, q.limits)
		case checkpointRecord:
			s, e := q.checkpoint(1, k, a)
			if e != nil {
				t.Fatal(e)
			}
			encoded, err = encodeCheckpoint(testNamespace(), 1, k, s, q.q.c, q.limits)
		case patchRecord:
			p, _, e := q.patch(1, k, a)
			if e != nil {
				t.Fatal(e)
			}
			encoded, err = encodePatch(testNamespace(), p, q.q.c, q.limits)
		}
		if err != nil || string(encoded) != string(b) {
			t.Fatal("accepted noncanonical frame", err)
		}
	})
}
func TestPageKeyShapesAndHardBounds(t *testing.T) {
	limits, _ := (Limits{}).resolve()
	for _, k := range []graphstate.ComponentKey{{}, {Owner: 1, Kind: 99}, {Owner: 1, Kind: graphstate.Presence, Life: 1}, {Owner: 1, Kind: graphstate.Label, Life: 1}, {Owner: 1, Kind: graphstate.ScalarProperty, Life: 1, Name: "s", Member: 2}, {Owner: 1, Kind: graphstate.SetMember, Life: 1, Name: "s"}} {
		if validComponent(k, limits) {
			t.Fatal("invalid key shape")
		}
		c := cursor{src: appendComponent(nil, k)}
		if _, err := decodeComponent(&c, limits); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	a, k, seeds := pageCodecSeeds(t)
	for _, field := range []int{28, 36} {
		b := exactCopy(seeds[directoryRecord])
		binary.BigEndian.PutUint64(b[field:field+8], 0)
		if err := decodePhysical(decoderReader(a, k, directoryRecord, b, DefaultPageLimits()), directoryRecord, k, a); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("malformed field %d %v", field, err)
		}
	}
}
