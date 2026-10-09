package idalloc

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func receiptImage(t *testing.T, r Request, first uint64) []byte {
	t.Helper()
	s, _ := fixture(t)
	image, err := MarshalCheckpoint(&s)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(image[45:53], first+r.Count-1)
	binary.BigEndian.PutUint64(image[61:69], r.Sequence)
	binary.BigEndian.PutUint64(image[69:77], r.Count)
	binary.BigEndian.PutUint64(image[77:85], first)
	d := r.Digest()
	copy(image[85:117], d[:])
	checksum(image)
	return image
}

func TestReceiptReachabilityAtCheckpointAndGrantBoundaries(t *testing.T) {
	for _, sequence := range []uint64{2, 100, math.MaxUint64} {
		t.Run("first-before-sequence", func(t *testing.T) {
			_, r := fixture(t)
			r.Sequence = sequence
			r.Count = 1
			image := receiptImage(t, r, sequence-1)
			if got, err := DecodeCheckpoint(image); !errors.Is(err, ErrCorrupt) || got != (State{}) {
				t.Fatalf("unreachable receipt accepted: %+v %v", got, err)
			}
			i, _ := NewIssuer(r.Graph, r.Authority)
			cursor, err := i.Acquire(t.Context(), r, func(_ context.Context, r Request) (Reservation, bool, error) {
				return Reservation{Request: r, First: sequence - 1, Last: sequence - 1}, false, nil
			})
			if !errors.Is(err, ErrPayloadMismatch) || cursor != nil {
				t.Fatal("unreachable grant issued", cursor, err)
			}
			if _, err = i.Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) {
				t.Fatal("burned receipt retried")
				return Reservation{}, false, nil
			}); !errors.Is(err, ErrAlreadyReserved) {
				t.Fatal(err)
			}
		})
	}
	// Equality is reachable at both the first reservation and the final uint64.
	for _, sequence := range []uint64{1, math.MaxUint64} {
		_, r := fixture(t)
		r.Sequence = sequence
		r.Count = 1
		restored, err := DecodeCheckpoint(receiptImage(t, r, sequence))
		if err != nil {
			t.Fatal(err)
		}
		_, b, replay, err := Reserve(&restored, r)
		if err != nil || !replay || b.First != sequence {
			t.Fatal(b, replay, err)
		}
		i, _ := NewIssuer(r.Graph, r.Authority)
		c, err := i.Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) { return b, false, nil })
		if err != nil {
			t.Fatal(err)
		}
		id, err := c.Next()
		if err != nil || id != sequence {
			t.Fatal(id, err)
		}
		if _, err = c.Next(); !errors.Is(err, ErrExhausted) {
			t.Fatal(err)
		}
	}
	// Epoch handoff resets sequence while preserving already allocated ranges.
	s, r := fixture(t)
	s, _, _, _ = Reserve(&s, r)
	replacement, _ := NewAuthority([16]byte{2}, r.Authority.Epoch()+1)
	next, err := Transfer(&s, r.Authority, replacement)
	if err != nil {
		t.Fatal(err)
	}
	r.Authority = replacement
	r.Count = 1
	next, b, replay, err := Reserve(&next, r)
	if err != nil || replay || b.First != 5 {
		t.Fatal(next, b, replay, err)
	}
	image, err := MarshalCheckpoint(&next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeCheckpoint(image); err != nil {
		t.Fatal(err)
	}
}
