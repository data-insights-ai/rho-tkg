package idalloc

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func checksum(b []byte) {
	binary.BigEndian.PutUint32(b[117:], crc32.Checksum(b[:117], crc32.MakeTable(crc32.Castagnoli)))
}

func TestCheckpointRoundTripAndStrictCorruption(t *testing.T) {
	s, r := fixture(t)
	for _, reserve := range []bool{false, true} {
		if reserve {
			var err error
			s, _, _, err = Reserve(&s, r)
			if err != nil {
				t.Fatal(err)
			}
		}
		image, err := MarshalCheckpoint(&s)
		if err != nil {
			t.Fatal(err)
		}
		if len(image) != CheckpointSize {
			t.Fatalf("size=%d", len(image))
		}
		restored, err := DecodeCheckpoint(image)
		if err != nil || restored != s {
			t.Fatalf("roundtrip=%+v error=%v", restored, err)
		}
		for length := range len(image) {
			_, err := DecodeCheckpoint(image[:length])
			requireError(t, err, ErrCorrupt)
		}
		_, err = DecodeCheckpoint(append(bytes.Clone(image), 0))
		requireError(t, err, ErrCorrupt)
		for offset := range len(image) {
			corrupt := bytes.Clone(image)
			corrupt[offset] ^= 1
			_, err := DecodeCheckpoint(corrupt)
			requireError(t, err, ErrCorrupt)
		}
		// Unknown versions fail even when their CRC is internally consistent.
		corrupt := bytes.Clone(image)
		corrupt[4]++
		checksum(corrupt)
		_, err = DecodeCheckpoint(corrupt)
		requireError(t, err, ErrCorrupt)
		corrupt = bytes.Clone(image)
		corrupt[0]++
		checksum(corrupt)
		_, err = DecodeCheckpoint(corrupt)
		requireError(t, err, ErrCorrupt)
	}
	image, err := MarshalCheckpoint(&s)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { clear(b[5:21]) },  // graph
		func(b []byte) { clear(b[21:37]) }, // allocation owner
		func(b []byte) { clear(b[37:45]) }, // epoch
		func(b []byte) { clear(b[45:53]) }, // high-water cannot precede block
		func(b []byte) { clear(b[53:61]) }, // maximum
		func(b []byte) { binary.BigEndian.PutUint64(b[53:61], MaxBlockSize+1) },
		func(b []byte) { clear(b[61:69]) }, // receipt without sequence
		func(b []byte) { clear(b[69:77]) }, // empty reserved block
		func(b []byte) { binary.BigEndian.PutUint64(b[69:77], 101) },
		func(b []byte) { clear(b[77:85]) }, // zero first ID
		func(b []byte) { binary.BigEndian.PutUint64(b[77:85], 5) },
		func(b []byte) { b[85]++ }, // incorrect request digest
	} {
		corrupt := bytes.Clone(image)
		mutate(corrupt)
		checksum(corrupt)
		_, err := DecodeCheckpoint(corrupt)
		requireError(t, err, ErrCorrupt)
	}
	// Decode owns fixed values; mutations of the source image cannot change it.
	restored, err := DecodeCheckpoint(image)
	if err != nil {
		t.Fatal(err)
	}
	clear(image)
	if restored != s {
		t.Fatal("checkpoint aliases input")
	}
	bad := s
	bad.digest[0]++
	_, err = MarshalCheckpoint(&bad)
	requireError(t, err, ErrInvalid)
}

func FuzzDecodeCheckpoint(f *testing.F) {
	s, r := fixture(f)
	s, _, _, err := Reserve(&s, r)
	if err != nil {
		f.Fatal(err)
	}
	image, err := MarshalCheckpoint(&s)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(image)
	f.Add([]byte{})
	f.Add(image[:20])
	f.Fuzz(func(t *testing.T, image []byte) {
		s, err := DecodeCheckpoint(image)
		if err != nil {
			return
		}
		canonical, err := MarshalCheckpoint(&s)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(image, canonical) {
			t.Fatal("noncanonical checkpoint accepted")
		}
	})
}

func BenchmarkMint(b *testing.B) {
	s, r := fixture(b)
	s.maxBlock = MaxBlockSize
	r.Count = MaxBlockSize
	e := embedding(b, s)
	c, err := issuer(b, r).Acquire(b.Context(), r, e.commit)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if c.done {
			c.next, c.done = c.block.First, false
		} // benchmark-only rewind
		if _, err := c.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReserveReducer(b *testing.B) {
	s, r := fixture(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := Reserve(&s, r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReserveAndCheckpoint(b *testing.B) {
	s, r := fixture(b)
	b.ReportAllocs()
	for b.Loop() {
		next, _, _, err := Reserve(&s, r)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := MarshalCheckpoint(&next); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(CheckpointSize, "checkpoint-B/block")
}
