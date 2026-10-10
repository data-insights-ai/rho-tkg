package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
)

// ApplicationProofWork counts attempted point records and bounded representation
// bytes. It is neither physical I/O nor a retained-memory or RSS measurement.
type ApplicationProofWork struct{ Records, Bytes int }

// ProveOnlyApplicationKV proves that the current bank contains exactly one KV
// version, equal to expected and present. Retained envelope triples and the
// no-application-GC invariant exclude hidden versions and tombstones without a
// scan. Installation must still guard the returned local base coordinates.
// Refusals return consumed work and no root; ordinary invalid/limit/cancellation
// refusals leave the borrowed view and store usable.
func (v *ApplicationView) ProveOnlyApplicationKV(ctx context.Context, expected KV, maxImageBytes, maxReadBytes int) (root ApplicationRoot, work ApplicationProofWork, err error) {
	if v == nil || v.s == nil || v.index == 0 || ctx == nil || maxImageBytes < 0 || maxReadBytes < 0 || len(expected.Key) == 0 || expected.Deleted {
		return root, work, ErrInvalid
	}
	if err = v.lock(ctx); err != nil {
		return root, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if len(expected.Key) > p.MaxKeyBytes || len(expected.Value) > p.MaxValueBytes || maxReadBytes > p.MaxPageBytes {
		return root, work, ErrLimit
	}
	charge := func(n int) error {
		if n < 0 || n > maxReadBytes-work.Bytes {
			return ErrLimit
		}
		work.Bytes += n
		return nil
	}
	if len(v.image) > maxImageBytes {
		return root, work, ErrLimit
	}
	if err = charge(len(v.image)); err != nil {
		return root, work, err
	}
	s := v.s
	if v.bank != s.activeBank() || v.generation != s.activeGeneration() || v.index != s.meta.Applied {
		return root, work, ErrInvalid
	}
	a := s.meta.App
	if !a.Policy.Enabled() || a.Through != v.index || a.Through == 0 || a.Through > (math.MaxUint64-1)/3 || uint64(len(v.image)) != s.meta.ImageBytes || len(v.image) > p.MaxImageBytes {
		return root, work, v.fail(ErrCorrupt)
	}
	envelopes := 3 * a.Through
	if a.Records < envelopes {
		return root, work, v.fail(ErrCorrupt)
	}
	if a.Records != envelopes+1 {
		return root, work, ErrInvalid
	}
	// Only a same-current view may classify authoritative metadata corruption.
	// Its image size/policy/ledger are checked before hashing or output copying.
	hash := sha256.Sum256(v.image)
	if hash != s.meta.ImageHash {
		return root, work, ErrInvalid
	}
	// appPrefix reserves its worst-case escaped capacity. The upper bound owns
	// only its exact bytes; no logical key/value output copy is needed.
	prefixBytes := len(expected.Key) + bytes.Count(expected.Key, []byte{0}) + 3
	if err = charge(2*len(expected.Key) + 3 + prefixBytes + 64); err != nil {
		return root, work, err
	}
	prefix := bankPrefix(v.bank, expected.Key)
	upper := copyApplicationBytes(prefix)
	upper[len(upper)-1]++
	it, e := v.iterator(prefix, upper)
	if e != nil {
		return root, work, v.fail(e)
	}
	defer func() {
		e := it.Close()
		if e != nil {
			err = errors.Join(err, v.fail(e))
		}
		if err != nil {
			root = ApplicationRoot{}
		}
	}()
	work.Records++
	if !it.SeekGE(prefix) {
		if e := it.Error(); e != nil {
			return root, work, v.fail(e)
		}
		return root, work, ErrInvalid
	}
	physical := it.Key()
	if len(physical) != len(prefix)+8 || !bytes.Equal(physical[:len(prefix)], prefix) {
		return root, work, v.fail(ErrCorrupt)
	}
	index := ^binary.BigEndian.Uint64(physical[len(prefix):])
	if index < 2 || index > v.index || index == math.MaxUint64 {
		return root, work, v.fail(ErrCorrupt)
	}
	raw, e := it.ValueAndErr()
	if e != nil {
		return root, work, v.fail(storedReadFailure(e))
	}
	if len(raw) > appFrameBytes+p.MaxValueBytes {
		return root, work, v.fail(ErrCorrupt)
	}
	if err = charge(len(physical) + len(raw)); err != nil {
		return root, work, err
	}
	value, deleted, e := inspectAppFrame(physical, raw, p.MaxValueBytes)
	if e != nil {
		return root, work, v.fail(e)
	}
	if deleted || !bytes.Equal(value, expected.Value) {
		return root, work, ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return root, work, err
	}
	if err = charge(len(v.image)); err != nil {
		return root, work, err
	}
	return ApplicationRoot{Generation: v.generation, Index: v.index, Image: copyApplicationBytes(v.image), ImageHash: hash}, work, nil
}
