package raftlog

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

// ApplicationControlPut carries one immutable evidence key and a present value
// from the trusted state machine. An empty value is present, not an absence;
// control storage grants no public graph admission authority.
type ApplicationControlPut struct{ Key, Value []byte }

// ApplicationControlRecord owns evidence aligned with a retained application
// root. Index identifies its original local installation, not a certified cut.
type ApplicationControlRecord struct {
	Key, Value []byte
	Index      uint64
}

// ApplicationControlReadWork reports admitted rows and conservative byte work,
// including examined duplicate keys. Error results retain diagnostic work only.
type ApplicationControlReadWork struct{ Rows, Bytes int }

// ApplicationControlUsage separates the active control subtotal from active
// graph-plus-control totals. These logical ledgers do not measure physical memory.
type ApplicationControlUsage struct{ Bytes, Records, TotalBytes, TotalRecords uint64 }

// ApplicationControlUsage reports serialized active ledgers in control mode.
// Errors return zero usage and preserve nil, closed and poisoned-store identity.
func (s *Store) ApplicationControlUsage() (ApplicationControlUsage, error) {
	if s == nil {
		return ApplicationControlUsage{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return ApplicationControlUsage{}, err
	}
	if !s.meta.Controls.Config.enabled() {
		return ApplicationControlUsage{}, ErrInvalid
	}
	b, n := applicationTotals(s.meta)
	return ApplicationControlUsage{s.meta.Controls.Bytes, s.meta.Controls.Records, b, n}, nil
}

// GetControl aligns the original insertion with this actual retained root/bank.
// Returned index is local evidence, not a cut. Error output is empty; work remains diagnostic.
func (v *ApplicationView) GetControl(ctx context.Context, key []byte, b ReadBudget) (out ApplicationControlRecord, found bool, work ApplicationControlReadWork, err error) {
	if err = v.lock(ctx); err != nil {
		return out, false, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !v.s.meta.Controls.Config.enabled() || len(key) == 0 || b.Rows < 1 || b.Bytes < 1 {
		return out, false, work, ErrInvalid
	}
	if len(key) > p.MaxKeyBytes || b.Rows > p.MaxPageRows || b.Bytes > p.MaxPageBytes {
		return out, false, work, ErrLimit
	}
	physicalBytes := len(key) + bytes.Count(key, []byte{0}) + 11
	fixed := 128 + 4*physicalBytes
	if fixed > b.Bytes {
		return out, false, work, ErrLimit
	}
	work.Bytes = fixed
	prefix := controlPrefix(v.bank, key)
	it, e := v.iterator(prefix, appNextPrefix(prefix))
	if e != nil {
		return out, false, work, v.fail(e)
	}
	defer func() {
		if e := it.Close(); e != nil {
			v.s.poison = e
			err = errors.Join(err, e)
			out = ApplicationControlRecord{}
			found = false
		}
	}()
	if !it.First() {
		if e := it.Error(); e != nil {
			return out, false, work, v.fail(e)
		}
		return out, false, work, nil
	}
	k := it.Key()
	logical, index, e := decodeControlKey(v.bank, k, p.MaxKeyBytes)
	through := v.s.meta.App.Through
	if v.generation != 0 {
		through = v.s.meta.Gen.Banks[v.bank].Through
	}
	if e != nil || !bytes.Equal(logical, key) || index > through {
		return out, false, work, v.fail(ErrCorrupt)
	}
	raw, e := it.ValueAndErr()
	if e != nil {
		return out, false, work, v.fail(storedReadFailure(e))
	}
	if len(raw) > p.MaxValueBytes+appFrameBytes {
		return out, false, work, v.fail(ErrCorrupt)
	}
	cost := len(k) + len(raw) + 64
	if cost > b.Bytes-work.Bytes {
		return out, false, work, ErrLimit
	}
	work.Bytes += cost
	work.Rows++
	value, deleted, e := inspectAppFrame(k, raw, p.MaxValueBytes)
	if e != nil || deleted {
		return out, false, work, v.fail(ErrCorrupt)
	}
	if index <= v.index {
		out = ApplicationControlRecord{copyApplicationBytes(key), copyApplicationBytes(value), index}
		found = true
	}
	if it.Next() {
		if work.Rows >= b.Rows {
			return ApplicationControlRecord{}, false, work, ErrLimit
		}
		second := it.Key()
		if len(second) < 12 || len(second) > 2*p.MaxKeyBytes+11 {
			return ApplicationControlRecord{}, false, work, v.fail(ErrCorrupt)
		}
		secondCost := len(second) + 64
		if secondCost > b.Bytes-work.Bytes {
			return ApplicationControlRecord{}, false, work, ErrLimit
		}
		work.Bytes += secondCost
		work.Rows++
		logical, index, e := decodeControlKey(v.bank, second, p.MaxKeyBytes)
		if e != nil || !bytes.Equal(logical, key) || index > through {
			return ApplicationControlRecord{}, false, work, v.fail(ErrCorrupt)
		}
		return ApplicationControlRecord{}, false, work, v.fail(ErrCorrupt)
	}
	if e := it.Error(); e != nil {
		return ApplicationControlRecord{}, false, work, v.fail(e)
	}
	return out, found, work, nil
}
func (s *Store) controlAbsent(key []byte, remaining uint64) (work uint64, err error) {
	prefix := controlPrefix(s.activeBank(), key)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: appNextPrefix(prefix)})
	if err != nil {
		s.poison = err
		return 0, err
	}
	defer func() {
		if e := it.Close(); e != nil {
			s.poison = e
			err = errors.Join(err, e)
		}
	}()
	if !it.First() {
		if e := it.Error(); e != nil {
			s.poison = e
			return 0, e
		}
		return 0, nil
	}
	raw, e := it.ValueAndErr()
	if e != nil {
		s.poison = storedReadFailure(e)
		return 0, s.poison
	}
	keyBytes, valueBytes := uint64(len(it.Key())), uint64(len(raw))
	if valueBytes > math.MaxUint64-64 || keyBytes > math.MaxUint64-64-valueBytes {
		return 0, ErrLimit
	}
	work = keyBytes + valueBytes + 64
	if work > remaining {
		return work, ErrLimit
	}
	actual, index, e := decodeControlKey(s.activeBank(), it.Key(), s.meta.App.Policy.MaxKeyBytes)
	if e != nil || !bytes.Equal(actual, key) || index > s.meta.Applied {
		s.poison = ErrCorrupt
		return work, ErrCorrupt
	}
	if _, deleted, e := inspectAppFrame(it.Key(), raw, s.meta.App.Policy.MaxValueBytes); e != nil || deleted {
		s.poison = ErrCorrupt
		return work, ErrCorrupt
	}
	return work, ErrInvalid
}
func controlGrowth(p ApplicationPolicy, b ApplicationBatch) (bytesAdded, recordsAdded, work uint64, err error) {
	for i, w := range b.ControlPuts {
		if len(w.Key) == 0 || i > 0 && bytes.Compare(b.ControlPuts[i-1].Key, w.Key) >= 0 {
			return 0, 0, 0, ErrInvalid
		}
		if len(w.Key) > p.MaxKeyBytes || len(w.Value) > p.MaxValueBytes {
			return 0, 0, 0, ErrLimit
		}
		n := controlPhysicalKeyBytes(w.Key)
		size := n + appFrameBytes + uint64(len(w.Value))
		bytesAdded += size
		recordsAdded++
		work += 2*size + 64 + 128 + 4*n
		if work > unsignedLimit(p.MaxInstallBytes) {
			return 0, 0, 0, ErrLimit
		}
	}
	return
}

func controlPhysicalKeyBytes(key []byte) uint64 {
	n := uint64(len(key)) + 11
	for _, c := range key {
		if c == 0 {
			n++
		}
	}
	return n
}
