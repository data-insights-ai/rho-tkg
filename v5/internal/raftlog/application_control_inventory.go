package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
)

// ApplicationControlPage owns immutable control evidence, not admission. Work
// includes future keys and charged lookahead. Error output keeps only Work.
type ApplicationControlPage struct {
	Records  []ApplicationControlRecord
	After    []byte
	Complete bool
	Work     ApplicationControlReadWork
}

// These intrusive owners are bounded by MaxViews. Identity is checked at
// runtime: copying a mutex-bearing handle does not copy its registration.
func (s *Store) registerApplicationView(v *ApplicationView) {
	v.self, v.next = v, s.applicationViews
	s.applicationViews = v
}
func (s *Store) ownsApplicationView(v *ApplicationView) bool {
	for owner := s.applicationViews; owner != nil; owner = owner.next {
		if owner == v {
			return true
		}
	}
	return false
}
func (s *Store) unregisterApplicationView(v *ApplicationView) {
	for link := &s.applicationViews; *link != nil; link = &(*link).next {
		if *link == v {
			*link = v.next
			v.next = nil
			return
		}
	}
}

// ControlInventoryUsage reports only a current active census or the complete
// verified prepared cut. Historical roots cannot borrow newer global totals.
func (v *ApplicationView) ControlInventoryUsage(ctx context.Context) (ApplicationControlUsage, error) {
	if err := v.lock(ctx); err != nil {
		return ApplicationControlUsage{}, err
	}
	defer v.unlock()
	s := v.s
	if !s.meta.Controls.Config.enabled() {
		return ApplicationControlUsage{}, ErrInvalid
	}
	if v.prepared != nil {
		m := v.prepared.i.manifest
		if v.index != m.Index {
			return ApplicationControlUsage{}, ErrInvalid
		}
		b, n, err := snapshotTotals(m)
		if err != nil {
			return ApplicationControlUsage{}, v.fail(err)
		}
		bank := s.meta.Gen.Banks[v.bank]
		if bank.Bytes != b || bank.Records != n || bank.ControlBytes != m.ControlBytes || bank.ControlRecords != m.ControlRecords || sha256.Sum256(v.image) != m.ImageHash {
			return ApplicationControlUsage{}, v.fail(ErrCorrupt)
		}
		return ApplicationControlUsage{Bytes: m.ControlBytes, Records: m.ControlRecords, TotalBytes: b, TotalRecords: n}, nil
	}
	if v.bank != s.activeBank() || v.generation != s.activeGeneration() || v.index != s.meta.Applied || sha256.Sum256(v.image) != s.meta.ImageHash {
		return ApplicationControlUsage{}, ErrInvalid
	}
	if err := validateControlMeta(s.meta); err != nil {
		return ApplicationControlUsage{}, v.fail(err)
	}
	if err := s.validateGenerationMeta(s.meta); err != nil {
		return ApplicationControlUsage{}, v.fail(err)
	}
	b, n := applicationTotals(s.meta)
	return ApplicationControlUsage{Bytes: s.meta.Controls.Bytes, Records: s.meta.Controls.Records, TotalBytes: b, TotalRecords: n}, nil
}

type controlPageBudget struct {
	limit                                ReadBudget
	work                                 ApplicationControlReadWork
	initial, currentScratch, nextScratch int
	owned, pending, cursor               int
}

func (b *controlPageBudget) admit(n, extraPeak int) bool {
	if n < 0 || n > b.limit.Bytes-b.work.Bytes || b.cursor > b.limit.Bytes-b.work.Bytes-n {
		return false
	}
	peak := b.initial + b.currentScratch + b.nextScratch + b.owned + b.pending + b.cursor
	if peak > b.limit.Bytes || extraPeak > b.limit.Bytes-peak {
		return false
	}
	b.work.Bytes += n
	return true
}

type controlPageKey struct {
	logical  []byte
	index    uint64
	physical int
}

// ControlPage proves an entire immutable key run before emission/continuation.
// Each physical lookahead is admitted once. Both cumulative work and peak
// ownership are independently bounded; reservations are not reported work.
func (v *ApplicationView) ControlPage(ctx context.Context, after []byte, limit ReadBudget) (page ApplicationControlPage, err error) {
	if err = v.lock(ctx); err != nil {
		return page, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !v.s.meta.Controls.Config.enabled() || after != nil && len(after) == 0 || limit.Rows < 1 || limit.Bytes < 1 {
		return page, ErrInvalid
	}
	if len(after) > p.MaxKeyBytes || limit.Rows > p.MaxPageRows || limit.Bytes > p.MaxPageBytes {
		return page, ErrLimit
	}
	prefixBytes := 1
	if after != nil {
		prefixBytes = len(after) + bytes.Count(after, []byte{0}) + 3
	}
	b := controlPageBudget{limit: limit, initial: 128 + 4*prefixBytes}
	defer func() {
		page.Work = b.work
		if err != nil {
			page.Records = nil
			page.After = nil
			page.Complete = false
		}
	}()
	if !b.admit(b.initial, 0) {
		return page, ErrLimit
	}
	lower := []byte{controlBankTag(v.bank)}
	upper := []byte{controlBankTag(v.bank) + 1}
	seek := lower
	if after != nil {
		seek = appNextPrefix(controlPrefix(v.bank, after))
	}
	it, e := v.iterator(lower, upper)
	if e != nil {
		return page, v.fail(e)
	}
	defer func() {
		if e := it.Close(); e != nil {
			err = errors.Join(err, v.fail(e))
		}
	}()
	through := v.s.meta.App.Through
	if v.generation != 0 {
		through = v.s.meta.Gen.Banks[v.bank].Through
	}
	var safe []byte
	safePhysical := 0
	finish := func() (ApplicationControlPage, error) {
		b.pending = 0
		if safe == nil {
			return page, ErrLimit
		}
		// Convert the refundable cursor allowance to one actual copy debit.
		// Its capacity still coexists with the admitted scratch/output.
		b.cursor = 0
		if !b.admit(len(safe), len(safe)) {
			return page, ErrLimit
		}
		b.owned += len(safe)
		page.After = copyApplicationBytes(safe)
		page.Complete = false
		return page, nil
	}
	readKey := func() (controlPageKey, error) {
		physical := it.Key()
		k := len(physical)
		if k < 12 || k > 2*p.MaxKeyBytes+11 {
			return controlPageKey{}, v.fail(ErrCorrupt)
		}
		b.nextScratch = 4 * k
		if b.work.Rows >= limit.Rows || !b.admit(5*k+64, 0) {
			b.nextScratch = 0
			return controlPageKey{}, ErrLimit
		}
		b.work.Rows++
		logical, index, e := decodeControlKey(v.bank, physical, p.MaxKeyBytes)
		if e != nil || index > through {
			return controlPageKey{}, v.fail(ErrCorrupt)
		}
		// Decode and this exact-cap key copy are covered by the 4K ranges.
		key := controlPageKey{logical: copyApplicationBytes(logical), index: index, physical: k}
		b.cursor = max(b.cursor, len(safe), len(key.logical))
		if !b.admit(0, 0) {
			b.nextScratch = 0
			return controlPageKey{}, ErrLimit
		}
		return key, nil
	}
	if !it.SeekGE(seek) {
		if e := it.Error(); e != nil {
			return page, v.fail(e)
		}
		page.Complete = true
		return page, nil
	}
	current, e := readKey()
	if e != nil {
		return page, e
	}
	b.currentScratch, b.nextScratch = 4*current.physical, 0
	for {
		if e := ctx.Err(); e != nil {
			return page, e
		}
		raw, e := it.ValueAndErr()
		if e != nil {
			return page, v.fail(storedReadFailure(e))
		}
		if len(raw) > p.MaxValueBytes+appFrameBytes {
			return page, v.fail(ErrCorrupt)
		}
		if !b.admit(len(raw), 0) {
			return finish()
		}
		value, deleted, e := inspectAppFrame(it.Key(), raw, p.MaxValueBytes)
		if e != nil || deleted {
			return page, v.fail(ErrCorrupt)
		}
		visible := current.index <= v.index
		var pending ApplicationControlRecord
		if visible {
			copyBytes := len(current.logical) + len(value)
			if !b.admit(copyBytes, copyBytes) {
				return finish()
			}
			pending = ApplicationControlRecord{Key: copyApplicationBytes(current.logical), Value: copyApplicationBytes(value), Index: current.index}
			b.pending = copyBytes
		}
		// Borrowed key/value cease to be valid at Next; pending owns every
		// visible byte. The decoded future key also owns its scratch bytes.
		hasNext := it.Next()
		var next controlPageKey
		if hasNext {
			next, e = readKey()
			if e != nil {
				pending = ApplicationControlRecord{}
				if errors.Is(e, ErrLimit) {
					return finish()
				}
				return page, e
			}
			if bytes.Equal(current.logical, next.logical) {
				return page, v.fail(ErrCorrupt)
			}
		} else {
			if e := it.Error(); e != nil {
				return page, v.fail(e)
			}
			b.cursor = 0
		}
		if visible {
			if len(page.Records) == cap(page.Records) {
				capacity := min(max(1, 2*cap(page.Records)), limit.Rows)
				growth := 64*capacity + 64*len(page.Records)
				if !b.admit(growth, 64*capacity) {
					pending = ApplicationControlRecord{}
					return finish()
				}
				rows := make([]ApplicationControlRecord, len(page.Records), capacity)
				copy(rows, page.Records)
				b.owned += 64 * (capacity - cap(page.Records))
				page.Records = rows
			}
			page.Records = append(page.Records, pending)
			b.owned += b.pending
			safe = pending.Key
		} else {
			safe = current.logical
		}
		b.pending = 0
		safePhysical = current.physical
		if !hasNext {
			page.Complete = true
			return page, nil
		}
		current = next
		b.currentScratch = 4 * max(current.physical, safePhysical)
		b.nextScratch = 0
		b.cursor = max(len(safe), len(current.logical))
	}
}
