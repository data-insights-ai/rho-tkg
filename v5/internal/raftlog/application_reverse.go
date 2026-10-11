package raftlog

import (
	"bytes"
	"context"
	"errors"
)

// ScanReverse enumerates visible logical keys in descending byte order within
// [lower, upper), seeking over version runs without decoding history or building
// a resident key map. nil upper is unbounded; nil lower starts the namespace.
// before is exclusive and must be empty or a key in this exact range; nil before
// starts immediately below upper. Reuse Next as before with the same view/range.
// Tombstones and future-only keys are omitted but consume the same work budget
// as Scan. Owned output and one borrowed lookahead obey Scan's capacity bounds.
func (v *ApplicationView) ScanReverse(ctx context.Context, lower, upper, before []byte, b ReadBudget) (page ApplicationPage, err error) {
	if err = v.lock(ctx); err != nil {
		return page, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if b.Rows < 1 || b.Bytes < 1 || len(upper) > 0 && bytes.Compare(lower, upper) >= 0 || len(before) > 0 && (bytes.Compare(before, lower) < 0 || len(upper) > 0 && bytes.Compare(before, upper) >= 0) {
		return page, ErrInvalid
	}
	if b.Rows > p.MaxPageRows || b.Bytes > p.MaxPageBytes || len(lower) > p.MaxKeyBytes || len(upper) > p.MaxKeyBytes || len(before) > p.MaxKeyBytes {
		return page, ErrLimit
	}
	lo := []byte{bankTag(v.bank, appDataTag)}
	if len(lower) > 0 {
		lo = bankPrefix(v.bank, lower)
	}
	hi := []byte{bankTag(v.bank, appDataTag) + 1}
	if len(upper) > 0 {
		hi = bankPrefix(v.bank, upper)
	}
	it, err := v.iterator(lo, hi)
	if err != nil {
		return page, v.fail(err)
	}
	defer func() {
		e := it.Close()
		err = errors.Join(err, e)
		if e != nil {
			v.s.poison = e
		}
	}()
	seek := hi
	if len(before) > 0 {
		seek = bankPrefix(v.bank, before)
	}
	// Work includes every visited key. Owned output additionally reserves the
	// complete Rows backing array, including capacity beyond its length.
	workBytes, outputBytes := 0, 0
	for valid := it.SeekLT(seek); valid; {
		if e := ctx.Err(); e != nil {
			return ApplicationPage{}, e
		}
		key, _, e := decodeBankAppKey(v.bank, it.Key(), p.MaxKeyBytes)
		if e != nil {
			return ApplicationPage{}, v.fail(e)
		}
		prefix := bankPrefix(v.bank, key)
		// SeekLT discovers the key through its oldest physical version. Seek
		// forward within that key to select its newest version at this view.
		// A future-only run may land on a higher key; never emit that key or
		// use it as the next reverse bound.
		visible := it.SeekGE(bankVersionKey(v.bank, key, v.index)) && bytes.HasPrefix(it.Key(), prefix)
		if e := it.Error(); e != nil {
			return ApplicationPage{}, v.fail(e)
		}
		var value []byte
		deleted := false
		if visible {
			actual, version, e := decodeBankAppKey(v.bank, it.Key(), p.MaxKeyBytes)
			if e != nil || !bytes.Equal(actual, key) || version > v.index {
				return ApplicationPage{}, v.fail(errors.Join(ErrCorrupt, e))
			}
			raw, readErr := it.ValueAndErr()
			if readErr != nil {
				return ApplicationPage{}, v.fail(storedReadFailure(readErr))
			}
			value, deleted, e = inspectAppFrame(it.Key(), raw, p.MaxValueBytes)
			if e != nil {
				return ApplicationPage{}, v.fail(e)
			}
		}
		// Every discovered logical key consumes work, even if it was created
		// after this view. Otherwise an old empty root could scan an unbounded
		// later database in a single Rows=1 call.
		cost := len(key) + len(value) + 64
		emit := visible && !deleted
		rowBytes, capacity := 0, cap(page.Rows)
		if emit {
			rowBytes = len(key) + len(value)
			if len(page.Rows) == capacity {
				// Grow geometrically where room permits, but never retain an
				// uncharged backing array. Header accounting is conservative
				// on supported Go architectures; byte copies have exact cap.
				available := b.Bytes - (outputBytes - 64*capacity) - rowBytes - len(key)
				capacity = min(max(1, 2*capacity), available/64, b.Rows)
			}
		}
		owned := outputBytes + 64*(capacity-cap(page.Rows)) + rowBytes + len(key)
		if page.Visited >= b.Rows || cost+len(key) > b.Bytes-workBytes || emit && capacity < len(page.Rows)+1 || owned > b.Bytes {
			if page.Visited == 0 {
				return ApplicationPage{}, ErrLimit
			}
			page.Bytes = max(workBytes+len(page.Next), outputBytes+len(page.Next))
			return page, nil
		}
		page.Visited++
		workBytes += cost
		page.Next = copyApplicationBytes(key)
		if emit {
			if capacity > cap(page.Rows) {
				rows := make([]KV, len(page.Rows), capacity)
				copy(rows, page.Rows)
				outputBytes += 64 * (capacity - cap(page.Rows))
				page.Rows = rows
			}
			page.Rows = append(page.Rows, KV{Key: copyApplicationBytes(key), Value: copyApplicationBytes(value)})
			outputBytes += rowBytes
		}
		valid = it.SeekLT(prefix)
	}
	if e := it.Error(); e != nil {
		return ApplicationPage{}, v.fail(e)
	}
	page.Complete = true
	page.Next = nil
	page.Bytes = max(workBytes, outputBytes)
	return page, nil
}
