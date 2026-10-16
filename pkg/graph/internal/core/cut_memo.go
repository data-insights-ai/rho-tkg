package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Values computed from the state recorded at a pin (count timelines, value
// statistics, value classes) are memoised under the state cuts'
// invalidation: each entry keeps the cut epoch it was built at (cutEpoch: a
// history rewrite at or below a noted pin, an in-place update, a clamping
// delete) and is used only while the epoch holds, and only once every write
// stamped at or below the pin has landed (pinLanded); forward writes leave
// it. One memo type and one build loop serve every such value.

// cutMemo holds the memoised values of one kind. Guarded by
// asOfColumnCache.mu; created on first use, dropped by clear.
type cutMemo[K comparable, V any] map[K]cutMemoEntry[V]

type cutMemoEntry[V any] struct {
	epoch uint64
	v     V
}

// cutMemoCap bounds the entries of one memo (each a value of one label or
// type at one pin).
const cutMemoCap = 256

// cutMemoAttempts is how many builds a call tries while writers move the
// cut epoch under it before it declines.
const cutMemoAttempts = 3

func memoGet[K comparable, V any](a *asOfColumnCache, m *cutMemo[K, V], key K, epoch uint64) (V, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := (*m)[key]
	if !ok || e.epoch != epoch {
		var zero V
		return zero, false
	}
	return e.v, true
}

func memoPut[K comparable, V any](a *asOfColumnCache, m *cutMemo[K, V], key K, epoch uint64, v V) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if *m == nil {
		*m = make(cutMemo[K, V])
	}
	if len(*m) >= cutMemoCap {
		for k, e := range *m {
			if e.epoch != epoch || len(*m) >= cutMemoCap {
				delete(*m, k)
			}
		}
	}
	(*m)[key] = cutMemoEntry[V]{epoch: epoch, v: v}
}

// memoAtPin returns key's value at pin: the memoised one at the current cut
// epoch of the entity kind (rel), else build's, built again when a write
// moved the epoch during the build and memoised when the pin had landed.
// ok=false declines: build declined, or writers moved the epoch on every
// attempt.
func memoAtPin[K comparable, V any](c *Core, m *cutMemo[K, V], rel bool, pin types.Instant, key K,
	build func() (V, bool, error)) (V, bool, error) {
	var zero V
	a := c.asOfColumns
	a.notePin(pin) // before the epoch is read (notePin)
	for range cutMemoAttempts {
		epoch := a.cutEpoch(rel)
		if v, ok := memoGet(a, m, key, epoch); ok {
			return v, true, nil
		}
		landed := c.pinLanded(pin)
		v, ok, err := build()
		if err != nil || !ok {
			return zero, false, err
		}
		if a.cutEpoch(rel) != epoch {
			continue // a history rewrite raced the read
		}
		if landed {
			memoPut(a, m, key, epoch, v)
		}
		return v, true, nil
	}
	return zero, false, nil
}
