package tiered

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ValidTimeMintCapability = (*Store)(nil)

// Valid-time minting (ADR-0013 step 2). With Config.MintAtValidFrom the store
// mints event IDs at their valid time on node field vtField, reserved for it
// (core keeps IngestLanes and SnowflakeNodeID clear of it). An ID is a counter
// c in units of one step, (microseconds since the epoch << 10 | step), placed
// as (c>>10)<<15 | vtField<<10 | c&1023. Per calendar window the store mints
// max(instant, high + 1), or, when that would leave the window, walks down
// from the window's lowest ID, so no value is handed out twice (nodes and
// relationships share the cursors). The cursors are leased: the lease ends
// live in the reference shard's meta (vtmint/<window start ms>, low and high,
// 8 bytes each), written durably before an ID of a lease is returned, so a
// restart starts past every ID a crash may have handed out.
const (
	vtField     = storecontract.ValidTimeMintField
	vtLease     = 1 << 20 // steps a lease covers past the ID that takes it
	vtMetaKey   = "vtmint/"
	vtStepBits  = 10
	vtNodeShift = 10
	vtTimeShift = 15
	vtMaxMicros = 1<<48 - 1
)

// errWindowFull is the impossible case of a window whose cursors met.
var errWindowFull = errors.New("graph: valid-time window has no free ID")

// vtWindow is one calendar window's cursors: lo and hi bound every ID minted
// (or leased: after a restart the whole lease counts as used), leaseLo and
// leaseHi the persisted lease.
type vtWindow struct {
	mu               sync.Mutex
	loaded, used     bool
	start, end       int64 // the window in counter units, end exclusive
	lo, hi           int64
	leaseLo, leaseHi int64
}

// vtCounter is t's counter at step 0.
func vtCounter(t time.Time) int64 { return t.Sub(idlayout.Epoch).Microseconds() << vtStepBits }

// vtInIDRange reports whether an ID can carry t.
func vtInIDRange(t time.Time) bool {
	return !t.Before(idlayout.Epoch) && t.Sub(idlayout.Epoch).Microseconds() <= vtMaxMicros
}

func vtID(c int64) int64 {
	return (c>>vtStepBits)<<vtTimeShift | vtField<<vtNodeShift | c&(1<<vtStepBits-1)
}

// MintsAtValidTime reports whether the store mints (Config.MintAtValidFrom).
func (ts *Store) MintsAtValidTime() bool { return ts.calendar }

// MintNodeIDAt mints an event node's ID at its valid time (at; unset or 0: the
// clock). A reference label, or a store without MintAtValidFrom, is left to
// the caller's generator (ok false).
func (ts *Store) MintNodeIDAt(primaryLabel uint16, at types.Instant, set bool) (types.NodeID, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, false, err
	}
	if !ts.calendar || ts.ontology.ClassifyByToken(primaryLabel) == ClassReference {
		return 0, false, nil
	}
	id, err := ts.mintAt(ts.validInstant(at, set), nil)
	return types.NodeID(id), err == nil, err
}

// MintRelIDAt mints a relationship's ID inside its start node's window, where
// the relationship lives: at its valid time when that falls in the window,
// else at the window's next free ID. A reference or archived start node's
// relationship is minted at its valid time, or at the clock when an ID cannot
// carry that (a "valid from forever" stamp): its stored ValidFrom answers
// every valid-time read, the ID's time only places it.
func (ts *Store) MintRelIDAt(start types.NodeID, at types.Instant, set bool) (types.RelID, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, false, err
	}
	if !ts.calendar {
		return 0, false, nil
	}
	t := ts.validInstant(at, set)
	raw := start.SnowflakeID()
	archive := ts.refArchive.Load()
	if ts.refShard.HasNodeID(raw) || (archive != nil && archive.HasNodeID(raw)) {
		if !vtInIDRange(t) {
			t = ts.clock().In(ts.shardLoc)
		}
		id, err := ts.mintAt(t, nil)
		return types.RelID(id), err == nil, err
	}
	home := time.UnixMilli(int64(start.MintInstant())).In(ts.shardLoc)
	id, err := ts.mintAt(t, &home)
	return types.RelID(id), err == nil, err
}

// validInstant is at as a time, or the clock when the entity is untimed.
func (ts *Store) validInstant(at types.Instant, set bool) time.Time {
	if !set || at == 0 {
		return ts.clock().In(ts.shardLoc)
	}
	return time.UnixMilli(int64(at)).In(ts.shardLoc)
}

// mintAt mints at t in t's window, or in home's window (t clamped into it)
// when home is set.
func (ts *Store) mintAt(t time.Time, home *time.Time) (int64, error) {
	anchor := t
	if home != nil {
		anchor = *home
	}
	ws, we := shardWindowStart(anchor, ts.shardWindow), shardWindowEnd(anchor, ts.shardWindow)
	c := vtCounter(t)
	if home == nil && !vtInIDRange(t) {
		return 0, fmt.Errorf("%w (%s)", ErrValidFromOutsideIDRange, t.UTC().Format(time.RFC3339))
	}
	w, err := ts.vtWindowFor(ws, we)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.loaded {
		if err := ts.loadVTWindow(ws, w); err != nil {
			return 0, err
		}
	}
	c = min(max(c, w.start), w.end-1) // a relationship's instant outside its home window
	switch {
	case !w.used, c > w.hi, c < w.lo:
	case w.hi+1 < w.end:
		c = w.hi + 1
	case w.lo-1 >= w.start:
		c = w.lo - 1
	default:
		return 0, errWindowFull
	}
	if !w.used || c > w.leaseHi || c < w.leaseLo {
		lo, hi := w.leaseLo, w.leaseHi
		if !w.used {
			lo, hi = c, c
		}
		if c >= hi {
			hi = min(c+vtLease, w.end-1)
		}
		if c <= lo {
			lo = max(c-vtLease, w.start)
		}
		if err := ts.saveVTLease(ws, lo, hi); err != nil {
			return 0, err
		}
		w.leaseLo, w.leaseHi = lo, hi
	}
	if !w.used {
		w.lo, w.hi, w.used = c, c, true
	}
	w.lo, w.hi = min(w.lo, c), max(w.hi, c)
	return vtID(c), nil
}

// vtWindowFor returns the cursors of the window [ws, we), made on first use.
func (ts *Store) vtWindowFor(ws, we time.Time) (*vtWindow, error) {
	key := ws.UnixMilli()
	ts.vtMu.Lock()
	defer ts.vtMu.Unlock()
	if w, ok := ts.vtWindows[key]; ok {
		return w, nil
	}
	start := max(vtCounter(ws), 0)
	end := min(vtCounter(we), (vtMaxMicros+1)<<vtStepBits)
	if end <= start {
		return nil, fmt.Errorf("%w (window %s)", ErrValidFromOutsideIDRange, ws.UTC().Format(time.RFC3339))
	}
	if ts.vtWindows == nil {
		ts.vtWindows = make(map[int64]*vtWindow)
	}
	w := &vtWindow{start: start, end: end}
	ts.vtWindows[key] = w
	return w, nil
}

func vtLeaseKey(ws time.Time) string { return fmt.Sprintf("%s%d", vtMetaKey, ws.UnixMilli()) }

// loadVTWindow reads the window's lease: every ID inside it may have been
// handed out before the last close or crash, so the whole lease counts as used.
func (ts *Store) loadVTWindow(ws time.Time, w *vtWindow) error {
	v, err := ts.refShard.MetaGet(vtLeaseKey(ws))
	if err != nil {
		return fmt.Errorf("graph: read valid-time mint lease: %w", err)
	}
	switch len(v) {
	case 0:
	case 16:
		w.leaseLo, w.leaseHi = int64(binary.BigEndian.Uint64(v[:8])), int64(binary.BigEndian.Uint64(v[8:])) // #nosec G115 -- counters are below 2^58
		w.lo, w.hi, w.used = w.leaseLo, w.leaseHi, true
	default:
		return fmt.Errorf("graph: valid-time mint lease %q: %d bytes: %w", vtLeaseKey(ws), len(v), storecontract.ErrCorruptWire)
	}
	w.loaded = true
	return nil
}

// saveVTLease stores the window's lease durably before an ID of it is returned.
func (ts *Store) saveVTLease(ws time.Time, lo, hi int64) error {
	v := binary.BigEndian.AppendUint64(make([]byte, 0, 16), uint64(lo)) // #nosec G115 -- counters are non-negative
	v = binary.BigEndian.AppendUint64(v, uint64(hi))                    // #nosec G115 -- counters are non-negative
	if err := ts.refShard.MetaSetDurable(vtLeaseKey(ws), v); err != nil {
		return fmt.Errorf("graph: save valid-time mint lease: %w", err)
	}
	return nil
}
