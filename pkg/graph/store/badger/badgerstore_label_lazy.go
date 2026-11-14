package badger

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Lazy label maps for a transient read-only handle (R3 cold open,
// tasks/evidence/R3/coldread.txt, Config.TransientRead).
//
// The label index in RAM is built at every open from one walk of the label
// keys: 2.3 MB and a third of the CPU of an open at 50,000 rows. A handle that
// serves one read and closes (a cold shard at MaxOpenColdShards 0) mostly
// does not read a label at all. With a trusted open image that carries the
// per-label counts (OPENWALK) such an open skips the walk, sets the counters
// from the image as LabelIndexOnDisk's open does, and leaves the maps to be
// built per label when a read first asks for one. Unlike LabelIndexOnDisk it
// keeps every door that needs the maps in RAM (the doc-values and columnar
// doors decline in disk mode): a label's members are read from its label keys
// once and kept for the life of the handle, which is what the open's walk did
// for every label at once.
//
// The maps are the open walk's: a label key counts when its node is live
// (nodeIDs), as openLabelMembers and the walk decide. A read-only store has no
// pending writes and no write that could change a map, so a map built once
// stays right; labelLazy is fixed at open.

// labelKeyMembers reads tok's label keys through txn: the live nodes carrying
// the label, nil for a label without members. The open's walk and
// openLabelMembers count a key the same way. With walked not nil it is called
// before each key past the first want (the live members the open's image holds)
// and a non-nil error stops the walk: a store whose keys outnumber its live
// members (stale label keys) pays for them under a budget too.
func (bs *Store) labelKeyMembers(txn *badgerv4.Txn, tok uint16, want int, walked func(n int) error) (map[types.NodeID]struct{}, error) {
	prefix := binary.BigEndian.AppendUint16([]byte{storepkg.KeyLabel}, tok)
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	var set map[types.NodeID]struct{}
	keys := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		if len(key) != storepkg.SizeLabelIdx {
			continue
		}
		if keys++; walked != nil && keys > want {
			if err := walked(1); err != nil {
				return nil, err
			}
		}
		if nid := types.NodeID(storepkg.ParseIDFromKey(key, 3)); bs.nodeIDs.Has(int64(nid)) {
			if set == nil {
				set = make(map[types.NodeID]struct{})
			}
			set[nid] = struct{}{}
		}
	}
	return set, nil
}

// labelMembersLocked returns tok's members in the RAM label index, nil for a
// label without. With labelLazy (the open built no maps) it reads the label's
// keys on first use and keeps the map. The caller holds idxMu (R or W), which
// does not guard the lazy maps (lazyLabelMu does, taken inside), and only
// reads the set. The budgeted doors that reach it call labelMembersAdmitLocked.
func (bs *Store) labelMembersLocked(tok uint16) (map[types.NodeID]struct{}, error) {
	set, _, err := bs.labelMembersAdmitLocked(tok, nil)
	return set, err
}

// labelMembersAdmitLocked is labelMembersLocked for a read under a budget
// (R2 late counts): a lazy label whose map is not built yet is counted before
// its keys are walked, admit(n) once for the n live members the open's image
// holds (the label counter) and admit(1) for each key past them (a stale
// label key), so a read past its Candidates limit stops before the walk, or at
// the key that passes it, and no map is kept. counted reports that this call
// admitted the members (it built the map): a caller that admits the members it
// lists admits them only when counted is false. A label index in RAM, and a
// lazy map built already, cost nothing to read and are not counted here. A nil
// admit is labelMembersLocked.
func (bs *Store) labelMembersAdmitLocked(tok uint16, admit func(n int) error) (set map[types.NodeID]struct{}, counted bool, err error) {
	if !bs.labelLazy {
		return bs.labelIdx[tok], false, nil
	}
	bs.lazyLabelMu.Lock()
	defer bs.lazyLabelMu.Unlock()
	if set, ok := bs.lazyLabelSets[tok]; ok {
		return set, false, nil
	}
	want := 0
	if admit != nil {
		if v, ok := bs.labelCounts.Load(tok); ok {
			want = int(v.(*atomic.Int64).Load()) // #nosec G115 -- a count within int range
		}
		if want > 0 {
			if err := admit(want); err != nil {
				return nil, false, err
			}
		}
	}
	var refused error // the budget's refusal at a stale key, handed back as it is
	if err := bs.view(func(txn *badgerv4.Txn) error {
		var walked func(int) error
		if admit != nil {
			walked = func(n int) error {
				refused = admit(n)
				return refused
			}
		}
		var werr error
		set, werr = bs.labelKeyMembers(txn, tok, want, walked)
		return werr
	}); err != nil {
		if refused != nil {
			return nil, false, refused
		}
		return nil, false, fmt.Errorf("graph: read the members of label %d: %w", tok, err)
	}
	if bs.lazyLabelSets == nil {
		bs.lazyLabelSets = make(map[uint16]map[types.NodeID]struct{})
	}
	bs.lazyLabelSets[tok] = set
	bs.lazyLabelBuilds.Add(1)
	return set, admit != nil, nil
}

// labelSetsLocked is labelMembersLocked for each of toks, in order.
func (bs *Store) labelSetsLocked(toks []uint16) ([]map[types.NodeID]struct{}, error) {
	sets := make([]map[types.NodeID]struct{}, len(toks))
	for i, tok := range toks {
		set, err := bs.labelMembersLocked(tok)
		if err != nil {
			return nil, err
		}
		sets[i] = set
	}
	return sets, nil
}

// inAllSets reports whether id is in every one of sets.
func inAllSets(sets []map[types.NodeID]struct{}, id types.NodeID) bool {
	for _, s := range sets {
		if _, ok := s[id]; !ok {
			return false
		}
	}
	return true
}

// labelOtherThanLocked reports whether some label other than keep has a live
// member. A lazy store has no map to range over, so it asks the label counters
// the open set from the image (every write keeps them in step with the maps).
func (bs *Store) labelOtherThanLocked(keep uint16) bool {
	if !bs.labelLazy {
		for tok, set := range bs.labelIdx {
			if tok != keep && len(set) > 0 {
				return true
			}
		}
		return false
	}
	other := false
	bs.labelCounts.Range(func(k, v any) bool {
		tok, _ := k.(uint16)
		if n, ok := v.(*atomic.Int64); ok && tok != keep && n.Load() > 0 {
			other = true
		}
		return !other
	})
	return other
}
