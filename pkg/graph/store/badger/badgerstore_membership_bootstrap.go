package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

const historicalMembershipProgressKey = "historical_membership_progress_v1"

// HistoricalMembershipLimits bound each auxiliary-index bootstrap page.
type HistoricalMembershipLimits struct {
	MaxRows  int
	MaxBytes int64
}

// HistoricalMembershipProgress describes a resumable complete current/history
// inventory. It acknowledges only durably flushed postings and cursor bytes.
type HistoricalMembershipProgress struct {
	Version  int    `json:"version"`
	Phase    int    `json:"phase"`
	After    []byte `json:"after,omitempty"`
	Rows     int64  `json:"rows"`
	Complete bool   `json:"complete"`
}

// HistoricalMembershipStatus reports durable bootstrap progress without
// mutating the directory. It is available on both writable and readonly stores.
func (bs *Store) HistoricalMembershipStatus() (HistoricalMembershipProgress, error) {
	if err := bs.checkOpen(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	if !bs.historicalMembershipOnDisk {
		return HistoricalMembershipProgress{}, storecontract.ErrCapabilityNotSupported
	}
	return bs.historicalMembershipProgress()
}

func (bs *Store) historicalMembershipProgress() (HistoricalMembershipProgress, error) {
	p := HistoricalMembershipProgress{Version: 1}
	raw, err := bs.MetaGet(historicalMembershipProgressKey)
	if err != nil {
		return p, err
	}
	if len(raw) == 0 {
		return p, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("graph: historical membership progress: %w", err)
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return p, fmt.Errorf("%w: trailing historical membership progress", ErrInvalidStoreMutation)
	}
	if p.Version != 1 || p.Phase < 0 || p.Phase > 4 || p.Rows < 0 || p.Complete != (p.Phase == 4) {
		return p, fmt.Errorf("%w: historical membership progress", ErrInvalidStoreMutation)
	}
	if len(p.After) > 0 {
		prefixes := []byte{storepkg.KeyNode, storepkg.KeyHistNode, storepkg.KeyRel, storepkg.KeyHistRel}
		want := storepkg.SizeNodeKey
		if p.Phase%2 == 1 {
			want = storepkg.SizeHistKey
		}
		if p.Complete || len(p.After) != want || p.After[0] != prefixes[p.Phase] || (binary.BigEndian.Uint64(p.After[1:9]) == 0 || binary.BigEndian.Uint64(p.After[1:9]) > math.MaxInt64) {
			return p, fmt.Errorf("%w: historical membership cursor", ErrInvalidStoreMutation)
		}
	}
	return p, nil
}

// BuildHistoricalMembership advances the opt-in index using one bounded page.
// Every current/history writer records postings before publishing its row from
// the moment the option is enabled. Scans may therefore resume between writes:
// a row moved behind the cursor is also recorded by its mutation door.
// Queries never trust a partial build. Call until Complete before serving a
// workload that requires bounded historical membership reads.
// The graph owner must serialize this call with GraphTx/group-commit windows;
// an already active window is rejected rather than flushing its mutations.
func (bs *Store) BuildHistoricalMembership(ctx context.Context, limits HistoricalMembershipLimits) (HistoricalMembershipProgress, error) {
	if err := ctx.Err(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	if err := bs.checkWritable(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	if !bs.historicalMembershipOnDisk {
		return HistoricalMembershipProgress{}, storecontract.ErrCapabilityNotSupported
	}
	if limits.MaxRows < 1 || limits.MaxRows > 10000 || limits.MaxBytes < 1 || limits.MaxBytes > 64<<20 {
		return HistoricalMembershipProgress{}, fmt.Errorf("%w: historical membership page limits", ErrInvalidStoreMutation)
	}
	if bs.groupCommit.Load() {
		return HistoricalMembershipProgress{}, fmt.Errorf("%w: membership bootstrap inside group commit", ErrInvalidStoreMutation)
	}
	bs.historicalMembershipMu.Lock()
	defer bs.historicalMembershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	// Commit pending source mutations before capturing the next source page.
	if err := bs.Flush(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	p, err := bs.historicalMembershipProgress()
	if err != nil || p.Complete {
		return p, err
	}
	prefixes := []byte{storepkg.KeyNode, storepkg.KeyHistNode, storepkg.KeyRel, storepkg.KeyHistRel}
	var ops []writeOp
	used, rows := int64(0), 0
	bs.idxMu.Lock()
	err = bs.db.View(func(txn *badgerv4.Txn) error {
		for p.Phase < 4 && rows < limits.MaxRows {
			prefix := []byte{prefixes[p.Phase]}
			options := badgerv4.DefaultIteratorOptions
			options.PrefetchValues = false
			options.Prefix = prefix
			it := txn.NewIterator(options)
			seek := prefix
			if len(p.After) > 0 {
				seek = p.After
			}
			it.Seek(seek)
			if len(p.After) > 0 && it.ValidForPrefix(prefix) && bytes.Equal(it.Item().Key(), p.After) {
				it.Next()
			}
			exhausted := true
			for ; it.ValidForPrefix(prefix); it.Next() {
				if err := ctx.Err(); err != nil {
					it.Close()
					return err
				}
				item := it.Item()
				key := item.KeyCopy(nil)
				want := storepkg.SizeNodeKey
				if p.Phase%2 == 1 {
					want = storepkg.SizeHistKey
				}
				if len(key) != want {
					continue
				}
				if item.ValueSize() > limits.MaxBytes-used {
					exhausted = false
					break
				}
				raw, err := item.ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				postings, err := bs.historicalMembershipRowOps(txn, p.Phase, key, raw)
				if err != nil {
					it.Close()
					return err
				}
				cost := int64(len(raw))
				for _, op := range postings {
					cost += int64(len(op.key) + len(op.value))
				}
				if cost > limits.MaxBytes-used {
					exhausted = false
					break
				}
				used += cost
				rows++
				p.Rows++
				p.After = key
				ops = append(ops, postings...)
				if rows >= limits.MaxRows {
					exhausted = false
					break
				}
			}
			it.Close()
			if !exhausted {
				break
			}
			p.Phase++
			p.After = nil
		}
		return nil
	})
	if err == nil && rows == 0 && p.Phase < 4 {
		err = fmt.Errorf("%w: one historical membership row exceeds page byte budget", ErrInvalidStoreMutation)
	}
	if err == nil {
		p.Complete = p.Phase == 4
		bs.appendOps(ops...)
	}
	bs.idxMu.Unlock()
	if err != nil {
		return HistoricalMembershipProgress{}, err
	}
	if err := bs.Flush(); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	// WriteBatch can split transactions. The cursor is deliberately published
	// only AFTER every posting is flushed under mandatory SyncWrites. A crash
	// before this small atomic commit replays the same idempotent page.
	raw, err := json.Marshal(p)
	if err != nil {
		return HistoricalMembershipProgress{}, err
	}
	if err := bs.MetaSet(historicalMembershipProgressKey, raw); err != nil {
		return HistoricalMembershipProgress{}, err
	}
	bs.historicalMembershipReady.Store(p.Complete)
	return p, nil
}

func (bs *Store) historicalMembershipRowOps(txn *badgerv4.Txn, phase int, key, raw []byte) ([]writeOp, error) {
	id := storepkg.ParseIDFromKey(key, 1)
	version := uint64(0)
	if phase%2 == 1 {
		version = historyVersionFromKey(key)
	}
	local := func(v uint64) ([]byte, bool) {
		k := storepkg.HistNodeKey(id, v)
		if phase >= 2 {
			k = storepkg.HistRelKey(id, v)
		}
		i, err := txn.Get(k)
		if err != nil {
			return nil, false
		}
		b, err := i.ValueCopy(nil)
		return b, err == nil
	}
	if phase < 2 {
		var wire storepkg.NodeWire
		var err error
		if phase == 1 {
			wire, err = bs.reconstructNodeHistoryWire(id, version, raw, local)
		} else {
			err = storepkg.SafeUnmarshal(raw, &wire)
		}
		if err != nil {
			return nil, err
		}
		n, err := bs.decodeNodeWireForKey(wire, id)
		if err != nil {
			return nil, err
		}
		return nodeDiskMembershipOps(n), nil
	}
	var wire storepkg.RelWire
	var err error
	if phase == 3 {
		wire, err = bs.reconstructRelHistoryWire(id, version, raw, local)
	} else {
		err = storepkg.SafeUnmarshal(raw, &wire)
	}
	if err != nil {
		return nil, err
	}
	r, err := bs.decodeRelWireForKey(wire, id)
	if err != nil {
		return nil, err
	}
	return relDiskMembershipOps(r), nil
}
