package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// These are storage-cost candidates only. They admit no graph writes or grants.
// All candidates use the same real allocation range, common application records,
// and immutable application installation; no unused identity gets a marker.
const consumptionSlots = 4096
const consumptionBitmapBytes = consumptionSlots / 8
const consumptionTailRecords = 16
const consumptionTailBytes = 1024

var errConsumptionDuplicate = errors.New("test consumption: identity already used")

type consumptionKind string

const (
	consumptionRows   consumptionKind = "used-id-kv"
	consumptionBitmap consumptionKind = "bitmap-rewrite"
	consumptionDeltas consumptionKind = "delta-checkpoint"
)

var consumptionKinds = []consumptionKind{consumptionRows, consumptionBitmap, consumptionDeltas}

type consumptionFixture struct {
	name  string
	ids   []uint64
	batch int
}

func consumptionFixtures() []consumptionFixture {
	sparse := consumptionFixture{name: "sparse-singleton", batch: 1}
	dense := consumptionFixture{name: "dense-singleton", batch: 1}
	scattered := consumptionFixture{name: "scattered-16-pages", batch: 1}
	for i := range 128 {
		sparse.ids = append(sparse.ids, uint64(i*31+1))
	}
	for i := range 4096 {
		dense.ids = append(dense.ids, uint64(i+1))
	}
	for i := range 256 {
		scattered.ids = append(scattered.ids, uint64((i%16)*4096+i/16+1))
	}
	return []consumptionFixture{sparse, dense, {name: "dense-batch128", ids: slices.Clone(dense.ids), batch: 128}, scattered}
}

type consumptionLedger struct {
	kind  consumptionKind
	block idalloc.Reservation
}

func newConsumptionLedger(kind consumptionKind) (consumptionLedger, []byte, error) {
	authority, err := idalloc.NewAuthority([16]byte{2}, 1)
	if err != nil {
		return consumptionLedger{}, nil, err
	}
	state, err := idalloc.NewState(idalloc.GraphID{1}, authority, 65536)
	if err != nil {
		return consumptionLedger{}, nil, err
	}
	next, block, _, err := idalloc.Reserve(&state, idalloc.Request{Graph: idalloc.GraphID{1}, Authority: authority, Sequence: 1, Count: 65536})
	if err != nil {
		return consumptionLedger{}, nil, err
	}
	image, err := idalloc.MarshalCheckpoint(&next)
	return consumptionLedger{kind, block}, image, err
}
func (c consumptionLedger) key(tag byte, page, index uint64) []byte {
	b := append([]byte{tag}, c.block.Request.Graph[:]...)
	if tag == 0x40 {
		return binary.BigEndian.AppendUint64(b, page)
	}
	digest := c.block.Request.Digest()
	b = append(b, digest[:]...)
	b = binary.BigEndian.AppendUint64(b, page)
	if tag == 0x43 || tag == 0x44 {
		b = binary.BigEndian.AppendUint64(b, index)
	}
	return b
}
func (c consumptionLedger) position(id uint64) (uint64, uint16, error) {
	if id < c.block.First || id > c.block.Last {
		return 0, 0, ErrInvalid
	}
	offset := id - c.block.First
	return offset / consumptionSlots, uint16(offset % consumptionSlots), nil
}
func consumptionGet(ctx context.Context, v *ApplicationView, key []byte, limit int) ([]byte, bool, error) {
	row, found, err := v.Get(ctx, key, len(key)+limit)
	if err != nil {
		return nil, false, err
	}
	if found && row.Deleted {
		return nil, false, ErrCorrupt
	}
	return row.Value, found, nil
}

type consumptionDirectory struct {
	base, head     uint64
	records, bytes uint32
}

func (d consumptionDirectory) encode() []byte {
	b := binary.BigEndian.AppendUint64(nil, d.base)
	b = binary.BigEndian.AppendUint64(b, d.head)
	b = binary.BigEndian.AppendUint32(b, d.records)
	return binary.BigEndian.AppendUint32(b, d.bytes)
}
func (c consumptionLedger) page(ctx context.Context, v *ApplicationView, page uint64) ([consumptionBitmapBytes]byte, consumptionDirectory, error) {
	var bitmap [consumptionBitmapBytes]byte
	if c.kind == consumptionBitmap {
		b, found, err := consumptionGet(ctx, v, c.key(0x41, page, 0), consumptionBitmapBytes)
		if err != nil {
			return bitmap, consumptionDirectory{}, err
		}
		if found {
			if len(b) != len(bitmap) {
				return bitmap, consumptionDirectory{}, ErrCorrupt
			}
			copy(bitmap[:], b)
		}
		return bitmap, consumptionDirectory{}, nil
	}
	b, found, err := consumptionGet(ctx, v, c.key(0x42, page, 0), 24)
	if err != nil || !found {
		return bitmap, consumptionDirectory{}, err
	}
	if len(b) != 24 {
		return bitmap, consumptionDirectory{}, ErrCorrupt
	}
	d := consumptionDirectory{binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:16]), binary.BigEndian.Uint32(b[16:20]), binary.BigEndian.Uint32(b[20:24])}
	root, err := v.Root()
	if err != nil {
		return bitmap, d, err
	}
	if d.records > consumptionTailRecords || d.bytes > consumptionTailBytes || d.base > root.Index || d.head > root.Index {
		return bitmap, d, ErrCorrupt
	}
	if d.base != 0 {
		b, found, err := consumptionGet(ctx, v, c.key(0x44, page, d.base), consumptionBitmapBytes)
		if err != nil {
			return bitmap, d, err
		}
		if !found || len(b) != len(bitmap) {
			return bitmap, d, ErrCorrupt
		}
		copy(bitmap[:], b)
	}
	index := d.head
	var records, total uint32
	for index != 0 {
		if records == consumptionTailRecords || index <= d.base {
			return bitmap, d, ErrCorrupt
		}
		b, found, err := consumptionGet(ctx, v, c.key(0x43, page, index), 10+2*128)
		if err != nil {
			return bitmap, d, err
		}
		if !found || len(b) < 10 {
			return bitmap, d, ErrCorrupt
		}
		previous := binary.BigEndian.Uint64(b[:8])
		count := int(binary.BigEndian.Uint16(b[8:10]))
		if previous >= index || count == 0 || count > 128 || len(b) != 10+2*count {
			return bitmap, d, ErrCorrupt
		}
		var last uint16
		for i := range count {
			slot := binary.BigEndian.Uint16(b[10+2*i:])
			if slot >= consumptionSlots || i > 0 && slot <= last {
				return bitmap, d, ErrCorrupt
			}
			bitmap[slot/8] |= 1 << (slot % 8)
			last = slot
		}
		records++
		total += uint32(len(b))
		index = previous
	}
	if records != d.records || total != d.bytes {
		return bitmap, d, ErrCorrupt
	}
	return bitmap, d, nil
}
func (c consumptionLedger) contains(ctx context.Context, v *ApplicationView, id uint64) (bool, error) {
	page, slot, err := c.position(id)
	if err != nil {
		return false, err
	}
	if c.kind == consumptionRows {
		b, found, err := consumptionGet(ctx, v, c.key(0x40, id, 0), 1)
		if err != nil {
			return false, err
		}
		if found && (len(b) != 1 || b[0] != 1) {
			return false, ErrCorrupt
		}
		return found, nil
	}
	bitmap, _, err := c.page(ctx, v, page)
	return bitmap[slot/8]&(1<<(slot%8)) != 0, err
}
func (c consumptionLedger) stage(ctx context.Context, v *ApplicationView, index uint64, ids []uint64) ([]KV, error) {
	ordered := slices.Sorted(slices.Values(ids))
	if len(ordered) == 0 || len(ordered) > 128 {
		return nil, ErrInvalid
	}
	groups := make(map[uint64][]uint16)
	for i, id := range ordered {
		if i > 0 && id == ordered[i-1] {
			return nil, errConsumptionDuplicate
		}
		page, slot, err := c.position(id)
		if err != nil {
			return nil, err
		}
		groups[page] = append(groups[page], slot)
	}
	if len(groups) > 16 {
		return nil, ErrLimit
	}
	var writes []KV
	if c.kind == consumptionRows {
		for _, id := range ordered {
			used, err := c.contains(ctx, v, id)
			if err != nil {
				return nil, err
			}
			if used {
				return nil, errConsumptionDuplicate
			}
			writes = append(writes, KV{Key: c.key(0x40, id, 0), Value: []byte{1}})
		}
	} else {
		for _, page := range slices.Sorted(maps.Keys(groups)) {
			bitmap, d, err := c.page(ctx, v, page)
			if err != nil {
				return nil, err
			}
			slots := groups[page]
			for _, slot := range slots {
				if bitmap[slot/8]&(1<<(slot%8)) != 0 {
					return nil, errConsumptionDuplicate
				}
				bitmap[slot/8] |= 1 << (slot % 8)
			}
			if c.kind == consumptionBitmap {
				writes = append(writes, KV{Key: c.key(0x41, page, 0), Value: slices.Clone(bitmap[:])})
				continue
			}
			delta := binary.BigEndian.AppendUint64(nil, d.head)
			delta = binary.BigEndian.AppendUint16(delta, uint16(len(slots)))
			for _, slot := range slots {
				delta = binary.BigEndian.AppendUint16(delta, slot)
			}
			if d.records == consumptionTailRecords || len(delta) > consumptionTailBytes-int(d.bytes) {
				writes = append(writes, KV{Key: c.key(0x44, page, index), Value: slices.Clone(bitmap[:])})
				d = consumptionDirectory{base: index}
			} else {
				writes = append(writes, KV{Key: c.key(0x43, page, index), Value: delta})
				d.head = index
				d.records++
				d.bytes += uint32(len(delta))
			}
			writes = append(writes, KV{Key: c.key(0x42, page, 0), Value: d.encode()})
		}
	}
	slices.SortFunc(writes, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
	return writes, nil
}

// Identical envelopes contain a140-byte opaque image, exact ID list CDC and a
// fixed outcome. This is not the graph root codec or a graph transaction API.
func consumptionBatch(previous []byte, base uint64, ids []uint64, writes []KV) ApplicationBatch {
	image := make([]byte, 140)
	copy(image, previous)
	binary.BigEndian.PutUint64(image[8:16], base+1)
	var changes []byte
	for _, id := range ids {
		changes = binary.BigEndian.AppendUint64(changes, id)
	}
	digest := sha256.Sum256(append(slices.Clone(previous), changes...))
	copy(image[52:84], digest[:])
	outcome := make([]byte, 48)
	copy(outcome, digest[:])
	binary.BigEndian.PutUint64(outcome[32:40], base+1)
	return ApplicationBatch{BaseIndex: base, BaseImageHash: sha256.Sum256(previous), Image: image, Writes: writes, Changes: changes, Outcome: outcome}
}
func installConsumption(s *Store, b ApplicationBatch) error {
	index := b.BaseIndex + 1
	entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: slices.Clone(b.Changes)}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{entry}}); err != nil {
		return err
	}
	return s.InstallApplication(index, b)
}
func consumptionOpen(tb testing.TB, fs vfs.FS, dir string, kind consumptionKind) (*Store, Config, consumptionLedger, uint64) {
	tb.Helper()
	ledger, allocation, err := newConsumptionLedger(kind)
	if err != nil {
		tb.Fatal(err)
	}
	cfg := Config{Dir: dir, FS: fs, Create: true, Application: DefaultApplicationPolicy(1)}
	s, err := Open(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, make([]byte, 140)); err != nil {
		tb.Fatal(err)
	}
	digest := ledger.block.Request.Digest()
	grant := binary.BigEndian.AppendUint64(slices.Clone(digest[:]), ledger.block.First)
	grant = binary.BigEndian.AppendUint64(grant, ledger.block.Last)
	// One same fixed recipient and committed reservation/checkpoint for all cases.
	session := make([]byte, 40)
	session[0], session[16] = 3, 4
	binary.BigEndian.PutUint64(session[32:], 1)
	writes := []KV{{Key: []byte("common/allocator"), Value: allocation}, {Key: []byte("common/grant"), Value: grant}, {Key: []byte("common/recipient"), Value: session}}
	b := consumptionBatch(make([]byte, 140), 1, nil, writes)
	if err := installConsumption(s, b); err != nil {
		tb.Fatal(err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil {
		tb.Fatal(err)
	}
	return s, cfg, ledger, usage.RetainedBytes
}
func consumptionCommit(tb testing.TB, s *Store, ledger consumptionLedger, ids []uint64) (uint64, uint64, int) {
	tb.Helper()
	base, image, err := s.Checkpoint()
	if err != nil {
		tb.Fatal(err)
	}
	v, err := s.ApplicationView(base)
	if err != nil {
		tb.Fatal(err)
	}
	writes, err := ledger.stage(tb.Context(), v, base+1, ids)
	if closeErr := v.Close(); err != nil || closeErr != nil {
		tb.Fatal(err, closeErr)
	}
	b := consumptionBatch(image, base, ids, writes)
	usage, err := s.ApplicationLimits().Preflight(b, s.Limits())
	if err != nil {
		tb.Fatal(err)
	}
	if err := installConsumption(s, b); err != nil {
		tb.Fatal(err)
	}
	// Representation bytes are actual conservative encoded KV retention,
	// including escaped keys/version suffixes/frames, not just current bitmap.
	common := uint64(3*(9+appFrameBytes) + len(b.Image) + len(b.Changes) + len(b.Outcome))
	return base + 1, usage.RetainedBytes - common, len(writes)
}
func consumptionCompact(ctx context.Context, s *Store) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Compact(ctx, []byte{0}, []byte{255}, false)
}
func consumptionAssert(tb testing.TB, s *Store, ledger consumptionLedger, index uint64, used []uint64) {
	tb.Helper()
	v, err := s.ApplicationView(index)
	if err != nil {
		tb.Fatal(err)
	}
	defer v.Close()
	var expected [16 * consumptionSlots]bool
	for _, id := range used {
		expected[id-1] = true
	}
	// Exact asserted set across all used numbers and surrounding sparse holes,
	// plus explicit unused/rejected/canonical-unused candidate identities.
	probes := slices.Clone(used)
	for i := range 4096 {
		probes = append(probes, uint64(i+1))
	}
	probes = append(probes, 65534, 65535, 65536)
	for _, id := range probes {
		got, err := ledger.contains(tb.Context(), v, id)
		if err != nil || got != expected[id-1] {
			tb.Fatalf("%s root%d id%d got%v want%v err%v", ledger.kind, index, id, got, expected[id-1], err)
		}
	}
}
func TestIDConsumptionRetainedMembershipAndCompaction(t *testing.T) {
	for _, kind := range consumptionKinds {
		for _, fixture := range consumptionFixtures() {
			t.Run(fmt.Sprintf("%s/%s", kind, fixture.name), func(t *testing.T) {
				ids := fixture.ids[:min(len(fixture.ids), 256)]
				fs := vfs.NewMem()
				s, cfg, ledger, _ := consumptionOpen(t, fs, "consumption", kind)
				initial := uint64(2)
				middle := initial
				var prefix []uint64
				for from := 0; from < len(ids); from += fixture.batch {
					to := min(from+fixture.batch, len(ids))
					index, _, _ := consumptionCommit(t, s, ledger, ids[from:to])
					if len(prefix) == 0 {
						middle = index
						prefix = slices.Clone(ids[:to])
					}
				}
				current, _, err := s.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				consumptionAssert(t, s, ledger, initial, nil)
				consumptionAssert(t, s, ledger, middle, prefix)
				consumptionAssert(t, s, ledger, current, ids)
				v, err := s.ApplicationView(current)
				if err != nil {
					t.Fatal(err)
				}
				if writes, err := ledger.stage(t.Context(), v, current+1, []uint64{65536, ids[0]}); !errors.Is(err, errConsumptionDuplicate) || len(writes) != 0 {
					t.Fatal(writes, err)
				}
				if writes, err := ledger.stage(t.Context(), v, current+1, []uint64{65536, 65536}); !errors.Is(err, errConsumptionDuplicate) || len(writes) != 0 {
					t.Fatal(writes, err)
				}
				if err := v.Close(); err != nil {
					t.Fatal(err)
				}
				if err := consumptionCompact(t.Context(), s); err != nil {
					t.Fatal(err)
				}
				consumptionAssert(t, s, ledger, middle, prefix)
				consumptionAssert(t, s, ledger, current, ids)
				before, err := s.ApplicationUsage()
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				cfg.Create = false
				s, err = Open(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				after, err := s.ApplicationUsage()
				if err != nil || after.RetainedBytes != before.RetainedBytes || after.RetainedRecords != before.RetainedRecords {
					t.Fatal(before, after, err)
				}
				consumptionAssert(t, s, ledger, initial, nil)
				consumptionAssert(t, s, ledger, middle, prefix)
				consumptionAssert(t, s, ledger, current, ids)
				if err := s.ScrubApplication(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
