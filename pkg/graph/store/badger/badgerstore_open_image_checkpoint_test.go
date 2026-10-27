package badger

import (
	"encoding/binary"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPENIMAGE step 2: a periodic open image (Config.OpenImageInterval) bounds
// what an open after a crash decodes by the writes since the last image, for
// a store that never closes cleanly.

const ckLabel = uint16(1)

func ckNode(id, k int64) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), ckLabel, nil)
	_ = n.SetProperty("k", k)
	return n
}

// ckStore opens dir; a new store gets a property and a temporal index.
func ckStore(t *testing.T, dir string) *Store {
	t.Helper()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if bs.nodeIDs.Len() == 0 { // a new store
		for _, err := range []error{bs.CreatePropertyIndex(ckLabel, "k"), bs.CreateTemporalIndex(ckLabel)} {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return bs
}

// ckCrash ends a session without Close: the write buffer flushed, no image.
func ckCrash(t *testing.T, bs *Store) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.SetDBClosedForTest(true)
	if err := bs.DBForTest().Close(); err != nil {
		t.Fatal(err)
	}
}

// ckCheckOpen reopens dir read-only and fails unless its index state equals
// the full rebuild's (a copy without its open state); it returns the rows the
// open decoded.
func ckCheckOpen(t *testing.T, dir string) int64 {
	t.Helper()
	oracle := t.TempDir()
	copyDirFiles(t, dir, oracle)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(DeleteOpenStateForTest); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	digest := func(d string) (string, int64) {
		var s *Store
		n := CountRowDecodesForTest(func() { s, err = New(Config{Dir: d, ReadOnly: true}) })
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		return s.IndexStateDigestForTest(), n
	}
	got, decoded := digest(dir)
	if want, _ := digest(oracle); got != want {
		t.Fatalf("open differs from the full rebuild:\n got: %.1200s\nwant: %.1200s", got, want)
	}
	return decoded
}

// ckHeader returns the kind and slot of dir's image header.
func ckHeader(t *testing.T, bs *Store) openImageHeader {
	t.Helper()
	var h openImageHeader
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(openImageHeaderKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var ok bool
			if h, ok = parseOpenImageHeader(val); !ok {
				t.Fatalf("unreadable header %x", val)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestCheckpointCrashReopenDecodesWritesSinceImage: a store that never closes
// writes a periodic image, then a few creates and updates, and crashes. The
// next open decodes those writes' rows only, at 10x the store as well, and
// ends in the full rebuild's index state.
func TestCheckpointCrashReopenDecodesWritesSinceImage(t *testing.T) {
	var decoded [2]int64
	for i, n := range [2]int64{500, 5_000} {
		dir := t.TempDir()
		bs := ckStore(t, dir)
		for id := int64(1); id <= n; id++ {
			if err := bs.PutNode(ckNode(id, id%9)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.checkpointOpenImage(); err != nil {
			t.Fatal(err)
		}
		for id := n + 1; id <= n+5; id++ {
			if err := bs.PutNode(ckNode(id, 1)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.ReplaceNode(ckNode(7, 100)); err != nil { // an imaged node changes its value
			t.Fatal(err)
		}
		if err := bs.DeleteNode(types.NodeID(snowflake.ID(8))); err != nil {
			t.Fatal(err)
		}
		ckCrash(t, bs)
		decoded[i] = ckCheckOpen(t, dir)
	}
	t.Logf("rows decoded after the image and 7 writes: %d at 500 nodes, %d at 5,000", decoded[0], decoded[1])
	if decoded[0] == 0 || decoded[1] != decoded[0] {
		t.Fatalf("decoded %d rows at 500 nodes and %d at 5,000: want the same few", decoded[0], decoded[1])
	}
}

// TestCheckpointWriteRacingImage: writes that commit after the image's encode
// and before its header (an update of an imaged node, a create) are applied
// by the next open, whether or not the header is then the store's newest.
func TestCheckpointWriteRacingImage(t *testing.T) {
	for _, later := range []bool{false, true} {
		dir := t.TempDir()
		bs := ckStore(t, dir)
		for id := int64(1); id <= 50; id++ {
			if err := bs.PutNode(ckNode(id, id%5)); err != nil {
				t.Fatal(err)
			}
		}
		openImageAfterEncodeHook = func() {
			openImageAfterEncodeHook = nil
			if err := bs.ReplaceNode(ckNode(3, 77)); err != nil {
				t.Error(err)
			}
			if err := bs.PutNode(ckNode(51, 77)); err != nil {
				t.Error(err)
			}
			if err := bs.Flush(); err != nil {
				t.Error(err)
			}
		}
		if err := bs.checkpointOpenImage(); err != nil {
			t.Fatal(err)
		}
		if later { // the header is not the newest commit
			if err := bs.PutNode(ckNode(52, 1)); err != nil {
				t.Fatal(err)
			}
		}
		ckCrash(t, bs)
		if decoded := ckCheckOpen(t, dir); decoded == 0 {
			t.Fatalf("later writes %v: the open decoded no row: it trusted the periodic image whole", later)
		}
	}
}

// TestCheckpointCrashWhileWritingKeepsPrevious: a crash after an image's
// chunks and before its header leaves the previous image (the other slot) in
// use: the open decodes only the writes since that one.
func TestCheckpointCrashWhileWritingKeepsPrevious(t *testing.T) {
	defer SetOpenImageChunkBytesForTest(256)() // several chunks per image
	dir := t.TempDir()
	bs := ckStore(t, dir)
	for id := int64(1); id <= 400; id++ {
		if err := bs.PutNode(ckNode(id, id%9)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.checkpointOpenImage(); err != nil {
		t.Fatal(err)
	}
	first := ckHeader(t, bs)
	for id := int64(401); id <= 403; id++ {
		if err := bs.PutNode(ckNode(id, 1)); err != nil {
			t.Fatal(err)
		}
	}
	openImageBeforeHeaderHook = func() { // the crash
		openImageBeforeHeaderHook = nil
		bs.SetDBClosedForTest(true)
		if err := bs.DBForTest().Close(); err != nil {
			t.Error(err)
		}
	}
	defer func() { openImageBeforeHeaderHook = nil }()
	if err := bs.checkpointOpenImage(); err == nil {
		t.Fatal("the image's header was written on a closed store")
	}
	decoded := ckCheckOpen(t, dir)
	if decoded == 0 || decoded > 10 {
		t.Fatalf("the open after a crash while writing the image decoded %d rows: want the 3 writes since the previous image (slot %d)", decoded, first.slot)
	}
}

// TestOpenImageIntervalWritesImages: with OpenImageInterval the running store
// writes periodic images; Close writes a Close image, which the next open
// trusts whole. A negative interval is refused.
func TestOpenImageIntervalWritesImages(t *testing.T) {
	if _, err := New(Config{InMemory: true, OpenImageInterval: -time.Second}); err == nil {
		t.Fatal("a negative OpenImageInterval was accepted")
	}
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, OpenImageInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(ckNode(1, 1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var found bool
		_ = bs.db.View(func(txn *badgerv4.Txn) error {
			_, err := txn.Get(openImageHeaderKey)
			found = err == nil
			return nil
		})
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no periodic image within 5 s at a 10 ms interval")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h := ckHeader(t, bs); h.kind != openImageKindCheckpoint {
		t.Fatalf("the running store's image has kind %d, want a periodic one", h.kind)
	}
	// Close while a periodic image is being written waits for it.
	encoding := make(chan struct{})
	openImageAfterEncodeHook = func() {
		openImageAfterEncodeHook = nil
		close(encoding)
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { openImageAfterEncodeHook = nil }()
	if err := bs.PutNode(ckNode(2, 1)); err != nil { // something for the next image
		t.Fatal(err)
	}
	<-encoding
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bs.imageDone:
	default:
		t.Fatal("Close returned while a periodic image was being written")
	}
	if decoded := ckCheckOpen(t, dir); decoded != 0 {
		t.Fatalf("the open after Close decoded %d rows: it did not trust the Close image", decoded)
	}
}

// TestOpenImageSlotsHoldNoStaleChunks: each slot holds exactly the chunks its
// last image wrote, however the image sizes change.
func TestOpenImageSlotsHoldNoStaleChunks(t *testing.T) {
	defer SetOpenImageChunkBytesForTest(256)()
	bs := ckStore(t, t.TempDir())
	defer bs.Close()
	written := [2]int{-1, -1} // chunks of each slot's last image
	for i, n := range []int64{400, 400, 20, 20, 200} {
		for id := int64(1); id <= 400; id++ {
			_ = bs.DeleteNode(types.NodeID(snowflake.ID(id)))
		}
		for id := int64(1); id <= n; id++ {
			if err := bs.PutNode(ckNode(id, id)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.checkpointOpenImage(); err != nil {
			t.Fatal(err)
		}
		h := ckHeader(t, bs)
		written[h.slot] = h.chunks
		for slot := range 2 {
			var keys int
			_ = bs.db.View(func(txn *badgerv4.Txn) error {
				opts := badgerv4.DefaultIteratorOptions
				opts.PrefetchValues = false
				opts.Prefix = []byte(openImageChunkKey(slot, 0)[:len(openImageChunkKey(slot, 0))-1])
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					keys++
				}
				return nil
			})
			if want := max(written[slot], 0); keys != want {
				t.Fatalf("image %d: slot %d holds %d chunks, its last image wrote %d", i, slot, keys, want)
			}
		}
	}
}

// TestCheckpointSkipsIdleStore: a periodic image with nothing committed since
// the last image (written, or trusted whole at open) writes nothing, so an
// idle shard does not rewrite its image every interval.
func TestCheckpointSkipsIdleStore(t *testing.T) {
	dir := t.TempDir()
	bs := ckStore(t, dir)
	headerVersion := func() uint64 {
		var v uint64
		if err := bs.db.View(func(txn *badgerv4.Txn) error {
			item, err := txn.Get(openImageHeaderKey)
			if err == nil {
				v = item.Version()
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return v
	}
	checkpoint := func(wantNew bool, when string) {
		t.Helper()
		before := headerVersion()
		if err := bs.checkpointOpenImage(); err != nil {
			t.Fatal(err)
		}
		if got := headerVersion() != before; got != wantNew {
			t.Fatalf("%s: a new image written %v, want %v", when, got, wantNew)
		}
	}
	if err := bs.PutNode(ckNode(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = ckStore(t, dir) // trusts the Close image whole
	defer bs.Close()
	checkpoint(false, "idle after a clean reopen")
	if err := bs.PutNode(ckNode(2, 1)); err != nil {
		t.Fatal(err)
	}
	checkpoint(true, "after a write")
	checkpoint(false, "idle after an image")
}

// TestOpenImageFormat3HeaderTrusted: a store a v4.88 release closed (format-3
// header: no slot, kind or since) opens from its image without a row decode.
func TestOpenImageFormat3HeaderTrusted(t *testing.T) {
	dir := t.TempDir()
	bs := ckStore(t, dir)
	for id := int64(1); id <= 50; id++ {
		if err := bs.PutNode(ckNode(id, id%5)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(openImageHeaderKey)
		if err != nil {
			return err
		}
		var h openImageHeader
		if err := item.Value(func(val []byte) error { h, _ = parseOpenImageHeader(val); return nil }); err != nil {
			return err
		}
		if h.slot != 0 {
			t.Fatalf("a first image went to slot %d", h.slot)
		}
		hdr := binary.AppendUvarint([]byte(openImageMagic3), uint64(h.chunks))
		hdr = binary.AppendUvarint(hdr, uint64(h.length))
		return txn.Set(openImageHeaderKey, binary.BigEndian.AppendUint32(hdr, h.crc))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if decoded := ckCheckOpen(t, dir); decoded != 0 {
		t.Fatalf("the open of a format-3 image decoded %d rows", decoded)
	}
}
