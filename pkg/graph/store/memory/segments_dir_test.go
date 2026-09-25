package memory

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S3, store level: with a segment directory, sealed segments are
// self-contained files listed in a manifest and read memory-mapped. Every
// read door answers as before (the S2 twin oracle, run again over mapped
// files), a reopen of the directory answers exactly what the segments hold
// (the oracle extended with reopen), a crash at any seal step leaves a
// directory whose reopen equals the oracle, and damage fails closed.

// segTwinOnDisk makes newSegTwin attach a segment directory to the declared
// store (set only by TestSegmentDir_S2SuiteOverMappedFiles, which runs
// sequentially); segTwinsOnDisk collects the twins it built.
var (
	segTwinOnDisk  bool
	segTwinsOnDisk []*segTwin
)

// segDirFiles lists the directory's file names, sorted.
func segDirFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Name())
	}
	slices.Sort(out)
	return out
}

// segDirSnapshot is every file's bytes (a failed open must touch nothing).
func segDirSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, name := range segDirFiles(t, dir) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		out[name] = string(b)
	}
	return out
}

// segDirSegmentFiles lists the segment files (seg-*.tkgs) of dir.
func segDirSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for _, name := range segDirFiles(t, dir) {
		if strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".tkgs") {
			out = append(out, name)
		}
	}
	return out
}

// assertSegDirConsistent checks that the directory holds exactly the files a
// clean directory holds: the lock, the manifest and one file per segment the
// store serves, every one of them mapped (no segment bytes on the heap).
func assertSegDirConsistent(t *testing.T, s *Store, dir string) {
	t.Helper()
	s.mu.RLock()
	var want []string
	mapped := 0
	for _, st := range s.segTypes {
		for _, sg := range st.segs {
			want = append(want, segdir.FileName(st.decl.TypeToken, sg.id))
			if sg.m != nil {
				mapped++
			}
		}
	}
	s.mu.RUnlock()
	if mapped != len(want) {
		t.Fatalf("%d of %d segments are mapped files", mapped, len(want))
	}
	slices.Sort(want)
	if got := segDirSegmentFiles(t, dir); !slices.Equal(got, want) {
		t.Fatalf("segment files %v, the store serves %v", got, want)
	}
	for _, name := range segDirFiles(t, dir) {
		if strings.HasSuffix(name, ".tmp") {
			t.Fatalf("a .tmp file outlived its seal: %s", name)
		}
	}
}

// sealedRowsForTest returns every row the declared type's segments hold (live
// or dead), as sealed, in segment then position order.
func sealedRowsForTest(t *testing.T, s *Store, tok uint16) []*types.Relationship {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*types.Relationship
	st := s.segTypes[tok]
	if st == nil {
		return nil
	}
	for _, sg := range st.segs {
		if err := sg.seg.Scan(func(_ int, r *types.Relationship) bool { out = append(out, r); return true }); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// segTestNode is the node addNode creates for id.
func segTestNode(id types.NodeID) *types.Node {
	n := types.NewNode(id, 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: strings.Repeat("ab", 32)})
	return n
}

// reopenSegDir opens a fresh declared store over dir (nodes first, as a
// caller that rebuilds its row store would).
func reopenSegDir(t *testing.T, dir string, decl storecontract.RelSegmentDeclaration, nodes []types.NodeID) *Store {
	t.Helper()
	s := New()
	if err := s.DeclareRelSegment(decl); err != nil {
		t.Fatal(err)
	}
	for _, id := range nodes {
		if err := s.PutNode(segTestNode(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.OpenRelSegmentDir(dir); err != nil {
		t.Fatalf("reopen %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// oracleOf is an undeclared store holding nodes and exactly rows.
func oracleOf(t *testing.T, nodes []types.NodeID, rows []*types.Relationship) *Store {
	t.Helper()
	s := New()
	for _, id := range nodes {
		if err := s.PutNode(segTestNode(id)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[types.RelID]bool{}
	for _, r := range rows {
		if seen[r.ID()] {
			t.Fatalf("row %d is held by two segments", r.ID())
		}
		seen[r.ID()] = true
		if err := s.PutRelationship(r.DeepCopy()); err != nil {
			t.Fatalf("oracle put %d: %v", r.ID(), err)
		}
	}
	return s
}

// TestSegmentDir_S2SuiteOverMappedFiles runs the S2 store tests again with a
// segment directory: every door over self-contained, memory-mapped segment
// files must answer like the undeclared twin, and every directory must end
// consistent (one listed, mapped file per segment, no .tmp). The two tests
// that measure the in-RAM shared dictionaries do not apply: files are
// self-contained by design.
func TestSegmentDir_S2SuiteOverMappedFiles(t *testing.T) {
	segTwinOnDisk = true
	t.Cleanup(func() { segTwinOnDisk, segTwinsOnDisk = false, nil })
	for name, fn := range map[string]func(*testing.T){
		"StoreDifferentialOracle":                     TestSegments_StoreDifferentialOracle,
		"UnsealableRowsStayInMemtable":                TestSegments_UnsealableRowsStayInMemtable,
		"ConcurrentReadsDuringSeal":                   TestSegments_ConcurrentReadsDuringSeal,
		"ClearKeepsDeclaration":                       TestSegments_ClearKeepsDeclaration,
		"WriteDoorsSeeSealedRows":                     TestSegments_WriteDoorsSeeSealedRows,
		"BudgetSealRunsOffTheWriter":                  TestSegments_BudgetSealRunsOffTheWriter,
		"ScanRelSegmentsEqualsTheRowPath":             TestSegments_ScanRelSegmentsEqualsTheRowPath,
		"ScanRelSegmentsIsInIDOrder":                  TestSegments_ScanRelSegmentsIsInIDOrder,
		"ScanRelSegmentsHoldsOverlappingSegmentsOnly": TestSegments_ScanRelSegmentsHoldsOverlappingSegmentsOnly,
		"ScanRelColumnsIsNativeAndEqual":              TestSegments_ScanRelColumnsIsNativeAndEqual,
	} {
		segTwinsOnDisk = nil
		t.Run(name, func(t *testing.T) {
			fn(t)
			sealed := 0
			for _, tw := range segTwinsOnDisk {
				waitSealsForTest(t, tw.declared)
				assertSegDirConsistent(t, tw.declared, tw.dir)
				sealed += len(segDirSegmentFiles(t, tw.dir))
			}
			if len(segTwinsOnDisk) == 0 || sealed == 0 {
				t.Fatalf("no twin sealed a segment file (%d twins)", len(segTwinsOnDisk))
			}
		})
	}
}

// The reopened directory answers every door exactly like an undeclared
// store holding the rows the segments hold (as sealed: the memtable, the
// overlay and the row store's history are process memory), then keeps
// working — new writes, updates of reopened sealed rows, more seals — and a
// second reopen answers again.
func TestSegmentDir_ReopenEqualsTheOracle(t *testing.T) {
	r := rand.New(rand.NewSource(101))
	dir := t.TempDir()
	tw := newSegTwin(t, 24<<10, 16)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 4; round++ {
		for i := 0; i < 150; i++ {
			switch x := r.Intn(10); {
			case x < 6:
				tw.put(r, segTestHOP)
			case x < 7:
				tw.put(r, segTestOther)
			default:
				tw.mutate(r)
			}
		}
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatal(err)
		}
		tw.compare(fmt.Sprintf("round %d before reopen", round))
	}
	waitSealsForTest(t, tw.declared)
	assertSegDirConsistent(t, tw.declared, dir)
	st := tw.stats()
	if st.Segments < 4 || st.LiveSealedRows == st.SealedRows {
		t.Fatalf("setup: seals and overlay writes must both have happened: %+v", st)
	}
	sealed := sealedRowsForTest(t, tw.declared, segTestHOP)
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}

	for gen := 1; gen <= 2; gen++ {
		re := reopenSegDir(t, dir, segTestDecl(24<<10), tw.nodes)
		oracle := oracleOf(t, tw.nodes, sealed)
		tw2 := &segTwin{t: t, plain: oracle, declared: re, nodes: tw.nodes, rels: tw.rels, nextID: tw.nextID + int64(gen)*1_000_000, clock: tw.clock}
		tw2.compare(fmt.Sprintf("reopen %d", gen))
		if got := tw2.stats(); got.SealedRows != int64(len(sealed)) || got.LiveSealedRows != int64(len(sealed)) || got.UnsealedRows != 0 {
			t.Fatalf("reopen %d: %+v, want %d sealed rows, all live", gen, got, len(sealed))
		}
		assertSegDirConsistent(t, re, dir)
		// The reopened store keeps working: indexes over reopened rows,
		// writes, updates and deletes of reopened sealed rows, more seals.
		tw2.both("CreateRelPropertyIndex", func(s *Store) error { return s.CreateRelPropertyIndex(segTestHOP, "actor") })
		for i := 0; i < 200; i++ {
			if r.Intn(3) == 0 {
				tw2.mutate(r)
			} else {
				tw2.put(r, segTestHOP)
			}
			if i%70 == 69 {
				if err := re.SealRelSegments(segTestHOP); err != nil {
					t.Fatal(err)
				}
			}
		}
		waitSealsForTest(t, re)
		tw2.compare(fmt.Sprintf("reopen %d after more writes and seals", gen))
		assertSegDirConsistent(t, re, dir)
		tw.rels, tw.nextID, tw.clock = tw2.rels, tw2.nextID, tw2.clock
		sealed = sealedRowsForTest(t, re, segTestHOP)
		if err := re.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A row the store already holds when the directory is opened: the same row
// (ID, version, hash) leaves the row store — idempotent recovery of a crash
// between the manifest write and the row removal; another version of the
// row stays and shadows the sealed copy.
func TestSegmentDir_ReopenMovesRowStoreDuplicatesOut(t *testing.T) {
	r := rand.New(rand.NewSource(103))
	dir := t.TempDir()
	tw := newSegTwin(t, 1<<40, 8)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	sealed := sealedRowsForTest(t, tw.declared, segTestHOP)
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}

	re := New()
	if err := re.DeclareRelSegment(segTestDecl(1 << 40)); err != nil {
		t.Fatal(err)
	}
	expect := append([]*types.Relationship(nil), sealed...)
	for _, id := range tw.nodes {
		if err := re.PutNode(segTestNode(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range sealed[:5] { // identical copies
		if err := re.PutRelationship(row.DeepCopy()); err != nil {
			t.Fatal(err)
		}
	}
	newer := sealed[9].DeepCopy() // same ID, a later version
	newer.SetVersion(newer.Version() + 1)
	if err := newer.SetProperty("actor", "newer"); err != nil {
		t.Fatal(err)
	}
	segHashed(newer)
	if err := re.PutRelationship(newer); err != nil {
		t.Fatal(err)
	}
	expect[9] = newer
	extra := tw.newRel(r, segTestHOP)
	if err := re.PutRelationship(extra); err != nil {
		t.Fatal(err)
	}
	expect = append(expect, extra)
	if err := re.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = re.Close() })
	st, err := re.RelSegmentStats(segTestHOP)
	if err != nil {
		t.Fatal(err)
	}
	if st.SealedRows != 40 || st.LiveSealedRows != 39 || st.UnsealedRows != 2 {
		t.Fatalf("after reopen: %+v, want 40 sealed (39 live: the newer version shadows one) and 2 unsealed", st)
	}
	re.mu.RLock()
	for _, row := range sealed[:5] {
		if _, ok := re.rels[row.ID()]; ok {
			t.Errorf("row %d is sealed and still in the row store", row.ID())
		}
	}
	re.mu.RUnlock()
	oracle := oracleOf(t, tw.nodes, expect)
	tw2 := &segTwin{t: t, plain: oracle, declared: re, nodes: tw.nodes, rels: append(tw.rels, extra.ID()), nextID: tw.nextID + 10, clock: tw.clock}
	tw2.compare("reopen over a row store that holds sealed rows")
}

// An I/O failure at any seal step before the manifest is durable fails the
// seal: every row stays in the memtable (the store answers like its twin),
// the directory keeps exactly its listed files, and the next seal succeeds.
func TestSegmentDir_FailedSealStepKeepsStoreAndDirectory(t *testing.T) {
	for _, step := range []string{segdir.StepSegmentWritten, segdir.StepSegmentSynced, segdir.StepManifestWritten} {
		t.Run(step, func(t *testing.T) {
			r := rand.New(rand.NewSource(107))
			dir := t.TempDir()
			tw := newSegTwin(t, 1<<40, 10)
			if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tw.declared.Close() })
			for i := 0; i < 80; i++ {
				tw.put(r, segTestHOP)
			}
			if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 60; i++ {
				tw.put(r, segTestHOP)
			}
			before := tw.stats()
			boom := errors.New("injected I/O failure")
			setSegDirHookForTest(t, tw.declared, func(s string) error {
				if s == step {
					return boom
				}
				return nil
			})
			if err := tw.declared.SealRelSegments(segTestHOP); !errors.Is(err, boom) {
				t.Fatalf("seal failing at %s = %v, want the injected error", step, err)
			}
			if st := tw.stats(); st.Segments != before.Segments || st.UnsealedRows != before.UnsealedRows || st.SealedRows != before.SealedRows {
				t.Fatalf("a failed seal changed the layout: %+v -> %+v", before, st)
			}
			tw.compare("after a failed seal at " + step)
			assertSegDirConsistent(t, tw.declared, dir)
			setSegDirHookForTest(t, tw.declared, nil)
			if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
			if st := tw.stats(); st.Segments != before.Segments+1 || st.UnsealedRows != 0 {
				t.Fatalf("the retry seal: %+v", st)
			}
			tw.compare("after the retry seal")
			assertSegDirConsistent(t, tw.declared, dir)
		})
	}
}

func setSegDirHookForTest(t *testing.T, s *Store, fn func(string) error) {
	t.Helper()
	s.mu.RLock()
	d := s.segDir
	s.mu.RUnlock()
	if d == nil {
		t.Fatal("the store has no segment directory")
	}
	d.SetHook(fn)
}

// --- damage ---

type testSegSection struct {
	name        string
	off, length int
	crcAt       int
}

// testSegSections parses a segment file's footer (the documented layout:
// footerVersion u16 | blockRows u16 | type name | columns | root [32] |
// section directory) far enough to locate sections and their CRCs.
func testSegSections(t *testing.T, data []byte) (secs []testSegSection, fstart, fend int) {
	t.Helper()
	n := len(data)
	flen := int(binary.LittleEndian.Uint32(data[n-16:]))
	fstart, fend = n-16-flen, n-16
	c := fstart + 4
	str16 := func() string {
		l := int(binary.LittleEndian.Uint16(data[c:]))
		s := string(data[c+2 : c+2+l])
		c += 2 + l
		return s
	}
	str16()
	ncols := int(binary.LittleEndian.Uint16(data[c:]))
	c += 2
	for i := 0; i < ncols; i++ {
		str16()
		c++
	}
	c += 32
	nsec := int(binary.LittleEndian.Uint16(data[c:]))
	c += 2
	for i := 0; i < nsec; i++ {
		var s testSegSection
		s.name = str16()
		s.off = int(binary.LittleEndian.Uint64(data[c:]))
		s.length = int(binary.LittleEndian.Uint64(data[c+8:]))
		s.crcAt = c + 16
		c += 20
		secs = append(secs, s)
	}
	return secs, fstart, fend
}

// testFixSegCRCs recomputes every section CRC and the footer CRC, so a byte
// change inside a section passes every CRC and only the integrity roots can
// catch it.
func testFixSegCRCs(t *testing.T, data []byte) {
	t.Helper()
	tab := crc32.MakeTable(crc32.Castagnoli)
	secs, fstart, fend := testSegSections(t, data)
	for _, s := range secs {
		binary.LittleEndian.PutUint32(data[s.crcAt:], crc32.Checksum(data[s.off:s.off+s.length], tab))
	}
	binary.LittleEndian.PutUint32(data[len(data)-12:], crc32.Checksum(data[fstart:fend], tab))
}

// testFlipWeightBit flips the lowest stored bit of segment position row's
// weight value (a declared float64 column every test row holds).
func testFlipWeightBit(t *testing.T, data []byte, row int) {
	t.Helper()
	secs, _, _ := testSegSections(t, data)
	for _, s := range secs {
		if s.name != "prop:weight" {
			continue
		}
		b := data[s.off : s.off+s.length]
		pc := int(binary.LittleEndian.Uint32(b[6:]))
		page := 10 + 4*(pc+1) + int(binary.LittleEndian.Uint32(b[10:]))
		if b[page] != 0 {
			t.Fatalf("weight page 0 has transform %d, want 0", b[page])
		}
		w := int(b[page+1])
		if w == 0 {
			t.Fatal("weight page 0 is constant: no bit to flip")
		}
		bit := row * w
		b[page+10+bit/8] ^= 1 << (bit % 8)
		return
	}
	t.Fatal("no prop:weight section")
}

// A damaged or missing segment file fails the reopen closed with
// ErrRelSegmentFileInvalid, touches nothing in the directory, and releases
// the lock (a later open of the repaired directory works). A value changed
// with every CRC recomputed is caught by the integrity roots, located to its
// IntegrityBlockRows group; a damaged manifest fails with
// ErrRelSegmentManifestInvalid.
func TestSegmentDir_DamageFailsClosed(t *testing.T) {
	for _, blockRows := range []int{1, 64, 4096} {
		t.Run(fmt.Sprint("IntegrityBlockRows=", blockRows), func(t *testing.T) {
			r := rand.New(rand.NewSource(109))
			dir := t.TempDir()
			decl := segTestDecl(1 << 40)
			decl.IntegrityBlockRows = blockRows
			s := New()
			if err := s.DeclareRelSegment(decl); err != nil {
				t.Fatal(err)
			}
			tw := &segTwin{t: t, plain: New(), declared: s, nextID: 1 << 40, clock: 1_700_000_000_000}
			for i := 0; i < 6; i++ {
				tw.addNode()
			}
			if err := s.OpenRelSegmentDir(dir); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 300; i++ {
				tw.put(r, segTestHOP)
			}
			if err := s.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			files := segDirSegmentFiles(t, dir)
			if len(files) != 1 {
				t.Fatalf("setup: %v", files)
			}
			segPath := filepath.Join(dir, files[0])
			good, err := os.ReadFile(segPath)
			if err != nil {
				t.Fatal(err)
			}
			manPath := filepath.Join(dir, "MANIFEST")
			goodMan, err := os.ReadFile(manPath)
			if err != nil {
				t.Fatal(err)
			}
			const row = 5
			cases := []struct {
				name    string
				damage  func()
				want    error
				also    error
				message string
			}{
				{"missing file", func() { _ = os.Remove(segPath) }, storecontract.ErrRelSegmentFileInvalid, segdir.ErrFile, ""},
				{"truncated file", func() { _ = os.WriteFile(segPath, good[:len(good)-7], 0o600) }, storecontract.ErrRelSegmentFileInvalid, segdir.ErrFile, ""},
				{"byte flipped (CRC)", func() {
					b := append([]byte(nil), good...)
					b[len(b)/2] ^= 0x10
					_ = os.WriteFile(segPath, b, 0o600)
				}, storecontract.ErrRelSegmentFileInvalid, segment.ErrCorrupt, ""},
				{"value changed, CRCs recomputed", func() {
					b := append([]byte(nil), good...)
					testFlipWeightBit(t, b, row)
					testFixSegCRCs(t, b)
					_ = os.WriteFile(segPath, b, 0o600)
				}, storecontract.ErrRelSegmentFileInvalid, segment.ErrIntegrity, fmt.Sprintf("groups %d", row/blockRows)},
				{"manifest damaged", func() {
					b := append([]byte(nil), goodMan...)
					b[len(b)-3] ^= 0x01
					_ = os.WriteFile(manPath, b, 0o600)
				}, storecontract.ErrRelSegmentManifestInvalid, segdir.ErrManifest, ""},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tc.damage()
					before := segDirSnapshot(t, dir)
					re := New()
					if err := re.DeclareRelSegment(decl); err != nil {
						t.Fatal(err)
					}
					err := re.OpenRelSegmentDir(dir)
					if !errors.Is(err, tc.want) || !errors.Is(err, tc.also) {
						t.Fatalf("OpenRelSegmentDir = %v, want %v wrapping %v", err, tc.want, tc.also)
					}
					if tc.message != "" && !strings.Contains(err.Error(), tc.message) {
						t.Fatalf("the error must locate the damaged integrity group (%q): %v", tc.message, err)
					}
					if after := segDirSnapshot(t, dir); fmt.Sprint(after) != fmt.Sprint(before) {
						t.Fatal("a failed open changed the directory")
					}
					if st, err := re.RelSegmentStats(segTestHOP); err != nil || st.Segments != 0 {
						t.Fatalf("a failed open left segments behind: %+v %v", st, err)
					}
					_ = re.Close()
					// Repair: the lock was released and the directory opens.
					if err := os.WriteFile(segPath, good, 0o600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(manPath, goodMan, 0o600); err != nil {
						t.Fatal(err)
					}
					ok := reopenSegDir(t, dir, decl, tw.nodes)
					if st, err := ok.RelSegmentStats(segTestHOP); err != nil || st.LiveSealedRows != 300 {
						t.Fatalf("repaired directory: %+v %v", st, err)
					}
					if err := ok.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

// Reopen removes .tmp files and segment files the manifest does not list —
// what a crash before the manifest write leaves — and nothing else.
func TestSegmentDir_ReopenRemovesTmpAndOrphans(t *testing.T) {
	r := rand.New(rand.NewSource(113))
	dir := t.TempDir()
	tw := newSegTwin(t, 1<<40, 6)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	listed := segDirSegmentFiles(t, dir)
	if len(listed) != 1 {
		t.Fatalf("setup: segment files %v, want one", listed)
	}
	sealed := sealedRowsForTest(t, tw.declared, segTestHOP)
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(filepath.Join(dir, listed[0]))
	if err != nil {
		t.Fatal(err)
	}
	strays := map[string]bool{ // name -> survives
		segdir.FileName(segTestHOP, 900):          false, // a valid segment the manifest never listed
		segdir.FileName(segTestHOP, 901) + ".tmp": false,
		"MANIFEST.tmp":       false,
		"operator-notes.txt": true,
	}
	for name := range strays {
		if err := os.WriteFile(filepath.Join(dir, name), good, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	re := reopenSegDir(t, dir, segTestDecl(1<<40), tw.nodes)
	for name, keep := range strays {
		_, err := os.Stat(filepath.Join(dir, name))
		if keep != (err == nil) {
			t.Errorf("%s: survives=%t after reopen, want %t", name, err == nil, keep)
		}
	}
	if st, err := re.RelSegmentStats(segTestHOP); err != nil || st.Segments != 1 || st.SealedRows != int64(len(sealed)) {
		t.Fatalf("the orphan's rows must not appear: %+v %v", st, err)
	}
	tw2 := &segTwin{t: t, plain: oracleOf(t, tw.nodes, sealed), declared: re, nodes: tw.nodes, rels: tw.rels, clock: tw.clock}
	tw2.compare("after orphan cleanup")
}

// Declarations are persisted in the manifest: a reopen with another schema,
// another token, or without a type the manifest lists fails closed; the
// directory is locked while a store has it open; the capability's call
// order is enforced.
func TestSegmentDir_DeclarationAndLockRules(t *testing.T) {
	r := rand.New(rand.NewSource(127))
	dir := t.TempDir()
	tw := newSegTwin(t, 1<<40, 4)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	second := New()
	if err := second.DeclareRelSegment(segTestDecl(1 << 40)); err != nil {
		t.Fatal(err)
	}
	if err := second.OpenRelSegmentDir(dir); !errors.Is(err, storecontract.ErrRelSegmentDirLocked) {
		t.Fatalf("a second store on an open directory = %v, want ErrRelSegmentDirLocked", err)
	}
	if err := tw.declared.OpenRelSegmentDir(dir); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("a second OpenRelSegmentDir = %v, want ErrRelSegmentDeclaration", err)
	}
	other := segTestDecl(1 << 40)
	other.TypeName, other.TypeToken = "OTHER", segTestOther
	if err := tw.declared.DeclareRelSegment(other); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("DeclareRelSegment after OpenRelSegmentDir = %v, want ErrRelSegmentDeclaration", err)
	}
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tw.declared.OpenRelSegmentDir(t.TempDir()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("OpenRelSegmentDir after Close = %v, want ErrStoreClosed", err)
	}
	mismatch := map[string]func(*storecontract.RelSegmentDeclaration){
		"another column kind": func(d *storecontract.RelSegmentDeclaration) { d.Columns[1].Kind = storecontract.SegmentFloat32 },
		"one column less":     func(d *storecontract.RelSegmentDeclaration) { d.Columns = d.Columns[:2] },
		"another block size":  func(d *storecontract.RelSegmentDeclaration) { d.IntegrityBlockRows = 16 },
		"another token":       func(d *storecontract.RelSegmentDeclaration) { d.TypeToken = 9 },
		"another type only":   func(d *storecontract.RelSegmentDeclaration) { d.TypeName, d.TypeToken = "OTHER", segTestOther },
	}
	for name, change := range mismatch {
		t.Run(name, func(t *testing.T) {
			before := segDirSnapshot(t, dir)
			d := segTestDecl(1 << 40)
			d.Columns = slices.Clone(d.Columns)
			change(&d)
			s := New()
			if err := s.DeclareRelSegment(d); err != nil {
				t.Fatal(err)
			}
			if err := s.OpenRelSegmentDir(dir); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
				t.Fatalf("reopen with %s = %v, want ErrRelSegmentDeclaration", name, err)
			}
			_ = s.Close()
			if after := segDirSnapshot(t, dir); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatal("a refused open changed the directory")
			}
		})
	}
	// A new type next to the listed one is fine, and is persisted.
	both := New()
	if err := both.DeclareRelSegment(segTestDecl(1 << 40)); err != nil {
		t.Fatal(err)
	}
	if err := both.DeclareRelSegment(other); err != nil {
		t.Fatal(err)
	}
	if err := both.OpenRelSegmentDir(dir); err != nil {
		t.Fatalf("an added type: %v", err)
	}
	if err := both.Close(); err != nil {
		t.Fatal(err)
	}
	only := New()
	if err := only.DeclareRelSegment(segTestDecl(1 << 40)); err != nil {
		t.Fatal(err)
	}
	if err := only.OpenRelSegmentDir(dir); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("a reopen that drops a persisted type = %v, want ErrRelSegmentDeclaration", err)
	}
	_ = only.Close()
	// Call-order rules.
	none := New()
	if err := none.OpenRelSegmentDir(t.TempDir()); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("OpenRelSegmentDir without a declared type = %v, want ErrRelSegmentDeclaration", err)
	}
	inRAM := newSegTwin(t, 1<<40, 3)
	inRAM.put(r, segTestHOP)
	if err := inRAM.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	if err := inRAM.declared.OpenRelSegmentDir(t.TempDir()); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("OpenRelSegmentDir after an in-RAM seal = %v, want ErrRelSegmentDeclaration", err)
	}
	var nilStore *Store
	if err := nilStore.OpenRelSegmentDir(dir); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
}

// Clear drops the directory's segments as well (files and manifest
// entries): a reopen after Clear holds nothing, and new seals continue.
func TestSegmentDir_ClearEmptiesTheDirectory(t *testing.T) {
	r := rand.New(rand.NewSource(131))
	dir := t.TempDir()
	tw := newSegTwin(t, 1<<40, 5)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	tw.both("Clear", func(s *Store) error { return s.Clear() })
	if got := segDirSegmentFiles(t, dir); len(got) != 0 {
		t.Fatalf("Clear left segment files: %v", got)
	}
	tw.rels, tw.nodes = nil, nil
	for i := 0; i < 4; i++ {
		tw.addNode()
	}
	for i := 0; i < 25; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	tw.compare("after Clear and a new seal")
	sealed := sealedRowsForTest(t, tw.declared, segTestHOP)
	if len(sealed) != 25 {
		t.Fatalf("sealed after Clear: %d rows", len(sealed))
	}
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}
	re := reopenSegDir(t, dir, segTestDecl(1<<40), tw.nodes)
	tw2 := &segTwin{t: t, plain: oracleOf(t, tw.nodes, sealed), declared: re, nodes: tw.nodes, rels: tw.rels, clock: tw.clock}
	tw2.compare("reopen after Clear")
}

// A scan that runs without the store lock pins the mapped segments it
// holds: Close (or Clear) while ScanRelSegments is inside its callback does
// not unmap them under it — the scan finishes with every row intact — and
// the mappings are released once the scan returns. A batch's Row door used
// after the scan fails instead of reading an unmapped segment.
func TestSegmentDir_ScanPinsMappingsAcrossClose(t *testing.T) {
	for _, closer := range []string{"Close", "Clear"} {
		t.Run(closer, func(t *testing.T) {
			r := rand.New(rand.NewSource(137))
			dir := t.TempDir()
			tw := newSegTwin(t, 1<<40, 10)
			if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
				t.Fatal(err)
			}
			for seal := 0; seal < 3; seal++ {
				for i := 0; i < 5000; i++ {
					tw.put(r, segTestHOP)
				}
				if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
					t.Fatal(err)
				}
			}
			props := []string{"weight", "actor", "n"}
			want := segScanOrdered(t, tw.declared, props)
			base := segdir.LiveMappings()
			entered, release := make(chan struct{}), make(chan struct{})
			var got []string
			var kept *storecontract.RelSegmentBatch
			done := make(chan error, 1)
			go func() {
				first := true
				_, err := tw.declared.ScanRelSegments(segTestHOP, props, func(b *storecontract.RelSegmentBatch) bool {
					if first {
						first = false
						close(entered)
						<-release
					}
					for k := 0; k < b.Len(); k++ {
						full, err := b.Row(k)
						if err != nil {
							done <- err
							return false
						}
						vals := make([]any, len(props))
						for c := range props {
							if b.Cols[c].Present[k] {
								vals[c] = b.Cols[c].Value(k)
							} else if b.Cols[c].Other[k] {
								vals[c], _ = full.GetProperty(props[c])
							}
						}
						got = append(got, segScanFact(b.IDs[k], b.StartIDs[k], b.EndIDs[k], b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.Versions[k], vals))
					}
					kept = b
					return true
				})
				done <- err
			}()
			<-entered
			switch closer {
			case "Close":
				if err := tw.declared.Close(); err != nil {
					t.Fatal(err)
				}
			default:
				if err := tw.declared.Clear(); err != nil {
					t.Fatal(err)
				}
			}
			if segdir.LiveMappings() < base {
				t.Fatalf("%s unmapped segments a running scan holds", closer)
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("scan did not finish")
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("the scan across %s handed out %d rows, want %d (or values differ)", closer, len(got), len(want))
			}
			if n := segdir.LiveMappings(); n != base-3 {
				t.Fatalf("after the scan: %d live mappings, want %d (the three segments released)", n, base-3)
			}
			if _, err := kept.Row(0); err == nil {
				t.Fatal("Row on a batch after its scan returned must fail, not read a released segment")
			}
			if closer == "Clear" {
				_ = tw.declared.Close()
			}
		})
	}
}

// Readers during background seals to disk and a reopen under -race: the S2
// concurrency test over mapped files (the suite above) plus readers that
// race the directory attach of a second store over another directory copy.
func TestSegmentDir_ConcurrentReadersAndReopen(t *testing.T) {
	r := rand.New(rand.NewSource(139))
	dir := t.TempDir()
	tw := newSegTwin(t, 16<<10, 12)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1500; i++ {
		tw.put(r, segTestHOP)
	}
	waitSealsForTest(t, tw.declared)
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	sealed := sealedRowsForTest(t, tw.declared, segTestHOP)
	if err := tw.declared.Close(); err != nil {
		t.Fatal(err)
	}
	re := reopenSegDir(t, dir, segTestDecl(16<<10), tw.nodes)
	want := map[types.RelID]string{}
	for _, row := range sealed {
		want[row.ID()] = strings.Replace(segFP(row), " f=false", " f=true", 1)
	}
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := 0
				check := func(row *types.Relationship) {
					if exp, ok := want[row.ID()]; ok {
						n++
						if got := strings.Replace(segFP(row), " f=false", " f=true", 1); got != exp {
							errc <- fmt.Errorf("reader %d: row %d changed", w, row.ID())
						}
					}
				}
				var err error
				switch i % 3 {
				case 0:
					err = re.ForEachRelByType(segTestHOP, QueryOpts{}, func(row *types.Relationship) bool { check(row); return true })
				case 1:
					_, err = re.ScanRelSegments(segTestHOP, []string{"n"}, func(b *storecontract.RelSegmentBatch) bool {
						for k := 0; k < b.Len(); k++ {
							if _, ok := want[b.IDs[k]]; ok {
								n++
							}
						}
						return true
					})
				default:
					for id := range want {
						row, gerr := re.GetRelationship(id)
						if gerr != nil {
							err = gerr
							break
						}
						check(row)
					}
				}
				if err != nil {
					errc <- err
					return
				}
				if n != len(want) {
					errc <- fmt.Errorf("reader %d (mode %d): saw %d of %d reopened rows", w, i%3, n, len(want))
					return
				}
			}
		}(w)
	}
	// Writer: new rows seal to the same directory while the readers run.
	tw2 := &segTwin{t: t, plain: New(), declared: re, nodes: tw.nodes, nextID: tw.nextID, clock: tw.clock}
	for _, id := range tw.nodes {
		if err := tw2.plain.PutNode(segTestNode(id)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 1500; i++ {
		rel := tw2.newRel(r, segTestHOP)
		if err := re.PutRelationship(rel); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	waitSealsForTest(t, re)
	assertSegDirConsistent(t, re, dir)
	if st, err := re.RelSegmentStats(segTestHOP); err != nil || st.Seals == 0 {
		t.Fatalf("the reopened store never sealed: %+v %v", st, err)
	}
}
