package segdir

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func testTypes() []TypeDecl {
	return []TypeDecl{
		{Name: "HOP", Token: 3, BlockRows: 64, Columns: []Column{{Name: "actor", Kind: 14}, {Name: "n", Kind: 6}}},
		{Name: "ORIGIN", Token: 5, BlockRows: 1},
	}
}

// segBytes is any byte string long enough to pass the size floor.
func segBytes(fill byte, n int) []byte { return bytes.Repeat([]byte{fill}, n) }

func openT(t *testing.T, dir string) *Dir {
	t.Helper()
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	slices.Sort(out)
	return out
}

// writeCommit writes and commits one segment.
func writeCommit(t *testing.T, d *Dir, token uint16, data []byte, rows int) Entry {
	t.Helper()
	var root [32]byte
	root[0] = byte(rows)
	e, err := d.WriteSegment(token, data, rows, root)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if err := d.Commit(e, d.Gen()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return e
}

func TestDir_ManifestRoundTripsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segs")
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m := d.Manifest(); len(m.Types) != 0 || len(m.Segments) != 0 {
		t.Fatalf("a new directory has an empty manifest: %+v", m)
	}
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	e1 := writeCommit(t, d, 3, segBytes(1, 200), 7)
	e2 := writeCommit(t, d, 5, segBytes(2, 300), 9)
	if e1.Seq == e2.Seq || e1.Size != 200 || e2.Rows != 9 {
		t.Fatalf("entries: %+v %+v", e1, e2)
	}
	want := d.Manifest()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2 := openT(t, dir)
	got := d2.Manifest()
	if !manifestEqual(got, want) {
		t.Fatalf("reopened manifest differs:\n got %+v\nwant %+v", got, want)
	}
	if got.NextSeq <= e2.Seq {
		t.Fatalf("NextSeq %d must be above every used sequence (%d)", got.NextSeq, e2.Seq)
	}
	m, err := d2.Map(e2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Bytes(), segBytes(2, 300)) {
		t.Fatal("mapped bytes differ from the written bytes")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func manifestEqual(a, b Manifest) bool {
	if a.NextSeq != b.NextSeq || len(a.Types) != len(b.Types) || !slices.Equal(a.Segments, b.Segments) {
		return false
	}
	for i := range a.Types {
		x, y := a.Types[i], b.Types[i]
		if x.Name != y.Name || x.Token != y.Token || x.BlockRows != y.BlockRows || !slices.Equal(x.Columns, y.Columns) {
			return false
		}
	}
	return true
}

func TestDir_SecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	d := openT(t, dir)
	if _, err := Open(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	openT(t, dir)
}

// Every failure before the manifest is durable leaves the directory as it
// was: no .tmp file, no unlisted segment file, the manifest unchanged.
func TestDir_FailedStepLeavesNothingBehind(t *testing.T) {
	for _, step := range []string{StepSegmentWritten, StepSegmentSynced, StepManifestWritten} {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			d := openT(t, dir)
			if err := d.SetTypes(testTypes()); err != nil {
				t.Fatal(err)
			}
			writeCommit(t, d, 3, segBytes(1, 128), 1)
			before, wantFiles := d.Manifest(), files(t, dir)
			boom := errors.New("injected")
			d.SetHook(func(s string) error {
				if s == step {
					return boom
				}
				return nil
			})
			e, err := d.WriteSegment(3, segBytes(9, 128), 1, [32]byte{})
			if err == nil {
				err = d.Commit(e, d.Gen())
				if err != nil {
					d.Discard(e)
				}
			}
			if !errors.Is(err, boom) {
				t.Fatalf("error at %s = %v, want the injected one", step, err)
			}
			if got := files(t, dir); !slices.Equal(got, wantFiles) {
				t.Fatalf("files after a failed %s: %v, want %v", step, got, wantFiles)
			}
			if !manifestEqual(d.Manifest(), before) {
				t.Fatalf("manifest changed after a failed %s", step)
			}
			d.SetHook(nil)
			writeCommit(t, d, 3, segBytes(4, 128), 2) // the directory still works
		})
	}
}

func TestDir_ResetMakesCommitStaleAndDropsFiles(t *testing.T) {
	dir := t.TempDir()
	d := openT(t, dir)
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	e := writeCommit(t, d, 3, segBytes(1, 128), 1)
	gen := d.Gen()
	pending, err := d.WriteSegment(3, segBytes(2, 128), 1, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Reset(); err != nil {
		t.Fatal(err)
	}
	if d.Gen() == gen {
		t.Fatal("Reset must advance the generation")
	}
	if err := d.Commit(pending, gen); !errors.Is(err, ErrStale) {
		t.Fatalf("Commit after Reset = %v, want ErrStale", err)
	}
	d.Discard(pending)
	if m := d.Manifest(); len(m.Segments) != 0 || len(m.Types) != 2 || m.NextSeq <= pending.Seq {
		t.Fatalf("after Reset: %+v (declarations and sequence stay, segments go)", m)
	}
	if _, err := os.Stat(filepath.Join(dir, FileName(3, e.Seq))); !os.IsNotExist(err) {
		t.Fatalf("Reset must remove segment files: %v", err)
	}
}

func TestDir_CleanupRemovesTmpAndOrphansOnly(t *testing.T) {
	dir := t.TempDir()
	d := openT(t, dir)
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	e := writeCommit(t, d, 3, segBytes(1, 128), 1)
	stray := map[string]bool{ // name -> must survive
		FileName(3, 999):                   false, // orphan segment
		FileName(5, 1000) + ".tmp":         false,
		"MANIFEST.tmp":                     false,
		"notes.txt":                        true, // not ours
		"seg-x.tkgs.bak":                   true,
		FileName(3, e.Seq):                 true, // listed
		"MANIFEST":                         true,
		"LOCK":                             true,
		"seg-t3-00000000000000000001":      true, // not a segment name
		"seg-t3-0000000000000000000a.tkgs": true, // not a segment name either
	}
	for name := range stray {
		if name == FileName(3, e.Seq) || name == "MANIFEST" || name == "LOCK" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := d.Cleanup()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(removed)
	for name, keep := range stray {
		_, err := os.Stat(filepath.Join(dir, name))
		if keep && err != nil {
			t.Errorf("%s must survive Cleanup: %v", name, err)
		}
		if !keep && !os.IsNotExist(err) {
			t.Errorf("%s must be removed by Cleanup (err %v)", name, err)
		}
		if !keep && !slices.Contains(removed, name) {
			t.Errorf("Cleanup must report %s", name)
		}
	}
}

func TestDir_MapChecksTheListedSize(t *testing.T) {
	dir := t.TempDir()
	d := openT(t, dir)
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	e := writeCommit(t, d, 3, segBytes(1, 256), 1)
	before := LiveMappings()
	m, err := d.Map(e)
	if err != nil {
		t.Fatal(err)
	}
	if LiveMappings() != before+1 {
		t.Fatalf("LiveMappings = %d, want %d", LiveMappings(), before+1)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if LiveMappings() != before {
		t.Fatalf("LiveMappings after Close = %d, want %d", LiveMappings(), before)
	}
	path := filepath.Join(dir, FileName(3, e.Seq))
	if err := os.Truncate(path, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Map(e); !errors.Is(err, ErrFile) {
		t.Fatalf("Map of a truncated file = %v, want ErrFile", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Map(e); !errors.Is(err, ErrFile) {
		t.Fatalf("Map of a missing file = %v, want ErrFile", err)
	}
}

// The manifest is a trust boundary: damage, an unknown version, unknown
// fields and impossible entries all fail Open with ErrManifest.
func TestDir_ManifestDamageFailsClosed(t *testing.T) {
	dir := t.TempDir()
	d := openT(t, dir)
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	writeCommit(t, d, 3, segBytes(1, 128), 1)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "MANIFEST")
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := func(b []byte) []byte { return append([]byte(nil), b[manifestHeaderSize:]...) }
	reframe := func(version uint16, b []byte) []byte { return frameManifest(version, b) }
	cases := map[string][]byte{
		"empty":           {},
		"truncated":       good[:len(good)-3],
		"flipped body":    flip(good, len(good)-5),
		"flipped header":  flip(good, 2),
		"bad magic":       append([]byte("XXXXXXXX"), good[8:]...),
		"future version":  reframe(manifestVersion+1, body(good)),
		"unknown field":   reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"next_seq"`, `"surprise":1,"next_seq"`, 1))),
		"not json":        reframe(manifestVersion, []byte("{")),
		"undeclared type": reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"token":3,"seq"`, `"token":4,"seq"`, 1))),
		"seq at next":     reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"next_seq":2`, `"next_seq":1`, 1))),
		"zero rows":       reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"rows":1`, `"rows":0`, 1))),
		"bad root":        reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"root":"01`, `"root":"zz`, 1))),
		"token zero":      reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"token":5`, `"token":0`, 1))),
		"block rows":      reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"block_rows":64`, `"block_rows":48`, 1))),
		"column kind":     reframe(manifestVersion, []byte(strings.Replace(string(body(good)), `"kind":6`, `"kind":99`, 1))),
		"oversize length": func() []byte {
			b := append([]byte(nil), good...)
			binary.LittleEndian.PutUint32(b[12:], 1<<31)
			return b
		}(),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if name != "empty" && name != "truncated" && name != "flipped body" && name != "flipped header" &&
				name != "bad magic" && name != "oversize length" && name != "future version" && string(b[manifestHeaderSize:]) == string(body(good)) {
				t.Fatalf("case %q did not change the manifest body: %s", name, body(good))
			}
			if err := os.WriteFile(path, b, 0o600); err != nil {
				t.Fatal(err)
			}
			if d, err := Open(dir); !errors.Is(err, ErrManifest) {
				if err == nil {
					_ = d.Close()
				}
				t.Fatalf("Open with a %s manifest = %v, want ErrManifest", name, err)
			}
		})
	}
	if err := os.WriteFile(path, good, 0o600); err != nil {
		t.Fatal(err)
	}
	openT(t, dir)
}

func flip(b []byte, i int) []byte {
	out := append([]byte(nil), b...)
	out[i] ^= 0x40
	return out
}

func TestDir_ClosedFailsClosed(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil (idempotent)", err)
	}
	if _, err := d.WriteSegment(3, segBytes(1, 128), 1, [32]byte{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteSegment after Close = %v", err)
	}
	if err := d.Commit(Entry{}, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Commit after Close = %v", err)
	}
	if err := d.SetTypes(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("SetTypes after Close = %v", err)
	}
	if err := d.Reset(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reset after Close = %v", err)
	}
	if _, err := d.Cleanup(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Cleanup after Close = %v", err)
	}
	if _, err := d.Map(Entry{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Map after Close = %v", err)
	}
}

func TestDir_RejectsUnusableInput(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") must fail")
	}
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file); err == nil {
		t.Fatal("Open of a regular file must fail")
	}
	d := openT(t, t.TempDir())
	if err := d.SetTypes([]TypeDecl{{Name: "", Token: 1, BlockRows: 64}}); !errors.Is(err, ErrManifest) {
		t.Fatalf("SetTypes with a blank name = %v, want ErrManifest", err)
	}
	if err := d.SetTypes(testTypes()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.WriteSegment(4, segBytes(1, 128), 1, [32]byte{}); !errors.Is(err, ErrManifest) {
		t.Fatalf("WriteSegment of an undeclared token = %v, want ErrManifest", err)
	}
	if _, err := d.WriteSegment(3, segBytes(1, 8), 1, [32]byte{}); !errors.Is(err, ErrManifest) {
		t.Fatalf("WriteSegment below the size floor = %v, want ErrManifest", err)
	}
	if got := FileName(3, 12); got != "seg-t3-00000000000000000012.tkgs" {
		t.Fatalf("FileName = %q", got)
	}
}
