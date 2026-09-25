// Package segdir is the durable half of ADR-0011 step S3: a directory of
// immutable column-segment files, a manifest that lists them, and the crash
// protocol that keeps the two consistent. It knows nothing about the codec
// (package segment) or a store; it moves bytes and keeps the manifest true.
//
// # Directory
//
//	LOCK                            flock'ed by the one open Dir
//	MANIFEST                        the truth: declarations + committed segments
//	seg-t<token>-<seq 20 digits>.tkgs  one immutable segment file
//	*.tmp                           a write in progress (never trusted)
//
// # Seal protocol (ADR-0011 §3.1)
//
//	WriteSegment: write seg-….tkgs.tmp, fsync, rename to seg-….tkgs, fsync dir
//	Commit:       write MANIFEST.tmp, fsync, rename over MANIFEST, fsync dir
//
// A segment exists iff the durable MANIFEST lists it. A crash before Commit
// completes leaves a .tmp or an unlisted segment file, both removed by
// Cleanup at the next open; a crash after it leaves a listed file.
//
// # Manifest (little-endian)
//
//	magic "TKGSMANI" | version u16 | reserved u16 | body length u32 |
//	body CRC32C u32 | body (JSON, see manifestJSON)
//
// It is a trust boundary: magic, version, length and CRC are checked before
// the body is parsed; the body is parsed with unknown fields refused and then
// validated (tokens, names, kinds, block sizes, sequence numbers, sizes,
// roots); nothing is allocated from a length the file only claims.
package segdir

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Seal-protocol steps, in order (ADR-0011 §3.1). A hook installed with
// SetHook runs after each; see SetHook for what an error at each step means.
const (
	// StepSegmentWritten: the segment's .tmp file holds every byte, not yet
	// synced.
	StepSegmentWritten = "segment-written"
	// StepSegmentSynced: the file is synced, renamed to its final name, and
	// the directory entry synced. Still not listed: a crash here leaves an
	// orphan that the next Open's Cleanup removes.
	StepSegmentSynced = "segment-synced"
	// StepManifestWritten: MANIFEST.tmp holds the new manifest, not yet
	// renamed over MANIFEST.
	StepManifestWritten = "manifest-written"
	// StepManifestDurable: MANIFEST is replaced and the directory synced —
	// the segment is committed.
	StepManifestDurable = "manifest-durable"
)

// Errors. Each error the package returns wraps exactly one of these (or is
// an operating-system error from creating, writing or syncing a file).
var (
	// ErrLocked: another open Dir holds the directory's lock.
	ErrLocked = errors.New("segdir: directory is in use by another open store")
	// ErrManifest: the manifest is unreadable, damaged, of an unknown
	// version, or lists something impossible.
	ErrManifest = errors.New("segdir: invalid manifest")
	// ErrFile: a segment file the manifest lists is missing, of the wrong
	// size, or cannot be mapped.
	ErrFile = errors.New("segdir: invalid segment file")
	// ErrStale: the directory was Reset after the caller took its
	// generation; the segment is not committed.
	ErrStale = errors.New("segdir: directory was reset")
	// ErrClosed: the Dir is closed.
	ErrClosed = errors.New("segdir: closed")
)

// File names and format constants.
const (
	lockName     = "LOCK"
	manifestName = "MANIFEST"
	tmpSuffix    = ".tmp"
	segPrefix    = "seg-t"
	segSuffix    = ".tkgs"

	manifestMagic      = "TKGSMANI"
	manifestVersion    = 1
	manifestHeaderSize = 20
	// maxManifestBytes bounds what Open reads (a manifest line is ~150 B,
	// so this is ~400 K segments).
	maxManifestBytes = 64 << 20
	// minSegmentBytes is the smallest segment the codec writes (a 64-byte
	// header and a 16-byte trailer).
	minSegmentBytes = 80
	// maxSegmentRows mirrors the codec's per-segment bound (64 M rows).
	maxSegmentRows = 64 << 20
	maxBlockRows   = 4096
	maxKind        = 14
	maxNameBytes   = 1<<16 - 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Column is one declared column of a type (name and exact kind).
type Column struct {
	Name string
	Kind uint8
}

// TypeDecl is one declared relationship type as the manifest persists it.
// BlockRows is the resolved IntegrityBlockRows (never 0).
type TypeDecl struct {
	Name      string
	Token     uint16
	BlockRows int
	Columns   []Column
}

// Entry is one segment file: its type, its sequence number (unique in the
// directory, the file name's second part), its size in bytes, its row count
// and its segment integrity root.
type Entry struct {
	Token uint16
	Seq   uint64
	Size  int64
	Rows  int
	Root  [32]byte
}

// Manifest is the directory's truth: the declarations and the committed
// segments, plus the next file sequence number.
type Manifest struct {
	NextSeq  uint64
	Types    []TypeDecl
	Segments []Entry
}

func (m Manifest) clone() Manifest {
	out := Manifest{NextSeq: m.NextSeq, Segments: slices.Clone(m.Segments)}
	for _, t := range m.Types {
		t.Columns = slices.Clone(t.Columns)
		out.Types = append(out.Types, t)
	}
	return out
}

func (m Manifest) declared(token uint16) bool {
	return slices.ContainsFunc(m.Types, func(t TypeDecl) bool { return t.Token == token })
}

// Dir is an open, locked segment directory. It is safe for concurrent use:
// manifest writes are serialized; segment writes run in parallel.
type Dir struct {
	path string
	lock lockHandle

	mu      sync.Mutex
	closed  bool
	man     Manifest // the committed manifest
	nextSeq uint64   // next sequence to hand out (>= man.NextSeq)
	gen     uint64
	hook    func(step string) error
}

// FileName is the file a segment of type token with sequence seq lives in.
func FileName(token uint16, seq uint64) string {
	return fmt.Sprintf("%s%d-%020d%s", segPrefix, token, seq, segSuffix)
}

// parseFileName reports whether name is a canonical segment file name.
func parseFileName(name string) (uint16, uint64, bool) {
	rest, ok := strings.CutPrefix(name, segPrefix)
	if !ok {
		return 0, 0, false
	}
	rest, ok = strings.CutSuffix(rest, segSuffix)
	if !ok {
		return 0, 0, false
	}
	tok, seq, ok := strings.Cut(rest, "-")
	if !ok || len(seq) != 20 {
		return 0, 0, false
	}
	t, err1 := strconv.ParseUint(tok, 10, 16)
	s, err2 := strconv.ParseUint(seq, 10, 64)
	if err1 != nil || err2 != nil || FileName(uint16(t), s) != name {
		return 0, 0, false
	}
	return uint16(t), s, true
}

// Open creates dir if needed, takes its lock (ErrLocked when another open
// Dir holds it) and reads its manifest (absent = empty; damaged =
// ErrManifest). It removes nothing: call Cleanup once the caller has
// validated the manifest.
func Open(path string) (*Dir, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: empty directory path", ErrFile)
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("segdir: create %s: %w", path, err)
	}
	if fi, err := os.Stat(path); err != nil || !fi.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrFile, path)
	}
	syncDirBestEffort(filepath.Dir(path)) // a new directory's own entry
	lk, err := lockDir(filepath.Join(path, lockName))
	if err != nil {
		return nil, err
	}
	man, err := readManifest(filepath.Join(path, manifestName))
	if err != nil {
		lk.release()
		return nil, err
	}
	return &Dir{path: path, lock: lk, man: man, nextSeq: man.NextSeq}, nil
}

// Path returns the directory.
func (d *Dir) Path() string { return d.path }

// Manifest returns a copy of the committed manifest.
func (d *Dir) Manifest() Manifest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.man.clone()
}

// Gen returns the directory's generation. Reset advances it; a Commit that
// names an older generation fails with ErrStale.
func (d *Dir) Gen() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen
}

// SetHook installs a test seam run after each seal-protocol step (nil
// removes it). An error from the hook after StepSegmentWritten,
// StepSegmentSynced or StepManifestWritten fails that WriteSegment or Commit
// as an I/O error would, and the step's file is removed; after
// StepManifestDurable the commit has happened and the hook's error is
// ignored (a kill there is the crash window between the manifest and the row
// removal).
func (d *Dir) SetHook(fn func(step string) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hook = fn
}

func (d *Dir) runHook(step string) error {
	d.mu.Lock()
	h := d.hook
	d.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(step)
}

// Cleanup removes .tmp files of segments and of the manifest, and segment
// files the committed manifest does not list — what a crash before a Commit
// leaves. Files that are not the directory's own are left alone. It must not
// run while a WriteSegment is in flight (callers run it once, at open). It
// returns the names it removed.
func (d *Dir) Cleanup() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	listed := make(map[string]bool, len(d.man.Segments))
	for _, e := range d.man.Segments {
		listed[FileName(e.Token, e.Seq)] = true
	}
	ents, err := os.ReadDir(d.path)
	if err != nil {
		return nil, fmt.Errorf("segdir: list %s: %w", d.path, err)
	}
	var removed []string
	for _, ent := range ents {
		name := ent.Name()
		base, isTmp := strings.CutSuffix(name, tmpSuffix)
		_, _, isSeg := parseFileName(base)
		switch {
		case isTmp && (isSeg || base == manifestName):
		case !isTmp && isSeg && !listed[name]:
		default:
			continue
		}
		if err := os.Remove(filepath.Join(d.path, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, fmt.Errorf("segdir: remove %s: %w", name, err)
		}
		removed = append(removed, name)
	}
	if len(removed) > 0 {
		if err := syncDir(d.path); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// SetTypes persists the declarations (the manifest's segments stay).
func (d *Dir) SetTypes(types []TypeDecl) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	m := d.man.clone()
	m.Types = Manifest{Types: types}.clone().Types
	if err := m.validate(); err != nil {
		return err
	}
	if err := d.writeManifest(m, false); err != nil {
		return err
	}
	d.man = m
	return nil
}

// WriteSegment writes data as a new segment file of a declared type:
// .tmp, fsync, rename, directory fsync. The file is not listed until Commit;
// on any failure it is removed and nothing is left behind.
func (d *Dir) WriteSegment(token uint16, data []byte, rows int, root [32]byte) (Entry, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return Entry{}, ErrClosed
	}
	if !d.man.declared(token) {
		d.mu.Unlock()
		return Entry{}, fmt.Errorf("%w: type token %d is not declared", ErrManifest, token)
	}
	e := Entry{Token: token, Seq: d.nextSeq, Size: int64(len(data)), Rows: rows, Root: root}
	if err := e.validate(); err != nil {
		d.mu.Unlock()
		return Entry{}, err
	}
	d.nextSeq++
	d.mu.Unlock()

	final := filepath.Join(d.path, FileName(e.Token, e.Seq))
	tmp := final + tmpSuffix
	if err := d.writeTmp(tmp, data, StepSegmentWritten); err != nil {
		return Entry{}, err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return Entry{}, fmt.Errorf("segdir: rename %s: %w", tmp, err)
	}
	err := syncDir(d.path)
	if err == nil {
		err = d.runHook(StepSegmentSynced)
	}
	if err != nil {
		_ = os.Remove(final)
		syncDirBestEffort(d.path)
		return Entry{}, err
	}
	return e, nil
}

// writeTmp creates path exclusively, writes data, runs the hook for step
// and fsyncs; on failure the file is removed.
func (d *Dir) writeTmp(path string, data []byte, step string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 -- inside the configured segment directory, canonical names only
	if err != nil {
		return fmt.Errorf("segdir: create %s: %w", path, err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = d.runHook(step)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// Commit adds e (written by WriteSegment) to the manifest durably. It fails
// with ErrStale when the directory was Reset after generation gen was taken;
// the caller then Discards e. Until Commit returns nil the segment does not
// exist.
func (d *Dir) Commit(e Entry, gen uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if gen != d.gen {
		return fmt.Errorf("%w: generation %d, now %d", ErrStale, gen, d.gen)
	}
	m := d.man.clone()
	m.Segments = append(m.Segments, e)
	m.NextSeq = max(m.NextSeq, d.nextSeq)
	if err := m.validate(); err != nil {
		return err
	}
	if err := d.writeManifest(m, true); err != nil {
		return err
	}
	d.man = m
	return nil
}

// Discard removes a written segment file that was not committed (best
// effort; Cleanup at the next open removes what this cannot).
func (d *Dir) Discard(e Entry) {
	final := filepath.Join(d.path, FileName(e.Token, e.Seq))
	_ = os.Remove(final)
	_ = os.Remove(final + tmpSuffix)
	syncDirBestEffort(d.path)
}

// Reset drops every segment: the manifest keeps its declarations and
// sequence and lists no segment, the generation advances (a pending Commit
// fails with ErrStale), and the files are removed.
func (d *Dir) Reset() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.gen++
	m := d.man.clone()
	old := m.Segments
	m.Segments = nil
	m.NextSeq = max(m.NextSeq, d.nextSeq)
	if err := d.writeManifest(m, false); err != nil {
		return err
	}
	d.man = m
	for _, e := range old {
		_ = os.Remove(filepath.Join(d.path, FileName(e.Token, e.Seq)))
	}
	syncDirBestEffort(d.path)
	return nil
}

// Map opens the file of e, checks its size against e.Size and maps it
// read-only. The mapping outlives the Dir; Close it when no reader holds it.
func (d *Dir) Map(e Entry) (*Mapping, error) {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	name := FileName(e.Token, e.Seq)
	f, err := os.Open(filepath.Join(d.path, name)) // #nosec G304 -- inside the configured segment directory, canonical names only
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrFile, name, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrFile, name, err)
	}
	if fi.Size() != e.Size || e.Size < minSegmentBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, the manifest lists %d", ErrFile, name, fi.Size(), e.Size)
	}
	data, unmap, err := mapFile(f, e.Size)
	if err != nil {
		return nil, fmt.Errorf("%w: map %s: %v", ErrFile, name, err)
	}
	liveMappings.Add(1)
	return &Mapping{data: data, unmap: unmap}, nil
}

// Close releases the lock. It is idempotent. Mappings stay valid.
func (d *Dir) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.lock.release()
	return nil
}

// Mapping is a read-only view of one segment file.
type Mapping struct {
	data  []byte
	unmap func() error
	once  sync.Once
}

var liveMappings atomic.Int64

// Bytes returns the mapped bytes. They must not be used after Close.
func (m *Mapping) Bytes() []byte { return m.data }

// Close unmaps. It is idempotent.
func (m *Mapping) Close() error {
	var err error
	m.once.Do(func() {
		m.data = nil
		if m.unmap != nil {
			err = m.unmap()
		}
		liveMappings.Add(-1)
	})
	return err
}

// LiveMappings is the number of mappings not yet closed (a test and
// measurement surface).
func LiveMappings() int64 { return liveMappings.Load() }

// --- manifest encoding ---

type manifestJSON struct {
	NextSeq  uint64      `json:"next_seq"`
	Types    []typeJSON  `json:"types"`
	Segments []entryJSON `json:"segments"`
}

type typeJSON struct {
	Name      string       `json:"name"`
	Token     uint16       `json:"token"`
	BlockRows int          `json:"block_rows"`
	Columns   []columnJSON `json:"columns"`
}

type columnJSON struct {
	Name string `json:"name"`
	Kind uint8  `json:"kind"`
}

type entryJSON struct {
	Token uint16 `json:"token"`
	Seq   uint64 `json:"seq"`
	Size  int64  `json:"size"`
	Rows  int    `json:"rows"`
	Root  string `json:"root"`
}

func frameManifest(version uint16, body []byte) []byte {
	out := make([]byte, 0, manifestHeaderSize+len(body))
	out = append(out, manifestMagic...)
	out = binary.LittleEndian.AppendUint16(out, version)
	out = binary.LittleEndian.AppendUint16(out, 0)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(body))) // #nosec G115 -- bounded by maxManifestBytes
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(body, castagnoli))
	return append(out, body...)
}

func encodeManifest(m Manifest) ([]byte, error) {
	j := manifestJSON{NextSeq: m.NextSeq, Types: []typeJSON{}, Segments: []entryJSON{}}
	for _, t := range m.Types {
		tj := typeJSON{Name: t.Name, Token: t.Token, BlockRows: t.BlockRows}
		for _, c := range t.Columns {
			tj.Columns = append(tj.Columns, columnJSON(c))
		}
		j.Types = append(j.Types, tj)
	}
	for _, e := range m.Segments {
		j.Segments = append(j.Segments, entryJSON{Token: e.Token, Seq: e.Seq, Size: e.Size, Rows: e.Rows, Root: hex.EncodeToString(e.Root[:])})
	}
	body, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("segdir: encode manifest: %w", err)
	}
	if len(body) > maxManifestBytes-manifestHeaderSize {
		return nil, fmt.Errorf("%w: %d bytes exceed the manifest bound", ErrManifest, len(body))
	}
	return frameManifest(manifestVersion, body), nil
}

func readManifest(path string) (Manifest, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Manifest{NextSeq: 1}, nil
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrManifest, err)
	}
	if fi.Size() > maxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: %d bytes", ErrManifest, fi.Size())
	}
	b, err := os.ReadFile(path) // #nosec G304 -- the manifest of the configured segment directory
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrManifest, err)
	}
	return decodeManifest(b)
}

func decodeManifest(b []byte) (Manifest, error) {
	if len(b) < manifestHeaderSize || string(b[:8]) != manifestMagic {
		return Manifest{}, fmt.Errorf("%w: bad header", ErrManifest)
	}
	if v := binary.LittleEndian.Uint16(b[8:]); v != manifestVersion {
		return Manifest{}, fmt.Errorf("%w: unsupported version %d (this build reads %d)", ErrManifest, v, manifestVersion)
	}
	n := binary.LittleEndian.Uint32(b[12:])
	body := b[manifestHeaderSize:]
	if uint64(n) != uint64(len(body)) {
		return Manifest{}, fmt.Errorf("%w: body of %d bytes, header says %d", ErrManifest, len(body), n)
	}
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(b[16:]) {
		return Manifest{}, fmt.Errorf("%w: CRC mismatch", ErrManifest)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var j manifestJSON
	if err := dec.Decode(&j); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrManifest, err)
	}
	if dec.More() {
		return Manifest{}, fmt.Errorf("%w: trailing data", ErrManifest)
	}
	m := Manifest{NextSeq: j.NextSeq}
	for _, tj := range j.Types {
		t := TypeDecl{Name: tj.Name, Token: tj.Token, BlockRows: tj.BlockRows}
		for _, c := range tj.Columns {
			t.Columns = append(t.Columns, Column(c))
		}
		m.Types = append(m.Types, t)
	}
	for _, ej := range j.Segments {
		e := Entry{Token: ej.Token, Seq: ej.Seq, Size: ej.Size, Rows: ej.Rows}
		root, err := hex.DecodeString(ej.Root)
		if err != nil || len(root) != 32 || ej.Root != strings.ToLower(ej.Root) {
			return Manifest{}, fmt.Errorf("%w: segment %d root %q", ErrManifest, ej.Seq, ej.Root)
		}
		copy(e.Root[:], root)
		m.Segments = append(m.Segments, e)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (e Entry) validate() error {
	switch {
	case e.Token == 0:
		return fmt.Errorf("%w: segment %d has type token 0", ErrManifest, e.Seq)
	case e.Seq == 0:
		return fmt.Errorf("%w: segment sequence 0", ErrManifest)
	case e.Size < minSegmentBytes:
		return fmt.Errorf("%w: segment %d of %d bytes (at least %d)", ErrManifest, e.Seq, e.Size, minSegmentBytes)
	case e.Rows < 1 || e.Rows > maxSegmentRows:
		return fmt.Errorf("%w: segment %d holds %d rows", ErrManifest, e.Seq, e.Rows)
	}
	return nil
}

// validate checks everything a manifest may claim.
func (m Manifest) validate() error {
	if m.NextSeq == 0 {
		return fmt.Errorf("%w: next sequence 0", ErrManifest)
	}
	tokens, names := map[uint16]bool{}, map[string]bool{}
	for _, t := range m.Types {
		if t.Token == 0 || tokens[t.Token] {
			return fmt.Errorf("%w: type token %d (zero or listed twice)", ErrManifest, t.Token)
		}
		if strings.TrimSpace(t.Name) == "" || len(t.Name) > maxNameBytes || names[t.Name] {
			return fmt.Errorf("%w: type name %q (blank, too long or listed twice)", ErrManifest, t.Name)
		}
		if t.BlockRows < 1 || t.BlockRows > maxBlockRows || t.BlockRows&(t.BlockRows-1) != 0 {
			return fmt.Errorf("%w: type %q integrity block size %d", ErrManifest, t.Name, t.BlockRows)
		}
		tokens[t.Token], names[t.Name] = true, true
		cols := map[string]bool{}
		for _, c := range t.Columns {
			if c.Name == "" || len(c.Name) > maxNameBytes || cols[c.Name] || c.Kind < 1 || c.Kind > maxKind {
				return fmt.Errorf("%w: type %q column %q kind %d", ErrManifest, t.Name, c.Name, c.Kind)
			}
			cols[c.Name] = true
		}
	}
	seqs := map[uint64]bool{}
	for _, e := range m.Segments {
		if err := e.validate(); err != nil {
			return err
		}
		if !tokens[e.Token] {
			return fmt.Errorf("%w: segment %d of undeclared type token %d", ErrManifest, e.Seq, e.Token)
		}
		if e.Seq >= m.NextSeq || seqs[e.Seq] {
			return fmt.Errorf("%w: segment sequence %d (next %d, or listed twice)", ErrManifest, e.Seq, m.NextSeq)
		}
		seqs[e.Seq] = true
	}
	return nil
}

// writeManifest replaces MANIFEST with m: MANIFEST.tmp, fsync, rename,
// directory fsync. hooked runs the seal-protocol hook (Commit only).
func (d *Dir) writeManifest(m Manifest, hooked bool) error {
	b, err := encodeManifest(m)
	if err != nil {
		return err
	}
	final := filepath.Join(d.path, manifestName)
	tmp := final + tmpSuffix
	_ = os.Remove(tmp) // a leftover from a crash before this open's Cleanup
	step := ""
	if hooked {
		step = StepManifestWritten
	}
	if err := d.writeTmpLocked(tmp, b, step); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("segdir: rename %s: %w", tmp, err)
	}
	if err := syncDir(d.path); err != nil {
		// The rename may or may not be durable: the manifest on disk is
		// the new one or the old one, both valid. Report it.
		return err
	}
	if hooked && d.hook != nil {
		_ = d.hook(StepManifestDurable) // committed: a failure here is a crash, not an error
	}
	return nil
}

// writeTmpLocked is writeTmp for a caller that holds d.mu (the hook is read
// directly).
func (d *Dir) writeTmpLocked(path string, data []byte, step string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 -- inside the configured segment directory
	if err != nil {
		return fmt.Errorf("segdir: create %s: %w", path, err)
	}
	_, err = f.Write(data)
	if err == nil && step != "" && d.hook != nil {
		err = d.hook(step)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// syncDir fsyncs a directory so renames and removals in it are durable.
func syncDir(path string) error {
	f, err := os.Open(path) // #nosec G304 -- the configured segment directory
	if err != nil {
		return fmt.Errorf("segdir: open %s: %w", path, err)
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("segdir: sync %s: %w", path, err)
	}
	return nil
}

func syncDirBestEffort(path string) { _ = syncDir(path) }
