// Package segdir is the durable half of ADR-0011 step S3: a directory of
// immutable column-segment files, a manifest that lists them, and the crash
// protocol that keeps the two consistent. It knows nothing about the codec
// (package segment) or a store; it moves bytes and keeps the manifest true.
package segdir

import "errors"

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

// Errors. Each error the package returns wraps exactly one of these.
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
	// errNotBuilt marks the S3 skeleton (removed once built).
	errNotBuilt = errors.New("segdir: not built")
)

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

// Entry is one committed segment file.
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

// Dir is an open, locked segment directory.
type Dir struct{}

// Open creates dir if needed, takes its lock and reads its manifest (absent =
// empty). It removes nothing: call Cleanup once the manifest is validated.
func Open(path string) (*Dir, error) { return nil, errNotBuilt }

// FileName is the file a segment of type token with sequence seq lives in.
func FileName(token uint16, seq uint64) string { return "" }

// Manifest returns a copy of the committed manifest.
func (d *Dir) Manifest() Manifest { return Manifest{} }

// Path returns the directory.
func (d *Dir) Path() string { return "" }

// Gen returns the directory's generation (Reset advances it).
func (d *Dir) Gen() uint64 { return 0 }

// SetHook installs a test seam run after each seal-protocol step.
func (d *Dir) SetHook(fn func(step string) error) {}

// Cleanup removes .tmp files and segment files the manifest does not list.
func (d *Dir) Cleanup() ([]string, error) { return nil, errNotBuilt }

// SetTypes persists the declarations.
func (d *Dir) SetTypes(types []TypeDecl) error { return errNotBuilt }

// WriteSegment writes data as a new, not yet committed segment file.
func (d *Dir) WriteSegment(token uint16, data []byte, rows int, root [32]byte) (Entry, error) {
	return Entry{}, errNotBuilt
}

// Commit adds e to the manifest durably.
func (d *Dir) Commit(e Entry, gen uint64) error { return errNotBuilt }

// Discard removes a written, uncommitted segment file.
func (d *Dir) Discard(e Entry) {}

// Reset drops every segment.
func (d *Dir) Reset() error { return errNotBuilt }

// Map opens and maps the file of a committed entry.
func (d *Dir) Map(e Entry) (*Mapping, error) { return nil, errNotBuilt }

// Close releases the lock.
func (d *Dir) Close() error { return errNotBuilt }

// Mapping is a read-only view of one segment file.
type Mapping struct{}

// Bytes returns the mapped bytes.
func (m *Mapping) Bytes() []byte { return nil }

// Close unmaps.
func (m *Mapping) Close() error { return errNotBuilt }

// LiveMappings is the number of mappings not yet closed.
func LiveMappings() int64 { return 0 }

const (
	manifestVersion    = 1
	manifestHeaderSize = 20
)

func frameManifest(version uint16, body []byte) []byte { return nil }
