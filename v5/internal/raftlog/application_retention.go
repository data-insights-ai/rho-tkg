package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"
)

// ApplicationRetentionConfig opts a fresh native store into sparse retained
// history. Version zero preserves prior retention/wire contracts exactly.
// Version one spends the existing application, generation, read and 64KiB
// metadata quotas; it does not increase them or authorize collection itself.
type ApplicationRetentionConfig struct{ Version uint32 }

func (c ApplicationRetentionConfig) enabled() bool { return c.Version != 0 }
func (c ApplicationRetentionConfig) validate() error {
	if c.Version > 1 {
		return ErrInvalid
	}
	return nil
}

// ApplicationRetentionRoot is an inert plan value. Data additionally protects
// each key's genuine predecessor at Index; an image-only semantic/genesis anchor
// does not retain a whole old data prefix. Actual Store admission must authenticate
// the retained index/hash before this value can protect or expose an anchor.
type ApplicationRetentionRoot struct {
	Index     uint64
	ImageHash [32]byte
	Data      bool
}

// ApplicationRetentionState describes a durable sparse-history plan. Floors
// fence ordinary NEW history access, not graph valid time or a physical clock.
// Roots are separate exact anchors; their existence cannot admit expired cuts.
// No caller-created state is an installation or collection capability.
type ApplicationRetentionState struct {
	ID, PreviousID, SourceHash, RetiredCutID                     [32]byte
	SourceIndex, InstalledIndex                                  uint64
	RootFloor, FeedFloor, OutcomeFloor, RecoveryFloor, DataFloor uint64
	Roots                                                        []ApplicationRetentionRoot
}

const maxApplicationRetentionRoots = 64
const retentionRootBytes = 41
const retentionMetadataFixedBytes = 4 + 4 + 4*32 + 7*8 + 4 + 1 + 4 + 32

// The cursor payload belongs to a separate bounded, accounted local record.
// Metadata binds its exact length/hash and section; it never owns an unchecked
// physical key. Local progress is not portable GC authority or a graph cut.
type applicationRetentionCursor struct {
	Section byte
	Bytes   uint32
	Hash    [32]byte
}
type applicationRetentionMetadata struct {
	Config ApplicationRetentionConfig
	State  ApplicationRetentionState
	Cursor applicationRetentionCursor
}

func (s ApplicationRetentionState) zero() bool {
	return s.ID == ([32]byte{}) && s.PreviousID == ([32]byte{}) && s.SourceHash == ([32]byte{}) && s.RetiredCutID == ([32]byte{}) && s.SourceIndex == 0 && s.InstalledIndex == 0 && s.RootFloor == 0 && s.FeedFloor == 0 && s.OutcomeFloor == 0 && s.RecoveryFloor == 0 && s.DataFloor == 0 && len(s.Roots) == 0
}
func (s ApplicationRetentionState) validate(applied uint64) error {
	if s.ID == ([32]byte{}) {
		if !s.zero() {
			return ErrInvalid
		}
		return nil
	}
	if s.SourceHash == ([32]byte{}) || s.SourceIndex == 0 || s.SourceIndex >= math.MaxUint64-1 || s.SourceIndex+1 != s.InstalledIndex || s.InstalledIndex > applied || s.InstalledIndex == math.MaxUint64 || s.PreviousID == s.ID || len(s.Roots) > maxApplicationRetentionRoots {
		return ErrInvalid
	}
	if s.RootFloor > s.SourceIndex || s.FeedFloor > s.SourceIndex+1 || s.OutcomeFloor > s.SourceIndex || s.RecoveryFloor > s.SourceIndex || s.DataFloor > s.SourceIndex {
		return ErrInvalid
	}
	for i, root := range s.Roots {
		if root.Index == 0 || root.Index > s.SourceIndex || root.ImageHash == ([32]byte{}) || i > 0 && s.Roots[i-1].Index >= root.Index {
			return ErrInvalid
		}
	}
	return nil
}
func (r applicationRetentionMetadata) zero() bool {
	return r.Config == (ApplicationRetentionConfig{}) && r.State.zero() && r.Cursor == (applicationRetentionCursor{})
}
func (r applicationRetentionMetadata) syntax(applied uint64) error {
	if r.Config.Version != 1 {
		return ErrInvalid
	}
	if err := r.State.validate(applied); err != nil {
		return err
	}
	if r.Cursor.Section > 6 || r.Cursor.Bytes > 128<<10 || (r.Cursor.Bytes == 0) != (r.Cursor.Hash == ([32]byte{})) || (r.Cursor.Section == 0 || r.Cursor.Section == 6) && r.Cursor.Bytes != 0 || r.State.ID == ([32]byte{}) && r.Cursor != (applicationRetentionCursor{}) {
		return ErrInvalid
	}
	return nil
}
func validateRetentionMetadata(m metadata) error {
	r := m.Retention
	if !r.Config.enabled() {
		if !r.zero() || m.Transfer.Contract.Version >= 5 {
			return ErrInvalid
		}
		return nil
	}
	if err := r.syntax(m.Applied); err != nil {
		return err
	}
	if !m.App.Policy.Enabled() || !m.Rep.Config.enabled() || m.Rep.SemanticContractID == (ApplicationSemanticContractID{}) || m.Controls.Config.Version != 1 || (m.Recovery.Config.Version != 2 || m.Transfer.Contract.Version != 5) && (m.Recovery.Config.Version != 3 || m.Transfer.Contract.Version != 6) || !m.Gen.Limits.enabled() || !m.Gen.Publication.Limits.enabled() {
		return ErrInvalid
	}
	return nil
}
func retentionMetadataBytes(r applicationRetentionMetadata) int {
	return retentionMetadataFixedBytes + retentionRootBytes*len(r.State.Roots)
}
func appendRetentionMetadata(b []byte, r applicationRetentionMetadata) []byte {
	b = append(b, 'R', 'T', 1, 0)
	b = binary.BigEndian.AppendUint32(b, r.Config.Version)
	for _, hash := range [][32]byte{r.State.ID, r.State.PreviousID, r.State.SourceHash, r.State.RetiredCutID} {
		b = append(b, hash[:]...)
	}
	for _, value := range []uint64{r.State.SourceIndex, r.State.InstalledIndex, r.State.RootFloor, r.State.FeedFloor, r.State.OutcomeFloor, r.State.RecoveryFloor, r.State.DataFloor} {
		b = binary.BigEndian.AppendUint64(b, value)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.State.Roots))) // #nosec G115 -- count prevalidated <=64 and existing metadata cap.
	for _, root := range r.State.Roots {
		b = binary.BigEndian.AppendUint64(b, root.Index)
		b = append(b, root.ImageHash[:]...)
		flag := byte(0)
		if root.Data {
			flag = 1
		}
		b = append(b, flag)
	}
	b = append(b, r.Cursor.Section)
	b = binary.BigEndian.AppendUint32(b, r.Cursor.Bytes)
	return append(b, r.Cursor.Hash[:]...)
}
func decodeRetentionMetadata(b []byte, applied uint64) (applicationRetentionMetadata, []byte, error) {
	var r applicationRetentionMetadata
	if len(b) < retentionMetadataFixedBytes || len(b) > maxMetadataBytes || !bytes.Equal(b[:4], []byte{'R', 'T', 1, 0}) || binary.BigEndian.Uint32(b[4:8]) != 1 {
		return r, nil, ErrCorrupt
	}
	r.Config.Version = 1
	offset := 8
	for _, hash := range []*[32]byte{&r.State.ID, &r.State.PreviousID, &r.State.SourceHash, &r.State.RetiredCutID} {
		copy(hash[:], b[offset:offset+32])
		offset += 32
	}
	for _, value := range []*uint64{&r.State.SourceIndex, &r.State.InstalledIndex, &r.State.RootFloor, &r.State.FeedFloor, &r.State.OutcomeFloor, &r.State.RecoveryFloor, &r.State.DataFloor} {
		*value = binary.BigEndian.Uint64(b[offset : offset+8])
		offset += 8
	}
	count := binary.BigEndian.Uint32(b[offset : offset+4])
	offset += 4
	if count > maxApplicationRetentionRoots || uint64(count)*retentionRootBytes > uint64(len(b)-retentionMetadataFixedBytes) {
		return applicationRetentionMetadata{}, nil, ErrCorrupt
	}
	if count > 0 {
		r.State.Roots = make([]ApplicationRetentionRoot, int(count))
	} // #nosec G115 -- count checked <=64 and delivered-length bounds before ownership.
	for i := range r.State.Roots {
		root := &r.State.Roots[i]
		root.Index = binary.BigEndian.Uint64(b[offset : offset+8])
		offset += 8
		copy(root.ImageHash[:], b[offset:offset+32])
		offset += 32
		if b[offset] > 1 {
			return applicationRetentionMetadata{}, nil, ErrCorrupt
		}
		root.Data = b[offset] == 1
		offset++
	}
	r.Cursor.Section = b[offset]
	offset++
	r.Cursor.Bytes = binary.BigEndian.Uint32(b[offset : offset+4])
	offset += 4
	copy(r.Cursor.Hash[:], b[offset:offset+32])
	offset += 32
	if err := r.syntax(applied); err != nil {
		return applicationRetentionMetadata{}, nil, ErrCorrupt
	}
	return r, b[offset:], nil
}

// ApplicationRetention reports configured capability only, including on closed
// handles. Nil reports disabled; it grants no plan, anchor or collection rights.
func (s *Store) ApplicationRetention() ApplicationRetentionConfig {
	if s == nil {
		return ApplicationRetentionConfig{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Retention.Config
}

// Portable retention has no local collection cursor. The canonical header is
// bounded by the same64-root/metadata limits and remains inert after decoding.
func cloneRetentionState(s ApplicationRetentionState) ApplicationRetentionState {
	s.Roots = slices.Clone(s.Roots)
	return s
}
func portableRetentionBytes(s ApplicationRetentionState) int {
	return retentionMetadataBytes(applicationRetentionMetadata{State: s})
}
func appendPortableRetention(b []byte, s ApplicationRetentionState) []byte {
	return appendRetentionMetadata(b, applicationRetentionMetadata{Config: ApplicationRetentionConfig{Version: 1}, State: s})
}
func decodePortableRetention(b []byte, index uint64) (ApplicationRetentionState, error) {
	r, tail, err := decodeRetentionMetadata(b, index)
	if err != nil || len(tail) != 0 || r.Cursor != (applicationRetentionCursor{}) {
		return ApplicationRetentionState{}, ErrCorrupt
	}
	if !bytes.Equal(appendPortableRetention(nil, r.State), b) {
		return ApplicationRetentionState{}, ErrCorrupt
	}
	return r.State, nil
}
func retentionStateHash(s ApplicationRetentionState) [32]byte {
	return sha256.Sum256(appendPortableRetention(nil, s))
}
func retainedThrough(index, floor uint64) uint64 {
	start := max(uint64(1), floor)
	if start > index {
		return 0
	}
	return index - start + 1
}
func retainedRootMinimum(index uint64, s ApplicationRetentionState) uint64 {
	n := retainedThrough(index, s.RootFloor)
	for _, root := range s.Roots {
		if root.Index < s.RootFloor {
			n++
		}
	}
	return n
}
func sparseRetentionStep(previous, index, floor uint64) bool {
	if index <= previous {
		return false
	}
	if index < max(uint64(1), floor) {
		return true
	}
	return index == max(previous+1, max(uint64(1), floor))
}
