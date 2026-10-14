package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// Native recovery roots use a separate physical namespace. Neither arbitrary
// application KV nor immutable controls can address these index keys. They are
// not graph facts or control records; their actual retained growth still spends
// the same application and generation quotas.
const recoveryRootTag byte = 20
const recoveryRootFixedBytes = 4 + 32 + 32 + 4

func recoveryRootBytes(r ApplicationRecoveryReservation) int {
	return recoveryRootFixedBytes + recoveryCreditBytes*len(r.Credits)
}
func appendRecoveryRoot(r ApplicationRecoveryReservation) []byte {
	b := make([]byte, 0, recoveryRootBytes(r))
	b = append(b, 'R', 'S', 1, 0)
	b = append(b, r.ID[:]...)
	b = append(b, r.Digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Credits))) // #nosec G115 -- credit count admitted under existing metadata frame before encoding.
	for _, c := range r.Credits {
		b = binary.BigEndian.AppendUint32(b, c.Phase)
		b = binary.BigEndian.AppendUint32(b, uint32(c.CommandBytes)) // #nosec G115 -- validated nonnegative entry-policy command ceiling.
		b = binary.BigEndian.AppendUint64(b, c.RetainedBytes)
		b = binary.BigEndian.AppendUint64(b, c.RetainedRecords)
	}
	return b
}
func readRecoveryRoot(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryReservation, error) {
	var r ApplicationRecoveryReservation
	if len(b) < recoveryRootFixedBytes || len(b) > maxMetadataBytes || !bytes.Equal(b[:4], []byte{'R', 'S', 1, 0}) {
		return r, ErrCorrupt
	}
	copy(r.ID[:], b[4:36])
	copy(r.Digest[:], b[36:68])
	count := uint64(binary.BigEndian.Uint32(b[68:72]))
	if count > uint64((len(b)-recoveryRootFixedBytes)/recoveryCreditBytes) || count*recoveryCreditBytes != uint64(len(b)-recoveryRootFixedBytes) { // #nosec G115 -- prior guard admits len(b) 72..65536; both differences are nonnegative <=65464.
		return ApplicationRecoveryReservation{}, ErrCorrupt
	}
	if count > 0 {
		r.Credits = make([]ApplicationRecoveryCredit, int(count))
	} // #nosec G115 -- count bounded by delivered <=64 KiB before allocation.
	for i := range r.Credits {
		row := b[72+i*recoveryCreditBytes : 72+(i+1)*recoveryCreditBytes]
		command := uint64(binary.BigEndian.Uint32(row[4:8]))
		if command > unsignedLimit(l.MaxEntryBytes-frameOverhead) {
			return ApplicationRecoveryReservation{}, ErrCorrupt
		}
		r.Credits[i] = ApplicationRecoveryCredit{Phase: binary.BigEndian.Uint32(row[:4]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(row[8:16]), RetainedRecords: binary.BigEndian.Uint64(row[16:24])} // #nosec G115 -- command bounded by concrete <=16 MiB entry ceiling.
	}
	if r.ID == ([32]byte{}) {
		if r.Digest != ([32]byte{}) || count != 0 {
			return ApplicationRecoveryReservation{}, ErrCorrupt
		}
	} else if _, err := recoveryGrowth(r, p, l); err != nil {
		return ApplicationRecoveryReservation{}, ErrCorrupt
	}
	return r, nil
}

// ApplicationContractForPolicyWithRecovery selects the explicit native recovery
// storage profile. Disabled recovery preserves existing contracts byte-exact.
// It adds no numerical quota or graph/cut/transaction authority.
func ApplicationContractForPolicyWithRecovery(p ApplicationPolicy, c ApplicationControlConfig, r ApplicationRecoveryConfig) (ApplicationContract, error) {
	if err := r.validate(); err != nil {
		return ApplicationContract{}, err
	}
	contract, err := ApplicationContractForPolicyWithControls(p, c)
	if err != nil {
		return ApplicationContract{}, err
	}
	if r.enabled() {
		if !c.enabled() {
			return ApplicationContract{}, ErrInvalid
		}
		contract.Version = 2 + r.Version
	}
	return contract, nil
}

// ApplicationRecovery reports immutable configured storage capability only.
// Nil is disabled; a closed handle conveys no validity or phase authority.
func (s *Store) ApplicationRecovery() ApplicationRecoveryConfig {
	if s == nil {
		return ApplicationRecoveryConfig{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Recovery.Config
}

// ApplicationRecoveryRootOverhead bounds the one additional retained recovery
// envelope of a native installation. Existing root/change/outcome frames remain
// separate. Counts are checked before encoding; the result is logical storage
// growth, not staging work, process memory or permission to reserve a phase.
func ApplicationRecoveryRootOverhead(creditCount int) (uint64, uint64, error) {
	if creditCount < 0 {
		return 0, 0, ErrInvalid
	}
	if creditCount > (maxMetadataBytes-recoveryRootFixedBytes)/recoveryCreditBytes {
		return 0, 0, ErrLimit
	}
	return uint64(9 + appFrameBytes + recoveryRootFixedBytes + recoveryCreditBytes*creditCount), 1, nil // #nosec G115 -- nonnegative credit count bounded by 64 KiB before arithmetic/conversion.
}

func applicationInstallRecords(m metadata) uint64 {
	n := uint64(m.App.Policy.MaxInstallWrites + 3) // #nosec G115 -- opened application policy validates MaxInstallWrites 1..4096; sum is 4..4099.
	if m.Recovery.Config.enabled() {
		n++
	}
	return n
}

func (p ApplicationPolicy) measureRecovery(b ApplicationBatch, r ApplicationRecoveryReservation) (uint64, uint64, error) {
	added, count, err := p.measure(b)
	if err != nil {
		return 0, 0, err
	}
	n, rows, err := ApplicationRecoveryRootOverhead(len(r.Credits))
	if err != nil {
		return 0, 0, err
	}
	if recoveryRootBytes(r) > p.MaxValueBytes {
		return 0, 0, ErrLimit
	}
	// p.measure already admitted the original owners/copies. This additional
	// envelope uses a disjoint tag/index, so there is no control absence read.
	work := 2*added + uint64(len(b.Image)+1024) + uint64(64*(len(b.Writes)+len(b.ControlPuts)))
	for _, w := range b.ControlPuts {
		work += 128 + 4*controlPhysicalKeyBytes(w.Key)
	}
	if work > unsignedLimit(p.MaxInstallBytes) || 2*n > unsignedLimit(p.MaxInstallBytes)-work {
		return 0, 0, ErrLimit
	}
	return added + n, count + rows, nil
}

// Snapshot credit syntax is inspected under hard portable ceilings. Receiver
// configured quotas are checked separately before import/reservation admission.
func portableRecoveryPolicy() ApplicationPolicy {
	p := DefaultApplicationPolicy(1)
	p.RetainedApplicationBytes = 1 << 40
	p.RetainedApplicationRecords = 1 << 40
	return p
}
func portableRecoveryLimits() Limits {
	l := DefaultLimits()
	l.MaxEntryBytes = 16 << 20
	return l
}

func recoveryRootDigest(r ApplicationRecoveryReservation) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte{'R', 'S', 1, 0})
	_, _ = h.Write(r.ID[:])
	_, _ = h.Write(r.Digest[:])
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(r.Credits))) // #nosec G115 -- validated native/portable reservation or equal framed count admits at most (65536-72)/24 credits.
	_, _ = h.Write(count[:])
	var row [24]byte
	for _, c := range r.Credits {
		binary.BigEndian.PutUint32(row[:4], c.Phase)
		binary.BigEndian.PutUint32(row[4:8], uint32(c.CommandBytes))
		binary.BigEndian.PutUint64(row[8:16], c.RetainedBytes)
		binary.BigEndian.PutUint64(row[16:24], c.RetainedRecords)
		_, _ = h.Write(row[:])
	} // #nosec G115 -- commands validated under nonnegative <=16 MiB entry policy.
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func (s *Store) loadRecoveryRootAt(bank byte, index uint64) (r ApplicationRecoveryReservation, err error) {
	key := appIndexKey(recoveryRootTag+bank, index)
	raw, closer, err := s.db.Get(key)
	if err != nil {
		return r, storedReadFailure(err)
	}
	defer func() { err = errors.Join(err, closer.Close()) }()
	data, deleted, err := inspectAppFrame(key, raw, min(s.applicationPolicy.MaxValueBytes, maxMetadataBytes))
	if err != nil || deleted {
		return r, ErrCorrupt
	}
	r, _, readErr := readNativeRecoveryRoot(data, s.applicationPolicy, s.limits)
	if readErr == nil && data[2] != byte(s.meta.Recovery.Config.Version) {
		return ApplicationRecoveryReservation{}, ErrCorrupt
	}
	return r, readErr
}
func cloneRecoveryReservation(r ApplicationRecoveryReservation) ApplicationRecoveryReservation {
	if len(r.Credits) > 0 {
		credits := make([]ApplicationRecoveryCredit, len(r.Credits))
		copy(credits, r.Credits)
		r.Credits = credits
	}
	return r
}

func recoveryHashForManifest(m ApplicationSnapshotManifest) [32]byte {
	if m.Version == 6 {
		return nativeRecoveryRootDigest(snapshotRecoveryState(m))
	}
	if m.Version != 5 {
		return [32]byte{}
	}
	return recoveryRootDigest(m.RecoveryReservation)
}

func validatePortableRecoveryReservation(r ApplicationRecoveryReservation) error {
	if r.ID == ([32]byte{}) {
		if r.Digest != ([32]byte{}) || len(r.Credits) != 0 {
			return ErrCorrupt
		}
		return nil
	}
	_, err := recoveryGrowth(r, portableRecoveryPolicy(), portableRecoveryLimits())
	return err
}

func recoveryGrowthIfPresent(r ApplicationRecoveryReservation, p ApplicationPolicy, l Limits) (applicationRecoveryGrowth, error) {
	if r.ID == ([32]byte{}) {
		if r.Digest != ([32]byte{}) || len(r.Credits) != 0 {
			return applicationRecoveryGrowth{}, ErrInvalid
		}
		return applicationRecoveryGrowth{}, nil
	}
	return recoveryGrowth(r, p, l)
}

// ApplicationRecoveryOwner is scalar evidence about one actual native root.
// It grants no phase, graph, decision or capacity authority.
type ApplicationRecoveryOwner struct{ ID, Digest [32]byte }

func inspectRecoveryOwner(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryOwner, int, error) {
	var out ApplicationRecoveryOwner
	if len(b) < recoveryRootFixedBytes || len(b) > maxMetadataBytes || !bytes.Equal(b[:4], []byte{'R', 'S', 1, 0}) {
		return out, 0, ErrCorrupt
	}
	copy(out.ID[:], b[4:36])
	copy(out.Digest[:], b[36:68])
	count := uint64(binary.BigEndian.Uint32(b[68:72]))
	if count != uint64((len(b)-recoveryRootFixedBytes)/recoveryCreditBytes) || count*recoveryCreditBytes != uint64(len(b)-recoveryRootFixedBytes) { // #nosec G115 -- prior guard admits len(b) 72..65536; subtraction and division stay nonnegative <=65464.
		return ApplicationRecoveryOwner{}, 0, ErrCorrupt
	}
	if out.ID == ([32]byte{}) {
		if out.Digest != ([32]byte{}) || count != 0 {
			return ApplicationRecoveryOwner{}, 0, ErrCorrupt
		}
		return out, 0, nil
	}
	if out.Digest == ([32]byte{}) || count == 0 {
		return ApplicationRecoveryOwner{}, 0, ErrCorrupt
	}
	var previous uint32
	var total applicationRecoveryGrowth
	for i := uint64(0); i < count; i++ {
		row := b[72+i*recoveryCreditBytes : 72+(i+1)*recoveryCreditBytes]
		phase := binary.BigEndian.Uint32(row[:4])
		command := uint64(binary.BigEndian.Uint32(row[4:8]))
		rb, rr := binary.BigEndian.Uint64(row[8:16]), binary.BigEndian.Uint64(row[16:24])
		if phase == 0 || phase <= previous || command > unsignedLimit(l.MaxEntryBytes-frameOverhead) || rb == 0 || rr == 0 || rb > p.RetainedApplicationBytes || rr > p.RetainedApplicationRecords {
			return ApplicationRecoveryOwner{}, 0, ErrCorrupt
		}
		previous = phase
		var err error
		total.bytes, err = checkedRecoveryAdd(total.bytes, rb)
		if err != nil {
			return ApplicationRecoveryOwner{}, 0, ErrCorrupt
		}
		total.records, err = checkedRecoveryAdd(total.records, rr)
		if err != nil {
			return ApplicationRecoveryOwner{}, 0, ErrCorrupt
		}
		total.logBytes, err = checkedRecoveryAdd(total.logBytes, command+frameOverhead+9)
		if err != nil {
			return ApplicationRecoveryOwner{}, 0, ErrCorrupt
		}
	}
	return out, int(count), nil // #nosec G115 -- count exactly framed by <=64 KiB delivered bytes.
}

// RecoveryOwner inspects the native root at this exact registered bank/cut.
// It owns no credit array, extra view or history pin. Work conservatively charges
// the point key/frame, CRC, syntax traversal, and two possible equality hashes;
// this is representation/source work, never a heap/RSS or quorum proof.
func (v *ApplicationView) RecoveryOwner(ctx context.Context, budget ReadBudget) (owner ApplicationRecoveryOwner, work ApplicationProofWork, err error) {
	if err = v.lock(ctx); err != nil {
		return owner, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !v.s.meta.Recovery.Config.enabled() || budget.Rows < 1 || budget.Bytes < 1 {
		return owner, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxPageBytes {
		return owner, work, ErrLimit
	}
	const fixed = 128 + 4*9
	if budget.Bytes < fixed {
		return owner, work, ErrLimit
	}
	work.Bytes = fixed
	key := appIndexKey(recoveryRootTag+v.bank, v.index)
	work.Records = 1
	raw, closer, e := v.s.db.Get(key)
	if e != nil {
		return owner, work, v.fail(storedReadFailure(e))
	}
	defer func() {
		if e := closer.Close(); e != nil {
			err = errors.Join(err, v.fail(e))
			owner = ApplicationRecoveryOwner{}
		}
	}()
	if len(raw) < appFrameBytes || len(raw) > appFrameBytes+min(p.MaxValueBytes, maxMetadataBytes) {
		return owner, work, v.fail(ErrCorrupt)
	}
	dataBytes := len(raw) - appFrameBytes
	cost := len(key) + len(raw) + len(key) + 4 + 4*dataBytes + 64
	if cost > budget.Bytes-work.Bytes {
		return owner, work, ErrLimit
	}
	work.Bytes += cost
	data, deleted, e := inspectAppFrame(key, raw, min(p.MaxValueBytes, maxMetadataBytes))
	if e != nil || deleted {
		return owner, work, v.fail(ErrCorrupt)
	}
	parsed, count, e := inspectNativeRecoveryOwner(data, p, v.s.limits)
	if e != nil {
		return owner, work, v.fail(e)
	}
	var expected *ApplicationRecoveryReservation
	var expectedNative *applicationRecoveryMetadata
	if v.prepared != nil && v.index == v.prepared.i.manifest.Index {
		expected = &v.prepared.i.manifest.RecoveryReservation
		if v.prepared.i.manifest.Version == 6 {
			imported := snapshotRecoveryState(v.prepared.i.manifest)
			expectedNative = &imported
		}
	} else if v.bank == v.s.activeBank() && v.generation == v.s.activeGeneration() && v.index == v.s.meta.Applied {
		expected = &v.s.meta.Recovery.Reservation
		expectedNative = &v.s.meta.Recovery
	}
	if expected != nil && (parsed.ID != expected.ID || parsed.Digest != expected.Digest || count != len(expected.Credits) || sha256.Sum256(data) != expectedNativeRecoveryDigest(expectedNative, *expected)) {
		return owner, work, v.fail(ErrCorrupt)
	}
	if e := ctx.Err(); e != nil {
		return owner, work, e
	}
	return parsed, work, nil
}

func (s *Store) loadRecoveryOwnerAt(bank byte, index uint64) (out ApplicationRecoveryOwner, err error) {
	key := appIndexKey(recoveryRootTag+bank, index)
	raw, closer, err := s.db.Get(key)
	if err != nil {
		return out, storedReadFailure(err)
	}
	defer func() { err = errors.Join(err, closer.Close()) }()
	data, deleted, err := inspectAppFrame(key, raw, min(s.applicationPolicy.MaxValueBytes, maxMetadataBytes))
	if err != nil || deleted {
		return out, ErrCorrupt
	}
	out, _, err = inspectNativeRecoveryOwner(data, s.applicationPolicy, s.limits)
	return out, err
}
