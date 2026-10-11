package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

// These private codecs return inert data, never registration or live authority.
const registryWorkMax = 46144850
const registryOwnedMax = 4198929
const registryFixed = 4096
const registryPreparedMax = 4 << 20

type registeredRegistryCodecBudget struct{ workBytes, ownedBytes int }
type registeredSubmittedRef struct {
	lookupKey                    [96]byte
	kind                         byte
	submissionHash, preparedHash [32]byte
}
type registeredRegistryRef struct {
	submitted      registeredSubmittedRef
	storedKey      [89]byte
	position, term uint64
}
type registeredRegistryOutcome struct {
	status          byte
	evaluationIndex uint64
	submitted       registeredSubmittedRef
	original        registeredRegistryRef
}
type registeredRegistryExport struct {
	representation               byte
	submissionHash, preparedHash [32]byte
	address                      []byte
	term                         uint64
	prepared                     []byte
}

// Work and ownership are independent consumptive logical reservations. Fixed
// state includes the borrowed inspector and all fixed keys/return tuples.
type registryLedger struct{ work, owned int }

func registryStart(b registeredRegistryCodecBudget, n, minimum, maximum int) (*registryLedger, error) {
	if b.workBytes < 1 || b.workBytes > registryWorkMax || b.ownedBytes < 1 || b.ownedBytes > registryOwnedMax {
		return nil, errInvalid
	}
	if n < minimum {
		return nil, errInvalid
	}
	if n > maximum {
		return nil, errLimit
	}
	q := &registryLedger{b.workBytes, b.ownedBytes}
	if err := q.spend(registryFixed+32, registryFixed); err != nil {
		return nil, err
	}
	return q, nil
}
func (q *registryLedger) spend(work, owned int) error {
	if work < 0 || owned < 0 || work > q.work || owned > q.owned {
		return errLimit
	}
	q.work -= work
	q.owned -= owned
	return nil
}
func (q *registryLedger) inspect(pij []byte, l registeredFormatBudget) (got registeredPreparedInspection, err error) {
	// All outer ranges/output were reserved already. Lend, then settle actual
	// work once; the child charges BEFORE actions and cannot exceed this loan.
	child := min(l.walkBytes, q.work)
	if child < registeredMetadataBytes {
		return registeredPreparedInspection{}, errLimit
	}
	l.walkBytes = child
	got, err = inspectPreparedV1Expected(pij, l, registeredInventorySemanticContractID())
	if err != nil {
		return registeredPreparedInspection{}, err
	}
	if got.canonicalWork > 32<<20 {
		return registeredPreparedInspection{}, errLimit
	}
	work := int(got.canonicalWork)
	if err := q.spend(work, 0); err != nil {
		return registeredPreparedInspection{}, err
	}
	return got, nil
}

// Both conversions have direct bounds before narrowing; no wire count sizes
// backing before validation. The largest outer payload is PIJ+220.
func registrySize(n int) (uint32, error) {
	if n < 0 {
		return 0, errInvalid
	}
	if n > registryPreparedMax+220 {
		return 0, errLimit
	}
	return uint32(n), nil
}
func registryReadSize(b []byte, maximum int) (int, error) {
	if len(b) < 4 {
		return 0, errInvalid
	}
	n := binary.BigEndian.Uint32(b[:4])
	if n > registryPreparedMax+220 {
		return 0, errLimit
	}
	value := int(n)
	if value > maximum {
		return 0, errLimit
	}
	return value, nil
}
func registryScopeBytes(b []byte, o registeredOriginScope) {
	copy(b[:16], o.graph[:])
	binary.BigEndian.PutUint64(b[16:24], o.partition)
	copy(b[24:40], o.group[:])
	copy(b[40:72], o.lineage[:])
	binary.BigEndian.PutUint16(b[72:74], o.protocol)
}
func registryReadScope(b []byte) (o registeredOriginScope) {
	copy(o.graph[:], b[:16])
	o.partition = binary.BigEndian.Uint64(b[16:24])
	copy(o.group[:], b[24:40])
	copy(o.lineage[:], b[40:72])
	o.protocol = binary.BigEndian.Uint16(b[72:74])
	return o
}
func registryKeys(o registeredOriginScope, key [16]byte, position uint64) (hki [96]byte, hkv [89]byte) {
	copy(hki[:5], "HKI1\x02")
	registryScopeBytes(hki[5:79], o)
	hki[79] = 1
	copy(hki[80:], key[:])
	copy(hkv[:5], "HKV1\x03")
	registryScopeBytes(hkv[5:79], o)
	binary.BigEndian.PutUint64(hkv[79:87], position)
	return hki, hkv // Mandatory slot[87:89] is exactly zero.
}
func registrySubmittedScope(ref registeredSubmittedRef) (registeredOriginScope, error) {
	if !bytes.Equal(ref.lookupKey[:5], []byte("HKI1\x02")) || ref.lookupKey[79] != 1 || ref.kind < 1 || ref.kind > 2 {
		return registeredOriginScope{}, errInvalid
	}
	o := registryReadScope(ref.lookupKey[5:79])
	var key [16]byte
	copy(key[:], ref.lookupKey[80:])
	fixed := registeredLedger{limit: 86}
	if err := registeredOriginCheck(o, key, &fixed); err != nil {
		return registeredOriginScope{}, err
	}
	return o, nil
}
func registryCheckRef(ref registeredRegistryRef) error {
	o, err := registrySubmittedScope(ref.submitted)
	if err != nil {
		return err
	}
	if ref.position == 0 || ref.term == 0 {
		return errInvalid
	}
	var key [16]byte
	copy(key[:], ref.submitted.lookupKey[80:])
	_, expected := registryKeys(o, key, ref.position)
	if ref.storedKey != expected {
		return errInvalid
	}
	return nil
}
func registryRef(got *registeredPreparedInspection, position, term uint64) registeredRegistryRef {
	hki, hkv := registryKeys(got.parsed.o, got.parsed.key, position)
	return registeredRegistryRef{registeredSubmittedRef{hki, got.parsed.s.kind, got.submissionHash, got.preparedHash}, hkv, position, term}
}
func registryChecksum(wire []byte) bool {
	sum := sha256.Sum256(wire[:len(wire)-32])
	return bytes.Equal(sum[:], wire[len(wire)-32:])
}
func registrySeal(wire []byte) {
	sum := sha256.Sum256(wire[:len(wire)-32])
	copy(wire[len(wire)-32:], sum[:])
}

func encodeRegistryCommand(pij []byte, l registeredFormatBudget, b registeredRegistryCodecBudget) ([]byte, error) {
	if err := registeredBudget(l); err != nil {
		return nil, err
	}
	q, err := registryStart(b, len(pij), 136, l.preparedBytes)
	if err != nil {
		return nil, err
	}
	n := len(pij) + 11
	if err := q.spend(2*n, n); err != nil {
		return nil, err
	}
	if _, err := q.inspect(pij, l); err != nil {
		return nil, err
	}
	size, err := registrySize(len(pij))
	if err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out[:7], "RJQ1\x00\x01\x00")
	binary.BigEndian.PutUint32(out[7:11], size)
	copy(out[11:], pij)
	return out, nil
}
func decodeRegistryCommand(wire []byte, l registeredFormatBudget, b registeredRegistryCodecBudget) (registeredPreparedInspection, error) {
	if err := registeredBudget(l); err != nil {
		return registeredPreparedInspection{}, err
	}
	q, err := registryStart(b, len(wire), 147, l.preparedBytes+11)
	if err != nil {
		return registeredPreparedInspection{}, err
	}
	if err := q.spend(len(wire), 0); err != nil {
		return registeredPreparedInspection{}, err
	}
	n, err := registryReadSize(wire[7:11], l.preparedBytes)
	if err != nil {
		return registeredPreparedInspection{}, err
	}
	if !bytes.Equal(wire[:7], []byte("RJQ1\x00\x01\x00")) || n != len(wire)-11 {
		return registeredPreparedInspection{}, errInvalid
	}
	return q.inspect(wire[11:], l)
}

func registryRecordHeader(wire []byte, family byte, o registeredOriginScope, position uint64, size uint32) {
	copy(wire[:6], "HCR1\x00\x01")
	wire[6] = family
	registryScopeBytes(wire[8:82], o)
	binary.BigEndian.PutUint64(wire[82:90], position)
	binary.BigEndian.PutUint32(wire[90:94], size)
}
func registryRecordFields(b []byte, ref registeredRegistryRef) {
	b[0] = 1
	copy(b[1:17], ref.submitted.lookupKey[80:])
	b[17] = ref.submitted.kind
	binary.BigEndian.PutUint64(b[18:26], ref.term)
	copy(b[26:58], ref.submitted.submissionHash[:])
	copy(b[58:90], ref.submitted.preparedHash[:])
}
func registryReadFields(b []byte, o registeredOriginScope, position uint64) (registeredRegistryRef, error) {
	if b[0] != 1 || b[17] < 1 || b[17] > 2 || position == 0 {
		return registeredRegistryRef{}, errInvalid
	}
	var key [16]byte
	copy(key[:], b[1:17])
	fixed := registeredLedger{limit: 86}
	if err := registeredOriginCheck(o, key, &fixed); err != nil {
		return registeredRegistryRef{}, err
	}
	hki, hkv := registryKeys(o, key, position)
	ref := registeredRegistryRef{submitted: registeredSubmittedRef{lookupKey: hki, kind: b[17]}, storedKey: hkv, position: position, term: binary.BigEndian.Uint64(b[18:26])}
	copy(ref.submitted.submissionHash[:], b[26:58])
	copy(ref.submitted.preparedHash[:], b[58:90])
	if ref.term == 0 {
		return registeredRegistryRef{}, errInvalid
	}
	return ref, nil
}
func registryRecordPayload(wire []byte, family byte, o registeredOriginScope, insertion uint64) ([]byte, error) {
	if !bytes.Equal(wire[:6], []byte("HCR1\x00\x01")) || wire[6] != family || wire[7] != 0 || registryReadScope(wire[8:82]) != o || insertion == 0 || binary.BigEndian.Uint64(wire[82:90]) != insertion {
		return nil, errInvalid
	}
	n, err := registryReadSize(wire[90:94], registryPreparedMax+94)
	if err != nil {
		return nil, err
	}
	if n != len(wire)-126 || !registryChecksum(wire) {
		return nil, errInvalid
	}
	return wire[94 : len(wire)-32], nil
}
func encodeRegistryRecords(pij []byte, r, term uint64, l registeredFormatBudget, b registeredRegistryCodecBudget) ([96]byte, []byte, [89]byte, []byte, error) {
	zero := func(err error) ([96]byte, []byte, [89]byte, []byte, error) {
		return [96]byte{}, nil, [89]byte{}, nil, err
	}
	if err := registeredBudget(l); err != nil {
		return zero(err)
	}
	q, err := registryStart(b, len(pij), 136, l.preparedBytes)
	if err != nil {
		return zero(err)
	}
	if r == 0 || term == 0 {
		return zero(errInvalid)
	}
	storedBytes := len(pij) + 220
	// Two checksums exclude their32-byte tails; output emission and backing
	// are separate work debits. Returned fixed key arrays live within4096.
	if err := q.spend(3*(309+storedBytes)-64, 309+storedBytes); err != nil {
		return zero(err)
	}
	got, err := q.inspect(pij, l)
	if err != nil {
		return zero(err)
	}
	ref := registryRef(&got, r, term)
	preparedSize, err := registrySize(len(pij))
	if err != nil {
		return zero(err)
	}
	payloadSize, err := registrySize(94 + len(pij))
	if err != nil {
		return zero(err)
	}
	lookup, stored := make([]byte, 309), make([]byte, storedBytes)
	registryRecordHeader(lookup, 2, got.parsed.o, r, 183)
	registryRecordHeader(stored, 3, got.parsed.o, r, payloadSize)
	registryRecordFields(lookup[94:277], ref)
	registryRecordFields(stored[94:len(stored)-32], ref)
	copy(lookup[184:273], ref.storedKey[:])
	binary.BigEndian.PutUint32(lookup[273:277], preparedSize)
	binary.BigEndian.PutUint32(stored[184:188], preparedSize)
	copy(stored[188:len(stored)-32], pij)
	registrySeal(lookup)
	registrySeal(stored)
	return ref.submitted.lookupKey, lookup, ref.storedKey, stored, nil
}
func decodeRegistryLookup(key, value []byte, insertion uint64, origin registeredOriginScope, b registeredRegistryCodecBudget) (registeredRegistryRef, int, error) {
	q, err := registryStart(b, len(value), 309, 309)
	if err != nil {
		return registeredRegistryRef{}, 0, err
	}
	if len(key) != 96 {
		return registeredRegistryRef{}, 0, errInvalid
	}
	if err := q.spend(2*len(value)-32, 0); err != nil {
		return registeredRegistryRef{}, 0, err
	}
	payload, err := registryRecordPayload(value, 2, origin, insertion)
	if err != nil {
		return registeredRegistryRef{}, 0, err
	}
	ref, err := registryReadFields(payload, origin, insertion)
	if err != nil {
		return registeredRegistryRef{}, 0, err
	}
	n, err := registryReadSize(payload[179:183], registryPreparedMax)
	if err != nil {
		return registeredRegistryRef{}, 0, err
	}
	if n < 136 || !bytes.Equal(key, ref.submitted.lookupKey[:]) || !bytes.Equal(payload[90:179], ref.storedKey[:]) {
		return registeredRegistryRef{}, 0, errInvalid
	}
	// No PIJ here: full hashes/NEW agreement require reciprocal Stored evidence.
	return ref, n, nil
}
func decodeRegistryStored(key, value []byte, insertion uint64, origin registeredOriginScope, l registeredFormatBudget, b registeredRegistryCodecBudget) (registeredRegistryRef, []byte, error) {
	if err := registeredBudget(l); err != nil {
		return registeredRegistryRef{}, nil, err
	}
	q, err := registryStart(b, len(value), 356, l.preparedBytes+220)
	if err != nil {
		return registeredRegistryRef{}, nil, err
	}
	if len(key) != 89 {
		return registeredRegistryRef{}, nil, errInvalid
	}
	if err := q.spend(2*len(value)-32, 0); err != nil {
		return registeredRegistryRef{}, nil, err
	}
	payload, err := registryRecordPayload(value, 3, origin, insertion)
	if err != nil {
		return registeredRegistryRef{}, nil, err
	}
	ref, err := registryReadFields(payload, origin, insertion)
	if err != nil {
		return registeredRegistryRef{}, nil, err
	}
	n, err := registryReadSize(payload[90:94], l.preparedBytes)
	if err != nil {
		return registeredRegistryRef{}, nil, err
	}
	pij := payload[94:len(payload):len(payload)]
	if n != len(pij) || !bytes.Equal(key, ref.storedKey[:]) {
		return registeredRegistryRef{}, nil, errInvalid
	}
	got, err := q.inspect(pij, l)
	if err != nil {
		return registeredRegistryRef{}, nil, err
	}
	if ref != registryRef(&got, insertion, ref.term) {
		return registeredRegistryRef{}, nil, errInvalid
	}
	return ref, pij, nil
}

func registryOutcomeCheck(status byte, e uint64, submitted registeredSubmittedRef, original registeredRegistryRef) error {
	if err := registryCheckRef(original); err != nil {
		return err
	}
	if _, err := registrySubmittedScope(submitted); err != nil {
		return err
	}
	if e < original.position || submitted.lookupKey != original.submitted.lookupKey {
		return errInvalid
	}
	switch status {
	case 1:
		if e != original.position || submitted != original.submitted {
			return errInvalid
		}
	case 2:
		if submitted != original.submitted {
			return errInvalid
		}
	case 3:
		if submitted.kind == original.submitted.kind && submitted.submissionHash == original.submitted.submissionHash {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}
func registrySubmittedBytes(b []byte, ref registeredSubmittedRef) {
	copy(b[:96], ref.lookupKey[:])
	b[96] = ref.kind
	copy(b[97:129], ref.submissionHash[:])
	copy(b[129:161], ref.preparedHash[:])
}
func registryReadSubmitted(b []byte) (ref registeredSubmittedRef) {
	copy(ref.lookupKey[:], b[:96])
	ref.kind = b[96]
	copy(ref.submissionHash[:], b[97:129])
	copy(ref.preparedHash[:], b[129:161])
	return ref
}
func encodeRegistryOutcome(status byte, e uint64, submitted registeredSubmittedRef, original registeredRegistryRef, b registeredRegistryCodecBudget) ([]byte, error) {
	q, err := registryStart(b, 469, 469, 469)
	if err != nil {
		return nil, err
	}
	if err := q.spend(3*469-32, 469); err != nil {
		return nil, err
	}
	if err := registryOutcomeCheck(status, e, submitted, original); err != nil {
		return nil, err
	}
	out := make([]byte, 469)
	copy(out[:6], "JOR1\x00\x01")
	out[6] = status
	binary.BigEndian.PutUint64(out[7:15], e)
	binary.BigEndian.PutUint16(out[15:17], 161)
	registrySubmittedBytes(out[17:178], submitted)
	out[178] = 2
	if status == 3 {
		out[178] = 3
	}
	registrySubmittedBytes(out[179:340], original.submitted)
	copy(out[340:429], original.storedKey[:])
	binary.BigEndian.PutUint64(out[429:437], original.term)
	registrySeal(out)
	return out, nil
}
func decodeRegistryOutcome(wire []byte, b registeredRegistryCodecBudget) (registeredRegistryOutcome, error) {
	q, err := registryStart(b, len(wire), 469, 469)
	if err != nil {
		return registeredRegistryOutcome{}, err
	}
	if err := q.spend(2*len(wire)-32, 0); err != nil {
		return registeredRegistryOutcome{}, err
	}
	status, tag := wire[6], wire[178]
	if !bytes.Equal(wire[:6], []byte("JOR1\x00\x01")) || binary.BigEndian.Uint16(wire[15:17]) != 161 || status == 3 && tag != 3 || status != 3 && tag != 2 || !registryChecksum(wire) {
		return registeredRegistryOutcome{}, errInvalid
	}
	out := registeredRegistryOutcome{status: status, evaluationIndex: binary.BigEndian.Uint64(wire[7:15]), submitted: registryReadSubmitted(wire[17:178]), original: registeredRegistryRef{submitted: registryReadSubmitted(wire[179:340]), term: binary.BigEndian.Uint64(wire[429:437])}}
	copy(out.original.storedKey[:], wire[340:429])
	out.original.position = binary.BigEndian.Uint64(out.original.storedKey[79:87])
	if err := registryOutcomeCheck(out.status, out.evaluationIndex, out.submitted, out.original); err != nil {
		return registeredRegistryOutcome{}, err
	}
	return out, nil
}

func registryIRJAddress(address []byte, term uint64, got registeredPreparedInspection) error {
	if len(address) != 89 || term == 0 {
		return errInvalid
	}
	position := binary.BigEndian.Uint64(address[79:87])
	if position == 0 {
		return errInvalid
	}
	ref := registryRef(&got, position, term)
	if !bytes.Equal(address, ref.storedKey[:]) {
		return errInvalid
	}
	return nil
}
func encodeRegistryIRJ(representation byte, pij, address []byte, term uint64, l registeredFormatBudget, b registeredRegistryCodecBudget) ([]byte, error) {
	if err := registeredBudget(l); err != nil {
		return nil, err
	}
	q, err := registryStart(b, len(pij), 136, l.preparedBytes)
	if err != nil {
		return nil, err
	}
	fixed := 109
	switch representation {
	case 1:
		if len(address) != 0 || term != 0 {
			return nil, errInvalid
		}
	case 2:
		if len(address) != 89 || term == 0 {
			return nil, errInvalid
		}
		fixed = 206
	default:
		return nil, errInvalid
	}
	n := len(pij) + fixed
	if err := q.spend(3*n-32, n); err != nil {
		return nil, err
	}
	got, err := q.inspect(pij, l)
	if err != nil {
		return nil, err
	}
	if representation == 2 {
		if err := registryIRJAddress(address, term, got); err != nil {
			return nil, err
		}
	}
	size, err := registrySize(len(pij))
	if err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out[:6], "IRJ1\x00\x01")
	out[6] = representation
	copy(out[7:39], got.submissionHash[:])
	copy(out[39:71], got.preparedHash[:])
	offset := 73
	if representation == 2 {
		binary.BigEndian.PutUint16(out[71:73], 89)
		copy(out[73:162], address)
		binary.BigEndian.PutUint64(out[162:170], term)
		offset = 170
	}
	binary.BigEndian.PutUint32(out[offset:offset+4], size)
	copy(out[offset+4:len(out)-32], pij)
	registrySeal(out)
	return out, nil
}
func decodeRegistryIRJ(wire []byte, l registeredFormatBudget, b registeredRegistryCodecBudget) (registeredRegistryExport, error) {
	if err := registeredBudget(l); err != nil {
		return registeredRegistryExport{}, err
	}
	q, err := registryStart(b, len(wire), 245, l.preparedBytes+206)
	if err != nil {
		return registeredRegistryExport{}, err
	}
	if !bytes.Equal(wire[:6], []byte("IRJ1\x00\x01")) {
		return registeredRegistryExport{}, errInvalid
	}
	representation := wire[6]
	offset := 73
	var address []byte
	var term uint64
	switch representation {
	case 1:
		// The common range admits Stored's larger wrapper. Candidate has its
		// own immutable maximum once this fixed representation is admitted.
		if len(wire) > l.preparedBytes+109 {
			return registeredRegistryExport{}, errLimit
		}
		if binary.BigEndian.Uint16(wire[71:73]) != 0 {
			return registeredRegistryExport{}, errInvalid
		}
	case 2:
		// Stored's maximum was admitted at start; its minimum additionally
		// requires the89-byte address/term and at least136 PIJ bytes.
		if len(wire) < 342 {
			return registeredRegistryExport{}, errInvalid
		}
		if binary.BigEndian.Uint16(wire[71:73]) != 89 {
			return registeredRegistryExport{}, errInvalid
		}
		address = wire[73:162]
		term = binary.BigEndian.Uint64(wire[162:170])
		offset = 170
	default:
		return registeredRegistryExport{}, errInvalid
	}
	n, err := registryReadSize(wire[offset:offset+4], l.preparedBytes)
	if err != nil {
		return registeredRegistryExport{}, err
	}
	pij := wire[offset+4 : len(wire)-32]
	if n != len(pij) || n < 136 {
		return registeredRegistryExport{}, errInvalid
	}
	owned := n + len(address)
	// I partitions fixed reads109/117 plus prepaid copyK=n+A. The finalK
	// reserves backing(workK,ownedK); copying consumes existingI credit.
	if err := q.spend(2*len(wire)-32+owned, owned); err != nil {
		return registeredRegistryExport{}, err
	}
	if !registryChecksum(wire) {
		return registeredRegistryExport{}, errInvalid
	}
	got, err := q.inspect(pij, l)
	if err != nil {
		return registeredRegistryExport{}, err
	}
	if !bytes.Equal(wire[7:39], got.submissionHash[:]) || !bytes.Equal(wire[39:71], got.preparedHash[:]) {
		return registeredRegistryExport{}, errInvalid
	}
	if representation == 2 {
		if err := registryIRJAddress(address, term, got); err != nil {
			return registeredRegistryExport{}, err
		}
	}
	out := registeredRegistryExport{representation: representation, submissionHash: got.submissionHash, preparedHash: got.preparedHash, term: term, prepared: make([]byte, n)}
	copy(out.prepared, pij)
	if representation == 2 {
		out.address = make([]byte, 89)
		copy(out.address, address)
	}
	return out, nil
}
