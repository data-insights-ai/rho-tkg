package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"reflect"
	"slices"
	"testing"
	"unsafe"
)

type registryVectorLiteral struct{ Hex string }
type registryVector struct {
	PIJ, DGP, RJQ, HKI, HKV, HCRLookup, HCRStored           registryVectorLiteral
	SubmissionHash, PreparedHash, CreatedJOR, OriginalE5JOR registryVectorLiteral
	IRJCandidate, IRJStored, ConflictAgainstBaseE6JOR       registryVectorLiteral
	Accounting                                              struct {
		Canonical uint64 `json:"PIJ_canonical_hash_work"`
	} `json:"accounting"`
}

func registryVectors(t *testing.T) (map[string]registryVector, registryVector) {
	t.Helper()
	data, err := os.ReadFile("testdata/registered_registry_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		New map[string]registryVector `json:"new_full_rows"`
		Old map[string]registryVector `json:"old_full_rows"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil || len(fixture.New) != 14 {
		t.Fatal("complete independent fourteen fixtures", err)
	}
	return fixture.New, fixture.Old["base"]
}
func registryBytes(t *testing.T, literal registryVectorLiteral) []byte {
	t.Helper()
	b, err := hex.DecodeString(literal.Hex)
	if err != nil || len(b) == 0 {
		t.Fatal("independent full literal", err)
	}
	return b
}
func registryTestFormat() registeredFormatBudget {
	return registeredFormatBudget{preparedBytes: 4 << 20, domainBytes: 4 << 20, axes: 1024, descriptorBytes: 4096, schemas: 4096, schemaNameBytes: 256, walkBytes: 32 << 20}
}
func registryTestBudget() registeredRegistryCodecBudget {
	return registeredRegistryCodecBudget{workBytes: 46144850, ownedBytes: 4198929}
}
func registryExpectedRef(t *testing.T, row registryVector) registeredRegistryRef {
	t.Helper()
	ref := registeredRegistryRef{position: 4, term: 2}
	copy(ref.submitted.lookupKey[:], registryBytes(t, row.HKI))
	copy(ref.storedKey[:], registryBytes(t, row.HKV))
	ref.submitted.kind = registryBytes(t, row.PIJ)[98]
	copy(ref.submitted.submissionHash[:], registryBytes(t, row.SubmissionHash))
	copy(ref.submitted.preparedHash[:], registryBytes(t, row.PreparedHash))
	return ref
}
func registryTestOrigin(t *testing.T, row registryVector) registeredOriginScope {
	t.Helper()
	wire := registryBytes(t, row.PIJ)
	var o registeredOriginScope
	copy(o.graph[:], wire[7:23])
	o.partition = binary.BigEndian.Uint64(wire[23:31])
	copy(o.group[:], wire[31:47])
	copy(o.lineage[:], wire[47:79])
	o.protocol = binary.BigEndian.Uint16(wire[79:81])
	return o
}
func registryReseal(wire []byte) {
	sum := sha256.Sum256(wire[:len(wire)-32])
	copy(wire[len(wire)-32:], sum[:])
}
func registryZero(t *testing.T, err error, got, zero any) {
	t.Helper()
	if err != nil && !reflect.DeepEqual(got, zero) {
		t.Fatalf("error must zero entire tuple: %v %#v", err, got)
	}
}

func TestRegistryFourteenIndependentPipelines(t *testing.T) {
	rows, _ := registryVectors(t)
	l, b := registryTestFormat(), registryTestBudget()
	base := registryExpectedRef(t, rows["base"])
	for _, name := range slices.Sorted(maps.Keys(rows)) {
		t.Run(name, func(t *testing.T) {
			r := rows[name]
			pij := registryBytes(t, r.PIJ)
			expected := registryExpectedRef(t, r)
			origin := registryTestOrigin(t, r)
			wire, err := encodeRegistryCommand(pij, l, b)
			if err != nil || !bytes.Equal(wire, registryBytes(t, r.RJQ)) || len(wire) != cap(wire) {
				t.Fatal("complete RJQ", err)
			}
			ins, err := decodeRegistryCommand(wire, l, b)
			domain := registryBytes(t, r.DGP)
			offset := bytes.Index(wire, domain)
			if err != nil || ins.canonicalWork != r.Accounting.Canonical || ins.parsed.o != origin || ins.submissionHash != expected.submitted.submissionHash || ins.preparedHash != expected.submitted.preparedHash || offset < 0 || &ins.parsed.domain[0] != &wire[offset] || ins.parsed.s.domain.axes != nil || ins.parsed.s.schemas != nil {
				t.Fatal("borrowed inspector/full hashes", err)
			}
			hki, lookup, hkv, stored, err := encodeRegistryRecords(pij, 4, 2, l, b)
			if err != nil || hki != expected.submitted.lookupKey || hkv != expected.storedKey || !bytes.Equal(lookup, registryBytes(t, r.HCRLookup)) || !bytes.Equal(stored, registryBytes(t, r.HCRStored)) || len(lookup) != cap(lookup) || len(stored) != cap(stored) {
				t.Fatal("complete records", err)
			}
			lr, length, err := decodeRegistryLookup(hki[:], lookup, 4, origin, b)
			if err != nil || lr != expected || length != len(pij) {
				t.Fatal("lookup ref/length", err)
			}
			sr, borrowed, err := decodeRegistryStored(hkv[:], stored, 4, origin, l, b)
			if err != nil || sr != lr || !bytes.Equal(borrowed, pij) || &borrowed[0] != &stored[188] || len(borrowed) != cap(borrowed) {
				t.Fatal("reciprocal exact Stored and borrowed PIJ", err)
			}
			for _, outcome := range []struct {
				status  byte
				index   uint64
				literal registryVectorLiteral
			}{{1, 4, r.CreatedJOR}, {2, 5, r.OriginalE5JOR}} {
				out, err := encodeRegistryOutcome(outcome.status, outcome.index, expected.submitted, expected, b)
				if err != nil || !bytes.Equal(out, registryBytes(t, outcome.literal)) || len(out) != cap(out) {
					t.Fatal("outcome full bytes", err)
				}
				got, err := decodeRegistryOutcome(out, b)
				if err != nil || got != (registeredRegistryOutcome{outcome.status, outcome.index, expected.submitted, expected}) {
					t.Fatal("outcome tuple", err)
				}
			}
			if name != "base" {
				out, err := encodeRegistryOutcome(3, 6, expected.submitted, base, b)
				if err != nil || !bytes.Equal(out, registryBytes(t, r.ConflictAgainstBaseE6JOR)) || out[178] != 3 {
					t.Fatal("Conflict MUST use tag3 and original base", err)
				}
				got, err := decodeRegistryOutcome(out, b)
				if err != nil || got.original != base || got.submitted != expected.submitted || got.status != 3 {
					t.Fatal("conflict union", err)
				}
			}
			for _, repr := range []byte{1, 2} {
				var address []byte
				var term uint64
				literal := r.IRJCandidate
				if repr == 2 {
					address = hkv[:]
					term = 2
					literal = r.IRJStored
				}
				out, err := encodeRegistryIRJ(repr, pij, address, term, l, b)
				if err != nil || !bytes.Equal(out, registryBytes(t, literal)) || len(out) != cap(out) {
					t.Fatal("IRJ complete bytes", err)
				}
				got, err := decodeRegistryIRJ(out, l, b)
				if err != nil || got.representation != repr || got.term != term || !bytes.Equal(got.prepared, pij) || len(got.prepared) != cap(got.prepared) || got.submissionHash != expected.submitted.submissionHash || got.preparedHash != expected.submitted.preparedHash {
					t.Fatal("IRJ owned full result", err)
				}
				if repr == 1 && got.address != nil || repr == 2 && (!bytes.Equal(got.address, address) || len(got.address) != cap(got.address)) {
					t.Fatal("IRJ address presence/ownership")
				}
				clear(out)
				if !bytes.Equal(got.prepared, pij) || !bytes.Equal(got.address, address) {
					t.Fatal("IRJ decode retained input alias")
				}
			}
			clear(pij)
			if !bytes.Equal(stored, registryBytes(t, r.HCRStored)) || !bytes.Equal(wire, registryBytes(t, r.RJQ)) {
				t.Fatal("encoder output aliases input")
			}
		})
	}
}

func TestRegistryExactWorkOwnedAndZeroTuples(t *testing.T) {
	rows, _ := registryVectors(t)
	r := rows["base"]
	l := registryTestFormat()
	origin := registryTestOrigin(t, r)
	ref := registryExpectedRef(t, r)
	pij := registryBytes(t, r.PIJ)
	n := len(pij)
	if r.Accounting.Canonical > 32<<20 {
		t.Fatal("canonical fixture tally bound")
	}
	c := int(r.Accounting.Canonical)
	v := n + 220
	cmd := registryBytes(t, r.RJQ)
	lookup := registryBytes(t, r.HCRLookup)
	stored := registryBytes(t, r.HCRStored)
	type door struct {
		name        string
		work, owned int
		invoke      func(registeredRegistryCodecBudget) error
	}
	doors := []door{
		{"command_encode", 4128 + 2*(n+11) + c, 4096 + n + 11, func(b registeredRegistryCodecBudget) error {
			out, e := encodeRegistryCommand(pij, l, b)
			registryZero(t, e, out, []byte(nil))
			return e
		}},
		{"command_decode", 4128 + n + 11 + c, 4096, func(b registeredRegistryCodecBudget) error {
			out, e := decodeRegistryCommand(cmd, l, b)
			registryZero(t, e, out, registeredPreparedInspection{})
			return e
		}},
		{"records", 4128 + 3*(309+v) - 64 + c, 4096 + 309 + v, func(b registeredRegistryCodecBudget) error {
			k, x, s, y, e := encodeRegistryRecords(pij, 4, 2, l, b)
			if e != nil && (k != ([96]byte{}) || s != ([89]byte{}) || x != nil || y != nil) {
				t.Fatal("record entire tuple zero", e)
			}
			return e
		}},
		{"lookup", 4714, 4096, func(b registeredRegistryCodecBudget) error {
			out, size, e := decodeRegistryLookup(ref.submitted.lookupKey[:], lookup, 4, origin, b)
			registryZero(t, e, out, registeredRegistryRef{})
			if e != nil && size != 0 {
				t.Fatal("lookup length zero")
			}
			return e
		}},
		{"stored", 4128 + 2*v - 32 + c, 4096, func(b registeredRegistryCodecBudget) error {
			out, span, e := decodeRegistryStored(ref.storedKey[:], stored, 4, origin, l, b)
			registryZero(t, e, out, registeredRegistryRef{})
			registryZero(t, e, span, []byte(nil))
			return e
		}},
		{"outcome_encode", 5503, 4565, func(b registeredRegistryCodecBudget) error {
			out, e := encodeRegistryOutcome(1, 4, ref.submitted, ref, b)
			registryZero(t, e, out, []byte(nil))
			return e
		}},
		{"outcome_decode", 5034, 4096, func(b registeredRegistryCodecBudget) error {
			out, e := decodeRegistryOutcome(registryBytes(t, r.CreatedJOR), b)
			registryZero(t, e, out, registeredRegistryOutcome{})
			return e
		}},
	}
	for _, repr := range []byte{1, 2} {
		var address []byte
		var term uint64
		lit := r.IRJCandidate
		fixed := 109
		if repr == 2 {
			address = ref.storedKey[:]
			term = 2
			lit = r.IRJStored
			fixed = 206
		}
		i := n + fixed
		copied := n + len(address)
		doors = append(doors, door{"IRJ_encode_" + string(rune('0'+repr)), 4128 + 3*i - 32 + c, 4096 + i, func(b registeredRegistryCodecBudget) error {
			out, e := encodeRegistryIRJ(repr, pij, address, term, l, b)
			registryZero(t, e, out, []byte(nil))
			return e
		}})
		doors = append(doors, door{"IRJ_decode_" + string(rune('0'+repr)), 4128 + 2*i - 32 + c + copied, 4096 + copied, func(b registeredRegistryCodecBudget) error {
			out, e := decodeRegistryIRJ(registryBytes(t, lit), l, b)
			registryZero(t, e, out, registeredRegistryExport{})
			return e
		}})
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			for _, delta := range []int{0, 1} {
				if e := d.invoke(registeredRegistryCodecBudget{d.work + delta, d.owned + delta}); e != nil {
					t.Fatal("exact/+1", e)
				}
			}
			for _, b := range []registeredRegistryCodecBudget{{d.work - 1, d.owned}, {d.work, d.owned - 1}} {
				if e := d.invoke(b); !errors.Is(e, errLimit) {
					t.Fatal("one-short work/owned cause", e)
				}
			}
			for _, b := range []registeredRegistryCodecBudget{{0, d.owned}, {-1, d.owned}, {registryWorkMax + 1, d.owned}, {d.work, 0}, {d.work, -1}, {d.work, registryOwnedMax + 1}} {
				if e := d.invoke(b); !errors.Is(e, errInvalid) {
					t.Fatal("invalid budget priority", e)
				}
			}
		})
	}
	q := registryLedger{work: 2047, owned: 4096}
	out, e := q.inspect(pij, l)
	if !errors.Is(e, errLimit) || !reflect.DeepEqual(out, registeredPreparedInspection{}) {
		t.Fatal("child metadata shortage is Limit, never zero-walk Invalid", e)
	}
	q = registryLedger{work: 10, owned: 10}
	if !errors.Is(q.spend(-1, 0), errLimit) || !errors.Is(q.spend(0, -1), errLimit) || q.work != 10 || q.owned != 10 {
		t.Fatal("atomic negative debit refusal")
	}
}

func TestRegistryFramingHashesAndSizePriority(t *testing.T) {
	rows, old := registryVectors(t)
	r := rows["base"]
	l, b := registryTestFormat(), registryTestBudget()
	for _, input := range [][]byte{nil, make([]byte, 146)} {
		got, e := decodeRegistryCommand(input, l, b)
		if !errors.Is(e, errInvalid) || !reflect.DeepEqual(got, registeredPreparedInspection{}) {
			t.Fatal("short command", e)
		}
	}
	narrow := l
	narrow.preparedBytes = 258
	for _, lit := range []registryVectorLiteral{r.IRJCandidate, r.IRJStored} {
		wire := append(registryBytes(t, lit), 0)
		got, e := decodeRegistryIRJ(wire, narrow, b)
		if !errors.Is(e, errLimit) || !reflect.DeepEqual(got, registeredRegistryExport{}) {
			t.Fatal("representation max+1 must be Limit before declared-length mismatch", e)
		}
	}
	for _, wire := range [][]byte{make([]byte, 244), append([]byte("IRJ1\x00\x01\x02"), make([]byte, 334)...)} {
		got, e := decodeRegistryIRJ(wire, l, b)
		if !errors.Is(e, errInvalid) || !reflect.DeepEqual(got, registeredRegistryExport{}) {
			t.Fatal("Candidate/Stored minimum", e)
		}
	}
	for _, offset := range []int{0, 4, 6, 7, 10} {
		wire := registryBytes(t, r.RJQ)
		wire[offset] ^= 1
		got, e := decodeRegistryCommand(wire, l, b)
		if !errors.Is(e, errInvalid) && !errors.Is(e, errLimit) || !reflect.DeepEqual(got, registeredPreparedInspection{}) {
			t.Fatal("command header/length", offset, e)
		}
	}
	if out, e := encodeRegistryCommand(registryBytes(t, old.PIJ), l, b); !errors.Is(e, errInvalid) || out != nil {
		t.Fatal("OLD PIJ must not enter NEW wrapper", e)
	}
	badFormat := l
	badFormat.walkBytes = 0
	if out, e := decodeRegistryCommand(nil, badFormat, b); !errors.Is(e, errInvalid) || !reflect.DeepEqual(out, registeredPreparedInspection{}) {
		t.Fatal("format priority", e)
	}
	for _, offset := range []int{0, 4, 6, 7, 39, 71, 72, 162, 170, 206} {
		wire := registryBytes(t, r.IRJStored)
		if offset == 162 {
			clear(wire[162:170])
		} else {
			wire[offset] ^= 1
		}
		registryReseal(wire)
		got, e := decodeRegistryIRJ(wire, l, b)
		if !errors.Is(e, errInvalid) && !errors.Is(e, errLimit) || !reflect.DeepEqual(got, registeredRegistryExport{}) {
			t.Fatal("IRJ malformed framing/reportedhash/address/term/PIJ", offset, e)
		}
	}
	wire := registryBytes(t, r.IRJCandidate)
	wire[len(wire)-1] ^= 1
	if out, e := decodeRegistryIRJ(wire, l, b); !errors.Is(e, errInvalid) || !reflect.DeepEqual(out, registeredRegistryExport{}) {
		t.Fatal("IRJ checksum", e)
	}
	for _, args := range []struct {
		repr    byte
		address []byte
		term    uint64
	}{{0, nil, 0}, {1, make([]byte, 89), 0}, {1, nil, 2}, {2, nil, 2}, {2, make([]byte, 89), 0}, {2, make([]byte, 89), 2}} {
		out, e := encodeRegistryIRJ(args.repr, registryBytes(t, r.PIJ), args.address, args.term, l, b)
		if !errors.Is(e, errInvalid) || out != nil {
			t.Fatal("IRJ union/address", e)
		}
	}
	for _, bad := range []registeredFormatBudget{{}, {preparedBytes: registryPreparedMax + 1, domainBytes: 1, axes: 1, descriptorBytes: 1, schemas: 1, schemaNameBytes: 1, walkBytes: 1}} {
		if out, e := encodeRegistryIRJ(1, nil, nil, 0, bad, b); !errors.Is(e, errInvalid) || out != nil {
			t.Fatal("IRJ immutable format", e)
		}
	}
}

func TestRegistryReciprocalRecordRefusals(t *testing.T) {
	rows, _ := registryVectors(t)
	r := rows["base"]
	l, b := registryTestFormat(), registryTestBudget()
	origin := registryTestOrigin(t, r)
	ref := registryExpectedRef(t, r)
	// Different positive original coordinates are data, not inferred PIJ time.
	alternate := ref
	alternate.position, alternate.term = 9, 7
	binary.BigEndian.PutUint64(alternate.storedKey[79:87], 9)
	k, lookup, v, stored, e := encodeRegistryRecords(registryBytes(t, r.PIJ), 9, 7, l, b)
	if e != nil || k != alternate.submitted.lookupKey || v != alternate.storedKey || len(lookup) != 309 || len(stored) != 478 {
		t.Fatal("alternate original full keys and record lengths", e)
	}
	gotLookup, length, e := decodeRegistryLookup(k[:], lookup, 9, origin, b)
	if e != nil || gotLookup != alternate || length != 258 {
		t.Fatal("alternate Lookup full ref/SH/PH/position/term", e)
	}
	gotStored, span, e := decodeRegistryStored(v[:], stored, 9, origin, l, b)
	if e != nil || gotStored != gotLookup || !bytes.Equal(span, registryBytes(t, r.PIJ)) || &span[0] != &stored[188] {
		t.Fatal("alternate Stored canonical reciprocal ref and borrow", e)
	}
	if wrong, n, e := decodeRegistryLookup(k[:], lookup, 4, origin, b); !errors.Is(e, errInvalid) || wrong != (registeredRegistryRef{}) || n != 0 {
		t.Fatal("alternate original cannot be relabeled insertion4", e)
	}
	for _, args := range []struct{ r, term uint64 }{{0, 2}, {4, 0}} {
		k, x, s, y, e := encodeRegistryRecords(registryBytes(t, r.PIJ), args.r, args.term, l, b)
		if !errors.Is(e, errInvalid) || k != ([96]byte{}) || s != ([89]byte{}) || x != nil || y != nil {
			t.Fatal("record coordinate zero tuple", e)
		}
	}
	for _, offset := range []int{0, 4, 6, 7, 8, 80, 82, 90, 94, 95, 111, 112, 183, 184, 273} {
		wire := registryBytes(t, r.HCRLookup)
		wire[offset] ^= 1
		registryReseal(wire)
		got, n, e := decodeRegistryLookup(ref.submitted.lookupKey[:], wire, 4, origin, b)
		if offset == 112 || offset == 183 { // Positive term and reportedPH are inert metadata, detected by pairing.
			if e != nil || got == ref || n != 258 {
				t.Fatal("opaque field difference must remain visible to pair", e)
			}
			continue
		}
		if !errors.Is(e, errInvalid) && !errors.Is(e, errLimit) || got != (registeredRegistryRef{}) || n != 0 {
			t.Fatal("lookup malformed fixed binding", offset, e)
		}
	}
	for _, offset := range []int{0, 4, 6, 7, 8, 80, 82, 90, 94, 95, 111, 112, 120, 152, 184, 188, 287} {
		wire := registryBytes(t, r.HCRStored)
		wire[offset] ^= 1
		registryReseal(wire)
		got, span, e := decodeRegistryStored(ref.storedKey[:], wire, 4, origin, l, b)
		if offset == 112 { // Original term is not inside PIJ; pair, not a codec, supplies truth.
			if e != nil || got.term == ref.term || got == ref || span == nil {
				t.Fatal("stored term is explicit inert pair evidence", e)
			}
			continue
		}
		if !errors.Is(e, errInvalid) && !errors.Is(e, errLimit) || got != (registeredRegistryRef{}) || span != nil {
			t.Fatal("Stored malformed fields/hash/PIJ", offset, e)
		}
	}
	for _, stored := range []bool{false, true} {
		key := slices.Clone(ref.submitted.lookupKey[:])
		value := registryBytes(t, r.HCRLookup)
		if stored {
			key = slices.Clone(ref.storedKey[:])
			value = registryBytes(t, r.HCRStored)
		}
		for _, badkey := range [][]byte{nil, key[:len(key)-1], append(slices.Clone(key), 0)} {
			if stored {
				got, span, e := decodeRegistryStored(badkey, value, 4, origin, l, b)
				if !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || span != nil {
					t.Fatal("Stored key length", e)
				}
			} else {
				got, n, e := decodeRegistryLookup(badkey, value, 4, origin, b)
				if !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || n != 0 {
					t.Fatal("Lookup key length", e)
				}
			}
		}
		key[0] ^= 1
		if stored {
			got, span, e := decodeRegistryStored(key, value, 4, origin, l, b)
			if !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || span != nil {
				t.Fatal("wrong stored key", e)
			}
		} else {
			got, n, e := decodeRegistryLookup(key, value, 4, origin, b)
			if !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || n != 0 {
				t.Fatal("wrong lookup key", e)
			}
		}
	}
	badOrigin := origin
	badOrigin.group[1] ^= 1
	if got, n, e := decodeRegistryLookup(ref.submitted.lookupKey[:], registryBytes(t, r.HCRLookup), 4, badOrigin, b); !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || n != 0 {
		t.Fatal("foreign expected scope", e)
	}
	if got, span, e := decodeRegistryStored(ref.storedKey[:], registryBytes(t, r.HCRStored), 5, origin, l, b); !errors.Is(e, errInvalid) || got != (registeredRegistryRef{}) || span != nil {
		t.Fatal("physical insertion mismatch", e)
	}
	// No PIJ in Lookup: preserve opaque zero hashes, while the Stored comparison
	// exposes the mismatch and MUST be checked before any actual current-view use.
	lookup = registryBytes(t, r.HCRLookup)
	clear(lookup[120:184])
	registryReseal(lookup)
	got, n, e := decodeRegistryLookup(ref.submitted.lookupKey[:], lookup, 4, origin, b)
	if e != nil || got.submitted.submissionHash != ([32]byte{}) || got.submitted.preparedHash != ([32]byte{}) || got == ref || n != 258 {
		t.Fatal("opaque hashes must not become an invented nonzero gate", e)
	}
	lookup = registryBytes(t, r.HCRLookup)
	binary.BigEndian.PutUint32(lookup[273:277], 259)
	registryReseal(lookup)
	got, n, e = decodeRegistryLookup(ref.submitted.lookupKey[:], lookup, 4, origin, b)
	if e != nil || got != ref || n == 258 {
		t.Fatal("paired length mismatch must remain explicit", e)
	}
}

func TestRegistryOutcomeUnionsAndOpaqueReferences(t *testing.T) {
	rows, _ := registryVectors(t)
	r := rows["base"]
	b := registryTestBudget()
	ref := registryExpectedRef(t, r)
	for _, offset := range []int{0, 4, 6, 15, 17, 22, 113, 178, 179, 275, 340, 427, 429} {
		wire := registryBytes(t, r.CreatedJOR)
		if offset == 429 {
			clear(wire[429:437])
		} else {
			wire[offset] ^= 1
		}
		registryReseal(wire)
		out, e := decodeRegistryOutcome(wire, b)
		if !errors.Is(e, errInvalid) || out != (registeredRegistryOutcome{}) {
			t.Fatal("wrong outcome union/ref", offset, e)
		}
	}
	conflict := registryBytes(t, rows["allocator-owner5"].ConflictAgainstBaseE6JOR)
	conflict[178] = 2
	registryReseal(conflict)
	if out, e := decodeRegistryOutcome(conflict, b); !errors.Is(e, errInvalid) || out != (registeredRegistryOutcome{}) {
		t.Fatal("Conflict tag2 regression", e)
	}
	for _, x := range []struct {
		status    byte
		e         uint64
		submitted registeredSubmittedRef
		original  registeredRegistryRef
	}{{0, 4, ref.submitted, ref}, {1, 5, ref.submitted, ref}, {2, 3, ref.submitted, ref}, {3, 6, ref.submitted, ref}} {
		if out, e := encodeRegistryOutcome(x.status, x.e, x.submitted, x.original, b); !errors.Is(e, errInvalid) || out != nil {
			t.Fatal("semantic union refusal", e)
		}
	}
	bad := ref
	bad.storedKey[88] = 1
	if out, e := encodeRegistryOutcome(1, 4, bad.submitted, bad, b); !errors.Is(e, errInvalid) || out != nil {
		t.Fatal("mandatory slot0", e)
	}
	bad = ref
	bad.term = 0
	if out, e := encodeRegistryOutcome(1, 4, bad.submitted, bad, b); !errors.Is(e, errInvalid) || out != nil {
		t.Fatal("term zero", e)
	}
	bad = ref
	bad.submitted.kind = 0
	if out, e := encodeRegistryOutcome(1, 4, bad.submitted, bad, b); !errors.Is(e, errInvalid) || out != nil {
		t.Fatal("unknown kind", e)
	}
	opaque := ref
	opaque.submitted.submissionHash = [32]byte{}
	opaque.submitted.preparedHash = [32]byte{}
	wire, e := encodeRegistryOutcome(1, 4, opaque.submitted, opaque, b)
	if e != nil {
		t.Fatal("standalone zero opaque hashes", e)
	}
	got, e := decodeRegistryOutcome(wire, b)
	if e != nil || got.original != opaque {
		t.Fatal("opaque fields cannot prove compiled meaning", e)
	}
}

func TestRegistrySizeAndPrecedenceHelpers(t *testing.T) {
	for _, n := range []int{0, 136, registryPreparedMax, registryPreparedMax + 220} {
		got, e := registrySize(n)
		if e != nil || uint64(got) != uint64(n) {
			t.Fatal("bounded size", n, e)
		}
	}
	for _, x := range []struct {
		n   int
		err error
	}{{-1, errInvalid}, {registryPreparedMax + 221, errLimit}} {
		got, e := registrySize(x.n)
		if !errors.Is(e, x.err) || got != 0 {
			t.Fatal("size refusal", e)
		}
	}
	for _, x := range []struct {
		n   uint32
		max int
		err error
	}{{0, registryPreparedMax, nil}, {136, registryPreparedMax, nil}, {registryPreparedMax + 220, registryPreparedMax + 220, nil}, {registryPreparedMax + 221, registryPreparedMax + 220, errLimit}, {137, 136, errLimit}} {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], x.n)
		got, e := registryReadSize(b[:], x.max)
		if x.err == nil {
			if e != nil || got != int(x.n) {
				t.Fatal("read size", e)
			}
		} else if !errors.Is(e, x.err) || got != 0 {
			t.Fatal("read size limit", e)
		}
	}
	if got, e := registryReadSize(nil, 1); !errors.Is(e, errInvalid) || got != 0 {
		t.Fatal("short size", e)
	}
	if _, e := registryStart(registeredRegistryCodecBudget{}, 0, 1, 1); !errors.Is(e, errInvalid) {
		t.Fatal("invalid budget first", e)
	}
	if _, e := registryStart(registryTestBudget(), 0, 1, 1); !errors.Is(e, errInvalid) {
		t.Fatal("short before workspace", e)
	}
	if _, e := registryStart(registeredRegistryCodecBudget{1, 1}, 2, 1, 1); !errors.Is(e, errLimit) {
		t.Fatal("size over before workspace", e)
	}
}

func TestRegistryFixedSimultaneousLayoutAndLedger(t *testing.T) {
	// Core's localParsed and coreInspection return are counted once here.
	// The bridge namedResult and API callerSlot are DISTINCT additional values.
	core := unsafe.Sizeof(registeredParsed{}) + unsafe.Sizeof(registeredPreparedInspection{}) + unsafe.Sizeof(registeredLedger{}) + unsafe.Sizeof(registeredFormatBudget{}) + 2*unsafe.Sizeof(registeredCursor{}) + unsafe.Sizeof(registeredAxisView{}) + 2*256 + 2*32 + 96 + 128
	if core > 2048 {
		t.Fatal("canonical simultaneous state", core)
	}
	type bridge struct {
		Receiver    *registryLedger
		Input       []byte
		Format      registeredFormatBudget
		NamedResult registeredPreparedInspection
		Err         error
		Child, Work int
	}
	type recordCaller struct {
		Input           []byte
		Format          registeredFormatBudget
		Budget          registeredRegistryCodecBudget
		Position, Term  uint64
		ReturnedHKI     [96]byte
		ReturnedHKV     [89]byte
		ReturnedBuffers [2][]byte
		Err             error
		Q               *registryLedger
		N               int
		CallerSlot      registeredPreparedInspection
		LocalRef        registeredRegistryRef
		Sizes           [2]uint32
		EmittedBuffers  [2][]byte
		ZeroClosure     func(error)
		LiveLedger      registryLedger
		LocalErr        error
	}
	type storedCaller struct {
		Key, Value   []byte
		Insertion    uint64
		Origin       registeredOriginScope
		Format       registeredFormatBudget
		Budget       registeredRegistryCodecBudget
		ReturnedRef  registeredRegistryRef
		ReturnedSpan []byte
		Err          error
		Q            *registryLedger
		Payload      []byte
		LocalRef     registeredRegistryRef
		N            int
		Pij          []byte
		CallerSlot   registeredPreparedInspection
		LiveLedger   registryLedger
		LocalErr     error
	}
	type refBuilder struct {
		Argument       *registeredPreparedInspection
		Position, Term uint64
		ReturnedRef    registeredRegistryRef
		LocalHKI       [96]byte
		LocalHKV       [89]byte
	}
	type keysBuilder struct {
		Origin      registeredOriginScope
		Key         [16]byte
		Position    uint64
		ReturnedHKI [96]byte
		ReturnedHKV [89]byte
	}
	type scopeWriter struct {
		Span     []byte
		Argument registeredOriginScope
	}
	type fieldsFrame struct {
		Input                []byte
		Origin               registeredOriginScope
		Position             uint64
		ReturnSlot, LocalRef registeredRegistryRef
		Key                  [16]byte
		Ledger               registeredLedger
		Hki                  [96]byte
		Hkv                  [89]byte
		Err                  error
	}
	type lineageFrame struct {
		Origin    registeredOriginScope
		Key       [16]byte
		Ledger    *registeredLedger
		Err       error
		Jgs       [46]byte
		Length    [4]byte
		Result    [32]byte
		Parts     [4][]byte
		HashState [256]byte
		ExtraErr  error
	}
	type digestFrame struct {
		Parts         [][]byte
		HashInterface [2]uintptr
		HashState     [256]byte
		Result        [32]byte
		Scratch       [64]byte
	}
	type outcomeCaller struct {
		Status     byte
		Index      uint64
		Submitted  registeredSubmittedRef
		Original   registeredRegistryRef
		Budget     registeredRegistryCodecBudget
		Returned   []byte
		Err        error
		Q          *registryLedger
		Emitted    []byte
		LiveLedger registryLedger
		LocalErr   error
	}
	type outcomeFrame struct {
		Status         byte
		Index          uint64
		Submitted      registeredSubmittedRef
		Original       registeredRegistryRef
		Err            error
		DiscardedScope registeredOriginScope
	}
	type refFrame struct {
		Argument     registeredRegistryRef
		Origin       registeredOriginScope
		Key          [16]byte
		Expected     [89]byte
		DiscardedHKI [96]byte
		Err          error
	}
	type submittedFrame struct {
		Argument              registeredSubmittedRef
		Returned, LocalOrigin registeredOriginScope
		Key                   [16]byte
		Ledger                registeredLedger
		Err                   error
		Span                  []byte
	}
	type irjCaller struct {
		InputSpans  [3][]byte                   // wire, PIJ, address; includes encoder/decoder superset
		ResultSlots [2]registeredRegistryExport // local result AND caller result
		Inspection  registeredPreparedInspection
		Format      registeredFormatBudget
		Budget      registeredRegistryCodecBudget
		Q           *registryLedger
		Coordinates [4]uint64
		Offsets     [4]int
		Err         error
		Emitted     []byte
		LiveLedger  registryLedger
		LocalErr    error
	}
	type irjAddressFrame struct {
		Address        []byte
		Term, Position uint64
		Argument       registeredPreparedInspection
		LocalRef       registeredRegistryRef
		Err            error
	}
	phases := map[string]uintptr{
		"command encode child (recordCaller superset)":                     unsafe.Sizeof(recordCaller{}) + unsafe.Sizeof(bridge{}) + core,
		"command decode child (storedCaller superset)":                     unsafe.Sizeof(storedCaller{}) + unsafe.Sizeof(bridge{}) + core,
		"lookup fields lineage (storedCaller superset)":                    unsafe.Sizeof(storedCaller{}) + unsafe.Sizeof(fieldsFrame{}) + unsafe.Sizeof(lineageFrame{}) + unsafe.Sizeof(digestFrame{}),
		"IRJ encode/decode child":                                          unsafe.Sizeof(irjCaller{}) + unsafe.Sizeof(bridge{}) + core,
		"IRJ address ref/keys AFTER child":                                 unsafe.Sizeof(irjCaller{}) + unsafe.Sizeof(irjAddressFrame{}) + unsafe.Sizeof(refBuilder{}) + unsafe.Sizeof(keysBuilder{}) + unsafe.Sizeof(scopeWriter{}),
		"outcome decode nested refs (encodeCaller+decodedResult superset)": unsafe.Sizeof(outcomeCaller{}) + 2*unsafe.Sizeof(registeredRegistryOutcome{}) + unsafe.Sizeof(outcomeFrame{}) + unsafe.Sizeof(refFrame{}) + unsafe.Sizeof(submittedFrame{}) + unsafe.Sizeof(lineageFrame{}) + unsafe.Sizeof(digestFrame{}),
		"records child":                                  unsafe.Sizeof(recordCaller{}) + unsafe.Sizeof(bridge{}) + core,
		"stored child":                                   unsafe.Sizeof(storedCaller{}) + unsafe.Sizeof(bridge{}) + core,
		"records ref/keys AFTER child returns":           unsafe.Sizeof(recordCaller{}) + unsafe.Sizeof(refBuilder{}) + unsafe.Sizeof(keysBuilder{}) + unsafe.Sizeof(scopeWriter{}),
		"stored fields lineage BEFORE child":             unsafe.Sizeof(storedCaller{}) + unsafe.Sizeof(fieldsFrame{}) + unsafe.Sizeof(lineageFrame{}) + unsafe.Sizeof(digestFrame{}),
		"outcome nested CheckRef/SubmittedScope/lineage": unsafe.Sizeof(outcomeCaller{}) + unsafe.Sizeof(outcomeFrame{}) + unsafe.Sizeof(refFrame{}) + unsafe.Sizeof(submittedFrame{}) + unsafe.Sizeof(lineageFrame{}) + unsafe.Sizeof(digestFrame{}),
	}
	// Every API uses an explicit caller/nested model or a documented larger
	// field superset; variable output backings are separately charged, not sizeof.
	for name, n := range phases {
		if n > registryFixed {
			t.Fatal("all nested/by-value/key/hash/result state", name, n)
		}
		t.Log(name, n, registryFixed)
	}
	q, e := registryStart(registryTestBudget(), 258, 136, registryPreparedMax)
	if e != nil || q.work != registryWorkMax-4128 || q.owned != registryOwnedMax-4096 {
		t.Fatal("one fixed reservation", e)
	}
	if e := q.spend(538, 269); e != nil {
		t.Fatal(e)
	}
	rows, _ := registryVectors(t)
	got, e := q.inspect(registryBytes(t, rows["base"].PIJ), registryTestFormat())
	if e != nil || got.canonicalWork != 3092 || q.work != registryWorkMax-4128-538-3092 || q.owned != registryOwnedMax-4096-269 {
		t.Fatal("child settled once; remaining variable debit", e)
	}
}

func TestRegistryLegalNearMaximumAndOversize(t *testing.T) {
	// Independent complete BE grammar; no candidate encoder creates the input.
	rows, _ := registryVectors(t)
	base := registryBytes(t, rows["base"].PIJ)
	domain := []byte("DGP1\x00\x01\x00\x04\x00")
	reference := bytes.Repeat([]byte{'a'}, 4066)
	for ordinal := uint16(1); ordinal <= 1024; ordinal++ {
		var id [16]byte
		binary.BigEndian.PutUint16(id[14:], ordinal)
		domain = append(domain, id[:]...)
		domain = append(domain, 1, 0, 1)
		domain = binary.BigEndian.AppendUint32(domain, 4066)
		domain = append(domain, reference...)
		domain = binary.BigEndian.AppendUint32(domain, 1)
		domain = append(domain, 'u')
	}
	var last [16]byte
	binary.BigEndian.PutUint16(last[14:], 1024)
	domain = append(domain, last[:]...)
	domain = append(domain, 0)
	if len(domain) != 4192282 {
		t.Fatal("independent domain length")
	}
	body := []byte("BDS1\x00\x01\x00")
	body = binary.BigEndian.AppendUint32(body, 4192282)
	body = append(body, domain...)
	body = append(body, 0, 0, 1)
	body = append(body, 4)
	body = append(body, make([]byte, 15)...)
	body = binary.BigEndian.AppendUint64(body, 1)
	body = binary.BigEndian.AppendUint64(body, 64)
	if len(body) != 4192328 {
		t.Fatal("independent complete body length")
	}
	pij := slices.Clone(base[:131])
	pij = binary.BigEndian.AppendUint32(pij, 4192328)
	pij = append(pij, body...)
	pij = append(pij, 0)
	if len(pij) != 4192464 {
		t.Fatal("legal near-max wire, not padding")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 4192464)
	ph := sha256.New()
	_, _ = ph.Write([]byte("rho-tkg:registered-prepared:v1\x00"))
	_, _ = ph.Write(length[:])
	_, _ = ph.Write(pij)
	sh := sha256.New()
	_, _ = sh.Write([]byte("rho-tkg:registered-submission:v1\x00"))
	_, _ = sh.Write(length[:])
	_, _ = sh.Write([]byte("SIJ1"))
	_, _ = sh.Write(pij[4:])
	work := 4128 + 2*(4192464+11) + 29379324
	owned := 4096 + 4192464 + 11
	wire, e := encodeRegistryCommand(pij, registryTestFormat(), registeredRegistryCodecBudget{work, owned})
	if e != nil || len(wire) != 4192475 || len(wire) != cap(wire) || !bytes.Equal(wire[:7], []byte("RJQ1\x00\x01\x00")) || !bytes.Equal(wire[11:], pij) {
		t.Fatal("large legal exact-work/owned encoding", e)
	}
	got, e := decodeRegistryCommand(wire, registryTestFormat(), registeredRegistryCodecBudget{4128 + 4192475 + 29379324, 4096})
	if e != nil || got.canonicalWork != 29379324 || got.parsed.axes != 1024 || !bytes.Equal(got.preparedHash[:], ph.Sum(nil)) || !bytes.Equal(got.submissionHash[:], sh.Sum(nil)) || &got.parsed.domain[0] != &wire[157] {
		t.Fatal("large borrowed inspection and independent hashes", e)
	}
	for _, b := range []registeredRegistryCodecBudget{{work - 1, owned}, {work, owned - 1}} {
		out, e := encodeRegistryCommand(pij, registryTestFormat(), b)
		if !errors.Is(e, errLimit) || out != nil {
			t.Fatal("large actual one-short", e)
		}
	}
	oversized := append(slices.Clone(pij), make([]byte, 4194305-len(pij))...)
	if out, e := encodeRegistryCommand(oversized, registryTestFormat(), registryTestBudget()); !errors.Is(e, errLimit) || out != nil {
		t.Fatal("actual4MiB+1 refused before allocation/parse", e)
	}
	// All large outer budgets use independent n4192464/C29379324 arithmetic.
	largeRef := registryExpectedRef(t, rows["base"])
	copy(largeRef.submitted.preparedHash[:], ph.Sum(nil))
	copy(largeRef.submitted.submissionHash[:], sh.Sum(nil))
	recordBudget := registeredRegistryCodecBudget{4128 + 29379324 + 3*(309+4192684) - 64, 4096 + 309 + 4192684}
	hki, lookup, hkv, stored, e := encodeRegistryRecords(pij, 4, 2, registryTestFormat(), recordBudget)
	if e != nil || hki != largeRef.submitted.lookupKey || hkv != largeRef.storedKey || len(lookup) != 309 || cap(lookup) != 309 || len(stored) != 4192684 || cap(stored) != 4192684 || !bytes.Equal(stored[188:len(stored)-32], pij) {
		t.Fatal("large exact-cap records/complete prepared bytes", e)
	}
	lookupBudget := registeredRegistryCodecBudget{4128 + 2*309 - 32, 4096}
	lookupRef, declared, e := decodeRegistryLookup(hki[:], lookup, 4, registryTestOrigin(t, rows["base"]), lookupBudget)
	if e != nil || lookupRef != largeRef || declared != 4192464 {
		t.Fatal("large Lookup reciprocal ref/length/hashes", e)
	}
	storedBudget := registeredRegistryCodecBudget{4128 + 29379324 + 2*4192684 - 32, 4096}
	storedRef, borrowed, e := decodeRegistryStored(hkv[:], stored, 4, registryTestOrigin(t, rows["base"]), registryTestFormat(), storedBudget)
	if e != nil || storedRef != lookupRef || !bytes.Equal(borrowed, pij) || &borrowed[0] != &stored[188] || cap(borrowed) != len(borrowed) {
		t.Fatal("large Stored canonical pair and clipped borrow", e)
	}
	checkShort := func(name string, exact registeredRegistryCodecBudget, run func(registeredRegistryCodecBudget) (error, bool)) {
		for _, b := range []registeredRegistryCodecBudget{{exact.workBytes - 1, exact.ownedBytes}, {exact.workBytes, exact.ownedBytes - 1}} {
			if err, zero := run(b); !errors.Is(err, errLimit) || !zero {
				t.Fatal("large one-short whole-zero", name, b, err)
			}
		}
	}
	checkShort("Records", recordBudget, func(b registeredRegistryCodecBudget) (error, bool) {
		k, x, s, y, err := encodeRegistryRecords(pij, 4, 2, registryTestFormat(), b)
		return err, k == ([96]byte{}) && s == ([89]byte{}) && x == nil && y == nil
	})
	checkShort("Lookup", lookupBudget, func(b registeredRegistryCodecBudget) (error, bool) {
		x, n, err := decodeRegistryLookup(hki[:], lookup, 4, registryTestOrigin(t, rows["base"]), b)
		return err, x == (registeredRegistryRef{}) && n == 0
	})
	checkShort("Stored", storedBudget, func(b registeredRegistryCodecBudget) (error, bool) {
		x, y, err := decodeRegistryStored(hkv[:], stored, 4, registryTestOrigin(t, rows["base"]), registryTestFormat(), b)
		return err, x == (registeredRegistryRef{}) && y == nil
	})
	for _, arm := range []struct {
		representation        byte
		address               []byte
		term                  uint64
		wireBytes, ownedBytes int
	}{{1, nil, 0, 4192573, 4192464}, {2, hkv[:], 2, 4192670, 4192553}} {
		encBudget := registeredRegistryCodecBudget{4128 + 29379324 + 3*arm.wireBytes - 32, 4096 + arm.wireBytes}
		irj, err := encodeRegistryIRJ(arm.representation, pij, arm.address, arm.term, registryTestFormat(), encBudget)
		if err != nil || len(irj) != arm.wireBytes || cap(irj) != arm.wireBytes || !bytes.Equal(irj[7:39], sh.Sum(nil)) || !bytes.Equal(irj[39:71], ph.Sum(nil)) {
			t.Fatal("large IRJ exact-cap encoding and independent hashes", arm.representation, err)
		}
		decBudget := registeredRegistryCodecBudget{4128 + 29379324 + 2*arm.wireBytes - 32 + arm.ownedBytes, 4096 + arm.ownedBytes}
		owned, err := decodeRegistryIRJ(irj, registryTestFormat(), decBudget)
		if err != nil || owned.representation != arm.representation || owned.term != arm.term || owned.submissionHash != largeRef.submitted.submissionHash || owned.preparedHash != largeRef.submitted.preparedHash || !bytes.Equal(owned.prepared, pij) || !bytes.Equal(owned.address, arm.address) || cap(owned.prepared) != 4192464 || cap(owned.address) != len(arm.address) {
			t.Fatal("large IRJ full owned tuple", arm.representation, err)
		}
		checkShort("IRJ encode", encBudget, func(b registeredRegistryCodecBudget) (error, bool) {
			x, err := encodeRegistryIRJ(arm.representation, pij, arm.address, arm.term, registryTestFormat(), b)
			return err, x == nil
		})
		checkShort("IRJ decode", decBudget, func(b registeredRegistryCodecBudget) (error, bool) {
			x, err := decodeRegistryIRJ(irj, registryTestFormat(), b)
			return err, reflect.DeepEqual(x, registeredRegistryExport{})
		})
		clear(irj)
		if !bytes.Equal(owned.prepared, pij) || !bytes.Equal(owned.address, arm.address) {
			t.Fatal("large IRJ owned copies survive complete input mutation", arm.representation)
		}
	}

}

func TestRegistryAllDoorShortFormatAndChildRefusals(t *testing.T) {
	rows, old := registryVectors(t)
	r := rows["base"]
	l, b := registryTestFormat(), registryTestBudget()
	o := registryTestOrigin(t, r)
	ref := registryExpectedRef(t, r)
	bad := l
	bad.walkBytes = 0
	if x, e := encodeRegistryCommand(nil, bad, b); !errors.Is(e, errInvalid) || x != nil {
		t.Fatal("command invalid format", e)
	}
	if k, x, s, y, e := encodeRegistryRecords(nil, 4, 2, bad, b); !errors.Is(e, errInvalid) || k != ([96]byte{}) || s != ([89]byte{}) || x != nil || y != nil {
		t.Fatal("records invalid format", e)
	}
	if x, p, e := decodeRegistryStored(nil, nil, 4, o, bad, b); !errors.Is(e, errInvalid) || x != (registeredRegistryRef{}) || p != nil {
		t.Fatal("stored invalid format", e)
	}
	if x, e := decodeRegistryIRJ(nil, bad, b); !errors.Is(e, errInvalid) || !reflect.DeepEqual(x, registeredRegistryExport{}) {
		t.Fatal("IRJ invalid format", e)
	}
	if x, n, e := decodeRegistryLookup(nil, nil, 4, o, b); !errors.Is(e, errInvalid) || x != (registeredRegistryRef{}) || n != 0 {
		t.Fatal("lookup short", e)
	}
	if x, p, e := decodeRegistryStored(nil, nil, 4, o, l, b); !errors.Is(e, errInvalid) || x != (registeredRegistryRef{}) || p != nil {
		t.Fatal("stored short", e)
	}
	if x, e := decodeRegistryOutcome(nil, b); !errors.Is(e, errInvalid) || x != (registeredRegistryOutcome{}) {
		t.Fatal("outcome short", e)
	}
	for _, decoder := range []func([]byte) error{
		func(x []byte) error { _, e := decodeRegistryCommand(x, l, b); return e },
		func(x []byte) error { _, _, e := decodeRegistryStored(ref.storedKey[:], x, 4, o, l, b); return e },
		func(x []byte) error { _, e := decodeRegistryIRJ(x, l, b); return e },
	} {
		if e := decoder(make([]byte, registryPreparedMax+221)); !errors.Is(e, errLimit) {
			t.Fatal("above immutable maximum before prefix", e)
		}
	}
	if k, x, s, y, e := encodeRegistryRecords(registryBytes(t, old.PIJ), 4, 2, l, b); !errors.Is(e, errInvalid) || k != ([96]byte{}) || s != ([89]byte{}) || x != nil || y != nil {
		t.Fatal("record NEW meaning", e)
	}
	if x, e := encodeRegistryIRJ(1, registryBytes(t, old.PIJ), nil, 0, l, b); !errors.Is(e, errInvalid) || x != nil {
		t.Fatal("IRJ NEW meaning", e)
	}
	q := registryLedger{work: registryWorkMax, owned: registryOwnedMax}
	invalid := registryBytes(t, r.PIJ)
	invalid[0] ^= 1
	if x, e := q.inspect(invalid, l); !errors.Is(e, errInvalid) || !reflect.DeepEqual(x, registeredPreparedInspection{}) {
		t.Fatal("child failure zero", e)
	}
}
