package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// registeredGenesisSource is inert data; actual Store truth is checked later.
// It deliberately contains no future Genesis entry index/term or authority flag.
type registeredGenesisSource struct {
	Scope                      raftlog.ApplicationIdentity
	SourceKind                 uint8
	SourcePosition, SourceTerm uint64
	ImageLength                uint32
	ImageHash                  [32]byte
	InitialOwnerEpoch          uint64
	Voters                     [3]uint64
	Meaning                    [32]byte
	SharedContract             [10]uint64
}

type registeredJournalBudget struct{ walkBytes uint64 }

// Reserve every logical traversal, fixed comparison and owned output before
// accessing data or allocating backing. This ledger does not measure heap/RSS.
func reserveRegisteredJournal(budget registeredJournalBudget, traversal, emission, copied, owned uint64) error {
	if budget.walkBytes == 0 || budget.walkBytes > 32<<20 {
		return errInvalid
	}
	remaining := budget.walkBytes
	for _, charge := range [6]uint64{2048, 512, traversal, emission, copied, owned} {
		if charge > remaining {
			return errLimit
		}
		remaining -= charge
	}
	return nil
}

func validateRegisteredGenesisSource(s registeredGenesisSource) error {
	if s.Scope.Graph == ([16]byte{}) || s.Scope.Group == ([16]byte{}) || s.Scope.Partition == 0 || s.Meaning == ([32]byte{}) {
		return errInvalid
	}
	if s.SourceKind != 1 || s.SourcePosition != 1 || s.SourceTerm != 1 || s.ImageLength != 140 || s.InitialOwnerEpoch == 0 {
		return errInvalid
	}
	for i, id := range s.Voters {
		if id == 0 || id >= math.MaxUint64-1 || i > 0 && id <= s.Voters[i-1] {
			return errInvalid
		}
	}
	c := s.SharedContract
	if c[0] != 2 {
		return errInvalid
	}
	for _, n := range c[1:] {
		if n == 0 || n > 64<<20 {
			return errInvalid
		}
	}
	if c[1] > 64<<10 || c[2] > 16<<20 || c[4] > 4096 || c[6] > 4096 || uint64(s.ImageLength) > c[3] {
		return errInvalid
	}
	// ImageHash is an opaque fixed-width value, including zero. Binding/meaning
	// agreement and the actual seed hash belong to subsequent real Store capture.
	return nil
}

// Only validated data and exact244/255-capacity callers reach this emitter.
// All numeric fields stay unsigned; no untrusted count sizes backing.
func appendRegisteredGenesisSource(out []byte, s registeredGenesisSource) []byte {
	out = append(out, "JGS1"...)
	out = binary.BigEndian.AppendUint16(out, 1)
	out = append(out, s.Scope.Graph[:]...)
	out = binary.BigEndian.AppendUint64(out, s.Scope.Partition)
	out = append(out, s.Scope.Group[:]...)
	out = append(out, s.SourceKind)
	out = binary.BigEndian.AppendUint64(out, s.SourcePosition)
	out = binary.BigEndian.AppendUint64(out, s.SourceTerm)
	out = binary.BigEndian.AppendUint32(out, s.ImageLength)
	out = append(out, s.ImageHash[:]...)
	out = binary.BigEndian.AppendUint64(out, s.InitialOwnerEpoch)
	out = append(out, 3)
	for _, id := range s.Voters {
		out = binary.BigEndian.AppendUint64(out, id)
	}
	out = append(out, s.Meaning[:]...)
	for _, n := range s.SharedContract {
		out = binary.BigEndian.AppendUint64(out, n)
	}
	return out
}

func encodeGenesisSource(source registeredGenesisSource, budget registeredJournalBudget) ([]byte, error) {
	// 244 traversal +512 checks +244 emission +2048 metadata +244 backing =3292.
	if err := reserveRegisteredJournal(budget, 244, 244, 0, 244); err != nil {
		return nil, err
	}
	if err := validateRegisteredGenesisSource(source); err != nil {
		return nil, err
	}
	return appendRegisteredGenesisSource(make([]byte, 0, 244), source), nil
}

func encodeGenesisCommand(source registeredGenesisSource, budget registeredJournalBudget) ([]byte, error) {
	// 244 traversal +512 checks +255 emission +2048 metadata +255 backing =3314.
	if err := reserveRegisteredJournal(budget, 244, 255, 0, 255); err != nil {
		return nil, err
	}
	if err := validateRegisteredGenesisSource(source); err != nil {
		return nil, err
	}
	out := make([]byte, 11, 255)
	copy(out, "GJQ1")
	binary.BigEndian.PutUint16(out[4:6], 1)
	out[6] = 0
	binary.BigEndian.PutUint32(out[7:11], 244)
	// Emit into this one owned backing; no intermediate244-byte allocation.
	return appendRegisteredGenesisSource(out, source), nil
}

func decodeGenesisCommand(wire []byte, budget registeredJournalBudget) (registeredGenesisSource, error) {
	// 255 traversal +512 checks +244 field copies +2048 fixed owned metadata =3059.
	if err := reserveRegisteredJournal(budget, 255, 0, 244, 0); err != nil {
		return registeredGenesisSource{}, err
	}
	if len(wire) != 255 {
		return registeredGenesisSource{}, errInvalid
	}
	// The complete11-byte prefix is in range before any prefix read.
	if !bytes.Equal(wire[:4], []byte("GJQ1")) || binary.BigEndian.Uint16(wire[4:6]) != 1 || wire[6] != 0 {
		return registeredGenesisSource{}, errInvalid
	}
	declared := binary.BigEndian.Uint32(wire[7:11])
	body := wire[11:]
	// No length narrowing: verify the declared244 against the exact remainder.
	if declared != 244 || len(body) != 244 {
		return registeredGenesisSource{}, errInvalid
	}
	// The exact244-byte remaining envelope proves every fixed span below fits:
	// JGS46, seed prefix108, voter bytes24, meaning32 and final contract80.
	if !bytes.Equal(body[:4], []byte("JGS1")) || binary.BigEndian.Uint16(body[4:6]) != 1 || body[107] != 3 {
		return registeredGenesisSource{}, errInvalid
	}
	var source registeredGenesisSource
	copy(source.Scope.Graph[:], body[6:22])
	source.Scope.Partition = binary.BigEndian.Uint64(body[22:30])
	copy(source.Scope.Group[:], body[30:46])
	source.SourceKind = body[46]
	source.SourcePosition = binary.BigEndian.Uint64(body[47:55])
	source.SourceTerm = binary.BigEndian.Uint64(body[55:63])
	source.ImageLength = binary.BigEndian.Uint32(body[63:67])
	copy(source.ImageHash[:], body[67:99])
	source.InitialOwnerEpoch = binary.BigEndian.Uint64(body[99:107])
	for i := range 3 {
		at := 108 + 8*i
		source.Voters[i] = binary.BigEndian.Uint64(body[at : at+8])
	}
	copy(source.Meaning[:], body[132:164])
	for i := range 10 {
		at := 164 + 8*i
		source.SharedContract[i] = binary.BigEndian.Uint64(body[at : at+8])
	}
	if err := validateRegisteredGenesisSource(source); err != nil {
		return registeredGenesisSource{}, err
	}
	return source, nil
}

// All codec work is admitted before fixed reads, checksum scans or output backing.
type registeredOrigin struct {
	source      registeredGenesisSource
	index, term uint64
}

func registeredGenesisIdentity(s registeredGenesisSource) (key [47]byte, lineage [32]byte) {
	copy(key[:], "HKS1")
	key[4] = 1
	copy(key[5:21], s.Scope.Graph[:])
	binary.BigEndian.PutUint64(key[21:29], s.Scope.Partition)
	copy(key[29:45], s.Scope.Group[:])
	binary.BigEndian.PutUint16(key[45:47], 1)
	var input [86]byte
	copy(input[:36], "rho-tkg:registered-genesis-scope:v1\x00")
	binary.BigEndian.PutUint32(input[36:40], 46)
	copy(input[40:44], "JGS1")
	binary.BigEndian.PutUint16(input[44:46], 1)
	copy(input[46:62], s.Scope.Graph[:])
	binary.BigEndian.PutUint64(input[62:70], s.Scope.Partition)
	copy(input[70:86], s.Scope.Group[:])
	return key, sha256.Sum256(input[:])
}

func encodeRegisteredOrigin(s registeredGenesisSource, index, term uint64, b registeredJournalBudget) ([]byte, []byte, error) {
	//2560 fixed +244 validation +86 lineage +346 checksum +425 emission +425 backing =4086.
	if err := reserveRegisteredJournal(b, 244+86+346, 425, 0, 425); err != nil {
		return nil, nil, err
	}
	if err := validateRegisteredGenesisSource(s); err != nil {
		return nil, nil, err
	}
	if index < 3 || term <= s.SourceTerm {
		return nil, nil, errInvalid
	}
	key, lineage := registeredGenesisIdentity(s)
	out := make([]byte, 0, 378)
	out = append(out, "HCR1"...)
	out = binary.BigEndian.AppendUint16(out, 1)
	out = append(out, 1, 0)
	out = append(out, s.Scope.Graph[:]...)
	out = binary.BigEndian.AppendUint64(out, s.Scope.Partition)
	out = append(out, s.Scope.Group[:]...)
	out = append(out, lineage[:]...)
	out = binary.BigEndian.AppendUint16(out, 1)
	out = binary.BigEndian.AppendUint64(out, index)
	out = binary.BigEndian.AppendUint32(out, 252)
	out = appendRegisteredGenesisSource(out, s)
	out = binary.BigEndian.AppendUint64(out, term)
	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	ownedKey := make([]byte, 47)
	copy(ownedKey, key[:])
	return ownedKey, out, nil
}

func decodeRegisteredOrigin(wire []byte, b registeredJournalBudget) (registeredOrigin, error) {
	//2560 fixed +378 frame +346 checksum +86 lineage +3059 decoder +255 wrapper emission/backing =6939.
	if err := reserveRegisteredJournal(b, 378+346+86+3059, 255, 0, 255); err != nil {
		return registeredOrigin{}, err
	}
	if len(wire) != 378 || !bytes.Equal(wire[:8], []byte{'H', 'C', 'R', '1', 0, 1, 1, 0}) || binary.BigEndian.Uint16(wire[80:82]) != 1 || binary.BigEndian.Uint32(wire[90:94]) != 252 {
		return registeredOrigin{}, errCorrupt
	}
	sum := sha256.Sum256(wire[:346])
	if !bytes.Equal(sum[:], wire[346:]) {
		return registeredOrigin{}, errCorrupt
	}
	command := make([]byte, 11, 255)
	copy(command, "GJQ1")
	binary.BigEndian.PutUint16(command[4:6], 1)
	binary.BigEndian.PutUint32(command[7:11], 244)
	command = append(command, wire[94:338]...)
	source, err := decodeGenesisCommand(command, registeredJournalBudget{3059})
	if err != nil {
		return registeredOrigin{}, err
	}
	_, lineage := registeredGenesisIdentity(source)
	index, term := binary.BigEndian.Uint64(wire[82:90]), binary.BigEndian.Uint64(wire[338:346])
	if index < 3 || term <= source.SourceTerm || !bytes.Equal(wire[8:24], source.Scope.Graph[:]) || binary.BigEndian.Uint64(wire[24:32]) != source.Scope.Partition || !bytes.Equal(wire[32:48], source.Scope.Group[:]) || !bytes.Equal(wire[48:80], lineage[:]) {
		return registeredOrigin{}, errCorrupt
	}
	return registeredOrigin{source, index, term}, nil
}

func encodeRegisteredGenesisOutcome(s registeredGenesisSource, index, term, evaluation uint64, status byte, b registeredJournalBudget) ([]byte, error) {
	//2560 fixed +244 validation +86 lineage +113 checksum +145 emission/backing +47 key =3340.
	if err := reserveRegisteredJournal(b, 244+86+113, 145, 0, 145+47); err != nil {
		return nil, err
	}
	if err := validateRegisteredGenesisSource(s); err != nil {
		return nil, err
	}
	if index < 3 || term <= s.SourceTerm || evaluation < index || status != 4 && status != 5 || status == 4 && evaluation != index {
		return nil, errInvalid
	}
	key, lineage := registeredGenesisIdentity(s)
	out := make([]byte, 0, 145)
	out = append(out, "JOR1"...)
	out = binary.BigEndian.AppendUint16(out, 1)
	out = append(out, status)
	out = binary.BigEndian.AppendUint64(out, evaluation)
	out = binary.BigEndian.AppendUint16(out, 0)
	out = append(out, 1)
	out = append(out, key[:]...)
	out = binary.BigEndian.AppendUint64(out, index)
	out = binary.BigEndian.AppendUint64(out, term)
	out = append(out, lineage[:]...)
	sum := sha256.Sum256(out)
	return append(out, sum[:]...), nil
}
