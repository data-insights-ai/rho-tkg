package graphapply

import (
	"bytes"
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
