package raftlog

import (
	"encoding/binary"
	"math"
	"slices"
)

// ApplicationRecoverySpan is a compact, ordered band of distinct completion
// phases. Next is the first unspent phase, inclusive; First and Last are immutable
// bounds. Each step owns the declared command/log and installation maxima. The
// materializer must still authenticate the actual command and protocol progress.
// A local cursor, caller count or copied span grants no installation authority.
type ApplicationRecoverySpan struct {
	First, Next, Last              uint32
	CommandBytes                   int
	RetainedBytes, RetainedRecords uint64
}

const maxApplicationRecoverySpans = 8
const recoverySpanBytes = 32

func recoveryEntries(r ApplicationRecoveryReservation) int { return len(r.Credits) + len(r.Spans) }
func recoveryReservationBytes(r ApplicationRecoveryReservation) int {
	return recoveryRootFixedBytes + recoveryCreditBytes*len(r.Credits) + recoverySpanBytes*len(r.Spans)
}
func recoverySpanProfile(c ApplicationRecoveryConfig, r ApplicationRecoveryReservation) error {
	if c.Version == 3 {
		if len(r.Credits) != 0 {
			return ErrInvalid
		}
	} else if len(r.Spans) != 0 {
		return ErrInvalid
	}
	return nil
}
func checkedRecoveryMul(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, ErrLimit
	}
	return a * b, nil
}
func recoverySpanGrowth(r ApplicationRecoveryReservation, p ApplicationPolicy, l Limits) (applicationRecoveryGrowth, error) {
	var g applicationRecoveryGrowth
	if r.ID == ([32]byte{}) || r.Digest == ([32]byte{}) || len(r.Credits) != 0 || len(r.Spans) == 0 || len(r.Spans) > maxApplicationRecoverySpans {
		return g, ErrInvalid
	}
	for i, s := range r.Spans {
		if s.First == 0 || s.Next < s.First || s.Next > s.Last || i > 0 && r.Spans[i-1].Last >= s.First {
			return applicationRecoveryGrowth{}, ErrInvalid
		}
		credit := ApplicationRecoveryCredit{Phase: s.Next, CommandBytes: s.CommandBytes, RetainedBytes: s.RetainedBytes, RetainedRecords: s.RetainedRecords}
		step, err := recoveryGrowth(ApplicationRecoveryReservation{ID: r.ID, Digest: r.Digest, Credits: []ApplicationRecoveryCredit{credit}}, p, l)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		count := uint64(s.Last) - uint64(s.Next) + 1
		for _, row := range []struct {
			each uint64
			out  *uint64
		}{{step.bytes, &g.bytes}, {step.records, &g.records}, {step.logBytes, &g.logBytes}, {step.logRecords, &g.logRecords}} {
			n, err := checkedRecoveryMul(count, row.each)
			if err != nil {
				return applicationRecoveryGrowth{}, err
			}
			*row.out, err = checkedRecoveryAdd(*row.out, n)
			if err != nil {
				return applicationRecoveryGrowth{}, err
			}
		}
	}
	return g, nil
}
func (m applicationRecoveryMetadata) spanCredit(u ApplicationRecoveryUse) (ApplicationRecoveryCredit, error) {
	if m.Config.Version != 3 || len(m.Reservation.Credits) != 0 {
		return ApplicationRecoveryCredit{}, ErrInvalid
	}
	for _, s := range m.Reservation.Spans {
		if s.Next != u.Phase {
			continue
		}
		if u.RetireBefore != 0 {
			// Only the exact terminal singleton, beyond every earlier band, may
			// retire unused suffixes. Projection never performs that retirement.
			if u.RetireBefore != u.Phase || s.First != s.Last || m.Reservation.Spans[len(m.Reservation.Spans)-1].Last != u.Phase {
				return ApplicationRecoveryCredit{}, ErrInvalid
			}
		}
		return ApplicationRecoveryCredit{Phase: u.Phase, CommandBytes: s.CommandBytes, RetainedBytes: s.RetainedBytes, RetainedRecords: s.RetainedRecords}, nil
	}
	return ApplicationRecoveryCredit{}, ErrInvalid
}
func (m applicationRecoveryMetadata) consumeSpan(u ApplicationRecoveryUse, credit ApplicationRecoveryCredit) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	r := m.Reservation
	r.Spans = slices.Clone(r.Spans)
	n := 0
	for _, s := range r.Spans {
		if u.RetireBefore != 0 && s.Last < u.RetireBefore {
			continue
		}
		if s.Next == u.Phase {
			if s.Next == s.Last {
				continue
			}
			s.Next++
		}
		r.Spans[n] = s
		n++
	}
	r.Spans = r.Spans[:n]
	if n == 0 {
		r = ApplicationRecoveryReservation{}
	}
	m.Reservation = r
	return m, credit, nil
}

// RS3 has a constant bounded owned representation; phase count never controls
// allocation. Terminal bank bytes are appended by the native root wrapper.
func appendRecoverySpanRoot(r ApplicationRecoveryReservation) []byte {
	b := make([]byte, 0, recoveryReservationBytes(r))
	b = append(b, 'R', 'S', 3, 0)
	b = append(b, r.ID[:]...)
	b = append(b, r.Digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Spans))) // #nosec G115 -- <=8 admitted spans.
	for _, s := range r.Spans {
		for _, n := range []uint32{s.First, s.Next, s.Last, uint32(s.CommandBytes)} {
			b = binary.BigEndian.AppendUint32(b, n)
		} // #nosec G115 -- command admitted under concrete <=16MiB entry bound.
		b = binary.BigEndian.AppendUint64(b, s.RetainedBytes)
		b = binary.BigEndian.AppendUint64(b, s.RetainedRecords)
	}
	return b
}
func readRecoverySpanRoot(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryReservation, int, error) {
	var r ApplicationRecoveryReservation
	if len(b) < recoveryRootFixedBytes || len(b) > maxMetadataBytes || b[0] != 'R' || b[1] != 'S' || b[2] != 3 || b[3] != 0 {
		return r, 0, ErrCorrupt
	}
	copy(r.ID[:], b[4:36])
	copy(r.Digest[:], b[36:68])
	n := binary.BigEndian.Uint32(b[68:72])
	if n > maxApplicationRecoverySpans || uint64(n)*recoverySpanBytes > uint64(len(b)-recoveryRootFixedBytes) {
		return ApplicationRecoveryReservation{}, 0, ErrCorrupt
	}
	if n > 0 {
		r.Spans = make([]ApplicationRecoverySpan, int(n))
	} // #nosec G115 -- <=8 and delivered bytes checked BEFORE allocation.
	for i := range r.Spans {
		row := b[72+i*recoverySpanBytes : 72+(i+1)*recoverySpanBytes]
		command := binary.BigEndian.Uint32(row[12:16])
		if uint64(command) > unsignedLimit(l.MaxEntryBytes-frameOverhead) {
			return ApplicationRecoveryReservation{}, 0, ErrCorrupt
		}
		r.Spans[i] = ApplicationRecoverySpan{First: binary.BigEndian.Uint32(row[:4]), Next: binary.BigEndian.Uint32(row[4:8]), Last: binary.BigEndian.Uint32(row[8:12]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(row[16:24]), RetainedRecords: binary.BigEndian.Uint64(row[24:32])} // #nosec G115 -- command <=16MiB before conversion.
	}
	if _, err := recoveryGrowthIfPresent(r, p, l); err != nil {
		return ApplicationRecoveryReservation{}, 0, ErrCorrupt
	}
	return r, recoveryRootFixedBytes + int(n)*recoverySpanBytes, nil // #nosec G115 -- <=8 above.
}

func recoveryReservationOverhead(r ApplicationRecoveryReservation) (uint64, uint64, error) {
	if len(r.Spans) == 0 {
		return ApplicationRecoveryRootOverhead(len(r.Credits))
	}
	if len(r.Credits) != 0 || len(r.Spans) > maxApplicationRecoverySpans {
		return 0, 0, ErrInvalid
	}
	return uint64(9 + appFrameBytes + recoveryReservationBytes(r)), 1, nil // #nosec G115 -- <=8 fixed spans.
}
