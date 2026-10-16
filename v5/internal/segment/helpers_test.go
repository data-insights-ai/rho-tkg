package segment

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// byteSrc hands out fuzz bytes; past the end it yields zeros.
type byteSrc struct {
	b []byte
}

func (s *byteSrc) u8() byte {
	if len(s.b) == 0 {
		return 0
	}
	v := s.b[0]
	s.b = s.b[1:]
	return v
}

func (s *byteSrc) u64() uint64 {
	var v [8]byte
	for i := range v {
		v[i] = s.u8()
	}
	return binary.LittleEndian.Uint64(v[:])
}

// rowsFromBytes derives up to 11 rows from fuzz input, reaching every
// refusal (zero/negative IDs, invalid coordinates, duplicates, non-scalar
// properties) as well as every accepted shape.
func rowsFromBytes(in []byte) []Row {
	s := &byteSrc{b: in}
	n := int(s.u8()) % 12
	coord := func() Coord {
		switch s.u8() % 5 {
		case 0:
			return NegInf()
		case 1:
			return PosInf()
		case 2:
			return Coord{}
		case 3:
			return Finite(int64(s.u64()))
		}
		return Finite(int64(s.u8()) - 3)
	}
	keys := []string{"s", "i64", "f64", "x", "b"}
	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		r := Row{ID: 0, Revision: uint32(s.u8() % 3)}
		if s.u8()%4 != 0 {
			r.ID = types.RelID(int64(s.u64()>>(s.u8()%64)) & math.MaxInt64)
		} else {
			r.ID = types.RelID(int64(s.u64()))
		}
		r.Start, r.End = types.NodeID(int64(s.u8()%4)), types.NodeID(int64(s.u64()&0xff)-1)
		r.ValidFrom, r.ValidTo, r.TxFrom, r.TxTo = coord(), coord(), coord(), coord()
		for p := int(s.u8() % 4); p > 0; p-- {
			key := keys[s.u8()%byte(len(keys))]
			var v graphstate.Scalar
			switch s.u8() % 6 {
			case 0:
				v = graphstate.Bool(s.u8()%2 == 0)
			case 1:
				v = graphstate.I64(int64(s.u64()))
			case 2:
				v = graphstate.String(string(in[:min(len(in), int(s.u8()%8))]))
			case 3:
				f, err := graphstate.F64(math.Float64frombits(s.u64()))
				if err != nil { // NaN/Inf are not storable F64
					f = graphstate.Null()
				}
				v = f
			case 4:
				v = graphstate.Null()
			default:
				v = graphstate.Scalar{}
			}
			r.Props = append(r.Props, Property{key, v})
		}
		rows = append(rows, r)
	}
	return rows
}

func isTyped(err error) bool {
	for _, e := range []error{ErrCorrupt, ErrUnsupportedVersion, ErrInvalidSchema, ErrInvalidOptions, ErrInvalidRow, ErrHashMismatch, ErrIntegrity, ErrOutOfRange} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
