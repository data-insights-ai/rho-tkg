package index

import (
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

// ErrInvalidHLLPrecision is returned by NewHyperLogLog when the requested
// precision is outside the supported range.
var ErrInvalidHLLPrecision = errors.New("index: hyperloglog precision out of supported range [4,18]")

// ErrHLLPrecisionMismatch is returned by HyperLogLog.Merge when the two
// sketches were built with different precisions (their register arrays are
// not directly comparable).
var ErrHLLPrecisionMismatch = errors.New("index: hyperloglog precision mismatch on merge")

const (
	// DefaultHLLPrecision is the register-index width NodePropertyStatsCapability
	// sketches use: m = 2^14 = 16384 registers, standard error
	// ≈ 1.04/sqrt(m) ≈ 0.81% — comfortably inside the accuracy bar this
	// package is held to (<5% at 10k distinct values, <3% at 100k; see
	// hyperloglog_test.go for the seeded regression that pins this).
	DefaultHLLPrecision uint8 = 14

	minHLLPrecision uint8 = 4
	maxHLLPrecision uint8 = 18

	// sparseToDenseDivisor bounds how large the sparse register map is
	// allowed to grow (as a fraction of m, the full dense register count)
	// before HyperLogLog converts to a dense []uint8 array. A (label,
	// property-key) pair with only a handful of distinct values — the common
	// case for enum-like properties — never pays the full m-byte dense
	// allocation; a high-cardinality key crosses the threshold once and
	// stays dense (no hysteresis, so no flapping conversion cost).
	sparseToDenseDivisor = 4
)

// HyperLogLog is an in-tree, dependency-free cardinality-estimation sketch
// (Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog: the analysis of a
// near-optimal cardinality estimation algorithm", AofA 2007) used to answer
// "how many DISTINCT values does this (label, property key) pair carry" for
// NodePropertyStatsCapability.
//
// It starts in a SPARSE representation — a map of only the non-zero
// registers — so a low-cardinality key costs a few map entries instead of
// the full 2^precision-byte dense array, then converts to dense once the
// sparse map would cost more than the dense array (see
// sparseToDenseDivisor) and never converts back.
//
// Add/AddString/Merge are NOT safe for concurrent use with each other or with
// Estimate/Clone: callers (the memory and badger store backends) already
// serialize every mutation through their existing entity/index locks — the
// same contract the property-key presence counters rely on.
type HyperLogLog struct {
	precision uint8
	m         uint32
	dense     []uint8          // nil while sparse
	sparse    map[uint32]uint8 // nil once dense
}

// NewHyperLogLog constructs a sketch at the given precision (4..18 register
// index bits inclusive; DefaultHLLPrecision = 14 is what the store backends
// use for every NodePropertyStatsCapability sketch).
func NewHyperLogLog(precision uint8) (*HyperLogLog, error) {
	if precision < minHLLPrecision || precision > maxHLLPrecision {
		return nil, ErrInvalidHLLPrecision
	}
	return &HyperLogLog{
		precision: precision,
		m:         uint32(1) << precision,
		sparse:    make(map[uint32]uint8),
	}, nil
}

// Precision returns the sketch's register-index width. Safe on a nil
// receiver (returns 0).
func (h *HyperLogLog) Precision() uint8 {
	if h == nil {
		return 0
	}
	return h.precision
}

// AddString hashes s with a fixed, deterministic 64-bit hash (FNV-1a — no new
// dependency, and determinism is what makes the seeded accuracy test in
// hyperloglog_test.go reproducible across runs) and folds the result into
// the sketch. Safe on a nil receiver (no-op).
func (h *HyperLogLog) AddString(s string) {
	if h == nil {
		return
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(s)) // hash.Hash.Write never returns an error
	h.addHash(sum.Sum64())
}

func (h *HyperLogLog) addHash(hash uint64) {
	idx := uint32(hash >> (64 - h.precision))
	h.setRegister(idx, rank(hash, h.precision))
}

// rank returns the 1-based position of the first set bit among the
// (64-precision) bits of hash BELOW the top `precision` index bits — the
// standard HyperLogLog "rho" function. Capped so an all-zero remainder (an
// astronomically rare hash) cannot overflow the uint8 register or read past
// the bit width being measured.
func rank(hash uint64, precision uint8) uint8 {
	rem := hash << precision
	lz := bits.LeadingZeros64(rem)
	maxLZ := int(64 - precision)
	if lz > maxLZ {
		lz = maxLZ
	}
	return uint8(lz + 1) // #nosec G115 — lz+1 <= 64-precision+1 <= 61, well inside uint8.
}

func (h *HyperLogLog) setRegister(idx uint32, rho uint8) {
	if h.dense != nil {
		if rho > h.dense[idx] {
			h.dense[idx] = rho
		}
		return
	}
	if cur, ok := h.sparse[idx]; !ok || rho > cur {
		h.sparse[idx] = rho
	}
	if uint32(len(h.sparse))*sparseToDenseDivisor > h.m {
		h.convertToDense()
	}
}

func (h *HyperLogLog) convertToDense() {
	dense := make([]uint8, h.m)
	for idx, rho := range h.sparse {
		dense[idx] = rho
	}
	h.dense = dense
	h.sparse = nil
}

// Merge folds other's registers into h by per-register max — the standard
// HyperLogLog merge. A sketch's entire state is its per-register maximum
// rank, so merging two sketches this way is exactly what a single sketch fed
// the union of both input streams would have recorded. Both sketches must
// share the same precision, or ErrHLLPrecisionMismatch is returned. Nil
// receivers/arguments are a no-op (nil error). This is what the tiered
// backend's cross-shard NDV fold uses — every shard's sketch
// (PropertyStatsAccumulator.Sketch()) is Merge()d register-max into one
// combined sketch, then Estimate() is called ONCE on the result (summing
// per-shard Estimate()s would over-count a value present on more than one
// shard) — see docs/adr/0005-tiered-parity.md §3.1 and
// docs/query-planners.md "Tiered NDV fold".
func (h *HyperLogLog) Merge(other *HyperLogLog) error {
	if h == nil || other == nil {
		return nil
	}
	if h.precision != other.precision {
		return ErrHLLPrecisionMismatch
	}
	if other.dense != nil {
		for idx, rho := range other.dense {
			if rho == 0 {
				continue
			}
			h.setRegister(uint32(idx), rho) // #nosec G115 — idx < m <= 2^18.
		}
		return nil
	}
	for idx, rho := range other.sparse {
		h.setRegister(idx, rho)
	}
	return nil
}

// Clone returns an independent deep copy.
func (h *HyperLogLog) Clone() *HyperLogLog {
	if h == nil {
		return nil
	}
	out := &HyperLogLog{precision: h.precision, m: h.m}
	if h.dense != nil {
		out.dense = append([]uint8(nil), h.dense...)
	} else {
		out.sparse = make(map[uint32]uint8, len(h.sparse))
		for k, v := range h.sparse {
			out.sparse[k] = v
		}
	}
	return out
}

// Estimate returns the bias-corrected cardinality estimate: the classic
// Flajolet et al. raw estimate with the small-range linear-counting
// correction (§4 of the paper). The large-range 2^32 correction from the
// original paper is intentionally omitted — with a 64-bit hash and
// precision<=18 the raw estimate never approaches the collision regime that
// correction exists for, at any cardinality this library will realistically
// see. Safe on a nil receiver (returns 0).
func (h *HyperLogLog) Estimate() int64 {
	if h == nil {
		return 0
	}
	m := float64(h.m)
	sum := 0.0
	var zeros int
	if h.dense != nil {
		for _, rho := range h.dense {
			sum += 1.0 / float64(uint64(1)<<rho)
			if rho == 0 {
				zeros++
			}
		}
	} else {
		zeros = int(h.m) - len(h.sparse)
		sum = float64(zeros) // every unset register has rho=0, contributing 2^-0=1
		for _, rho := range h.sparse {
			sum += 1.0 / float64(uint64(1)<<rho)
		}
	}

	raw := alphaForM(h.m) * m * m / sum
	if raw <= 2.5*m && zeros > 0 {
		return int64(math.Round(m * math.Log(m/float64(zeros))))
	}
	return int64(math.Round(raw))
}

// alphaForM is the Flajolet et al. bias-correction constant, using the
// paper's exact small-m values and its asymptotic formula for m>=128.
func alphaForM(m uint32) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/float64(m))
	}
}
