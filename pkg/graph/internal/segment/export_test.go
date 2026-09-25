package segment

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// SectionSizes exposes each section's byte size to the external scale test.
func SectionSizes(s *Segment) map[string]int {
	out := make(map[string]int, len(s.secs))
	for name, sec := range s.secs {
		out[name] = sec.length
	}
	return out
}

// ScanNoHash decodes every row without recomputing its hash, so the scale
// test can report the decode cost and the hash cost separately.
func ScanNoHash(s *Segment, fn func(i int, r *types.Relationship)) error {
	return s.scan(0, s.rows, false, func(i int, r *types.Relationship) error { fn(i, r); return nil })
}

// DecodeColumns decodes the columns a bulk reader needs — start and end
// ordinals, valid_from, valid_to, tx_from, rel_id and every declared column's
// stored values — page by page without building Relationship objects, and
// returns a checksum so the work cannot be optimized away. It measures the
// codec itself, the ceiling for a columnar scan door (ADR-0011 §5.3, S5).
func DecodeColumns(s *Segment) int64 {
	pr := s.pageRows
	buf := make([][]int64, 6+len(s.props))
	for i := range buf {
		buf[i] = make([]int64, pr)
	}
	runStart := make([]bool, pr)
	var sum int64
	for p := 0; p*pr < s.rows; p++ {
		ps, cnt := p*pr, min(pr, s.rows-p*pr)
		o := s.startOrdinal(ps)
		for k := 0; k < cnt; k++ {
			prev := o
			for o < s.nodes-1 && s.outcsr.atNoRef(o+1) <= int64(ps+k) {
				o++
			}
			runStart[k] = k == 0 || o != prev
			sum += int64(o)
		}
		s.end.decodePage(p, cnt, buf[0], nil, runStart)
		s.vf.decodePage(p, cnt, buf[1], nil, nil)
		s.vt.decodePage(p, cnt, buf[2], buf[1], nil)
		s.tx.decodePage(p, cnt, buf[3], buf[1], nil)
		s.relID.decodePage(p, cnt, buf[4], nil, nil)
		for c := range s.props {
			if s.props[c].col.Kind == KindString && s.props[c].strs.coded() {
				s.props[c].strs.codes.decodePage(p, cnt, buf[6+c], nil, nil)
			} else {
				s.props[c].ints.decodePage(p, cnt, buf[6+c], nil, nil)
			}
		}
		for _, b := range buf {
			for _, x := range b[:cnt] {
				sum += x
			}
		}
	}
	return sum
}
