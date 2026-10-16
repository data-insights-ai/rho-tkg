package index

import (
	"runtime"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 62 review: the window view's write cost (pending changes folded
// into the sorted trees), a build's recording outside the lock against the
// merge under it, and the sidecar's RAM with the views open.

const winBenchMembers = 250_000

// winBenchSidecar returns one value with n members (every fifth replaced
// while open), its window view open.
func winBenchSidecar(n int) *PropertyTxMembers[int64] {
	m := NewPropertyTxMembers[int64]()
	for i := 0; i < n; i++ {
		from := types.Instant(i * 100)
		id := int64(i + 1)
		if i%5 == 0 {
			m.RecordRow("v", id, row(1, 10, 0, from, inf), true)
			m.RecordReplaced("v", id, row(1, 10, 20, from, inf), row(2, 20, 0, from+10, from+50))
			m.RecordRow("v", id, row(2, 20, 0, from+10, from+50), true)
			continue
		}
		m.RecordRow("v", id, row(1, 10, 0, from, from+50), true)
	}
	m.OpenWindow("v")
	return m
}

// BenchmarkWindowViewWrite: one indexed write (a member's current row
// replaced) on a value of 250,000 members with its view open, amortised over
// the folds of the pending changes.
func BenchmarkWindowViewWrite(b *testing.B) {
	m := winBenchSidecar(winBenchMembers)
	b.ReportAllocs()
	i := 0
	ver := uint32(10)
	for b.Loop() {
		id := int64(i%winBenchMembers + 1)
		from := types.Instant(i%winBenchMembers*100) + 5
		ver++
		m.RecordRow("v", id, RowInfo{Version: ver, TxFrom: types.Instant(100 + i), Span: Span{From: from, To: from + 40}}, true)
		i++
	}
}

// BenchmarkWindowViewFold: one fold of a full pending batch into a view of
// 250,000 members (both trees: the late envelopes and the last stamps).
func BenchmarkWindowViewFold(b *testing.B) {
	m := winBenchSidecar(winBenchMembers)
	w := m.windows["v"]
	ver := uint32(10)
	write := func(k int) {
		ver++
		id := int64(k%winBenchMembers + 1)
		from := types.Instant(k%winBenchMembers*100) + 7
		m.RecordRow("v", id, RowInfo{Version: ver, TxFrom: types.Instant(1000 + k), Span: Span{From: from, To: from + 30}}, true)
	}
	b.ReportAllocs()
	k := 0
	for b.Loop() {
		b.StopTimer()
		for len(w.late.pending) < foldAt(len(w.late.main)) {
			write(k)
			k++
		}
		b.StartTimer()
		write(k) // this one folds
		k++
	}
}

// BenchmarkRowScanIntoVsMerge: a build of 250,000 rows (every fifth with its
// replaced copy) while 1,000 writes were recorded live: RowScan.Into (outside
// the lock), the scan merged into the live sidecar (the lock's work when the
// sidecar has open views) and the live writes merged into the adopted scan
// (the lock's work otherwise), reported separately.
func BenchmarkRowScanIntoVsMerge(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		var s RowScan[int64]
		for i := 0; i < winBenchMembers; i++ {
			from := types.Instant(i * 100)
			id := int64(i + 1)
			if i%5 == 0 {
				s.Add(id, row(1, 10, 20, from, inf), false, []string{"v"})
				s.Add(id, row(2, 20, 0, from+10, from+50), true, []string{"v"})
				continue
			}
			s.Add(id, row(1, 10, 0, from, from+50), true, []string{"v"})
		}
		scratch := NewPropertyTxMembers[int64]()
		live := NewPropertyTxMembers[int64]()
		for i := 0; i < 1000; i++ { // writes recorded while the pass ran
			live.RecordRow("v", int64(i*7+1), row(3, 30, 0, 5, 9), true)
		}
		other := NewPropertyTxMembers[int64]()
		other.Merge(live)
		b.StartTimer()
		start := nanotime()
		s.Into(scratch)
		into := nanotime() - start
		start = nanotime()
		other.Merge(scratch) // the scan merged into the live sidecar
		merge := nanotime() - start
		start = nanotime()
		scratch.Merge(live) // the live writes merged into the adopted scan
		adopt := nanotime() - start
		b.ReportMetric(float64(into)/1e6, "into-ms")
		b.ReportMetric(float64(merge)/1e6, "merge-ms")
		b.ReportMetric(float64(adopt)/1e6, "adopt-ms")
	}
}

// BenchmarkPropertyTxMembersBytesWithViews: the sidecar's RAM per member for
// one value of 250,000 members, without and with its window view open.
func BenchmarkPropertyTxMembersBytesWithViews(b *testing.B) {
	for b.Loop() {
		heap := func() uint64 {
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			return ms.HeapAlloc
		}
		base := heap()
		m := NewPropertyTxMembers[int64]()
		for i := 0; i < winBenchMembers; i++ {
			from := types.Instant(i * 100)
			m.RecordRow("v", int64(i+1), row(1, 10, 0, from, from+50), true)
		}
		without := heap()
		m.OpenWindow("v")
		with := heap()
		b.ReportMetric(float64(without-base)/winBenchMembers, "B/member")
		b.ReportMetric(float64(with-base)/winBenchMembers, "B/member-views")
		runtime.KeepAlive(m)
	}
}

func nanotime() int64 { return time.Now().UnixNano() }
