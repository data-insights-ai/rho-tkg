package badger

import (
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadSnapshotColdPresenceBudgetScope(t *testing.T) {
	for _, kind := range presenceKinds() {
		t.Run(kind.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for id := int64(100); id < 200; id++ {
				if err := kind.put(bs, id, 0); err != nil {
					t.Fatal(err)
				}
			}
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 3})
			s := bs.NewReadSnapshot().(*readSnapshot)
			defer s.Close()
			s.MeterReads(m.Tally("presence"))
			visited, stop := CountPendingKeysVisitedForTest()
			defer stop()
			var got bool
			var err error
			if kind.name == "node" {
				got, err = s.HasNodeHistory(100)
			} else {
				got, err = s.HasRelHistory(100)
			}
			stats := bs.HistoryPresenceStats()
			if err != nil || !got || visited() != 1 || stats.NodesBuilt || stats.RelsBuilt || m.Work().DecodedBytes != 0 || m.Work().HistoryExamined != 0 {
				t.Fatalf("presence %v, pending visits %d, stats %+v, work %+v, err %v; want one scoped key without whole-store build", got, visited(), stats, m.Work(), err)
			}
		})
	}
}

func TestReadSnapshotDeltaAnchorCopyAdmission(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true; c.HistoryAnchorInterval = 8 })
	for version := range uint32(2) {
		n := types.NewNode(100, 1, nil)
		n.SetVersion(version)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(version + 1), ValidFrom: 1})
		if err := n.SetProperty("payload", strings.Repeat("x", 8192)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNodeVersion(100, version, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	anchor, err := bs.readHistoryNodeRaw(100, 0)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := bs.readHistoryNodeRaw(100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if storepkg.HistoryValueKindOf(delta) != storepkg.HistoryDelta {
		t.Fatal("fixture is not a delta")
	}
	read := func(b storecontract.ReadBudget) (*types.Node, int64, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		s := bs.NewReadSnapshot().(*readSnapshot)
		defer s.Close()
		s.MeterReads(m.Tally("winner"))
		copied, stop := CountHistoryBytesCopiedForTest()
		defer stop()
		n, err := s.GetNodeVersion(100, 1)
		return n, copied(), err
	}
	n, copies, err := read(storecontract.ReadBudget{BufferBytes: uint64(len(delta) + len(anchor) - 1)})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || n != nil || copies != int64(len(delta)) {
		t.Fatalf("limited: node %v, native copied bytes %d, err %v; want refusal before anchor copy without fallback", n, copies, err)
	}
	n, copies, err = read(storecontract.ReadBudget{})
	if err != nil || n == nil || n.ID() != 100 || n.Version() != 1 || copies != int64(len(delta)+len(anchor)) {
		t.Fatalf("control: node %v, native copied bytes %d, err %v", n, copies, err)
	}
}

func TestReadSnapshotSkippedKeyAdmission(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "unlimited", true: "limited"}[limited], func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
			for id := types.NodeID(1); id <= 12; id++ {
				if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			budget := storecontract.ReadBudget{}
			if limited {
				budget.Candidates = 2
			}
			_, m := storecontract.WithReadBudget(t.Context(), budget)
			s := bs.NewReadSnapshot().(*readSnapshot)
			defer s.Close()
			s.MeterReads(m.Tally("walk"))
			steps, stop := CountIndexEntriesVisitedForTest()
			defer stop()
			n, err := s.LendNode(1)
			if err != nil || n.ID() != 1 {
				t.Fatalf("first: %v, %v", n, err)
			}
			n, err = s.LendNode(10)
			if limited {
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != "candidates" || n != nil || steps() != 2 {
					t.Fatalf("limited: row %v, native skip steps %d, err %v", n, steps(), err)
				}
			} else if err != nil || n.ID() != 10 || steps() != snapWalk {
				t.Fatalf("control: row %v, native skip steps %d, err %v", n, steps(), err)
			}
		})
	}
}

func TestReadSnapshotPendingHistoryAdmission(t *testing.T) {
	testtier.Slow(t)
	for _, limit := range []string{"history examined", "buffer bytes"} {
		t.Run(limit, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(12) {
				n := types.NewNode(100, 1, nil)
				n.SetVersion(v)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v + 1), ValidFrom: 1})
				if err := bs.PutNodeVersion(100, v, n); err != nil {
					t.Fatal(err)
				}
			}
			budget := storecontract.ReadBudget{HistoryExamined: 1}
			// Under one examination the first row is decoded, as a committed one
			// would be; a buffer refusal comes before the first decode.
			maxVisits, maxDecodes := int64(2), int64(1)
			if limit == "buffer bytes" {
				budget = storecontract.ReadBudget{BufferBytes: 1}
				maxVisits, maxDecodes = 1, 0
			}
			read := func(b storecontract.ReadBudget) ([]storecontract.VersionTemporalMeta, int64, int64, error) {
				_, m := storecontract.WithReadBudget(t.Context(), b)
				s := bs.NewReadSnapshot().(*readSnapshot)
				defer s.Close()
				s.MeterReads(m.Tally("meta"))
				visited, stop := CountPendingKeysVisitedForTest()
				defer stop()
				before := bs.HistoryMetaDecodesForTest()
				got, err := s.NodeHistoryTemporalMeta(100)
				return got, visited(), bs.HistoryMetaDecodesForTest() - before, err
			}
			got, visits, decodes, err := read(budget)
			var refusal *storecontract.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != limit || len(got) != 0 || visits > maxVisits || decodes > maxDecodes {
				t.Errorf("limited: metas %d, visits %d, decodes %d, err %v; want %s refusal before the next decode", len(got), visits, decodes, err, limit)
			}
			got, visits, decodes, err = read(storecontract.ReadBudget{})
			if err != nil || len(got) != 12 || visits != 12 || decodes != 12 {
				t.Fatalf("control: metas %d, visits %d, decodes %d, err %v", len(got), visits, decodes, err)
			}
			for i, m := range got {
				if m.Version != uint32(i) || m.Temporal.TxFrom != types.Instant(i+1) {
					t.Fatalf("meta %d = %+v", i, m)
				}
			}
		})
	}
}

func TestReadSnapshotCommittedHistoryBufferAdmission(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for v := range uint32(6) {
		n := types.NewNode(100, 1, nil)
		n.SetVersion(v)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(v + 1), ValidFrom: 1})
		if err := bs.PutNodeVersion(100, v, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	read := func(b storecontract.ReadBudget) ([]*types.Node, int64, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		s := bs.NewReadSnapshot().(*readSnapshot)
		defer s.Close()
		s.MeterReads(m.Tally("history"))
		before := bs.HistoryRowCopiesForTest()
		got, err := s.GetNodeHistory(100)
		return got, bs.HistoryRowCopiesForTest() - before, err
	}
	got, copies, err := read(storecontract.ReadBudget{BufferBytes: 1})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || len(got) != 0 || copies != 0 {
		t.Errorf("limited: rows %d, native copies %d, err %v", len(got), copies, err)
	}
	got, copies, err = read(storecontract.ReadBudget{})
	if err != nil || len(got) != 6 || copies != 6 {
		t.Fatalf("control: rows %d, native copies %d, err %v", len(got), copies, err)
	}
	for i, n := range got {
		if n.ID() != 100 || n.Version() != uint32(i) {
			t.Fatalf("row %d: id %d version %d", i, n.ID(), n.Version())
		}
	}
}
