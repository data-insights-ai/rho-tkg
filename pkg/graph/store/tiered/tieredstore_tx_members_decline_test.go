package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredMemberDeclineCostsNothing (R2 cumulative, Fable review 4): the
// membership of a value answers only when every shard carries the anchor's
// index; a shard that does not declines the read (store.ErrIndexNotFound) and
// the graph falls back to the history fold on the same meter. The shards that
// had already collected their members when the decline came charged them, and
// the fallback charged its own work again. Now the collection is staged: the
// charges of a fan-out are held back until every shard has answered and are
// added to the meter only then, so a decline leaves the meter exactly as it
// found it. Here the reference shard holds 3 members of the value, an event
// shard lost its copy of the index: the read declines with 0 counted, while the
// same read without the lost index answers the 3 members, counts them and
// charges its buffers. The staged limits still hold across the shards
// (TestTieredMemberCollectionCumulativeAcrossShards).
func TestTieredMemberDeclineCostsNothing(t *testing.T) {
	const members = 3
	ts := newTestTieredStore(t)
	caseTok, _, _ := installDefaultTestLabelRegistry(t, ts) // Case is a reference label: its members live on the reference shard
	if err := ts.CreatePropertyIndex(caseTok, "seat"); err != nil {
		t.Fatal(err)
	}
	gen := tieredNodeGen(t)
	for i := 0; i < members; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		_ = n.SetProperty("seat", int64(1))
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{types.IndexablePropertyValueKey(int64(1))}
	read := func(limits storecontract.ReadBudget) (int, *storecontract.ReadMeter, error) {
		_, m := storecontract.WithReadBudget(context.Background(), limits)
		handed := 0
		err := ts.ForEachNodePropertyTxMemberMetered(caseTok, "seat", keys, nil, m.Tally("d"), func(types.NodeID, types.Instant) bool { handed++; return true })
		return handed, m, err
	}

	// The control: every shard carries the index, the read answers and is counted.
	if handed, m, err := read(storecontract.ReadBudget{Candidates: 1 << 20}); err != nil || handed != members || m.Work().Candidates != members || m.Work().BufferBytes == 0 {
		t.Fatalf("control: %d members, %v, work %+v; want the %d members counted and their buffers charged", handed, err, m.Work(), members)
	}

	// An event shard loses its copy of the index: the read declines and costs nothing.
	lost := ""
	for name, bs := range allShardStoresForTest(t, ts) {
		if name != "reference" && name != "archive" {
			if err := bs.DropPropertyIndex(caseTok, "seat"); err != nil {
				t.Fatal(err)
			}
			lost = name
		}
	}
	if lost == "" {
		t.Fatal("fixture: no event shard")
	}
	handed, m, err := read(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	if !errors.Is(err, storecontract.ErrIndexNotFound) || handed != 0 || m.Work() != (storecontract.ReadWork{}) {
		t.Fatalf("decline (shard %s without the index): %d members, %v, work %+v; want ErrIndexNotFound, no member and nothing counted", lost, handed, err, m.Work())
	}
}

// TestTieredMemberFanoutChargesItsBuffers (R2 cumulative, Fable review 6): the
// buffers a membership fan-out holds are charged as BufferBytes before they
// grow: each shard's own list (the sidecar's snapshot slot of 16 bytes, or on
// disk the fold's 24-byte entry and the list's 16), the member each shard keeps
// in the slice it returns (16) and the entity in the union (32). 9 members over
// 3 shards: a RAM index costs 16 + 16 + 32 = 64 bytes a member, a disk index
// 24 + 16 + 16 + 32 = 88. The limit at that count answers the 9 members; one
// byte under it refuses with the typed error and hands none on.
func TestTieredMemberFanoutChargesItsBuffers(t *testing.T) {
	const perShard, shards = 3, 3
	const total = perShard * shards
	value := types.IndexablePropertyValueKey(int64(1))
	for _, c := range []struct {
		name      string
		disk      bool
		perMember uint64
	}{{"ram", false, 16 + 16 + 32}, {"disk", true, 24 + 16 + 16 + 32}} {
		t.Run(c.name, func(t *testing.T) {
			ts := newCumulativeTieredStore(t, c.disk)
			installDefaultTestLabelRegistry(t, ts)
			read := spreadMembers(t, ts, "node", perShard, shards)
			budget := func(limit uint64) (*storecontract.ReadMeter, *storecontract.ReadTally) {
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{BufferBytes: limit})
				return m, m.Tally("d")
			}
			want := total * c.perMember
			members := 0
			m, tally := budget(1 << 20)
			if err := read(value, nil, tally, func() { members++ }); err != nil || members != total || m.Work().BufferBytes != want {
				t.Fatalf("control: %d members, %v, %d buffer bytes; want the %d members and exactly %d bytes", members, err, m.Work().BufferBytes, total, want)
			}
			members = 0
			_, tally = budget(want)
			if err := read(value, nil, tally, func() { members++ }); err != nil || members != total {
				t.Fatalf("BufferBytes %d: %d members, %v; want the %d members", want, members, err, total)
			}
			members = 0
			m, tally = budget(want - 1)
			err := read(value, nil, tally, func() { members++ })
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || members != 0 {
				t.Fatalf("BufferBytes %d: %d members, %v, work %+v; want the buffer bytes refusal and no member handed on", want-1, members, err, m.Work())
			}
		})
	}
}
