package tiered

import (
	"context"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestDDLMeter_ColdOpensCharged (DDLMETER): a property index build on a store
// with closed cold shards charges each cold open to the caller's meter, so
// the store's cold opens equal the meter's count; a meter with room for one
// open refuses the build before any shard opens and the index is not made;
// a create of the existing index charges nothing.
func TestDDLMeter_ColdOpensCharged(t *testing.T) {
	ts, shards, _ := holdFixture(t, 2)
	coldOpens := func() (n int64) {
		for _, es := range shards {
			n += es.coldOpens.Load()
		}
		return n
	}
	_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	if err := ts.CreatePropertyIndexCharged(1, "k", storecontract.PropertyIndexOptions{}, m.Tally("CreateProperty")); err != nil {
		t.Fatal(err)
	}
	if got, counted := coldOpens(), int64(m.Work().ShardOpens); got != 2 || counted != got {
		t.Fatalf("cold opens %d, meter counted %d; want 2 and 2", got, counted)
	}
	for _, es := range shards {
		es.SetLastAccessForTest(0)
	}
	ts.CloseIdleShardsForTest()
	_, em := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	if err := ts.CreatePropertyIndexCharged(1, "k", storecontract.PropertyIndexOptions{}, em.Tally("CreateProperty")); !errors.Is(err, storecontract.ErrIndexExists) || em.Work().ShardOpens != 0 {
		t.Fatalf("existing index: err %v, %d opens charged; want ErrIndexExists and 0", err, em.Work().ShardOpens)
	}

	ts2, shards2, _ := holdFixture(t, 2)
	_, lm := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 1})
	err := ts2.CreatePropertyIndexCharged(1, "k", storecontract.PropertyIndexOptions{}, lm.Tally("CreateProperty"))
	var be *storecontract.ReadBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("limited meter: err %v, want a read budget refusal", err)
	}
	if n := shards2[0].coldOpens.Load() + shards2[1].coldOpens.Load(); n != 0 {
		t.Fatalf("limited meter: %d cold opens, want 0 (refused before the first open)", n)
	}
	if ok, herr := ts2.HasPropertyIndex(1, "k"); herr != nil || ok {
		t.Fatalf("refused build left the index (%v, %v)", ok, herr)
	}
}

// TestDDLMeter_RefusalIsAtomic (DDLMETER M1): with no open-handle cap a
// changed shard closes at its check-in; a meter with room for two of three
// cold opens refuses the build before any open, so no shard keeps the
// definition and later metered read-only opens of every shard succeed.
func TestDDLMeter_RefusalIsAtomic(t *testing.T) {
	ts, shards, _ := holdFixture(t, 3)
	ts.maxOpenColdShards = 0
	_, lm := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 2})
	err := ts.CreatePropertyIndexCharged(1, "k", storecontract.PropertyIndexOptions{}, lm.Tally("CreateProperty"))
	var be *storecontract.ReadBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("err %v, want a read budget refusal", err)
	}
	opened := int64(0)
	for _, es := range shards {
		opened += es.coldOpens.Load()
	}
	def := propertyIndexDef(false, 1, "k", storecontract.PropertyIndexOptions{}, false)
	for i, es := range shards {
		_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
		s, rel, rerr := es.checkoutForReadCharged(ts, true, m.Tally("read"))
		if rerr != nil {
			t.Fatalf("shard %d: later metered read-only open refused: %v", i, rerr)
		}
		has, herr := def.has(s)
		rel()
		if herr != nil || has {
			t.Fatalf("shard %d keeps the definition (%v, %v)", i, has, herr)
		}
	}
	if opened != 0 {
		t.Fatalf("refused build opened %d cold shards, want 0", opened)
	}
}
