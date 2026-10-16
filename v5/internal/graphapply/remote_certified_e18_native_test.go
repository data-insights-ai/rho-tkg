package graphapply

import (
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// This parent-process companion provides direct instrumented coverage through
// authenticated native services. The OS-process test remains the deployment
// witness; these genuine six native voters do not claim six OS processes.
func TestRemoteCertifiedGraphE18NativeIndexStartingHydrationAtSameCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	edge, claims := crossEdgeFixture(t, n)
	sourceGrant := claims[0].grant
	remote := n.read(1, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: [16]byte{2}}})
	targetGrant := grantReference{session: remote.observation.recipient.session, sequence: 1}
	create := graphstate.Operation{Kind: graphstate.CreateNode, Owner: 19, Life: 20, Scope: edge[0].Scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}
	proposal, e := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{create}, 23, []freshBinding{{role: entityBinding, id: 19, grant: targetGrant}, {role: lifeBinding, owner: 19, id: 20, grant: targetGrant}})
	if e != nil {
		t.Fatal(e)
	}
	n.submitPartitionGraph(1, proposal, reasonNone)
	tx, p := n.crossPlan(edge, claims, 24)
	n.crossResolve(n.crossCommit(tx, p))
	for _, q := range []struct {
		id, ref uint64
		value   int64
		grant   grantReference
	}{{1, 5, 9, sourceGrant}, {17, 22, 12, targetGrant}, {19, 21, 11, targetGrant}} {
		label := "remote-control"
		if q.id == 1 {
			label = "old-label"
		}
		ops := []graphstate.Operation{{Kind: graphstate.Set, Owner: graphstate.EntityID(q.id), Life: graphstate.LifeID(q.id + 1), Name: "p", Value: graphstate.I64(q.value), ValueID: graphstate.ValueID(q.ref), Scope: edge[0].Scope}, {Kind: graphstate.AddLabel, Owner: graphstate.EntityID(q.id), Life: graphstate.LifeID(q.id + 1), Name: label, Scope: edge[0].Scope}}
		tx, p = n.crossPlan(ops, []freshBinding{{role: valueBinding, id: q.ref, grant: q.grant}}, 31)
		n.crossResolve(n.crossCommit(tx, p))
	}
	c := remoteCertifiedFixture(t, n)
	old, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	oldCut, e := old.Cut()
	if e != nil {
		t.Fatal(e)
	}
	before := graphE18NativeQuery(t, c, old, true)
	graphE18NativeRemainder(t, c, oldCut, before)
	graphE18Assert(t, before, true)
	changes := []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(10), ValueID: 6, Scope: edge[0].Scope}, {Kind: graphstate.RemoveLabel, Owner: 1, Life: 2, Name: "old-label", Scope: edge[0].Scope}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "new-label", Scope: edge[0].Scope}, {Kind: graphstate.Correct, Owner: 17, Life: 18, Scope: edge[0].Scope, Present: false}}
	tx, p = n.crossPlan(changes, []freshBinding{{role: valueBinding, id: 6, grant: sourceGrant}}, 31)
	decision := n.crossCommit(tx, p)
	n.crossResolve(decision)
	current, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal(e)
	}
	currentCut, e := current.Cut()
	if e != nil {
		t.Fatal(e)
	}
	now := graphE18NativeQuery(t, c, current, true)
	if e := current.Close(); e != nil {
		t.Fatal(e)
	}
	graphE18NativeRemainder(t, c, currentCut, now)
	graphE18Assert(t, now, false)
	if now["p10"][0].revision != decision.status.round || !reflect.DeepEqual(now["p11"], before["p11"]) {
		t.Fatal("exact corrected revision and unchanged remote value")
	}
	held := graphE18NativeQuery(t, c, old, true)
	graphE18NativeRemainder(t, c, oldCut, held)
	graphE18Assert(t, held, true)
	if !reflect.DeepEqual(before, held) {
		t.Fatal("held index substituted current hydration")
	}
	if e := old.Close(); e != nil {
		t.Fatal(e)
	}
	at, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: oldCut}, nil)
	if e != nil {
		t.Fatal(e)
	}
	got := graphE18NativeQuery(t, c, at, true)
	if e := at.Close(); e != nil {
		t.Fatal(e)
	}
	graphE18NativeRemainder(t, c, oldCut, got)
	graphE18Assert(t, got, true)
	if !reflect.DeepEqual(got, before) {
		t.Fatal("reopened retained index/hydration differed")
	}
	for group := range 2 {
		usage, err := n.hosts[group][0].machine.store.ApplicationUsage()
		if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
			t.Fatal("native remote query owners leaked", usage)
		}
	}
}

func graphE18NativeQuery(t *testing.T, c *crossProcessCoordinator, read *certifiedGraphRead, primary bool) map[string][]graphE18Row {
	t.Helper()
	sessions := &graphProcessCertifiedSessions{coordinator: c, owners: map[uint64]graphProcessCertifiedOwner{1: {read: read}}}
	out := make(map[string][]graphE18Row)
	for _, q := range []struct {
		key          string
		kind         byte
		value, owner uint64
	}{{"p9", 1, 9, 0}, {"p10", 1, 10, 0}, {"p11", 1, 11, 0}, {"p12", 1, 12, 0}, {"phantom", 1, 999, 0}, {"out-declared", 2, 2, 1}, {"out-canonical", 2, 0, 1}, {"in-declared", 2, 18, 17}, {"in-canonical", 2, 0, 17}} {
		if (q.key == "p9" || q.key == "p10" || q.key == "p11") != primary {
			continue
		}
		wire, e := graphE18IndexObserve(t.Context(), sessions, processRequest{index: 1, entity: q.owner, data: graphE18IndexInput(q.kind, q.value)})
		if e != nil {
			t.Fatal("actual authenticated index query", q.key, e)
		}
		out[q.key], e = decodeGraphE18Rows(wire)
		if e != nil {
			t.Fatal(e)
		}
	}
	return out
}
func graphE18NativeRemainder(t *testing.T, c *crossProcessCoordinator, cut graphCut, rows map[string][]graphE18Row) {
	t.Helper()
	at, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer at.Close()
	for key, value := range graphE18NativeQuery(t, c, at, false) {
		rows[key] = value
	}
}
