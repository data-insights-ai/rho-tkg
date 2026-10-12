package graphapply

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

func graphE18Prepare(t *testing.T, n *certifiedProcessNetwork, actor, kind, owner uint64, data []byte) allocationProposal {
	t.Helper()
	r := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: kind, index: owner, data: data})
	p, e := decodeCertifiedProcessProposal(r.aux)
	if e != nil || p.target.semantic != certifiedGraphSemanticContractID() {
		t.Fatal("genuine child proposal", p.target, e)
	}
	return p
}
func graphE18Open(t *testing.T, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	t.Helper()
	n.indexReads++
	if n.indexReads > n.indexReadPhases {
		t.Fatal("undeclared E18 certified query operation")
	}
	wire, e := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if e != nil {
		t.Fatal(e)
	}
	r := n.all[actor].call(t, processRequest{op: processSourceCertifiedOpen, data: wire})
	id, actual, e := decodeGraphE15ReadToken(r.aux)
	if e != nil || len(actual.certificates) != 2 {
		t.Fatal("actual certified source sessions", e)
	}
	return id, actual
}
func graphE18Observe(t *testing.T, n *certifiedProcessNetwork, actor, handle uint64, primary bool) map[string][]graphE18Row {
	t.Helper()
	out := make(map[string][]graphE18Row)
	for _, q := range []struct {
		key   string
		value uint64
	}{{"p9", 9}, {"p10", 10}, {"p11", 11}, {"p12", 12}, {"phantom", 999}} {
		if (q.value == 9 || q.value == 10 || q.value == 11) != primary {
			continue
		}
		r := graphE18Exchange(t, n.all[actor], processRequest{op: processCertifiedIndexObserve, index: handle, data: graphE18IndexInput(1, q.value)}, q.key)
		rows, e := decodeGraphE18Rows(r.aux)
		if e != nil {
			t.Fatal("unique index hydration", q.key, e)
		}
		out[q.key] = rows
	}
	if primary {
		return out
	}
	for _, q := range []struct {
		name        string
		owner, life uint64
	}{{"out-declared", 1, 2}, {"out-canonical", 1, 0}, {"in-declared", 17, 18}, {"in-canonical", 17, 0}} {
		r := graphE18Exchange(t, n.all[actor], processRequest{op: processCertifiedIndexObserve, index: handle, entity: q.owner, data: graphE18IndexInput(2, q.life)}, q.name)
		rows, e := decodeGraphE18Rows(r.aux)
		if e != nil {
			t.Fatal("incident index hydration", q.name, e)
		}
		out[q.name] = rows
	}
	return out
}
func graphE18Assert(t *testing.T, all map[string][]graphE18Row, old bool) {
	t.Helper()
	wantRaw := map[string][]uint64{"p9": {1}, "p11": {19}, "p12": {17}}
	if !old {
		wantRaw["p10"] = []uint64{1}
	}
	for key, rows := range all {
		var ids []uint64
		for _, r := range rows {
			ids = append(ids, r.owner)
		}
		want := wantRaw[key]
		if key == "out-declared" || key == "out-canonical" || key == "in-declared" || key == "in-canonical" {
			want = []uint64{3}
		}
		if !slices.Equal(ids, want) {
			t.Fatal("exact raw index candidates", key, ids, want)
		}
		for _, r := range rows {
			if r.revision == 0 || len(r.component) == 0 || r.life != r.owner+1 {
				t.Fatal("retained component/ref/life", key, r)
			}
			if r.owner == 3 {
				if r.source != 1 || r.target != 17 || r.sourceLife != 2 || r.targetLife != 18 || r.ref != 4 || r.active != old || r.effectiveLife != 4 || len(r.labels) != 0 {
					t.Fatal("exact historical endpoint/relationship hydration", key, r)
				}
				continue
			}
			value, ref, label, active := int64(11), uint64(21), "remote-control", true
			if r.owner == 17 {
				value, ref, active = 12, 22, old
			}
			if r.owner == 1 {
				value, ref, label = 9, 5, "old-label"
				if !old {
					value, ref, label = 10, 6, "new-label"
				}
			}
			if r.value != value || r.ref != ref || r.provenance != 31 || r.active != active {
				t.Fatal("exact retained typed value/reference/provenance/presence", key, r, value, ref, active)
			}
			labels := []string(nil)
			if active {
				labels = []string{label}
			}
			if !slices.Equal(r.labels, labels) || active && r.effectiveLife != r.life || !active && r.effectiveLife != 0 {
				t.Fatal("exact effective labels/life", key, r)
			}
		}
	}
	// Postings are a historical superset. Residual evaluation uses hydrated
	// values AND effective owner life at precisely the same certified view.
	for _, q := range []struct {
		key   string
		value int64
		want  []uint64
	}{{"p9", 9, nil}, {"p10", 10, nil}, {"p11", 11, []uint64{19}}, {"p12", 12, nil}, {"phantom", 999, nil}} {
		if old && q.value == 9 {
			q.want = []uint64{1}
		}
		if !old && q.value == 10 {
			q.want = []uint64{1}
		}
		if old && q.value == 12 {
			q.want = []uint64{17}
		}
		var ids []uint64
		for _, r := range all[q.key] {
			if r.active && r.effectiveLife == r.life && r.value == q.value {
				ids = append(ids, r.owner)
			}
		}
		if !slices.Equal(ids, q.want) {
			t.Fatal("exact effective index answer", q.key, ids, q.want)
		}
	}
	if len(all) != 9 {
		t.Fatal("selector omitted", len(all))
	}
}

func TestRemoteCertifiedGraphE18SixProcessesHistoricalIndexStartingHydration(t *testing.T) {
	n := newCertifiedProcessNetwork(t)
	n.indexReadPhases = 7
	n.action(t, 4, certifiedCreateNode, 19, nil)
	edgeDecision := graphE17Cross(t, n, 1, graphE18Prepare(t, n, 1, certifiedCreateCrossEdge, 3, nil))
	setupRounds := make(map[uint64]uint64)
	for _, q := range []struct{ actor, id, ref, value uint64 }{{1, 1, 5, 9}, {4, 17, 22, 12}, {4, 19, 21, 11}} {
		r := graphE17Cross(t, n, q.actor, graphE18Prepare(t, n, q.actor, certifiedE18Property, q.id, graphE17GrantInput(q.ref, q.value)))
		setupRounds[q.id] = r.decision.round
		graphE18ReplicatedTransaction(t, n, r)
	}
	// Observe actual Get/Scan traffic, without supplying data or authority.
	hub := n.all[1].mux.hub
	var mu sync.Mutex
	traffic := make(map[uint64][2]int)
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		q, e := decodeGraphSourceRequest(wire)
		if e != nil {
			return e
		}
		if q.kind == sourceGet || q.kind == sourceScan {
			mu.Lock()
			counts := traffic[partition]
			if q.kind == sourceGet {
				counts[0]++
			} else {
				counts[1]++
			}
			traffic[partition] = counts
			mu.Unlock()
		}
		return ctx.Err()
	}
	hub.mu.Unlock()
	held, oldCut := graphE18Open(t, n, 5, graphReadFresh, graphCut{})
	closed := false
	defer func() {
		if !closed {
			n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: held})
		}
	}()
	mu.Lock()
	openedTraffic := map[uint64][2]int{3: traffic[3], 8: traffic[8]}
	mu.Unlock()
	before := graphE18Observe(t, n, 5, held, true)
	mu.Lock()
	initial := map[uint64][2]int{3: {traffic[3][0] - openedTraffic[3][0], traffic[3][1] - openedTraffic[3][1]}, 8: {traffic[8][0] - openedTraffic[8][0], traffic[8][1] - openedTraffic[8][1]}}
	mu.Unlock()
	before = graphE18Remainder(t, n, 5, oldCut, before)
	graphE18Assert(t, before, true)
	for _, key := range []string{"p9", "p11", "p12"} {
		r := before[key][0]
		if r.revision != setupRounds[r.owner] {
			t.Fatal("original property revision", key, r.revision, setupRounds[r.owner])
		}
	}
	if initial[3][0] == 0 || initial[8][0] == 0 {
		t.Fatal("index and hydration did not use actual remote Get traffic", initial)
	}
	correction := graphE17Cross(t, n, 1, graphE18Prepare(t, n, 1, certifiedE18Correction, 0, nil))
	graphE18ReplicatedTransaction(t, n, correction)
	current, currentCut := graphE18Open(t, n, 2, graphReadFresh, graphCut{})
	now := graphE18Observe(t, n, 2, current, true)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: current})
	now = graphE18Remainder(t, n, 2, currentCut, now)
	graphE18Assert(t, now, false)
	if now["p10"][0].revision != correction.decision.round || now["p9"][0].revision != correction.decision.round || now["p10"][0].revision <= before["p9"][0].revision || bytes.Equal(now["p9"][0].component, before["p9"][0].component) {
		t.Fatal("correction did not move current retained component/revision")
	}
	if now["p12"][0].revision != before["p12"][0].revision || !bytes.Equal(now["p12"][0].component, before["p12"][0].component) {
		t.Fatal("closure changed retained remote property revision/value")
	}
	for _, key := range []string{"out-declared", "out-canonical", "in-declared", "in-canonical"} {
		if before[key][0].revision != edgeDecision.decision.round || before[key][0].provenance != 23 || now[key][0].revision != before[key][0].revision || !bytes.Equal(before[key][0].component, now[key][0].component) {
			t.Fatal("immutable relationship presence/ref/revision/provenance", key)
		}
	}
	if !reflect.DeepEqual(now["p11"], before["p11"]) {
		t.Fatal("unchanged remote owner/value was altered")
	}
	after := graphE18Observe(t, n, 5, held, true)
	after = graphE18Remainder(t, n, 5, oldCut, after)
	graphE18Assert(t, after, true)
	if !reflect.DeepEqual(after, before) {
		t.Fatal("held old index/hydration substituted current state")
	}
	n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: held})
	closed = true
	reopened, actualOld := graphE18Open(t, n, 2, graphReadAt, oldCut)
	restored := graphE18Observe(t, n, 2, reopened, true)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: reopened})
	restored = graphE18Remainder(t, n, 2, oldCut, restored)
	graphE18Assert(t, restored, true)
	if !reflect.DeepEqual(restored, before) || !reflect.DeepEqual(actualOld, oldCut) {
		t.Fatal("reopened At old indexes/components/valueIDs/revisions changed")
	}
	for _, cut := range []graphCut{oldCut, currentCut} {
		for _, cert := range cut.certificates {
			leader := uint64(1)
			if cert.scope.partition == 8 {
				leader = 4
			}
			for id, child := range n.group(leader).nodes {
				evidence, e := decodeGraphE15CutEvidence(child.call(t, processRequest{op: processSourceCutEvidence, entity: cert.round}).aux)
				actual, decodeErr := decodeGraphCertificate(evidence.certificate)
				if e != nil || decodeErr != nil || actual != cert {
					t.Fatal("all-voter persisted certificate", id, evidence, e)
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if e := drainGraphProcessMuxes(ctx, n.all); e != nil {
		t.Fatal(e)
	}
	for id, child := range n.all {
		r := child.call(t, processRequest{op: processStateOp})
		if r.stats[statViews] != 0 || r.stats[statPinnedBytes] != 0 || len(r.handles) != 2 || r.handles[0] != 0 || r.handles[1] != 0 {
			t.Fatal("six-process index query leaked sessions/views/pins", id, r.stats, r.handles)
		}
	}
	mu.Lock()
	t.Logf("actual E18 remote source Get/Scan counts before correction=%v final=%v", initial, traffic)
	mu.Unlock()
}

func TestRemoteCertifiedE18PrivateIndexCodecAndRefusals(t *testing.T) {
	row := graphE18Row{owner: 1, life: 2, effectiveLife: 2, active: true, ref: 5, revision: 7, provenance: 31, value: 9, labels: []string{"old-label"}, component: []byte{1, 2}}
	wire, e := encodeGraphE18Rows([]graphE18Row{row})
	if e != nil {
		t.Fatal(e)
	}
	out, e := decodeGraphE18Rows(wire)
	if e != nil || !reflect.DeepEqual(out, []graphE18Row{row}) {
		t.Fatal(out, e)
	}
	for i := range len(wire) {
		out, e := decodeGraphE18Rows(wire[:i])
		if !errors.Is(e, errCorrupt) || out != nil {
			t.Fatal("truncated bounded index observation", i, out, e)
		}
	}
	if out, e := encodeGraphE18Rows(make([]graphE18Row, 17)); !errors.Is(e, errLimit) || out != nil {
		t.Fatal(out, e)
	}
	if out, e := graphE18IndexObserve(t.Context(), nil, processRequest{}); !errors.Is(e, errInvalid) || out != nil {
		t.Fatal(out, e)
	}
	for _, q := range []processRequest{{index: 1, data: graphE18IndexInput(1, 9)}, {index: 1, data: []byte{1}}, {index: 1, data: graphE18IndexInput(3, 9)}} {
		if out, e := graphE18IndexObserve(t.Context(), &graphProcessCertifiedSessions{}, q); !errors.Is(e, errInvalid) || out != nil {
			t.Fatal(out, e)
		}
	}
}

func graphE18Exchange(t *testing.T, child *graphProcess, request processRequest, name string) processResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	r, err := child.exchange(ctx, request)
	if err != nil {
		t.Fatalf("index selector %s: %v mask=%x limit=%v corrupt=%v", name, errors.Join(err, errors.New(r.detail)), r.causeMask, errors.Is(err, errLimit), errors.Is(err, errCorrupt))
	}
	return r
}

// Each remainder is a new explicit At operation with a fresh original ledger,
// never a reset of the held reader. Both source sessions remain capped at two.
func graphE18Remainder(t *testing.T, n *certifiedProcessNetwork, actor uint64, cut graphCut, primary map[string][]graphE18Row) map[string][]graphE18Row {
	t.Helper()
	handle, actual := graphE18Open(t, n, actor, graphReadAt, cut)
	defer n.all[actor].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	if !reflect.DeepEqual(actual, cut) {
		t.Fatal("independent query changed cut")
	}
	for key, rows := range graphE18Observe(t, n, actor, handle, false) {
		primary[key] = rows
	}
	return primary
}

func graphE18ReplicatedTransaction(t *testing.T, n *certifiedProcessNetwork, result crossProcessResult) {
	t.Helper()
	for _, group := range n.groups {
		var baseline, original processResponse
		for id, child := range group.nodes {
			current := child.call(t, processRequest{op: processStateOp})
			record := child.call(t, processRequest{op: processEntryRecord, index: current.applied})
			status, err := decodeCrossStatus(record.outcome)
			if err != nil || status.phase != crossResolve || !status.commit || status.tx != result.decision.tx || status.round != result.decision.round || status.digest != result.decision.digest || len(record.changes) == 0 {
				t.Fatal("native cross resolve outcome/CDC", id, status, err)
			}
			if baseline.image == nil {
				baseline, original = current, record
			} else if baseline.applied != current.applied || !bytes.Equal(baseline.image, current.image) || !bytes.Equal(original.outcome, record.outcome) || !bytes.Equal(original.changes, record.changes) {
				t.Fatal("exact voter outcome/CDC/root parity", id)
			}
		}
	}
}
