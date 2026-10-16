package graphapply

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// Canonical recipes retain original bytes, never source authority. The complete
// saved roster is disk-owned, <=512KiB per fact and <=Facts*512KiB total, while
// each actual caller window owns at most32 requests/results in RAM.
type nativeGraphEdgeUniqueSaved struct {
	Fact, Winner               uint64
	FrozenProposal, FrozenPlan []byte
}

type nativeGraphEdgeUniquePointTrace struct {
	Stage    string
	At       []byte
	Winner   uint64
	Observed nativeGraphEdgeUniqueObservation
}

// Historical scalar cells remain physically present after their exact life
// stops supporting them. Effective projection and the complete unique index
// candidate set are checked together; a raw candidate is never called live.
func nativeGraphEdgeUniquePoints(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, file *os.File, cut graphCut, updated bool, stage string, stream *json.Encoder) {
	t.Helper()
	var batch []nativeGraphEdgeUniqueSaved
	flush := func() {
		if len(batch) == 0 {
			return
		}
		nativeGraphEdgeUniqueObserveAt(t, n, cut, batch[0].Fact, uint64(len(batch)), func(observed nativeGraphEdgeUniqueObservation) error {
			saved := batch[observed.Fact-batch[0].Fact]
			winner := saved.Winner
			if updated {
				winner = 1 - winner
			}
			if !observed.Source.Found || !observed.OldTarget.Found || observed.NewTarget.Found != updated || !observed.ValueFound || observed.ValueID != 4*spec.Facts+8+observed.Fact || len(observed.Claims) < 1 || len(observed.Claims) > 2 {
				return errCorrupt
			}
			seen := [2]bool{}
			for _, claim := range observed.Claims {
				id := uint64(claim.Owner)
				if id < 7+4*observed.Fact || id > 9+4*observed.Fact || (id-7)%2 != 0 {
					return errCorrupt
				}
				candidate := (id - (7 + 4*observed.Fact)) / 2
				if seen[candidate] || uint64(claim.Life) != id+1 || claim.Key != (graphstate.ComponentKey{Owner: claim.Owner, Life: claim.Life, Kind: graphstate.ScalarProperty, Name: "p"}) {
					return errCorrupt
				}
				seen[candidate] = true
			}
			if !seen[winner] {
				return errCorrupt
			}
			for candidate, x := range observed.Entities {
				id := 7 + 4*observed.Fact + 2*uint64(candidate)
				found := updated || uint64(candidate) == saved.Winner
				if x.Entity.ID != id || x.Entity.Found != found || x.Entity.Effective != (uint64(candidate) == winner) {
					return errCorrupt
				}
				if !found {
					if x.Entity.Presence.Present || x.Scalar.Present || x.Life.Found {
						return errCorrupt
					}
					continue
				}
				target, life := uint64(3), uint64(4)
				if spec.cross(observed.Fact) {
					target, life = spec.remoteNode(), spec.remoteNode()+1
				}
				if uint64(candidate) != saved.Winner {
					life = 4*spec.Facts + 7
					if spec.cross(observed.Fact) {
						life = spec.remoteNode() + 2
					}
				}
				if !x.Life.Found || x.Life.SourceLife != 2 || x.Life.TargetLife != life || x.Entity.Kind != byte(graphstate.Relationship) || x.Entity.Type != "edge" || x.Entity.Mode != byte(graphstate.LifeBound) || x.Entity.Source != 1 || x.Entity.Target != target || !x.Scalar.Present || x.Scalar.Null || x.Scalar.Kind != "i64" || x.Scalar.I64 != int64(observed.Fact+5) || x.Scalar.Handle != 4*spec.Facts+8+observed.Fact {
					return errCorrupt
				} // #nosec G115 -- admitted fact<1M.
				present := !updated || uint64(candidate) == winner
				if x.Entity.Presence.Present != present || present && (x.Entity.Presence.Handle != id+1 || x.Entity.EffectiveLife != id+1) || !present && x.Entity.Presence.Handle != 0 {
					return errCorrupt
				}
			}
			return stream.Encode(nativeGraphEdgeUniquePointTrace{stage, nativeGraphCostColumnCutInput(t, cut), saved.Winner, observed})
		})
		batch = batch[:0]
	}
	if err := nativeGraphEdgeUniqueWalkSaved(file, spec.Facts, func(saved nativeGraphEdgeUniqueSaved) error {
		batch = append(batch, saved)
		if len(batch) == 16 {
			flush()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	flush()
}

type nativeGraphEdgeUniqueSourceTrace struct {
	Stage  string
	Member uint64
	Page   nativeGraphEdgeUniqueRecordPage
}

func nativeGraphEdgeUniqueSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, initial, final graphCut, stage string, stream *json.Encoder) [2]uint64 {
	t.Helper()
	var rows [2]uint64
	for group, certificate := range final.certificates {
		first := initial.certificates[group]
		if first.scope != certificate.scope || first.sourceIndex > certificate.sourceIndex {
			t.Fatal("same exact native retained interval")
		}
		wire, err := encodeGraphCertificate(certificate)
		if err != nil {
			t.Fatal(err)
		}
		for after := first.sourceIndex; after < certificate.sourceIndex; {
			body, err := json.Marshal(nativeGraphEdgeUniqueRecordRequest{wire, after})
			if err != nil {
				t.Fatal(err)
			}
			var canonical nativeGraphEdgeUniqueRecordPage
			for member := uint64(group*3 + 1); member <= uint64(group*3+3); member++ {
				response, err := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processCostObserve, data: append([]byte(nativeEdgeRecords), body...)})
				if err != nil {
					t.Fatal("complete authentic voter prefix", err)
				}
				var page nativeGraphEdgeUniqueRecordPage
				if json.Unmarshal(response.aux, &page) != nil || !bytes.Equal(page.Certificate, wire) || page.After != after || page.Next <= after || page.Next > certificate.sourceIndex || page.Complete != (page.Next == certificate.sourceIndex) || len(page.Frames) != int(page.Next-after) {
					t.Fatal("source page omission/foreign cut")
				}
				if member == uint64(group*3+1) {
					canonical = page
				} else if !reflect.DeepEqual(canonical, page) {
					t.Fatal("actual native voters disagree")
				}
				if err := stream.Encode(nativeGraphEdgeUniqueSourceTrace{stage, member, page}); err != nil {
					t.Fatal(err)
				}
			}
			for index, frame := range canonical.Frames {
				if frame.Partition != certificate.scope.partition || frame.Index != after+uint64(index)+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("missing raw retained frames")
				}
				rows[group] += uint64(len(frame.Rows))
			}
			after = canonical.Next
		}
	}
	return rows
}

func nativeGraphEdgeUniqueRestart(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, deadline time.Time, seconds uint64, stream, states *json.Encoder) nativeWorkloadTransportReceipt {
	t.Helper()
	nativeGraphCostDrain(t, n)
	var before [6]processResponse
	for id := uint64(1); id <= 6; id++ {
		before[id-1] = n.all[id].call(t, processRequest{op: processStateOp})
		wire, err := encodeProcessResponse(before[id-1])
		if err != nil || states.Encode(struct {
			Stage  string
			Member uint64
			Raw    []byte
		}{"before-stop", id, wire}) != nil {
			t.Fatal(err)
		}
	}
	oldHub := n.all[1].mux.hub
	if nativeGraphCostStop(t, n, 1, stream) != 6 {
		t.Fatal("original six child cohort not reaped")
	}
	var receipt nativeWorkloadTransportReceipt
	if oldHub.workload != nil {
		receipt = oldHub.workload.receipt(oldHub)
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_COST_SPEC="+string(encoded), "RHO_GRAPH_WORKLOAD_RUN="+os.Getenv("RHO_GRAPH_WORKLOAD_RUN"), "RHO_GRAPH_WORKLOAD_SECONDS="+strconv.FormatUint(seconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE="+strconv.FormatInt(deadline.UnixNano(), 10))
	}
	attachNativeGraphEdgeUniqueNetwork(t, n, spec, deadline)
	for id := uint64(1); id <= 6; id++ {
		actual, old := n.all[id].call(t, processRequest{op: processStateOp}), before[id-1]
		if actual.incarnation == old.incarnation || actual.applied != old.applied || !bytes.Equal(actual.image, old.image) || actual.stats[statViews] != 0 || actual.stats[statPinnedBytes] != 0 || actual.stats[statGeneration] != old.stats[statGeneration] || actual.stats[statCommit] != old.stats[statCommit] {
			t.Fatal("real same-directory restart changed durable prefix", id)
		}
		wire, err := encodeProcessResponse(actual)
		if err != nil || states.Encode(struct {
			Stage  string
			Member uint64
			Raw    []byte
		}{"restored-before-election", id, wire}) != nil {
			t.Fatal(err)
		}
	}
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
	return receipt
}

func nativeGraphEdgeUniqueReplay(t *testing.T, d *nativeGraphEdgeUniqueDriver, original nativeGraphEdgeUniqueAttempt, stream *json.Encoder) {
	t.Helper()
	if original.Classification != "commit" || original.Operation.Phase != "close" || len(original.Proposal) == 0 {
		t.Fatal("actual saved original close required")
	}
	target, err := decodeAllocationScope(original.Target)
	if err != nil {
		t.Fatal(err)
	}
	var before [2]processResponse
	for group, leader := range d.n.leaders {
		before[group] = d.n.all[leader].call(t, processRequest{op: processStateOp})
	}
	a, err := d.submit(nativeGraphEdgeUniqueJob{Operation: original.Operation, Proposal: allocationProposal{target: target, wire: original.Proposal}, Attempt: original.Ordinal}, original.Client)
	if err != nil || a.Decision != original.Decision || a.Request != original.Request || !bytes.Equal(a.Proposal, original.Proposal) {
		t.Fatal("same original retry changed decision/body", err)
	}
	for group, leader := range d.n.leaders {
		after := d.n.all[leader].call(t, processRequest{op: processStateOp})
		if !bytes.Equal(before[group].image, after.image) {
			t.Fatal("duplicate old incumbent changed graph image")
		}
		for index := before[group].applied + 1; index <= after.applied; index++ {
			for _, peer := range d.n.groups[group].nodes {
				r := peer.call(t, processRequest{op: processEntryRecord, index: index})
				if r.changes == nil || len(r.changes) != 0 {
					t.Fatal("duplicate original emitted new CDC")
				}
			}
		}
	}
	if err := stream.Encode(struct{ Original, Replay nativeGraphEdgeUniqueAttempt }{original, a}); err != nil {
		t.Fatal(err)
	}
}

func nativeGraphEdgeUniqueGenerationID(stage string, admission [32]byte) [16]byte {
	sum := sha256.Sum256(append(append([]byte("edge-unique-generation:"+stage+":"), admission[:]...), 0))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

func nativeGraphEdgeUniqueWalkSaved(file *os.File, facts uint64, consume func(nativeGraphEdgeUniqueSaved) error) error {
	if file == nil || facts < 1 || facts > 1_000_000 || consume == nil {
		return errInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 512<<10)
	var next, used uint64
	for scanner.Scan() {
		line := scanner.Bytes()
		used += uint64(len(line) + 1)
		if next >= facts || used > facts*(512<<10) {
			return errLimit
		}
		var saved nativeGraphEdgeUniqueSaved
		if err := json.Unmarshal(line, &saved); err != nil {
			return err
		}
		canonical, err := json.Marshal(saved)
		if err != nil || !bytes.Equal(canonical, line) || saved.Fact != next || saved.Winner > 1 || len(saved.FrozenPlan) > nativeEdgeRecipeBytes {
			return errInvalid
		}
		if err := consume(saved); err != nil {
			return err
		}
		next++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if next != facts {
		return errCorrupt
	}
	return nil
}

func nativeGraphEdgeUniqueOperation(spec nativeGraphCostSpec, phase string, fact, candidate uint64) nativeGraphEdgeUniqueCommand {
	q := nativeGraphEdgeUniqueCommand{Version: 1, Family: nativeGraphEdgeUniqueFamily, Phase: phase, Fact: fact, Candidate: candidate}
	switch phase {
	case "setup":
		q.Ordinal = 1 + fact
	case "claim":
		q.Ordinal = 4 + 2*fact + candidate
	case "stale":
		q.Ordinal = 4 + 2*spec.Facts + fact
	case "replace":
		q.Ordinal = 4 + 3*spec.Facts + fact
		q.Reopened = true
	case "frozen-close":
		q.Ordinal = 4 + 4*spec.Facts + fact
	case "close":
		q.Ordinal = 4 + 5*spec.Facts + fact
	case "life":
		q.Ordinal = 4 + 6*spec.Facts + fact
		q.Reopened = fact%2 == 1
	}
	return q
}

func nativeGraphEdgeUniqueObserveAt(t *testing.T, n *certifiedProcessNetwork, cut graphCut, first, count uint64, emit func(nativeGraphEdgeUniqueObservation) error) {
	t.Helper()
	if count < 1 || count > 16 {
		t.Fatal("finite native observation window")
	}
	handle, actual := nativeColumnProcessOpen(t, n, graphReadAt, cut)
	if !reflect.DeepEqual(actual, cut) {
		t.Fatal("native At substituted cut")
	}
	defer n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	for fact := first; fact < first+count; fact++ {
		wire := binary.BigEndian.AppendUint64([]byte(nativeEdgeObserve), fact)
		r := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, data: wire})
		var observed nativeGraphEdgeUniqueObservation
		if err := json.Unmarshal(r.aux, &observed); err != nil || observed.Fact != fact || !observed.Complete || observed.PhantomFound {
			t.Fatal("actual unique/life observer", err)
		}
		if err := emit(observed); err != nil {
			t.Fatal(err)
		}
	}
}

func nativeGraphEdgeUniqueFresh(t *testing.T, n *certifiedProcessNetwork) graphCut {
	t.Helper()
	handle, cut := nativeColumnProcessOpen(t, n, graphReadFresh, graphCut{})
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	return cut
}

// Both actual empty unique-footprint plans are captured before ANY candidate
// in the bounded wave registers. After real terminal ABORTs, quiet priority may
// retry one candidate; no forced serialization precedes the genuine wave.
func nativeGraphEdgeUniqueClaims(t *testing.T, d *nativeGraphEdgeUniqueDriver, winners *os.File, emit func(nativeGraphEdgeUniqueAttempt) error) {
	t.Helper()
	writer := json.NewEncoder(winners)
	for first := uint64(0); first < d.spec.Facts; first += 16 {
		count := min(uint64(16), d.spec.Facts-first)
		jobs := make([]nativeGraphEdgeUniqueJob, 0, 2*count)
		for fact := first; fact < first+count; fact++ {
			for candidate := uint64(0); candidate < 2; candidate++ {
				q := nativeGraphEdgeUniqueOperation(d.spec, "claim", fact, candidate)
				p, err := d.factory(d.ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := d.freeze(d.ctx, p)
				if err != nil {
					t.Fatal(err)
				}
				jobs = append(jobs, nativeGraphEdgeUniqueJob{Operation: q, Proposal: p, Plan: plan, Attempt: 1})
			}
		}
		attempts, err := d.window(jobs, emit)
		if err != nil {
			t.Fatal(err)
		}
		for offset := uint64(0); offset < count; offset++ {
			pair := attempts[2*offset : 2*offset+2]
			commits := 0
			for _, a := range pair {
				if a.Classification == "commit" {
					commits++
				} else if a.Classification != "abort" {
					t.Fatal("frozen unique contender has no actual decision", a.Classification)
				}
			}
			if commits > 1 {
				t.Fatal("same scalar acquired by two effective lives")
			}
			if commits == 0 {
				if _, err := d.priority(jobs[2*offset], pair[0], emit); err != nil {
					t.Fatal("settled genuine unique wave failed priority", err)
				}
			}
		}
		cut := nativeGraphEdgeUniqueFresh(t, d.n)
		nativeGraphEdgeUniqueObserveAt(t, d.n, cut, first, count, func(observed nativeGraphEdgeUniqueObservation) error {
			winner := uint64(2)
			for candidate, entity := range observed.Entities {
				if entity.Entity.Effective {
					if winner != 2 {
						return errCorrupt
					}
					winner = uint64(candidate)
				}
			}
			if winner > 1 || len(observed.Claims) != 1 || !observed.ValueFound || observed.ValueID != 4*d.spec.Facts+8+observed.Fact {
				return errCorrupt
			}
			active := observed.Entities[winner]
			if observed.Claims[0].Owner != graphstate.EntityID(active.Entity.ID) || active.Entity.EffectiveLife != active.Entity.ID+1 || !active.Scalar.Present || active.Scalar.I64 != int64(observed.Fact+5) {
				return errCorrupt
			} // #nosec G115 -- fact<1M.
			return writer.Encode(nativeGraphEdgeUniqueSaved{Fact: observed.Fact, Winner: winner})
		})
	}
	if err := winners.Sync(); err != nil {
		t.Fatal(err)
	}
}

func nativeGraphEdgeUniqueFreezeOld(t *testing.T, d *nativeGraphEdgeUniqueDriver, winners, frozen *os.File, emit func(nativeGraphEdgeUniqueAttempt) error) {
	t.Helper()
	writer := json.NewEncoder(frozen)
	err := nativeGraphEdgeUniqueWalkSaved(winners, d.spec.Facts, func(saved nativeGraphEdgeUniqueSaved) error {
		// A new loser intent while the winning exact endpoint lives are live
		// must encounter REAL native uniqueness, independently of coarse-root NO.
		q := nativeGraphEdgeUniqueOperation(d.spec, "claim", saved.Fact, 1-saved.Winner)
		attempt, e := d.submit(nativeGraphEdgeUniqueJob{Operation: q, Attempt: 1, Role: "occupied"}, int(saved.Fact%uint64(d.spec.Writers)))
		if emitErr := emit(attempt); e != nil || emitErr != nil {
			return errors.Join(e, emitErr)
		}
		if attempt.Classification != "pre-prepare-unique" || len(attempt.Refusal) == 0 {
			return errCorrupt
		}
		q = nativeGraphEdgeUniqueOperation(d.spec, "frozen-close", saved.Fact, saved.Winner)
		proposal, e := d.factory(d.ctx, q)
		if e != nil {
			return e
		}
		saved.FrozenPlan, e = d.freeze(d.ctx, proposal)
		if e != nil {
			return e
		}
		saved.FrozenProposal, e = encodeCertifiedProcessProposal(proposal)
		if e != nil {
			return e
		}
		body, e := json.Marshal(saved)
		if e != nil || len(body)+1 > 512<<10 {
			return errors.Join(errLimit, e)
		}
		return writer.Encode(saved)
	})
	if err != nil {
		t.Fatal("genuine pre-ABA plan roster", err)
	}
	if err := frozen.Sync(); err != nil {
		t.Fatal(err)
	}
}

func nativeGraphEdgeUniquePhase(t *testing.T, d *nativeGraphEdgeUniqueDriver, file *os.File, phase string, emit func(nativeGraphEdgeUniqueAttempt) error) {
	t.Helper()
	jobs := make([]nativeGraphEdgeUniqueJob, 0, 32)
	flush := func() error {
		if len(jobs) == 0 {
			return nil
		}
		attempts, err := d.window(jobs, emit)
		if err != nil {
			return err
		}
		for index, a := range attempts {
			switch phase {
			case "stale":
				if a.Classification != "pre-prepare-endpoint-life" && a.Classification != "abort" {
					return errCorrupt
				}
			case "frozen-close":
				if a.Classification != "abort" {
					return errCorrupt
				}
			default:
				if a.Classification == "abort" {
					if _, err := d.priority(jobs[index], a, emit); err != nil {
						return err
					}
				} else if a.Classification != "commit" {
					return errCorrupt
				}
			}
		}
		jobs = jobs[:0]
		return nil
	}
	err := nativeGraphEdgeUniqueWalkSaved(file, d.spec.Facts, func(saved nativeGraphEdgeUniqueSaved) error {
		candidate := saved.Winner
		if phase == "stale" || phase == "replace" {
			candidate = 1 - candidate
		}
		job := nativeGraphEdgeUniqueJob{Operation: nativeGraphEdgeUniqueOperation(d.spec, phase, saved.Fact, candidate), Attempt: 1}
		if phase == "frozen-close" {
			var e error
			job.Proposal, e = decodeCertifiedProcessProposal(saved.FrozenProposal)
			if e != nil {
				return e
			}
			job.Plan = saved.FrozenPlan
		}
		jobs = append(jobs, job)
		if len(jobs) == 32 {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		t.Fatal("native effective-unique phase", phase, err)
	}
}
