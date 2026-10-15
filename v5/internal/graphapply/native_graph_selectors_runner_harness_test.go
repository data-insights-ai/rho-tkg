package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

type nativeSelectorsTrace struct {
	Stage                        string
	Member                       uint64
	Input                        nativeSelectorsInput
	Reply                        nativeSelectorsReply
	Error                        string `json:",omitempty"`
	Call                         uint64
	Attempt                      int
	AttemptNanos, OperationNanos int64
}
type nativeSelectorsClient struct {
	t         *testing.T
	ctx       context.Context
	n         *certifiedProcessNetwork
	stream    *json.Encoder
	stage     string
	next      uint64
	extra     int
	openNanos int64
	// Optional fresh-family observation only; no decision/source authority.
	observed func(nativeSelectorsInput, nativeSelectorsReply) error
}

type nativeSelectorsTiming struct {
	Type, Stage                                          string
	OpenNanos, ScanNanos, PointsNanos, ScopeRefusalNanos int64
	Rows                                                 uint64
}

func (c *nativeSelectorsClient) invoke(q nativeSelectorsInput, expected error) (reply nativeSelectorsReply) {
	c.t.Helper()
	c.next++
	q.Version = 1
	wire, err := nativeSelectorsInputWire(q)
	if err != nil {
		c.t.Fatal(err)
	}
	started := time.Now()
	for attempt := 1; attempt <= 8; attempt++ {
		if attempt > 1 {
			if c.extra >= 64 {
				c.t.Fatal("finite same-query exchange pool exhausted")
			}
			c.extra++
		}
		begin := time.Now()
		response, actual := nativeGraphCostExchange(c.ctx, c.n.all[2], processRequest{op: processCostObserve, data: wire})
		reply = nativeSelectorsReply{}
		if len(response.aux) != 0 {
			if err := nativeFeedDecodeJSON(response.aux, &reply); err != nil || reply.Version != 1 || reply.Action != q.Action || q.Handle != 0 && reply.Handle != q.Handle || q.Sequence != 0 && reply.Sequence != q.Sequence {
				c.t.Fatal("exact fresh query response identity", err)
			}
			if reply.Failure > 1 || reply.Failure != 0 && actual == nil {
				c.t.Fatal("scope classifier cannot manufacture native success")
			}
			if actual != nil && reply.Failure != 0 {
				actual = nativeSelectorsRemoteError{cause: actual, failure: reply.Failure}
			}
		}
		trace := nativeSelectorsTrace{Stage: c.stage, Member: 2, Input: q, Reply: reply, Call: c.next, Attempt: attempt, AttemptNanos: time.Since(begin).Nanoseconds(), OperationNanos: time.Since(started).Nanoseconds()}
		if actual != nil {
			trace.Error = actual.Error()
		}
		if err := c.stream.Encode(trace); err != nil {
			c.t.Fatal(err)
		}
		if expected != nil {
			if !errors.Is(actual, expected) || reply.Failure != 1 || len(reply.Points)+len(reply.Candidates)+len(reply.Token) != 0 {
				c.t.Fatal("scope refusal was absent, misclassified or became empty success", actual)
			}
			return reply
		}
		if actual == nil {
			if reply.Version != 1 || reply.Action != q.Action || reply.Failure != 0 {
				c.t.Fatal("missing completed native query reply")
			}
			if (q.Action == "index") != (reply.Index != nil) {
				c.t.Fatal("fresh index metadata cannot relabel an ordinary read")
			}
			if c.observed != nil {
				if err := c.observed(q, reply); err != nil {
					c.t.Fatal("actual completed observation refused", err)
				}
			}
			return reply
		}
		if c.ctx.Err() != nil || errors.Is(actual, errInvalid) || errors.Is(actual, errCorrupt) || errors.Is(actual, errGraphPinReleased) || errors.Is(actual, errGraphPinExpired) || errors.Is(actual, errGraphHistoryExpired) || !nativeGraphCostRetrySame(actual) {
			c.t.Fatal("native query refused/pending without invented answer", actual)
		}
	}
	c.t.Fatal("same query remains unresolved")
	return reply
}

func nativeSelectorsChildEnv(s nativeSelectorsSpec, a nativeSelectorsAdmission, reopen bool) ([]string, error) {
	cost, err := json.Marshal(s.cost())
	if err != nil {
		return nil, err
	}
	spec, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	transport, err := json.Marshal(a.Transport)
	if err != nil {
		return nil, err
	}
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_SELECTORS_PROCESS=1", "RHO_GRAPH_FEED_PROCESS=", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=", "RHO_GRAPH_RANGE_PROCESS=", "RHO_GRAPH_CONDITIONAL_PROCESS=", "RHO_GRAPH_RETENTION_PROCESS=", "RHO_GRAPH_NATIVE_FAULT_FAMILY=", "RHO_GRAPH_COST_SPEC=" + string(cost), "RHO_GRAPH_WORKLOAD_SPEC=" + string(spec), "RHO_GRAPH_WORKLOAD_RUN=" + a.Run, "RHO_GRAPH_WORKLOAD_SECONDS=" + strconv.FormatUint(a.BudgetSeconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE=" + strconv.FormatInt(a.DeadlineUnixNanos, 10), "RHO_GRAPH_WORKLOAD_TRANSPORT=" + string(transport)}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	}
	return env, nil
}
func nativeSelectorsAttach(t *testing.T, n *certifiedProcessNetwork, s nativeSelectorsSpec, a nativeSelectorsAdmission) {
	t.Helper()
	frames, err := nativeSelectorsFrames(s.cost())
	if err != nil || frames > (1<<41)/1024 || frames > uint64(^uint(0)>>1)/1024 {
		t.Fatal("finite actual native/source frame grants", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	t.Cleanup(cancel)
	transport, err := newNativeWorkloadTransport(ctx, a.Transport, frames*1024)
	if err != nil {
		t.Fatal(err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*1024), transport) // #nosec G115 -- checked native int ceiling; bounded queues unchanged.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}
func nativeSelectorsNetwork(t *testing.T, s nativeSelectorsSpec, a nativeSelectorsAdmission, stream *json.Encoder) *certifiedProcessNetwork {
	t.Helper()
	env, err := nativeSelectorsChildEnv(s, a, false)
	if err != nil {
		t.Fatal(err)
	}
	return nativeScalarWorkloadNetwork(t, s.cost(), env, "rho-native-selectors-", durableColumnGraphSemanticContractID(), stream, func(n *certifiedProcessNetwork) { nativeSelectorsAttach(t, n, s, a) })
}

type nativeSelectorsDriver struct {
	ctx        context.Context
	n          *certifiedProcessNetwork
	spec       nativeSelectorsSpec
	admissions *nativeGraphCostAdmissions
	started    time.Time
	mu         sync.Mutex
	extra      int
	originals  uint64
}

func (d *nativeSelectorsDriver) retry() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= 64 {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeSelectorsDriver) beginOriginal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.originals >= nativeSelectorsOriginals(d.spec.cost()) {
		return errLimit
	}
	d.originals++
	return nil
}
func (d *nativeSelectorsDriver) phase(t *testing.T, phase, from uint64, attempts, completed *json.Encoder, census *[32]uint64) nativeGraphCostSample {
	t.Helper()
	if nativeSelectorsValidate(d.spec.cost()) != nil {
		t.Fatal("unadmitted scalar family")
	}
	return nativeScalarWorkloadPhase(t, d.ctx, d.n, d.spec.cost(), d.admissions, d.started, phase, from, attempts, completed, census)
}

func nativeSelectorsSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, initial, cut graphCut, stage string, stream *json.Encoder) [2]uint64 {
	t.Helper()
	for _, cert := range cut.certificates {
		if cert.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("foreign actual source agreement")
		}
	}
	return nativeScalarWorkloadSources(t, ctx, n, initial, cut, stage, stream, func(after, through uint64, boundary []byte) ([]byte, error) {
		return nativeSelectorsInputWire(nativeSelectorsInput{Version: 1, Action: "records", From: after, Through: through, Cut: boundary})
	})
}

// Full native owners stop and reopen in the SAME six directories. No cut,
// token, derived manifest or journal substitutes for native restored state.
func nativeSelectorsRestart(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeSelectorsSpec, a nativeSelectorsAdmission, stream *json.Encoder) nativeWorkloadTransportReceipt {
	t.Helper()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	var before [6]processResponse
	for id := uint64(1); id <= 6; id++ {
		var err error
		before[id-1], err = nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
	}
	old := n.all[1].mux.hub
	if nativeGraphCostStop(t, n, 1, stream) != 6 {
		t.Fatal("owned process cohort not reaped")
	}
	receipt := old.workload.receipt(old)
	env, err := nativeSelectorsChildEnv(s, a, true)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, env...)
	}
	nativeSelectorsAttach(t, n, s, a)
	for id := uint64(1); id <= 6; id++ {
		restored, err := nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
		previous := before[id-1]
		if restored.incarnation == previous.incarnation || restored.applied != previous.applied || !bytes.Equal(restored.image, previous.image) || restored.stats[statViews] != 0 || restored.stats[statPinnedBytes] != 0 || restored.stats[statGeneration] != previous.stats[statGeneration] || restored.stats[statCommit] != previous.stats[statCommit] {
			t.Fatal("actual native restored prefix/bank/ownership changed", id)
		}
		b, err := encodeProcessResponse(previous)
		if err != nil {
			t.Fatal(err)
		}
		r, err := encodeProcessResponse(restored)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Encode(struct {
			Stage            string
			Member           uint64
			Before, Restored []byte
		}{"full-cohort-restore-before-election", id, b, r}); err != nil {
			t.Fatal(err)
		}
	}
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
	return receipt
}
