package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"testing"
	"time"
)

const nativeEdgeColumns = "GEC1"

type nativeGraphEdgeUniqueColumnControl struct {
	Action string
	Body   []byte
}
type nativeGraphEdgeUniqueColumnReply struct {
	Body     []byte
	Creation *nativeColumnCreationFailure `json:",omitzero"`
	Error    string                       `json:",omitempty"`
}

func nativeGraphEdgeUniqueColumnInput(q nativeGraphEdgeUniqueColumnControl) ([]byte, error) {
	if q.Action != "columns" && q.Action != "owner" || len(q.Body) > 16<<10+len(nativeColumnsMarker) {
		return nil, errInvalid
	}
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	return append([]byte(nativeEdgeColumns), body...), nil
}
func nativeGraphEdgeUniqueColumnControlDecode(wire []byte) (q nativeGraphEdgeUniqueColumnControl, err error) {
	if len(wire) > 32<<10 || !bytes.HasPrefix(wire, []byte(nativeEdgeColumns)) {
		return q, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(wire[len(nativeEdgeColumns):]))
	d.DisallowUnknownFields()
	if err := d.Decode(&q); err != nil {
		return q, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return q, errors.Join(errInvalid, err)
	}
	canonical, err := nativeGraphEdgeUniqueColumnInput(q)
	if err != nil || !bytes.Equal(canonical, wire) {
		return q, errInvalid
	}
	return q, nil
}
func nativeGraphEdgeUniqueColumnServe(ctx context.Context, columns *nativeGraphCostColumnSessions, r processRequest) ([]byte, error) {
	q, err := nativeGraphEdgeUniqueColumnControlDecode(r.data)
	if err != nil {
		return nil, err
	}
	if r.index != 0 || r.entity != 0 {
		return nil, errInvalid
	}
	var reply nativeGraphEdgeUniqueColumnReply
	if q.Action == "owner" {
		reply.Body, err = columns.observeOpen(ctx, q.Body)
	} else {
		reply.Body, err = columns.serve(ctx, processRequest{data: q.Body})
	}
	if failure, ok := errors.AsType[*nativeColumnCreationFailure](err); ok {
		reply.Creation = failure
	}
	if err != nil {
		reply.Error = err.Error()
	}
	body, encodeErr := json.Marshal(reply)
	if len(body) > 1<<20 {
		return nil, errLimit
	}
	return body, errors.Join(err, encodeErr)
}
func nativeGraphEdgeUniqueColumnClose(r processRequest) bool {
	if r.op != processCostObserve {
		return false
	}
	control, err := nativeGraphEdgeUniqueColumnControlDecode(r.data)
	if err != nil || control.Action != "columns" {
		return false
	}
	q, err := nativeGraphCostColumnDecode(processRequest{data: control.Body})
	return err == nil && q.Action == nativeColumnsClose
}

type nativeGraphEdgeUniqueColumnClient struct {
	t        *testing.T
	ctx      context.Context
	p        *graphProcess
	member   uint64
	stage    string
	stream   *json.Encoder
	cookie   nativeGraphCostColumnCookie
	sequence uint64
	binding  nativeColumnBinding
	last     nativeGraphCostColumnReceipt
	failed   *uint64
}
type nativeGraphEdgeUniqueColumnTrace struct {
	Stage    string
	Member   uint64
	Input    nativeGraphCostColumnRequest
	Receipt  *nativeGraphCostColumnReceipt `json:",omitzero"`
	Creation *nativeColumnCreationFailure  `json:",omitzero"`
	Error    string                        `json:",omitempty"`
}

func (c *nativeGraphEdgeUniqueColumnClient) invoke(q nativeGraphEdgeUniqueColumnControl) (reply nativeGraphEdgeUniqueColumnReply, err error) {
	input, err := nativeGraphEdgeUniqueColumnInput(q)
	if err != nil {
		return reply, err
	}
	call, cancel := context.WithTimeout(c.ctx, 75*time.Second)
	response, err := nativeGraphCostExchange(call, c.p, processRequest{op: processCostObserve, data: input})
	cancel()
	if len(response.aux) != 0 {
		if decodeErr := json.Unmarshal(response.aux, &reply); decodeErr != nil {
			return reply, errors.Join(errCorrupt, decodeErr, err)
		}
	}
	return reply, err
}
func (c *nativeGraphEdgeUniqueColumnClient) call(q nativeGraphCostColumnRequest) nativeGraphCostColumnReceipt {
	c.t.Helper()
	seconds, err := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	if err != nil || seconds == 0 || seconds > 365*24*60*60 {
		c.t.Fatal("fresh fixed column budget", err)
	}
	if q.Action == nativeColumnsRecoverRetire || q.Action == nativeColumnsPinProbe {
		c.t.Fatal("separate native terminal response grammar")
	}
	if q.Action != nativeColumnsOpen {
		q.Cookie = c.cookie
		if q.Action != nativeColumnsClose && q.Action != nativeColumnsRetire {
			q.Sequence = c.sequence + 1
		}
	}
	for attempt := uint64(0); attempt < seconds/60+13; attempt++ {
		input, err := nativeGraphCostColumnInput(q)
		if err != nil {
			c.t.Fatal(err)
		}
		reply, err := c.invoke(nativeGraphEdgeUniqueColumnControl{Action: "columns", Body: input})
		if err == nil {
			r, e := nativeGraphCostColumnDecodeReceipt(reply.Body)
			if e != nil || r.Sequence != q.Sequence || q.Action != nativeColumnsOpen && (r.Cookie != c.cookie || !nativeColumnSame(r.Binding, c.binding)) {
				c.t.Fatal("same genuine column owner", e)
			}
			if c.last.Cookie == r.Cookie && r.Cookie.Owner != 0 {
				if r.DeadlineUnixNanos != c.last.DeadlineUnixNanos || !nativeColumnWorkMonotone(c.last.Charged, r.Charged) || !nativeColumnWorkMonotone(c.last.DurableWork, r.DurableWork) || r.RPCs < c.last.RPCs || r.RequestBytes < c.last.RequestBytes || r.ReplyBytes < c.last.ReplyBytes {
					c.t.Fatal("column deadline/work/meter reset")
				}
			}
			if e := c.stream.Encode(nativeGraphEdgeUniqueColumnTrace{Stage: c.stage, Member: c.member, Input: q, Receipt: &r}); e != nil {
				c.t.Fatal(e)
			}
			c.cookie, c.binding, c.last = r.Cookie, r.Binding, r
			if q.Action != nativeColumnsOpen && q.Action != nativeColumnsClose && q.Action != nativeColumnsRetire {
				c.sequence = q.Sequence
			}
			return r
		}
		if e := c.stream.Encode(nativeGraphEdgeUniqueColumnTrace{Stage: c.stage, Member: c.member, Input: q, Creation: reply.Creation, Error: err.Error()}); e != nil {
			c.t.Fatal(e)
		}
		if c.ctx.Err() != nil || errors.Is(err, errInvalid) || errors.Is(err, errCorrupt) || errors.Is(err, errGraphPinExpired) || errors.Is(err, errGraphPinReleased) || errors.Is(err, errGraphHistoryExpired) {
			c.t.Fatal("column unavailable; partial real evidence retained", err)
		}
		fullTTL := reply.Creation != nil && reply.Creation.FullTTL && reply.Creation.Expired && reply.Creation.DeadlineUnixNanos-reply.Creation.IssuedUnixNanos == int64(time.Minute)
		if !fullTTL {
			*c.failed++
			if *c.failed > 32+seconds/60+4 {
				c.t.Fatal("global failed column grant pool exhausted", err)
			}
		}
		if q.Action == nativeColumnsOpen {
			if reply.Creation == nil {
				observed, e := c.invoke(nativeGraphEdgeUniqueColumnControl{Action: "owner", Body: input})
				if e != nil || len(observed.Body) == 0 {
					c.t.Fatal("unknown constructor remains pending", errors.Join(err, e))
				}
				r, e := nativeGraphCostColumnDecodeReceipt(observed.Body)
				if e != nil {
					c.t.Fatal(e)
				}
				if e := c.stream.Encode(nativeGraphEdgeUniqueColumnTrace{Stage: c.stage, Member: c.member, Input: q, Receipt: &r}); e != nil {
					c.t.Fatal(e)
				}
				c.cookie, c.binding, c.last = r.Cookie, r.Binding, r
				return r
			}
			q.Create = false
		}
		// All other retries retain exact cookie/sequence/action. Genuine renewal
		// binds only the existing private cursor; caller positions grant nothing.
	}
	c.t.Fatal("fixed whole-query source starts exhausted")
	return nativeGraphCostColumnReceipt{}
}

func nativeGraphEdgeUniqueColumnClients(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, pair *nativeGraphCostColumnPair, stage string, create bool, failed *uint64, stream *json.Encoder) [2]*nativeGraphEdgeUniqueColumnClient {
	t.Helper()
	var clients [2]*nativeGraphEdgeUniqueColumnClient
	for group, member := range n.leaders {
		c := &nativeGraphEdgeUniqueColumnClient{t: t, ctx: ctx, p: n.all[member], member: member, stage: stage, stream: stream, failed: failed}
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, pair.Initial), Final: nativeGraphCostColumnCutInput(t, pair.Final), Generation: pair.ID, Create: create, RetainedExpires: 20})
		if r.Binding.Coverage != 1<<group || r.Binding.Version != 2 || r.Binding.Policy.Version != 1 || r.Binding.Policy.Admission == ([32]byte{}) || r.Retention == nil || r.Retention.State != nativeColumnPinLive || r.Retention.Generation != pair.ID || r.Retention.Pin == ([32]byte{}) {
			t.Fatal("actual complete native generation owner")
		}
		if pair.Bindings[group].Coverage != 0 && !nativeColumnSame(pair.Bindings[group], r.Binding) {
			t.Fatal("reopen substituted durable policy/original/cuts")
		}
		pair.Bindings[group], clients[group] = r.Binding, c
	}
	return clients
}
func nativeGraphEdgeUniqueColumnCloseClients(clients [2]*nativeGraphEdgeUniqueColumnClient) {
	for _, c := range clients {
		c.call(nativeGraphCostColumnRequest{Action: nativeColumnsClose})
	}
}

// A closed local journal is descriptive only. This separately admitted call
// opens the exact original GPT through the real logical keepers after Retire.
func (c *nativeGraphEdgeUniqueColumnClient) terminalProbe(id [16]byte, token []byte) {
	c.t.Helper()
	question, err := nativeGraphCostColumnInput(nativeGraphCostColumnRequest{Action: nativeColumnsPinProbe, Generation: id, RetainedToken: token})
	if err != nil {
		c.t.Fatal(err)
	}
	wire, err := nativeGraphEdgeUniqueColumnInput(nativeGraphEdgeUniqueColumnControl{Action: "columns", Body: question})
	if err != nil {
		c.t.Fatal(err)
	}
	response, err := nativeGraphCostExchange(c.ctx, c.p, processRequest{op: processCostObserve, data: wire})
	if !errors.Is(err, errGraphPinReleased) {
		c.t.Fatal("actual original keepers did not refuse retired token", err)
	}
	raw, encodeErr := encodeProcessResponse(response)
	if encodeErr != nil {
		c.t.Fatal(encodeErr)
	}
	if err := c.stream.Encode(struct {
		Stage           string
		Member          uint64
		Generation      [16]byte
		Token, Response []byte
	}{"actual-keeper-retired-token", c.member, id, token, raw}); err != nil {
		c.t.Fatal(err)
	}
}
func nativeGraphEdgeUniqueColumnPublish(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, spec nativeGraphCostSpec, initial, final graphCut, id [16]byte, stage string, updated bool, failed *uint64, stream *json.Encoder) nativeGraphCostColumnPair {
	t.Helper()
	pair := nativeGraphCostColumnPair{Initial: initial, Final: final, ID: id, Updated: updated}
	clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pair, stage, true, failed, stream)
	for group, c := range clients {
		for page := uint64(0); ; page++ {
			if page > c.binding.Policy.MaxChunks {
				t.Fatal("bounded actual publication")
			}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsPublish})
			if r.EOF {
				pair.Manifests[group] = r.Manifest
				break
			}
			if r.Manifest.Generation.ID != ([16]byte{}) {
				t.Fatal("partial publication invented seal")
			}
		}
	}
	if err := nativeColumnJoinBoundaries(pair.Bindings, pair.Manifests); err != nil {
		t.Fatal("both real publisher EOFs", err)
	}
	for _, mode := range []struct {
		name         string
		cached, fill bool
	}{{"native", false, false}, {"fill", false, true}, {"cache", true, false}} {
		for _, c := range clients {
			c.stage = stage + "/" + mode.name
		}
		nativeGraphEdgeUniqueColumnReadPair(t, clients, &pair, spec, final, mode.cached, mode.fill)
	}
	nativeGraphEdgeUniqueColumnCloseClients(clients)
	return pair
}
