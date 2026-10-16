package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeTransitionPinMarker = "GTP1"
const nativeTransitionPinProfile = "native-owned-pin-transition-v1"

// A private pending wrapper must not hide invalid/limit/corrupt or an unknown
// joined error. Only these genuine native/transport/cancellation leaves qualify
// the interrupted cell. The bounded walk owns no input-derived collection.
func nativeTransitionPinErrorClass(err error) string {
	if err == nil {
		return "applied"
	}
	for _, invalid := range []error{errInvalid, replica.ErrInvalid, raftlog.ErrInvalid, graphstore.ErrInvalid} {
		if errors.Is(err, invalid) {
			return "invalid"
		}
	}
	for _, limit := range []error{errLimit, replica.ErrLimit, raftlog.ErrLimit, graphstore.ErrResourceLimit, graphstate.ErrResourceLimit} {
		if errors.Is(err, limit) {
			return "limit"
		}
	}
	for _, corrupt := range []error{errCorrupt, raftlog.ErrCorrupt, graphstore.ErrCorrupt} {
		if errors.Is(err, corrupt) {
			return "corrupt"
		}
	}
	visited := 0
	var admitted func(error, int) bool
	admitted = func(value error, depth int) bool {
		if value == nil {
			return true
		}
		visited++
		if depth > 32 || visited > 64 {
			return false
		}
		if joined, ok := value.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) == 0 || len(children) > 64-visited {
				return false
			}
			for _, child := range children {
				if !admitted(child, depth+1) {
					return false
				}
			}
			return true
		}
		if inner := errors.Unwrap(value); inner != nil {
			return admitted(inner, depth+1)
		}
		return errors.Is(value, errGraphPinReleased) || errors.Is(value, errGraphPinExpired) || errors.Is(value, errRetainedPinPending) || errors.Is(value, replica.ErrUnavailable) || errors.Is(value, graphstore.ErrSourceUnavailable) || errors.Is(value, context.Canceled) || errors.Is(value, context.DeadlineExceeded) || errors.Is(value, errRemoteOperationFailure)
	}
	if admitted(err, 0) {
		if errors.Is(err, errGraphPinReleased) {
			return "released"
		}
		if errors.Is(err, errGraphPinExpired) {
			return "expired"
		}
		return "pending-or-unavailable"
	}
	return "unexpected-error"
}

// This finite diagnostic door retains the real publisher request in the child.
// Original is accepted only by the existing inert Resume API; no decoded GPM
// receives the private born stamp or bypasses native original/cut authentication.
type nativeTransitionPinControl struct {
	Version  uint8
	Cookie   [16]byte
	Sequence uint64
	Action   string
	Original []byte
	Token    []byte
}

type nativeTransitionPinRead struct {
	Cut      lifecycleRawCut
	Entities []nativeLifecycleEntity
	Facts    []certifiedProcessFact
}

type nativeTransitionPinReply struct {
	Version       uint8
	Cookie        [16]byte
	Sequence      uint64
	Action        string
	Original      []byte
	Token         []byte
	Actions       [8][16]byte
	Result        string
	Snapshot      *nativeTransitionPinRead
	AttemptedWork uint64
}

type nativeTransitionPinOwner struct {
	mu       sync.Mutex
	busy     bool
	sequence uint64
	resumes  uint8
	last     nativeTransitionPinControl
	reply    []byte
	err      error
	request  *retainedPinRequest
	original []byte
	token    retainedPinToken
}

func decodeNativeTransitionPinControl(wire []byte) (nativeTransitionPinControl, error) {
	var c nativeTransitionPinControl
	if !bytes.HasPrefix(wire, []byte(nativeTransitionPinMarker)) || len(wire) > 32<<10 {
		return c, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(wire[4:]))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, wire[4:]) || c.Version != 1 || c.Cookie == ([16]byte{}) || c.Sequence == 0 || c.Sequence > 16 || len(c.Original) > graphPinManifestBytes || len(c.Token) > graphPinManifestBytes+1024 {
		return c, errors.Join(errInvalid, err)
	}
	switch c.Action {
	case "begin", "issue", "publish", "feed", "release", "expire", "close", "epoch", "cancel", "pin-read", "fresh-read", "at-read":
		if len(c.Original)+len(c.Token) != 0 {
			return c, errInvalid
		}
	case "resume":
		if len(c.Original) == 0 {
			return c, errInvalid
		}
	default:
		return c, errInvalid
	}
	return c, nil
}

func nativeTransitionPinAction(m graphPinManifest, wire []byte, phase byte) [16]byte {
	if phase == pinPublish {
		return m.request
	}
	return pinTerminalAction(m, wire, phase)
}

func nativeTransitionPinSnapshot(ctx context.Context, host *allocationHost, read *certifiedGraphRead) (snapshot *nativeTransitionPinRead, err error) {
	defer func() {
		err = errors.Join(err, read.Close())
		if err != nil {
			snapshot = nil
		}
	}()
	return nativeTransitionPinProjection(ctx, host, read)
}

// Projection borrows the actual owner's read. The caller, never a decoded cut,
// controls its lifetime; the legacy consuming wrapper above still closes it.
func nativeTransitionPinProjection(ctx context.Context, host *allocationHost, read *certifiedGraphRead) (snapshot *nativeTransitionPinRead, err error) {
	return nativeTransitionPinProjectionFor(ctx, host, read, false)
}

func nativeTransitionPinProjectionFor(ctx context.Context, host *allocationHost, read *certifiedGraphRead, scalarNodes bool) (snapshot *nativeTransitionPinRead, err error) {
	if ctx == nil || host == nil || read == nil {
		return nil, errInvalid
	}
	cut, err := read.Cut()
	if err != nil {
		return nil, err
	}
	view, err := read.ReadView()
	if err != nil {
		return nil, err
	}
	if scalarNodes {
		// One complete projection may revisit the same immutable scalar while
		// assembling facts and the richer entity/property rows. Reuse only
		// within THIS invocation, charged to the actual held source's arena.
		arena, owners, count, err := nativeTransitionPinObservationSources(read)
		if err != nil {
			return nil, err
		}
		var finish func() error
		ctx, finish, err = nativeTransitionPinGetReuse(ctx, read, arena)
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, finish()) }()
		view, err = newNativeObservationRead(view, arena, host.machine.limits.graph.Planner.Component, owners[:count])
		if err != nil {
			return nil, err
		}
	}
	wire, err := lifecycleRawCertificate(cut)
	if err != nil {
		return nil, err
	}
	host.mu.Lock()
	cfg, err := host.currentConfiguration()
	host.mu.Unlock()
	if err != nil {
		return nil, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return nil, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return nil, err
	}
	snapshot = &nativeTransitionPinRead{Cut: wire}
	for id := uint64(1); id <= 32; id++ {
		fact, err := graphProcessSourceFact(ctx, host, view, id)
		if err != nil {
			return nil, err
		}
		snapshot.Facts = append(snapshot.Facts, fact)
		if id != 1 && id != 3 && id != 7 && id != 17 {
			continue
		}
		entity, err := view.Entity(ctx, graphstate.EntityID(id))
		if err != nil {
			return nil, err
		}
		e := nativeLifecycleEntity{ID: id, Found: entity.Found, Labels: []string{}}
		if entity.Found {
			e.Kind, e.Type, e.Mode, e.Source, e.Target = byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode), uint64(entity.Record.Source), uint64(entity.Record.Target)
			e.Effective, e.EffectiveLife, e.Labels = fact.Active, fact.Life, fact.Labels
			key := graphstate.ComponentKey{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}
			e.Presence, err = nativeLifecycleCellRead(ctx, view, key, scope, point, host.machine.limits.graph.Planner.Component)
			if err != nil {
				return nil, err
			}
			if id == 7 || scalarNodes && (id == 1 || id == 17) {
				key = graphstate.ComponentKey{Owner: graphstate.EntityID(id), Life: graphstate.LifeID(id + 1), Kind: graphstate.ScalarProperty, Name: "p"}
				e.Property, err = nativeLifecycleCellRead(ctx, view, key, scope, point, host.machine.limits.graph.Planner.Component)
				if err != nil {
					return nil, err
				}
			}
		}
		snapshot.Entities = append(snapshot.Entities, e)
	}
	return snapshot, ctx.Err()
}

// The token is private to this invocation; unrelated calls on the SAME source
// keep using ordinary Get. Setup and cleanup take no request/callback mutex
// across an RPC, and an ended token cannot be borrowed by a later observation.
func nativeTransitionPinGetReuse(ctx context.Context, read *certifiedGraphRead, arena *graphstate.OutputBudget) (context.Context, func() error, error) {
	var sources [16]*remoteGraphSource
	if ctx == nil || read == nil || arena == nil {
		return nil, nil, errInvalid
	}
	read.mu.Lock()
	if read.closed || len(read.sources) == 0 || len(read.sources) > len(sources) {
		read.mu.Unlock()
		return nil, nil, graphstore.ErrClosed
	}
	count := len(read.sources)
	for i, source := range read.sources {
		if wrapper, ok := source.(*retainedFeedSnapshotSource); ok {
			source = wrapper.certifiedGraphSource
		}
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote == nil {
			read.mu.Unlock()
			return nil, nil, errInvalid
		}
		sources[i] = remote
	}
	read.mu.Unlock()
	owner := new(graphSourceGetOwner)
	admitted := 0
	finish := func() error {
		owner.closed.Store(true)
		var err error
		for _, source := range sources[:admitted] {
			err = errors.Join(err, source.endGetReuse(owner))
		}
		return err
	}
	for _, source := range sources[:count] {
		if err := source.beginGetReuse(ctx, owner, arena); err != nil {
			return nil, nil, errors.Join(err, finish())
		}
		admitted++
	}
	return context.WithValue(ctx, graphSourceGetContextKey{}, owner), finish, nil
}

func nativeTransitionPinObservationSources(read *certifiedGraphRead) (arena *graphstate.OutputBudget, owners [16]context.Context, count int, err error) {
	if read == nil {
		return nil, owners, 0, errInvalid
	}
	read.mu.Lock()
	defer read.mu.Unlock()
	if read.closed {
		return nil, owners, 0, graphstore.ErrClosed
	}
	if len(read.sources) == 0 || len(read.sources) > len(owners) {
		return nil, owners, 0, errInvalid
	}
	for _, source := range read.sources {
		if wrapper, ok := source.(*retainedFeedSnapshotSource); ok {
			source = wrapper.certifiedGraphSource
		}
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote == nil || remote.ctx == nil || remote.endpoint == nil || remote.endpoint.budget == nil {
			return nil, owners, 0, errInvalid
		}
		budget := remote.endpoint.budget
		budget.mu.Lock()
		actual, ok := budget.output.(*graphstate.OutputBudget)
		budget.mu.Unlock()
		if !ok || actual == nil || arena != nil && actual != arena {
			return nil, owners, 0, errInvalid
		}
		arena, owners[count] = actual, remote.ctx
		count++
	}
	return arena, owners, count, nil
}

func (o *nativeLogicalTransitionOwner) servePin(caller context.Context, coordinator *crossProcessCoordinator, incarnation [16]byte, r processRequest) ([]byte, error) {
	if o == nil || caller == nil || coordinator == nil || coordinator.host != o.host || o.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || incarnation == ([16]byte{}) {
		return nil, errInvalid
	}
	c, err := decodeNativeTransitionPinControl(r.data)
	if err != nil || c.Cookie != o.cookie {
		return nil, errors.Join(errInvalid, err)
	}
	o.mu.Lock()
	if o.controls >= 64 || o.closed || o.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, replica.ErrUnavailable
	}
	o.controls++
	if err := o.reserveLocked(nativeLogicalTransitionFrameBytes); err != nil {
		o.mu.Unlock()
		return nil, err
	}
	if o.pin == nil {
		o.pin = &nativeTransitionPinOwner{}
	}
	p := o.pin
	o.mu.Unlock()
	p.mu.Lock()
	if p.busy {
		p.mu.Unlock()
		return nil, errRetainedPinPending
	}
	if c.Sequence == p.sequence && nativeColumnSame(c, p.last) {
		body, err := owned(p.reply), p.err
		p.mu.Unlock()
		return body, err
	}
	if c.Sequence != p.sequence+1 || c.Action != "begin" && c.Action != "resume" && p.request == nil || c.Action == "begin" && p.request != nil || c.Action == "resume" && (p.resumes >= 2 || len(p.original) != 0 && !bytes.Equal(p.original, c.Original)) {
		p.mu.Unlock()
		return nil, errInvalid
	}
	// Each method can construct at most one ordinary production invocation.
	// Fund its immutable source/output ceilings and the full32-ID snapshot before
	// any native read or allocation. Failure/unknown consumes the complete grant.
	o.mu.Lock()
	err = o.reserveLocked(o.pinGrant)
	o.mu.Unlock()
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.busy, p.sequence, p.last = true, c.Sequence, c
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	publisher, actual := newRetainedPinPublisher(coordinator)
	reply := nativeTransitionPinReply{Version: 1, Cookie: o.cookie, Sequence: c.Sequence, Action: c.Action, Result: "applied"}
	if actual == nil {
		switch c.Action {
		case "begin":
			id := requestID(incarnation)
			binary.BigEndian.PutUint64(id[8:], r.sequence)
			p.request, actual = publisher.Begin(ctx, id, graphReadRequest{mode: graphReadFresh}, 1)
			if actual == nil {
				p.original, actual = p.request.Original()
			}
		case "resume":
			p.resumes++
			p.request, actual = publisher.Resume(ctx, c.Original)
			if actual == nil {
				p.original, actual = p.request.Original()
				p.token = retainedPinToken{}
			}
			if actual == nil && len(c.Token) != 0 {
				p.token, actual = decodeRetainedPinToken(c.Token)
				if actual == nil && !bytes.Equal(p.token.manifest, p.original) {
					actual = errInvalid
					p.token = retainedPinToken{}
				}
			}
		case "issue":
			actual = p.request.Issue(ctx, publisher)
		case "publish":
			p.token, actual = p.request.Publish(ctx)
		case "feed":
			positions := make([]uint64, len(p.request.definition.parts))
			for i, part := range p.request.definition.parts {
				positions[i] = part.certificate.index + 1
			}
			actual = p.request.AdvanceFeed(ctx, positions, nativeTransitionPinAction(p.request.definition, p.original, pinAdvanceFeed))
		case "release":
			actual = p.request.Release(ctx)
		case "expire":
			actual = p.request.Expire(ctx)
		case "close":
			actual = p.request.Close(ctx)
		case "cancel":
			actual = p.request.CancelIssue(ctx)
		case "epoch":
			m := p.request.definition
			actual = publisher.AdvanceEpoch(ctx, m.issuer, nativeTransitionPinAction(m, p.original, pinAdvanceEpoch), m.epoch)
		case "pin-read", "fresh-read", "at-read":
			var read *certifiedGraphRead
			if c.Action == "pin-read" {
				read, actual = publisher.Open(ctx, p.token)
			} else {
				request := graphReadRequest{mode: graphReadFresh}
				if c.Action == "at-read" {
					request.mode = graphReadAt
					for _, part := range p.request.definition.parts {
						request.cut.certificates = append(request.cut.certificates, part.certificate)
					}
				}
				read, actual = coordinator.OpenCertified(ctx, request, nil)
			}
			if actual == nil {
				reply.Snapshot, actual = nativeTransitionPinSnapshot(ctx, o.host, read)
			}
		}
	}
	actual = errors.Join(actual, ctx.Err())
	reply.Original = p.original
	if p.request != nil {
		for phase := byte(1); phase <= 8; phase++ {
			reply.Actions[phase-1] = nativeTransitionPinAction(p.request.definition, p.original, phase)
		}
	}
	if len(p.token.publications) != 0 {
		reply.Token, err = encodeRetainedPinToken(p.token)
		actual = errors.Join(actual, err)
	}
	reply.Result = nativeTransitionPinErrorClass(actual)
	if actual != nil {
		reply.Snapshot = nil
	}
	o.mu.Lock()
	reply.AttemptedWork = o.work
	o.mu.Unlock()
	body, encodeErr := json.Marshal(reply)
	if len(body) > nativeLogicalTransitionFrameBytes {
		encodeErr = errors.Join(encodeErr, errLimit)
		body = nil
	}
	actual = errors.Join(actual, encodeErr)
	p.mu.Lock()
	p.reply, p.err, p.busy = body, actual, false
	p.mu.Unlock()
	return owned(body), actual
}

type nativeTransitionPinRecipe struct {
	Version   uint8
	Phase     byte
	Partition uint64
	Side      string
}

func (r nativeTransitionPinRecipe) valid() bool {
	return r.Version == 1 && r.Phase >= pinPublish && r.Phase <= pinCancelIssue && (r.Partition == 3 || r.Partition == 8) && (r.Phase != pinIssue || r.Partition == 3) && (r.Side == "before-submit" || r.Side == "after-applied")
}

type nativeTransitionPinCall struct {
	Member          uint64
	Input, Response []byte
}

type nativePinTransitionArtifact struct {
	Version                             uint8
	Profile                             string
	Recipe                              nativeTransitionPinRecipe
	Run                                 [16]byte
	DeadlineUnixNanos                   int64
	Gate                                nativeLogicalTransitionReceipt
	Owners                              []nativeLogicalTransitionReceipt
	Calls                               []nativeTransitionPinCall
	Original, Token                     []byte
	Business                            nativeGraphCostAttempt
	Before, Restored, Retained, Current nativeTransitionPinRead
	Processes                           []pairedFaultProcess
	From, Through                       [2]uint64
	Records                             []lifecycleRawRecord
}

// Replacement keeps the genuine native directories and exact constructor
// profile, then observes Restore before any campaign/catch-up. The old six
// transports are all reaped first; no endpoint or Host is synthesized in parent.
func nativeTransitionPinRestart(t *testing.T, f *pairedGraphFaultNetwork, spec nativeGraphCostSpec, killed pairedFaultProcess, beforeCampaign func()) []pairedFaultProcess {
	t.Helper()
	processes := []pairedFaultProcess{killed}
	prior := f.n.all
	for id := uint64(1); id <= 6; id++ {
		if id != killed.Member {
			processes = append(processes, f.kill(id, "pin-cohort"))
		}
	}
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	f.n.all = make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		f.n.all[id] = startGraphProcess(t, filepath.Join(f.n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_COST_SPEC="+string(wire))
		f.n.retired = append(f.n.retired, prior[id])
	}
	attachNativeColumnProcessNetwork(t, f.n, spec)
	f.mu.Lock()
	clear(f.down)
	f.mu.Unlock()
	f.installFilter()
	// This cleanup precedes the NEW hub's cleanup. The older network callback
	// later sees already closed children; it must not cancel their live transport.
	all := f.n.all
	t.Cleanup(func() {
		for _, p := range all {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, t.Failed())
			cancel()
			if err != nil && !t.Failed() {
				t.Error(err)
			}
		}
	})
	for id := uint64(1); id <= 6; id++ {
		p := f.n.all[id]
		state := p.call(t, processRequest{op: processStateOp})
		startup := p06ProcessWire(t, state)
		if p.incarnation == prior[id].incarnation || state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("native restored pin cohort identity/views")
		}
		raw, err := json.Marshal(map[string]any{"Member": id, "PID": p.cmd.Process.Pid, "PreviousInstance": pairedGraphFaultInstance(id, prior[id]), "Volume": filepath.Join(f.n.dir, fmt.Sprint(id)), "Ready": true, "ReadyKind": "native-restored-before-campaign", "RecoveryRaw": startup})
		if err != nil {
			t.Fatal(err)
		}
		processes = append(processes, pairedFaultProcess{EventID: f.event(), Action: "reopen", Member: id, Instance: pairedGraphFaultInstance(id, p), PreviousInstance: pairedGraphFaultInstance(id, prior[id]), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Ready: true, Raw: raw, RecoveryRaw: startup})
	}
	if beforeCampaign != nil {
		beforeCampaign()
	}
	for g, leader := range f.n.leaders {
		f.n.groups[g].event(t, leader, processRequest{op: processCampaign})
		f.n.read(t, leader, observeConfiguration, 0)
	}
	return processes
}

func nativeTransitionPinCase(t *testing.T, recipe nativeTransitionPinRecipe) string {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("finite actual pin transition")
	}
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw != "" {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != raw {
			t.Fatal("same run identity", err)
		}
		copy(run[:], wire)
	} else if _, err := rand.Read(run[:]); err != nil {
		t.Fatal(err)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero run")
	}
	deadline := time.Now().Add(3 * time.Minute).UnixNano()
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != raw || value <= time.Now().UnixNano() || value-time.Now().UnixNano() > int64(1800*time.Second) {
			t.Fatal("original pin deadline", err)
		}
		deadline = value
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	t.Setenv("RHO_GRAPH_TRANSITION_PROCESS", "1")
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	spec := nativeGraphCostSpec{Version: 1, Facts: 5, CrossPercent: 100, Writers: 1}
	n := newNativeColumnProcessNetwork(t, spec)
	f := &pairedGraphFaultNetwork{t: t, n: n, down: make(map[uint64]bool), intentionalKills: make(map[*graphProcess]bool)}
	f.installFilter()
	for phase := uint64(1); phase <= 3; phase++ {
		nativeColumnProcessSubmit(t, n, spec, phase, 0)
	}
	from := f.indexes()
	artifact := nativePinTransitionArtifact{Version: 1, Profile: nativeTransitionPinProfile, Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, From: from}
	var owner nativeLogicalTransitionReceipt
	var sequence uint64
	var original, token []byte
	reset := func() {
		owner = nativeLogicalTransitionCall(t, n.all[2], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
		sequence = 0
	}
	reset()
	call := func(action string) (nativeTransitionPinReply, processResponse, error) {
		sequence++
		input := nativeTransitionPinControl{Version: 1, Cookie: owner.Cookie, Sequence: sequence, Action: action}
		if action == "resume" {
			input.Original, input.Token = original, token
		}
		wire, err := json.Marshal(input)
		if err != nil {
			return nativeTransitionPinReply{}, processResponse{}, err
		}
		wire = append([]byte(nativeTransitionPinMarker), wire...)
		response, actual := n.all[2].exchange(ctx, processRequest{op: processNativeTransitionControl, data: wire})
		raw, encodeErr := encodeProcessResponse(response)
		if encodeErr != nil {
			return nativeTransitionPinReply{}, response, errors.Join(actual, encodeErr)
		}
		artifact.Calls = append(artifact.Calls, nativeTransitionPinCall{2, wire, raw})
		var decoded nativeTransitionPinReply
		if err := json.Unmarshal(response.aux, &decoded); err != nil || decoded.Cookie != owner.Cookie || decoded.Sequence != sequence || decoded.Action != action || decoded.Version != 1 {
			return decoded, response, errors.Join(errCorrupt, actual, err)
		}
		if len(original) != 0 && !bytes.Equal(original, decoded.Original) {
			return decoded, response, errInvalid
		}
		original = decoded.Original
		if len(decoded.Token) != 0 {
			token = decoded.Token
		}
		return decoded, response, errors.Join(actual, processResponseError(response))
	}
	must := func(action string) nativeTransitionPinReply {
		reply, _, err := call(action)
		if err != nil || reply.Result != "applied" {
			t.Fatal("actual pin method", action, err, reply.Result)
		}
		return reply
	}
	initial := must("begin")
	if recipe.Phase == pinIssue && recipe.Side == "before-submit" {
		first := artifact.Calls[len(artifact.Calls)-1]
		beforeReplay := f.indexes()
		replayed, err := n.all[2].exchange(ctx, processRequest{op: processNativeTransitionControl, data: first.Input})
		previous, decodeErr := decodeProcessResponseForProfile(first.Response, true)
		if errors.Join(err, decodeErr, processResponseError(replayed)) != nil || !bytes.Equal(replayed.aux, previous.aux) || f.indexes() != beforeReplay {
			t.Fatal("last genuine pin reply recaptured/replaced source", err, decodeErr)
		}
		artifact.Calls = append(artifact.Calls, nativeTransitionPinCall{2, first.Input, p06ProcessWire(t, replayed)})
		changed := nativeTransitionPinControl{Version: 1, Cookie: owner.Cookie, Sequence: sequence, Action: "publish"}
		wire, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		refused, actual := n.all[2].exchange(ctx, processRequest{op: processNativeTransitionControl, data: append([]byte(nativeTransitionPinMarker), wire...)})
		if !errors.Is(errors.Join(actual, processResponseError(refused)), errInvalid) || len(refused.aux) != 0 || f.indexes() != beforeReplay {
			t.Fatal("changed duplicate granted a new pin method", actual)
		}
	}
	before := must("at-read")
	if before.Snapshot == nil {
		t.Fatal("missing original cut source")
	}
	artifact.Before = *before.Snapshot
	if recipe.Phase != pinIssue {
		must("issue")
	}
	if recipe.Phase != pinPublish && recipe.Phase != pinIssue && recipe.Phase != pinCancelIssue {
		must("publish")
	}
	// A genuine later original creates edge7/p5. The manifest and its old cut
	// remain the earlier three-node graph; no expected row supplies source data.
	events := make(chan nativeGraphCostClientEvent, 8)
	if _, err := nativeGraphCostClientSubmit(ctx, n, spec, 4, 0, true, 0, nil, events); err != nil {
		t.Fatal("later genuine edge original", err)
	}
	close(events)
	attempts := 0
	for event := range events {
		if event.attempt == nil || event.attempt.Classification != "commit" {
			t.Fatal("quiet fixture business attempt refused")
		}
		artifact.Business = *event.attempt
		attempts++
	}
	if attempts != 1 {
		t.Fatal("quiet fixture requires one actual immutable original", attempts)
	}
	if len(token) != 0 {
		read := must("pin-read")
		if read.Snapshot == nil || !reflect.DeepEqual(read.Snapshot, before.Snapshot) {
			t.Fatal("portable pin read substituted later graph")
		}
	}
	if recipe.Phase == pinExpire {
		must("epoch")
	}
	member := uint64(1)
	if recipe.Partition == 8 {
		member = 4
	}
	selected := nativeLogicalTransitionCall(t, n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	arm := nativeLogicalTransitionControl{Version: 1, Action: "arm", Cookie: selected.Cookie, Sequence: 1, Family: "pin", Phase: recipe.Phase, Side: recipe.Side, Identity: initial.Actions[recipe.Phase-1], Partition: recipe.Partition}
	nativeLogicalTransitionCall(t, n.all[member], arm)
	actions := [...]string{"publish", "feed", "release", "expire", "close", "epoch", "issue", "cancel"}
	action := actions[recipe.Phase-1]
	expectedOriginal := owned(original)
	type answer struct {
		reply nativeTransitionPinReply
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		reply, response, err := call(action)
		if response.code != 3 && response.code != 6 {
			err = errors.Join(errInvalid, err)
			reply.Result = "unexpected-error"
		}
		done <- answer{reply, err}
	}()
	gate := nativeLogicalTransitionCall(t, n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "await", Cookie: selected.Cookie, Sequence: 1})
	command, err := decodeGraphPinCommand(gate.Proposal)
	m, manifestErr := decodeGraphPinManifest(expectedOriginal)
	if err != nil || manifestErr != nil || gate.State != "reached" || gate.Control != arm || command.phase != recipe.Phase || command.scope.partition != recipe.Partition || command.issuer != m.issuer || recipe.Phase != pinAdvanceEpoch && (command.id != m.id() || command.digest != sha256.Sum256(expectedOriginal)) {
		t.Fatal("actual original-bound pin transition", err, manifestErr)
	}
	killed := f.kill(member, "pin-source")
	select {
	case actual := <-done:
		if actual.err == nil || actual.reply.Result != "pending-or-unavailable" {
			t.Fatal("interrupted pin method invented terminal result", actual.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("interrupted original pin worker not reaped")
	}
	artifact.Gate = gate
	artifact.Owners = append(artifact.Owners, nativeLogicalTransitionCall(t, n.all[2], nativeLogicalTransitionControl{Version: 1, Action: "inspect"}))
	artifact.Processes = nativeTransitionPinRestart(t, f, spec, killed, func() {
		reset()
		must("resume")
		read := must("at-read")
		if read.Snapshot == nil || !reflect.DeepEqual(read.Snapshot, before.Snapshot) {
			t.Fatal("same-directory original cut changed before campaign/catch-up")
		}
		artifact.Restored = *read.Snapshot
		if recipe.Side == "after-applied" {
			record := n.all[member].call(t, processRequest{op: processEntryRecord, index: gate.Index})
			if !bytes.Equal(record.outcome, gate.Outcome) || !bytes.Equal(record.changes, gate.CDC) {
				t.Fatal("replacement local selected GPO record changed before campaign")
			}
		}
	})
	must(action)
	if recipe.Phase == pinIssue {
		must("publish")
	}
	if recipe.Phase == pinPublish || recipe.Phase == pinAdvanceFeed || recipe.Phase == pinIssue {
		if len(token) == 0 {
			must("publish")
		}
		read := must("pin-read")
		if read.Snapshot == nil || !reflect.DeepEqual(read.Snapshot, before.Snapshot) {
			t.Fatal("recovered original pin substituted current")
		}
		artifact.Retained = *read.Snapshot
		must("close")
	} else {
		// A token decoded after restart is inert. Actual keeper authentication,
		// not this control's cached bytes, must reject a native terminal token.
		if len(token) != 0 {
			refused, response, err := call("pin-read")
			want := "released"
			if recipe.Phase == pinExpire || recipe.Phase == pinAdvanceEpoch {
				want = "expired"
			}
			if err == nil || response.code != 3 && response.code != 6 || refused.Result != want || refused.Snapshot != nil {
				t.Fatal("terminal native original token reopened", err, refused.Result)
			}
		}
		read := must("at-read")
		if read.Snapshot == nil || !reflect.DeepEqual(read.Snapshot, before.Snapshot) {
			t.Fatal("ordinary retained old cut changed")
		}
		artifact.Retained = *read.Snapshot
	}
	// A new bounded Resume authenticates the SAME original before cancelling
	// creation intent. Close/Release's local closed flag cannot stand in for a
	// native CancelIssue fence or confer fresh source authority.
	must("resume")
	must("cancel")
	current := must("fresh-read")
	if current.Snapshot == nil {
		t.Fatal("missing actual current graph")
	}
	artifact.Current = *current.Snapshot
	artifact.Owners = append(artifact.Owners, nativeLogicalTransitionCall(t, n.all[2], nativeLogicalTransitionControl{Version: 1, Action: "inspect"}))
	artifact.Original, artifact.Token = original, token
	artifact.Through = f.indexes()
	artifact.Records = p06Records(f, from, artifact.Through)
	nativeTransitionPinBusinessCDC(t, artifact.Records, artifact.Business)
	if err := ctx.Err(); err != nil {
		t.Fatal("original whole pin deadline", err)
	}
	records := artifact.Records
	artifact.Records = nil
	return nativeTransitionWriteRows(t, filepath.Join(n.dir, "native-pin-transition.jsonl"), artifact, records, artifact.Through)
}

func nativeTransitionPinBusinessCDC(t *testing.T, records []lifecycleRawRecord, business nativeGraphCostAttempt) {
	t.Helper()
	participants := map[uint64]bool{}
	for _, record := range records {
		if !bytes.HasPrefix(record.Outcome, []byte{'G', 'C', 'S', 1}) {
			continue
		}
		status, err := decodeCrossStatus(record.Outcome)
		if err != nil || status.tx != business.Transaction || status.digest != business.DecisionDigest {
			t.Fatal("foreign business original in pin cell", err)
		}
		if status.phase != crossResolve || status.index != record.Index {
			continue
		}
		if participants[record.Partition] || !status.commit {
			t.Fatal("duplicate/noncommitted business Resolve")
		}
		participants[record.Partition] = true
		original := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		logical, err := decodeTypedChangeEnvelope(record.CDC, original, defaultMaterializerLimits(), true)
		if err != nil {
			t.Fatal("actual complete pin fixture business CDC", err)
		}
		if record.Partition == 8 {
			if len(logical.entities)+len(logical.lives)+len(logical.values)+len(logical.groups) != 0 {
				t.Fatal("validation-only participant invented effects")
			}
			continue
		}
		if record.Partition != 3 || len(logical.entities) != 1 || len(logical.lives) != 1 || len(logical.values) != 1 || len(logical.groups) != 2 || logical.entities[0].ID != 7 || logical.entities[0].Source != 1 || logical.entities[0].Target != 17 || logical.lives[0].Life != 8 || logical.lives[0].SourceLife != 2 || logical.lives[0].TargetLife != 18 || logical.values[0].ID != 5 {
			t.Fatal("literal whole-edge dictionary/life/group census")
		}
		value, ok := logical.values[0].Value.Int64Value()
		if !ok || value != 5 {
			t.Fatal("literal scalar5 CDC")
		}
		for i, group := range logical.groups {
			key, handle := graphstate.ComponentKey{Owner: 7, Kind: graphstate.Presence}, uint64(8)
			if i == 1 {
				key, handle = graphstate.ComponentKey{Owner: 7, Life: 8, Kind: graphstate.ScalarProperty, Name: "p"}, 5
			}
			if group.Key != key || len(group.Changes) != 1 || group.Changes[0].Before().Present() || !group.Changes[0].After().Present() || group.Changes[0].After().Value().ID() != handle || group.Changes[0].After().Revision().ID() != status.round || group.Changes[0].After().Revision().Provenance() != 4 {
				t.Fatal("literal complete original before/after cells")
			}
			p06CheckScope(t, group.Changes[0].Scope())
		}
	}
	if len(participants) != 2 {
		t.Fatal("missing whole-original participant CDC")
	}
}
