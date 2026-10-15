package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Explicit caller grants are persisted in the native catalog policy before
// workload mutation. No matrix-sized quota is silently substituted for core1.
type graphTransferGrant struct {
	MaxRecords, MaxControlBytes, MaxGrants, MaxBlock uint64
	Census                                           graphOwnerCensusPolicy
}

func (g graphTransferGrant) policy(p graphTransportPolicy) (graphOwnerTransferPolicy, error) {
	c := graphTransportConfig(p.Spec, hex.EncodeToString(p.Run[:]))
	policy := graphOwnerTransferPolicy{Version: 2, Config: c, Run: p.Run, BudgetSeconds: p.BudgetSeconds, AdmittedUnixNanos: p.AdmittedUnixNanos, DeadlineUnixNanos: p.DeadlineUnixNanos,
		MaxRecords: g.MaxRecords, MaxControlBytes: g.MaxControlBytes, MaxProposalBytes: 32 << 10, MaxGrants: g.MaxGrants, MaxBlock: g.MaxBlock, MaxAttempts: 64, MaxSubmissions: 8, MaxCallbacks: 16, CleanupMillis: 30000, MaxCleanupCalls: 8, Admission: new(p), Census: new(g.Census)}
	if p.Spec.Family != graphOwnerTransferFamily || g.MaxBlock < 8 || g.MaxBlock%2 != 0 {
		return policy, errInvalid
	}
	if err := policy.validate(); err != nil {
		return policy, err
	}
	minimum := 33 + 21*p.Spec.Facts
	pages := (minimum+uint64(g.Census.PageRows)-1)/uint64(g.Census.PageRows) + 2
	rows, deliveredBytes := censusPageAllowance(g.Census)
	if g.Census.MaxCalls < pages || g.Census.MaxWorkRows < pages*rows || g.Census.MaxWorkBytes < pages*deliveredBytes || g.Census.MaxBytes < minimum*uint64(2*len(c.Graph)+6) {
		return policy, errInvalid
	}
	return policy, nil // Necessary useful-window funding, not worst-case ABORT/capacity proof.
}
func graphTransferEnvironment() (graphFeedWorkloadSpec, graphTransportPolicy, graphOwnerTransferPolicy, error) {
	var zeroSpec graphFeedWorkloadSpec
	var zeroTransport graphTransportPolicy
	var zeroPolicy graphOwnerTransferPolicy
	admission, err := graphTransportFromEnv()
	if err != nil || admission == nil || admission.Spec.Family != graphOwnerTransferFamily {
		return zeroSpec, zeroTransport, zeroPolicy, errors.Join(errInvalid, err)
	}
	raw := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(raw) == 0 || len(raw) > 1024 {
		return zeroSpec, zeroTransport, zeroPolicy, errInvalid
	}
	spec, err := decode[graphFeedWorkloadSpec](raw)
	if err != nil || spec != admission.Spec || spec.Version != 2 || hex.EncodeToString(admission.Run[:]) != os.Getenv("RHO_GRAPH_WORKLOAD_RUN") {
		return zeroSpec, zeroTransport, zeroPolicy, errors.Join(errMismatch, err)
	}
	body := []byte(os.Getenv("RHO_FDB_TRANSFER_GRANT"))
	if len(body) == 0 || len(body) > 16384 {
		return zeroSpec, zeroTransport, zeroPolicy, errInvalid
	}
	grant, err := decode[graphTransferGrant](body)
	if err != nil {
		return zeroSpec, zeroTransport, zeroPolicy, err
	}
	policy, err := grant.policy(*admission)
	return spec, *admission, policy, err
}

type graphTransferBlock struct {
	Receipt graphOwnerAllocationReceipt
	Session graphOwnerAllocationSession
}
type graphTransferCleanup struct {
	Owner   uint8
	Grant   string
	Secret  [32]byte
	Session *graphOwnerAllocationSession
}
type graphTransferDriver struct {
	ctx            context.Context
	h              *harness
	root           *os.Root
	catalog        *graphOwnerCatalog
	source         *graphHistoryRetention
	feed           *graphFeedStore
	a              *graphTransferArtifacts
	authorities    [2]graphOwnerAuthority
	processes      [2]*graphTransferProcess
	blocks         []graphTransferBlock // <=64 genuine native blocks, never per-ID state.
	nodes          [3]endpoint
	values         [2]graphOwnerAllocatedID
	cleanupClients [2]*graphOwnerClient
	cleanups       []graphTransferCleanup // Caller-chosen originals recorded BEFORE native acquisition.
}

func (d *graphTransferDriver) rpc(owner uint8, call graphTransferCall) (graphTransferReply, error) {
	if owner > 1 || d.processes[owner] == nil {
		return graphTransferReply{}, errInvalid
	}
	pending, beginErr := d.processes[owner].begin(call)
	if pending == nil {
		return graphTransferReply{}, beginErr
	}
	r, waitErr := d.processes[owner].await(pending)
	err := errors.Join(beginErr, waitErr)
	writeErr := d.a.event(struct {
		Type, Owner string
		Call        graphTransferCall
		Reply       graphTransferReply
	}{"rpc", d.authorities[owner].ID, pending.Call, r})
	return r, errors.Join(err, writeErr)
}
func (d *graphTransferDriver) claims(command graphCommand, metadata lifecycleMetadata) ([]graphOwnerAllocatedID, error) {
	ids, err := ownerCommandIDs(transaction{GraphChange: new(command), Lifecycle: new(metadata)})
	if err != nil {
		return nil, err
	}
	claims := make([]graphOwnerAllocatedID, 0, len(ids))
	for _, id := range ids {
		found := false
		for _, block := range d.blocks {
			g := block.Receipt.Grant
			if id >= g.First && id <= g.Last {
				claims = append(claims, graphOwnerAllocatedID{ID: id, Offset: id - g.First, Grant: g.Request, Session: block.Session.Request})
				found = true
				break
			}
		}
		if !found {
			return nil, errUnavailable
		}
	}
	// Range provenance alone never claims new issuance. Every create below uses
	// actual child Cursor.Next; later native Prepare joins actual first-use rows.
	return claims, nil
}
func (d *graphTransferDriver) operation(owner uint8, phase string, fact uint64, client int, previous *graphTransferAttempt) (graphTransferAttempt, error) {
	begin := time.Now()
	p := d.h.config.Cost
	base := p
	base.Version = 1
	op := graphCostOperationID(base, phase, fact)
	command := graphCommand{Kind: "node-create"}
	metadata := lifecycleMetadata{Provenance: op.Ordinal}
	if phase == "setup" {
		command.Node = d.nodes[fact]
	} else {
		value := int64(5)
		kind := "edge-create"
		definition := d.values[0]
		if phase == "update" {
			value, kind, definition = 6, "edge-update", d.values[1]
		}
		command = graphCommand{Kind: kind, Edge: p.edge(fact, value)}
		metadata.EdgeLife = command.Edge.ID + 1
		metadata.Definition = &lifecycleDefinition{ID: definition.ID, Kind: "i64", I64: value}
	}
	claims, err := d.claims(command, metadata)
	if err != nil {
		return graphTransferAttempt{}, err
	}
	first := 1
	prior := ""
	if previous != nil {
		if previous.Classification != "aborted" || previous.Proof.Outcome.Commit || previous.Operation != op {
			return graphTransferAttempt{}, errMismatch
		}
		first = previous.AttemptOrdinal + 1
		prior = previous.Proof.Outcome.Request
	}
	for number := first; number <= 64; number++ {
		name := fmt.Sprintf("transfer-%s-%d-c%d-a%d", phase, fact, client, number)
		prepared, err := d.rpc(owner, graphTransferCall{Client: client, Method: "prepare", Prepare: &graphTransferPrepare{name, command, metadata, claims}})
		if err != nil || prepared.Intent == nil {
			return graphTransferAttempt{}, errors.Join(errUnavailable, err)
		}
		attempt := graphTransferAttempt{Operation: op, ClientID: client, AttemptOrdinal: number, PreviousRequest: prior, Intent: *prepared.Intent, Prepare: prepared}
		// Native intent was already committed; persist its exact body before the
		// first Submit can become unknown or a process can lose a reply.
		if err = d.a.event(struct {
			Type    string
			Attempt graphTransferAttempt
		}{"prepared", attempt}); err != nil {
			return attempt, err
		}
		for invocation := 1; invocation <= 8; invocation++ {
			r, submitErr := d.rpc(owner, graphTransferCall{Client: client, Method: "submit", Intent: &attempt.Intent})
			attempt.Submissions = append(attempt.Submissions, r)
			if submitErr != nil {
				if invocation < 8 && (r.Retryable || r.MaybeCommitted || errors.Is(submitErr, errOwnerBusy) || errors.Is(submitErr, errLimit)) {
					continue
				}
				attempt.Classification = "pending"
				attempt.Nanos = time.Since(begin).Nanoseconds()
				return attempt, errors.Join(submitErr, d.a.attempt(attempt))
			}
			if r.Proof == nil || r.Outcome == nil || r.Proof.Outcome != *r.Outcome {
				return attempt, errMismatch
			}
			attempt.Proof = *r.Proof
			attempt.Classification = "aborted"
			if r.Outcome.Commit {
				attempt.Classification = "committed"
			}
			attempt.Nanos = time.Since(begin).Nanoseconds()
			if err = d.a.attempt(attempt); err != nil {
				return attempt, err
			}
			if r.Outcome.Commit {
				return attempt, nil
			}
			if !graphCostParallelReplan(*r.Outcome) {
				return attempt, errMismatch
			}
			prior = r.Outcome.Request
			break // Genuine authoritative ABORT alone permits fresh capture/body.
		}
	}
	return graphTransferAttempt{}, errLimit
}
func (d *graphTransferDriver) issueEdge(fact uint64) error {
	expected := d.h.config.Cost.edge(fact, 5)
	for _, block := range d.blocks {
		if expected.ID >= block.Receipt.Grant.First && expected.ID+1 <= block.Receipt.Grant.Last {
			r, err := d.rpc(0, graphTransferCall{Method: "next-edge", Grant: block.Receipt.Grant.Request, Source: &expected.Source, Target: &expected.Target})
			if err != nil || r.Edge == nil || *r.Edge != expected || r.Life != expected.ID+1 || len(r.Allocated) != 2 || r.Allocated[0].ID != expected.ID || r.Allocated[1].ID != expected.ID+1 {
				return errors.Join(errMismatch, err)
			}
			d.a.end.Issued += 2
			return nil
		}
	}
	return errUnavailable
}
func (d *graphTransferDriver) phase(owner uint8, phase string, start uint64) error {
	begin := time.Now()
	if d.h.config.Cost.Facts-start < uint64(d.h.config.Cost.Writers) {
		return errInvalid
	}
	if phase == "create" {
		for i := uint64(0); i < uint64(d.h.config.Cost.Writers); i++ {
			if err := d.issueEdge(start + i); err != nil {
				return err
			}
		}
	}
	jobs := make(chan uint64, 32)
	results := make(chan error, 32)
	halt := make(chan struct{})
	var workers sync.WaitGroup
	for client := range d.h.config.Cost.Writers {
		workers.Go(func() {
			_, err := d.operation(owner, phase, start+uint64(client), client, nil)
			results <- err
			for fact := range jobs {
				_, err := d.operation(owner, phase, fact, client, nil)
				results <- err
			}
		})
	}
	producer := make(chan error, 1)
	go func() {
		defer close(jobs)
		for fact := start + uint64(d.h.config.Cost.Writers); fact < d.h.config.Cost.Facts; fact++ {
			select {
			case <-halt:
				producer <- nil
				return
			default:
			}
			if phase == "create" {
				if err := d.issueEdge(fact); err != nil {
					producer <- err
					return
				}
			}
			select {
			case jobs <- fact:
			case <-halt:
				producer <- nil
				return
			case <-d.ctx.Done():
				producer <- d.ctx.Err()
				return
			}
		}
		producer <- nil
	}()
	go func() { workers.Wait(); close(results) }()
	var err error
	stopped := false
	for failure := range results {
		if failure != nil && !stopped {
			close(halt)
			stopped = true
		}
		err = errors.Join(err, failure)
	}
	err = errors.Join(err, <-producer)
	if err != nil {
		return err
	}
	count := d.h.config.Cost.Facts - start
	cross := uint64(0)
	for i := start; i < d.h.config.Cost.Facts; i++ {
		if d.h.config.Cost.cross(i) {
			cross++
		}
	}
	index := 0
	if phase == "update" {
		index = 1
	}
	d.a.end.Phases[index] = graphTransferPhase{phase, count, cross, count - cross, time.Since(begin).Nanoseconds()}
	return nil
}

func (d *graphTransferDriver) block(home, recipient uint8, name string, count uint64, bucket uint8) (graphTransferBlock, error) {
	var zero graphTransferBlock
	if uint64(len(d.cleanups)) >= d.catalog.binding.Transfer.MaxGrants {
		return zero, errLimit
	}
	secret := feedIdentity(d.catalog.binding.Transfer.runSecret(), name)
	index := len(d.cleanups)
	d.cleanups = append(d.cleanups, graphTransferCleanup{Owner: recipient, Grant: name, Secret: secret})
	if err := d.a.event(struct {
		Type     string
		Original graphTransferCleanup
	}{"allocation-reservation-issued", d.cleanups[index]}); err != nil {
		return zero, err
	}
	r, err := d.rpc(home, graphTransferCall{Method: "reserve", Request: name, Secret: secret, Count: count, Bucket: bucket})
	if err != nil || r.Receipt == nil {
		return zero, errors.Join(errUnavailable, err)
	}
	original := graphOwnerAllocationSession{Version: 1, Grant: name, Request: "session-" + name, Secret: feedIdentity(secret, "session"), Route: r.Receipt.Grant.Recipient}
	d.cleanups[index].Session = &original
	if err = d.a.event(struct {
		Type     string
		Original graphOwnerAllocationSession
	}{"allocation-session-issued", original}); err != nil {
		return zero, err
	}
	session, err := d.rpc(recipient, graphTransferCall{Method: "session", Request: original.Request, Grant: name, GrantSecret: secret, Secret: original.Secret})
	if err != nil || session.Session == nil || *session.Session != original {
		return zero, errors.Join(errMismatch, err)
	}
	installed, err := d.rpc(recipient, graphTransferCall{Method: "install", Session: &original})
	if err != nil || installed.Receipt == nil || installed.Receipt.Grant != r.Receipt.Grant {
		return zero, errors.Join(errMismatch, err)
	}
	return graphTransferBlock{*installed.Receipt, original}, nil
}
func (d *graphTransferDriver) setup() error {
	p := d.catalog.binding.Transfer
	for number, remaining := uint64(0), 2*d.h.config.Cost.Facts+6; remaining > 0; number++ {
		count := min(remaining, p.MaxBlock)
		block, err := d.block(0, 0, fmt.Sprintf("home-%02d", number), count, 0)
		if err != nil {
			return err
		}
		d.blocks = append(d.blocks, block)
		remaining -= count
	}
	remote, err := d.block(0, 1, "remote", 2, 1)
	if err != nil {
		return err
	}
	d.blocks = append(d.blocks, remote)
	for i := range 2 {
		r, err := d.rpc(0, graphTransferCall{Method: "next-node", Grant: d.blocks[0].Receipt.Grant.Request})
		if err != nil || r.Node == nil {
			return errors.Join(errUnavailable, err)
		}
		d.nodes[i] = *r.Node
		d.a.end.Issued += 2
	}
	for i := range 2 {
		r, err := d.rpc(0, graphTransferCall{Method: "next-value", Grant: d.blocks[0].Receipt.Grant.Request})
		if err != nil || len(r.Allocated) != 1 {
			return errors.Join(errUnavailable, err)
		}
		d.values[i] = r.Allocated[0]
		d.a.end.Issued++
	}
	r, err := d.rpc(1, graphTransferCall{Method: "next-node", Grant: "remote"})
	if err != nil || r.Node == nil {
		return errors.Join(errUnavailable, err)
	}
	d.nodes[2] = *r.Node
	d.a.end.Issued += 2
	if d.nodes != d.h.config.Cost.nodes() || d.values[0].ID != 5 || d.values[1].ID != 6 {
		return errMismatch
	}
	for i := uint64(0); i < 3; i++ {
		owner := uint8(0)
		if i == 2 {
			owner = 1
		}
		if _, err := d.operation(owner, "setup", i, 0, nil); err != nil {
			return err
		}
	}
	return nil
}

// Domain-separated local factory secrets are fixed by the original run, not by
// a replacement PID or elapsed time. They are private workload capabilities.
func (p graphOwnerTransferPolicy) runSecret() [32]byte {
	return sha256.Sum256(append([]byte("rho-fdb-transfer/"), p.Run[:]...))
}

func (d *graphTransferDriver) captureUpdate(fact uint64) (graphTransferAttempt, error) {
	p := d.h.config.Cost
	base := p
	base.Version = 1
	op := graphCostOperationID(base, "update", fact)
	command := graphCommand{Kind: "edge-update", Edge: p.edge(fact, 6)}
	metadata := lifecycleMetadata{Provenance: op.Ordinal, EdgeLife: command.Edge.ID + 1, Definition: &lifecycleDefinition{ID: d.values[1].ID, Kind: "i64", I64: 6}}
	claims, err := d.claims(command, metadata)
	if err != nil {
		return graphTransferAttempt{}, err
	}
	name := fmt.Sprintf("transfer-update-%d-c0-a1", fact)
	r, err := d.rpc(0, graphTransferCall{Method: "prepare", Prepare: &graphTransferPrepare{name, command, metadata, claims}})
	if err != nil || r.Intent == nil {
		return graphTransferAttempt{}, errors.Join(errUnavailable, err)
	}
	a := graphTransferAttempt{Operation: op, ClientID: 0, AttemptOrdinal: 1, Intent: *r.Intent, Prepare: r}
	err = d.a.event(struct {
		Type    string
		Attempt graphTransferAttempt
	}{"prepared", a})
	return a, err
}
func (d *graphTransferDriver) loseCommitted(owner uint8, attempt *graphTransferAttempt) error {
	p := d.processes[owner]
	pending, err := p.begin(graphTransferCall{Method: "hold-submit", Client: attempt.ClientID, Intent: &attempt.Intent})
	if err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type, Owner string
		Call        graphTransferCall
	}{"submit-issued", d.authorities[owner].ID, pending.Call}); err != nil {
		return err
	}
	marker := fmt.Sprintf("fdb-transfer-lost-%s-%016x.json", d.authorities[owner].ID, pending.Call.ID)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var ready graphTransferReply
	for {
		ready, err = feedChildRead[graphTransferReply](d.root, marker)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-tick.C:
		}
	}
	if ready.ID != pending.Call.ID || ready.PID != p.command.Process.Pid || ready.Outcome == nil || !ready.Outcome.Commit || ready.Proof == nil || ready.Proof.Outcome != *ready.Outcome || ready.Error != "" || ready.Nanos < 1 {
		return errMismatch
	}
	body, err := wire(ready.Proof.Group.Tx)
	if err != nil || !bytes.Equal(body, attempt.Intent.Body) {
		return errors.Join(errMismatch, err)
	}
	exit, err := p.finish(true)
	if err != nil {
		return err
	}
	missing, unknown := p.await(pending)
	if !errors.Is(unknown, errUnknown) || missing.NativeCode != nil || missing.Outcome != nil {
		return errors.Join(errMismatch, unknown)
	}
	attempt.Submissions = append(attempt.Submissions, missing)
	d.a.end.Processes = append(d.a.end.Processes, exit)
	if err = d.a.event(struct {
		Type                 string
		Issued               graphTransferCall
		Observation, Missing graphTransferReply
		Process              graphTransferProcessExit
	}{"committed-reply-lost", pending.Call, ready, missing, exit}); err != nil {
		return err
	}
	d.processes[owner], err = startGraphTransferProcess(d.ctx, d.root, d.authorities[owner].ID, "replacement")
	if err != nil {
		return err
	}
	_, err = d.rpc(owner, graphTransferCall{Method: "describe"})
	return err
}

func (d *graphTransferDriver) successorGrant() error {
	block, err := d.block(1, 1, "successor-block", 8, 0)
	if err != nil || block.Receipt.Grant.First != 2*d.h.config.Cost.Facts+9 || block.Receipt.Grant.Home != (graphOwnerRoute{Owner: "B", Epoch: 2}) || block.Receipt.Grant.Recipient != block.Receipt.Grant.Home {
		return errors.Join(errMismatch, err)
	}
	issued, err := d.rpc(1, graphTransferCall{Method: "next-value", Grant: "successor-block"})
	if err != nil || len(issued.Allocated) != 1 || issued.Allocated[0].ID != block.Receipt.Grant.First {
		return errors.Join(errMismatch, err)
	}
	closed, err := d.rpc(1, graphTransferCall{Method: "close-cursor", Grant: "successor-block"})
	if err != nil || closed.Receipt == nil || closed.Receipt.Slot.Consumed != 1 || closed.Receipt.Slot.State != 2 {
		return errors.Join(errMismatch, err)
	}
	d.a.end.Issued++
	d.a.end.Burned += closed.Receipt.Grant.Count - closed.Receipt.Slot.Consumed
	// Candidate issuance is opaque, not a graph fact or current-owner permit.
	// SAME installed session may return its receipt, never a reset cursor.
	if _, refused := d.rpc(1, graphTransferCall{Method: "install", Session: &block.Session}); !errors.Is(refused, errAllocationConsumed) {
		return errors.Join(errMismatch, refused)
	}
	first := d.blocks[0].Receipt.Grant
	if _, refused := d.rpc(0, graphTransferCall{Method: "reserve", Request: first.Request, Secret: feedIdentity(d.catalog.binding.Transfer.runSecret(), first.Request), Count: first.Count, Bucket: first.Bucket}); !errors.Is(refused, errStale) {
		return errors.Join(errMismatch, refused)
	}
	return nil
}
func (d *graphTransferDriver) transfer(initial cut) (err error) {
	// All actual Next-backed identities have either a committed first-use row
	// or this session's authenticated terminal consumed count before process loss.
	for _, block := range d.blocks {
		owner := uint8(0)
		if block.Receipt.Grant.Bucket == 1 {
			owner = 1
		}
		if _, err = d.rpc(owner, graphTransferCall{Method: "close-cursor", Grant: block.Receipt.Grant.Request}); err != nil {
			return err
		}
	}
	d.feed, err = newGraphRetainedFeedStore(d.ctx, d.catalog, graphFeedPolicyFor(d.h.config.Cost))
	if err != nil {
		return err
	}
	defer d.feed.stopNormal()
	secret, err := newGraphFeedSecret()
	if err != nil {
		return err
	}
	begin := time.Now()
	original, err := d.feed.Begin(d.ctx, "transfer", secret)
	if err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type     string
		Original graphFeedOriginal
		Nanos    int64
	}{"feed-begin", original, time.Since(begin).Nanoseconds()}); err != nil {
		return err
	}
	begin = time.Now()
	old, token, err := d.feed.Publish(d.ctx, original)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, old.Close()) }()
	d.a.end.Old, d.a.end.FeedOriginal, d.a.end.FeedToken = original.Cut, original, token
	d.a.column.old = cut{original.Cut}
	if err = d.a.inventory("old", cut{original.Cut}); err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type  string
		Token graphFeedToken
		Nanos int64
	}{"feed-publish", token, time.Since(begin).Nanoseconds()}); err != nil {
		return err
	}
	if d.a.end.Spec.Layout == "columns" {
		if err = d.a.column.columnsStart(); err != nil {
			return err
		}
	}
	updateBegin := time.Now()
	abort, err := d.captureUpdate(1)
	if err != nil {
		return err
	}
	k, err := d.captureUpdate(0)
	if err != nil {
		return err
	}
	if err = d.loseCommitted(0, &k); err != nil {
		return err
	}
	view, _, err := d.catalog.Observe(d.ctx)
	if err != nil {
		return err
	}
	stepBegin := time.Now()
	move, err := d.catalog.ReserveMove(d.ctx, d.authorities[0], view.State.Routes[0], "business-handoff", feedIdentity(secret, "move"), 0, d.authorities[1].ID, cut{original.Cut})
	if err != nil {
		return err
	}
	activated := false
	defer func() {
		if !activated {
			err = errors.Join(err, d.catalog.Cancel(d.ctx, d.authorities[0], move))
		}
	}()
	if err = d.a.handoffTiming("reserve", time.Since(stepBegin).Nanoseconds()); err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type string
		Move graphOwnerMove
	}{"move-reserved", move}); err != nil {
		return err
	}
	stepBegin = time.Now()
	frozen, err := d.catalog.Freeze(d.ctx, d.authorities[0], move)
	if err != nil {
		return err
	}
	if err = d.a.handoffTiming("freeze", time.Since(stepBegin).Nanoseconds()); err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type   string
		Move   graphOwnerMove
		Frozen graphOwnerMoveProgress
		View   graphOwnerView
	}{"handoff-frozen", move, frozen, view}); err != nil {
		return err
	}
	if err = d.a.census(d.catalog, d.authorities[0], move, "handoff"); err != nil {
		return err
	}
	stepBegin = time.Now()
	after, err := d.catalog.Activate(d.ctx, d.authorities[1], move)
	if err != nil {
		return err
	}
	activated = true
	if err = d.a.handoffTiming("activate", time.Since(stepBegin).Nanoseconds()); err != nil {
		return err
	}
	d.a.end.Move = move
	d.a.end.Handoff, err = d.catalog.ObserveCensus(d.ctx, d.authorities[1], move, "handoff")
	if err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type   string
		State  graphOwnerState
		Census graphOwnerCensusObservation
	}{"handoff-activated", after, d.a.end.Handoff}); err != nil {
		return err
	}
	if _, err = d.rpc(1, graphTransferCall{Method: "refresh"}); err != nil {
		return err
	}
	// A true target SDK process receives the old immutable request through its
	// authenticated successor route. Its ACK is lost in another genuine kill.
	if err = d.loseCommitted(1, &k); err != nil {
		return err
	}
	recovered, err := d.rpc(1, graphTransferCall{Method: "submit", Intent: &k.Intent})
	if err != nil || recovered.Proof == nil || !recovered.Proof.Outcome.Commit {
		return errors.Join(errMismatch, err)
	}
	k.Submissions = append(k.Submissions, recovered)
	k.Proof = *recovered.Proof
	k.Classification = "committed"
	k.Nanos = time.Since(updateBegin).Nanoseconds()
	if err = d.a.attempt(k); err != nil {
		return err
	}
	if err = d.successorGrant(); err != nil {
		return err
	}
	for _, method := range []string{"stale-submit", "prepare"} {
		call := graphTransferCall{Method: method, Intent: &k.Intent}
		if method == "prepare" {
			tx, e := decode[transaction](k.Intent.Body)
			if e != nil {
				return e
			}
			call.Intent = nil
			call.Prepare = &graphTransferPrepare{tx.ID, *tx.GraphChange, *tx.Lifecycle, k.Intent.Allocated}
		}
		if _, refused := d.rpc(0, call); !errors.Is(refused, errStale) {
			return errors.Join(errMismatch, refused)
		}
	}
	if _, refused := d.rpc(0, graphTransferCall{Method: "install", Session: &d.blocks[0].Session}); !errors.Is(refused, errStale) {
		return errors.Join(errMismatch, refused)
	}
	changed := k.Intent
	tx, err := decode[transaction](changed.Body)
	if err != nil {
		return err
	}
	tx.Participants[0].Generation++
	changed.Body, err = wire(tx)
	if err != nil {
		return err
	}
	changed.Digest = sha256.Sum256(changed.Body)
	if _, refused := d.rpc(1, graphTransferCall{Method: "submit", Intent: &changed}); !errors.Is(refused, errMismatch) {
		return errors.Join(errMismatch, refused)
	}
	r, err := d.rpc(1, graphTransferCall{Method: "resolve", Intent: &abort.Intent})
	if err != nil || r.Proof == nil || r.Proof.Outcome.Commit || r.Proof.Outcome.Reason != "owner-epoch" {
		return errors.Join(errMismatch, err)
	}
	abort.Submissions = []graphTransferReply{r}
	abort.Proof = *r.Proof
	abort.Classification = "aborted"
	abort.Nanos = time.Since(updateBegin).Nanoseconds()
	if err = d.a.attempt(abort); err != nil {
		return err
	}
	again, err := d.rpc(1, graphTransferCall{Method: "resolve", Intent: &abort.Intent})
	if err != nil || again.Proof == nil || !graphColumnSame(*again.Proof, abort.Proof) {
		return errors.Join(errMismatch, err)
	}
	if _, err = d.operation(1, "update", 1, 0, &abort); err != nil {
		return err
	}
	if err = d.phase(1, "update", 2); err != nil {
		return err
	}
	d.a.end.Phases[1].WallNanos = time.Since(updateBegin).Nanoseconds()
	d.a.end.Phases[1].Operations += 2
	for fact := uint64(0); fact < 2; fact++ {
		if d.h.config.Cost.cross(fact) {
			d.a.end.Phases[1].BusinessCrossTransactions++
		} else {
			d.a.end.Phases[1].BusinessLocalTransactions++
		}
	}
	// Real C remains held across routing and target replacement, while every
	// current edge/posting is now6. It never silently reopens at current state.
	if err = d.a.observe("held-old-after-target-restart", old, 5); err != nil {
		return err
	}
	current, err := d.source.OpenGraphSnapshot(d.ctx, graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}}, graphFeedWorkloadPolicy(d.h.config).Read)
	if err != nil {
		return err
	}
	final, err := current.Cut()
	if err != nil {
		return errors.Join(err, current.Close())
	}
	d.a.end.Final = final
	d.a.column.final = cut{final}
	if err = d.a.inventory("final", cut{final}); err != nil {
		return errors.Join(err, current.Close())
	}
	if err = d.a.observe("current-before-gc", current, 6); err != nil {
		return errors.Join(err, current.Close())
	}
	if err = current.Close(); err != nil {
		return err
	}
	if d.a.end.Spec.Layout == "columns" {
		if err = d.a.column.columnsFinish(); err != nil {
			return err
		}
		d.a.end.Columns = d.a.column.end.Columns
		if err = d.root.Link("fdb-feed-columns.jsonl", "fdb-transfer-columns.jsonl"); err != nil {
			return err
		}
		if err = d.root.Remove("fdb-feed-columns.jsonl"); err != nil {
			return err
		}
	}
	if err = d.a.raw(d.source, initial, cut{original.Cut}, cut{final}, false); err != nil {
		return err
	}
	refused := d.source.beginContext(d.ctx, "held-gc", cut{final})
	if !errors.Is(refused, errHistoryInUse) {
		return errors.Join(errMismatch, refused)
	}
	records, err := d.a.native(d.ctx, []fdb.Key{d.h.historyKey("state"), d.source.ownerKey(original.Snapshot.Request), d.source.ownerKey(original.Pin.Request)})
	if err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type, Error string
		Records     []graphCostStoredRecord
	}{"held-source-gc-refused", refused.Error(), records}); err != nil {
		return err
	}
	if err = old.Close(); err != nil {
		return err
	}
	retained, err := d.feed.ToRetainedOnly(d.ctx, token, "snapshot-released")
	if err != nil {
		return err
	}
	if err = d.a.event(struct {
		Type   string
		Record graphCostStoredRecord
	}{"feed-retained-only", retained}); err != nil {
		return err
	}
	if reopened, _, refused := d.feed.Publish(d.ctx, original); reopened != nil || !errors.Is(refused, errHistoryExpired) {
		return errors.Join(errMismatch, refused)
	} else if err = d.a.event(struct {
		Type, Error string
		Original    graphFeedOriginal
	}{"retained-physical-refused", refused.Error(), original}); err != nil {
		return err
	}
	if err = d.source.beginContext(d.ctx, "source-gc", cut{final}); err != nil {
		return err
	}
	for calls := uint64(0); calls < d.source.binding.Policy.MaxWorkRows; calls++ {
		budget := graphHistoryBudget{Rows: 32, Bytes: 4 << 20, OutputBytes: 4096}
		page, e := d.source.collectContext(d.ctx, "source-gc", budget)
		if e != nil {
			return e
		}
		if err = d.a.event(struct {
			Type   string
			Budget graphHistoryBudget
			Page   graphHistoryPage
		}{"source-gc-page", budget, page}); err != nil {
			return err
		}
		if page.Done {
			d.a.end.Retired = page.Seal
			break
		}
	}
	if d.a.end.Retired.Request != "source-gc" {
		return errLimit
	}
	if err = d.a.raw(d.source, initial, cut{original.Cut}, cut{final}, true); err != nil {
		return err
	}
	if expired, e := d.source.OpenGraphSnapshot(d.ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: original.Cut}, graphFeedWorkloadPolicy(d.h.config).Read); expired != nil || !errors.Is(e, errHistoryExpired) {
		return errors.Join(errMismatch, e)
	} else if err = d.a.event(struct {
		Type, Error string
		Cut         certificate
	}{"old-cut-refused", e.Error(), original.Cut}); err != nil {
		return err
	}
	if err = d.replayFeed(token, final); err != nil {
		return err
	}
	if _, err = d.rpc(1, graphTransferCall{Method: "submit", Intent: &k.Intent}); err != nil {
		return err
	}
	if _, err = d.rpc(1, graphTransferCall{Method: "submit", Intent: &abort.Intent}); err != nil {
		return err
	}
	for i, process := range d.processes {
		exit, e := process.finish(false)
		if e != nil {
			return e
		}
		d.a.end.Processes = append(d.a.end.Processes, exit)
		d.processes[i] = nil
		if err = d.a.event(struct {
			Type    string
			Process graphTransferProcessExit
		}{"owner-reaped", exit}); err != nil {
			return err
		}
	}
	if err = d.finalCost(final); err != nil {
		return err
	}
	if err = d.a.complete(); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(d.a.end)
}

func (d *graphTransferDriver) replayFeed(token graphFeedToken, final certificate) (err error) {
	begin := time.Now()
	reader, err := d.feed.OpenRetained(d.ctx, token, "retained-one")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	if err = d.a.event(struct {
		Type     string
		Identity graphFeedRetainedIdentity
		Nanos    int64
	}{"retained-open", reader.retained.id, time.Since(begin).Nanoseconds()}); err != nil {
		return err
	}
	var first graphFeedDelivery
	for page := uint64(0); page < d.feed.binding.Policy.MaxPages; page++ {
		name := fmt.Sprintf("page-%d", page)
		begin = time.Now()
		delivery, e := reader.Page(d.ctx, name, final)
		if e != nil {
			return e
		}
		pageNanos := time.Since(begin).Nanoseconds()
		if page == 0 {
			first = delivery
			if err = reader.Close(); err != nil {
				return err
			}
			begin = time.Now()
			reader, err = d.feed.OpenRetained(d.ctx, token, "retained-replacement")
			if err != nil {
				return err
			}
			if err = d.a.event(struct {
				Type     string
				Identity graphFeedRetainedIdentity
				Nanos    int64
			}{"retained-reopen", reader.retained.id, time.Since(begin).Nanoseconds()}); err != nil {
				return err
			}
			begin = time.Now()
			replay, e := reader.Page(d.ctx, name, final)
			if e != nil || !replay.Replay || !bytes.Equal(replay.Record.Value, first.Record.Value) {
				return errors.Join(errMismatch, e)
			}
			if err = d.a.event(struct {
				Type     string
				Delivery graphFeedDelivery
				Nanos    int64
			}{"retained-page-replay", replay, time.Since(begin).Nanoseconds()}); err != nil {
				return err
			}
			d.a.end.ReplayedPages++
		}
		begin = time.Now()
		ack, e := reader.Ack(d.ctx, name, sha256.Sum256(delivery.Record.Value))
		if e != nil {
			return e
		}
		if err = d.a.event(struct {
			Type                string
			Delivery            graphFeedDelivery
			Ack                 graphFeedAcknowledgement
			PageNanos, AckNanos int64
		}{"retained-page", delivery, ack, pageNanos, time.Since(begin).Nanoseconds()}); err != nil {
			return err
		}
		d.a.end.FeedPages++
		d.a.end.FeedRows += uint64(len(delivery.Page.Rows))
		if delivery.Page.EOF {
			if !sameCertificate(ack.Ack.Completed, final) {
				return errMismatch
			}
			break
		}
	}
	if d.a.end.FeedRows != d.h.config.Cost.Facts {
		return errMismatch
	}
	if err = d.feed.Finish(d.ctx, token, "feed-expired", true); err != nil {
		return err
	}
	if expired, e := d.feed.OpenRetained(d.ctx, token, "expired-new"); expired != nil || !errors.Is(e, errHistoryExpired) {
		return errors.Join(errMismatch, e)
	} else if err = d.a.event(struct {
		Type, Error string
		Token       graphFeedToken
	}{"retained-new-refused", e.Error(), token}); err != nil {
		return err
	}
	return reader.Close()
}

func (d *graphTransferDriver) finalCost(at certificate) (err error) {
	// A storage-only native freeze preserves B processing ownership: its real
	// proposed reverse move is CANCELLED after complete two-prefix output. No
	// second activation or fabricated copied replica is claimed.
	view, _, err := d.catalog.Observe(d.ctx)
	if err != nil {
		return err
	}
	move, err := d.catalog.ReserveMove(d.ctx, d.authorities[1], view.State.Routes[0], "terminal-storage-fence", feedIdentity(d.catalog.binding.Transfer.runSecret(), "storage"), 0, "A", cut{at})
	if err != nil {
		return err
	}
	canceled := false
	defer func() {
		if !canceled {
			err = errors.Join(err, d.catalog.Cancel(d.ctx, d.authorities[1], move))
		}
	}()
	if _, err = d.catalog.Freeze(d.ctx, d.authorities[1], move); err != nil {
		return err
	}
	if err = d.a.census(d.catalog, d.authorities[1], move, "terminal-storage"); err != nil {
		return err
	}
	before, _, err := graphCostParallelRead(d.ctx, d.h.db, func(tr fdb.ReadTransaction) (graphOwnerCensusObservation, error) {
		o, p, e := d.catalog.censusOriginalIn(tr, move, "terminal-storage")
		if e != nil {
			return graphOwnerCensusObservation{}, e
		}
		if !p.EOF || p.Pending != "" {
			return graphOwnerCensusObservation{}, errUnavailable
		}
		out := graphOwnerCensusObservation{Original: o, Progress: p}
		for i, kind := range []string{"original", "progress", "seal"} {
			out.Controls[i], e = snapshotStored(tr, d.catalog.censusKey(o.Request, kind), 64<<10)
			if e != nil {
				return out, e
			}
		}
		return out, nil
	})
	if err != nil {
		return err
	}
	if err = d.catalog.Cancel(d.ctx, d.authorities[1], move); err != nil {
		return err
	}
	canceled = true
	after, _, err := graphCostParallelRead(d.ctx, d.h.db, func(tr fdb.ReadTransaction) ([]graphCostStoredRecord, error) {
		actual, e := d.h.ownerCatalogIn(tr)
		if e != nil {
			return nil, e
		}
		state, e := actual.stateIn(tr)
		if e != nil {
			return nil, e
		}
		expected := view.State
		expected.Version++ // Authenticated Cancel fences another delayed reservation.
		if state != expected {
			return nil, errMismatch
		}
		return actual.censusHeadsIn(tr)
	})
	if err != nil {
		return err
	}
	if len(after) != len(before.Original.Heads) {
		return errMismatch
	}
	cost := graphTransferNativeCost{Rows: before.Progress.Rows + 3, Bytes: before.Progress.Bytes, FrozenRows: before.Progress.Rows, FrozenBytes: before.Progress.Bytes, Scope: "complete logical native source/control key-value census; physical replicated/compressed disk and RSS remain actual external measurements"}
	for _, record := range before.Controls {
		cost.Bytes += uint64(len(record.Key) + len(record.Value))
	}
	for i, record := range after {
		previous := before.Original.Heads[i]
		if !bytes.Equal(previous.Key, record.Key) {
			return errMismatch
		}
		if err := graphTransferCanceledHead(d.catalog, move, previous, record); err != nil {
			return err
		}
		oldBytes := uint64(len(previous.Key) + len(previous.Value))
		if cost.Bytes < oldBytes {
			return errMismatch
		}
		cost.Bytes = cost.Bytes - oldBytes + uint64(len(record.Key)+len(record.Value))
	}
	d.a.end.Storage = cost
	return d.a.event(struct {
		Type, Access string
		Move         graphOwnerMove
		Captured     graphOwnerCensusObservation
		FinalHeads   []graphCostStoredRecord
	}{"storage-eof", "logical native two-prefix key/value bytes plus actual census controls; apply bounded exact predecessor head replacements after funded Cancel; not physical/compressed replicated disk or RSS", move, before, after})
}

// The complete frozen raw census remains valid after Cancel only for these
// genuine native transitions. Unrelated source writes/owner activity are not
// converted into diagnostic byte replacements under an older physical EOF.
func graphTransferCanceledHead(c *graphOwnerCatalog, move graphOwnerMove, before, after graphCostStoredRecord) error {
	if c == nil || c.h == nil || c.binding.Transfer == nil || len(before.Key) == 0 || len(before.Key) > 1024 || len(before.Value) == 0 || len(before.Value) > 64<<10 || len(after.Value) == 0 || len(after.Value) > 64<<10 || !bytes.Equal(before.Key, after.Key) {
		return errInvalid
	}
	expected := before.Value
	var replacement any
	switch {
	case bytes.Equal(before.Key, c.key("state", "current")):
		v := move.Before
		v.Moving = move.Request
		stored, err := decode[graphOwnerState](before.Value)
		if err != nil || stored != v {
			return errors.Join(errMismatch, err)
		}
		v.Version++
		v.Moving = ""
		replacement = v
	case bytes.Equal(before.Key, c.key("move-progress", move.Request)):
		v, err := decode[graphOwnerMoveProgress](before.Value)
		if err != nil || v.Phase != 1 {
			return errors.Join(errMismatch, err)
		}
		v.Phase = 3
		replacement = v
	case bytes.Equal(before.Key, c.key("cleanup", "move/"+move.Request)):
		old, err := decode[graphOwnerCleanup](before.Value)
		if err != nil {
			return err
		}
		next, err := decode[graphOwnerCleanup](after.Value)
		if err != nil {
			return err
		}
		p := c.binding.Transfer
		if old.Request != "move/"+move.Request || next.Request != old.Request || next.Calls != old.Calls+1 || next.Calls > p.MaxCleanupCalls || next.DeadlineUnixNanos <= 0 || next.DeadlineUnixNanos > p.DeadlineUnixNanos+int64(p.CleanupMillis)*int64(time.Millisecond) || old.DeadlineUnixNanos != 0 && next.DeadlineUnixNanos != old.DeadlineUnixNanos {
			return errMismatch
		}
		expected = after.Value // Native first-close wall time is observed, not reconstructed.
	case bytes.Equal(before.Key, (&graphHistoryRetention{h: c.h}).ownerKey(move.Owner.Request)):
		stored, err := decode[graphHistoryOwner](before.Value)
		if err != nil || !graphColumnSame(stored, move.Owner) {
			return errors.Join(errMismatch, err)
		}
		v := move.Owner
		v.State, v.TerminalRequest = historyReleased, move.Request
		replacement = v
	case bytes.Equal(before.Key, c.h.historyKey("state")):
		v, err := decode[graphHistoryState](before.Value)
		if err != nil || v.Owners == 0 {
			return errors.Join(errMismatch, err)
		}
		v.Owners--
		replacement = v
	}
	if replacement != nil {
		var err error
		expected, err = wire(replacement)
		if err != nil {
			return err
		}
	}
	if !bytes.Equal(expected, after.Value) {
		return errMismatch
	}
	return nil
}

func (d *graphTransferDriver) cleanupFailedOriginals() error {
	var failure error
	for _, original := range d.cleanups {
		client := d.cleanupClients[original.Owner]
		if client == nil {
			failure = errors.Join(failure, errUnavailable)
			continue
		}
		var receipt graphOwnerAllocationReceipt
		var err error
		if original.Session != nil {
			receipt, err = client.CloseAllocationSession(d.ctx, *original.Session)
		}
		if original.Session == nil || err != nil {
			// Native CloseReserved itself checks genuine absence of a session in
			// the same transaction. Missing replies are never absence authority.
			var reserved graphOwnerAllocationReceipt
			reserved, closeErr := client.CloseReservedIDs(d.ctx, original.Grant, original.Secret)
			if closeErr == nil {
				receipt, err = reserved, nil
			} else {
				err = errors.Join(err, closeErr)
			}
		}
		observed := graphTransferReply{Receipt: &receipt}
		observed.failure(err)
		failure = errors.Join(failure, err, d.a.event(struct {
			Type     string
			Original graphTransferCleanup
			Result   graphTransferReply
		}{"failed-original-cleanup", original, observed}))
	}
	return failure
}

func runGraphTransferWorkload(db fdb.Database) (err error) {
	spec, admission, policy, err := graphTransferEnvironment()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, policy.DeadlineUnixNanos))
	defer cancel()
	h, err := newHarnessContext(ctx, db, policy.Config)
	if err != nil {
		return err
	}
	transport, err := h.graphTransportInstall(ctx, admission)
	if err != nil {
		return err
	}
	initial, _, err := historyTransactContext(ctx, db, func() error { return nil }, func(tr fdb.Transaction) (cut, error) {
		v, e := h.freshIn(tr, []uint8{0, 1}, nil)
		if e != nil {
			return cut{}, e
		}
		return v.(cut), nil
	})
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, initial, graphFeedWorkloadPolicy(h.config).History)
	if err != nil {
		return err
	}
	authorities := [2]graphOwnerAuthority{{ID: "A"}, {ID: "B"}}
	for i := range authorities {
		if _, err = rand.Read(authorities[i].Secret[:]); err != nil {
			return err
		}
	}
	catalog, err := newGraphOwnerCatalogTransfer(ctx, source, authorities, policy)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	a, err := newGraphTransferArtifacts(ctx, h, root, spec, catalog, graphTransportRecord{admission, transport}, initial)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	d := &graphTransferDriver{ctx: ctx, h: h, root: root, catalog: catalog, source: source, a: a, authorities: authorities}
	for i, authority := range authorities {
		d.cleanupClients[i], err = catalog.Client(ctx, authority)
		if err != nil {
			return err
		}
	}
	defer func() {
		for _, process := range d.processes {
			if process != nil && !process.waited {
				_, e := process.finish(false)
				err = errors.Join(err, e)
			}
		}
		if err != nil {
			err = errors.Join(err, d.cleanupFailedOriginals())
		}
	}()
	for i, authority := range authorities {
		input := graphTransferChildInput{catalog.binding, authority, authority.ID, policy.DeadlineUnixNanos}
		if err = feedChildWrite(root, "fdb-transfer-"+authority.ID+"-input.json", input); err != nil {
			return err
		}
		d.processes[i], err = startGraphTransferProcess(ctx, root, authority.ID, "initial")
		if err != nil {
			return err
		}
		if _, err = d.rpc(uint8(i), graphTransferCall{Method: "describe"}); err != nil {
			return err
		} // #nosec G115 -- two fixed owner indices.
	}
	if err = d.setup(); err != nil {
		return err
	}
	createBegin := time.Now()
	if err = d.issueEdge(0); err != nil {
		return err
	}
	if _, err = d.operation(0, "create", 0, 0, nil); err != nil {
		return err
	}
	if err = d.phase(0, "create", 1); err != nil {
		return err
	}
	a.end.Phases[0].WallNanos = time.Since(createBegin).Nanoseconds()
	a.end.Phases[0].Operations++
	if h.config.Cost.cross(0) {
		a.end.Phases[0].BusinessCrossTransactions++
	} else {
		a.end.Phases[0].BusinessLocalTransactions++
	}
	return d.transfer(initial) // Genuine process/retention lifecycle, no printable-only receipt.
}
