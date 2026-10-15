package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphFeedRetainedNamespace = "graph-workload-feed-retained-v1-"
const graphFeedRetainedFormat = "fdb-snapshot-feed-retained-only-workload-v1"
const graphFeedRetainedTransportFormat = "fdb-owned-ingress-transport-v3"

func graphFeedRetainedConfig(s graphFeedWorkloadSpec, run string) config {
	return graphCostConfig(graphFeedRetainedNamespace+s.Layout+"-"+run, graphCostProfile{10, s.Facts, s.CrossPercent, s.Writers})
}
func graphFeedRetainedLimit(p graphCostProfile) limits {
	// At most 64 distinct originals per useful logical operation; SAME-body
	// submissions reuse those originals. Physical controls have their own
	// explicit owner/feed/column ledgers and are not graph business versions.
	return limits{Bytes: (64 << 20) + p.Facts*(64<<10) + (3+2*p.Facts)*64*2048,
		Transactions: (3 + 2*p.Facts) * 64, Versions: 3 + 6*p.Facts, Certificates: 1024}
}
func validGraphFeedRetainedConfig(c config) bool {
	p := c.Cost
	if p.Version != 10 || p.Facts < 20 || p.Facts > 1_000_000 || p.Facts%20 != 0 || p.Writers != 1 && p.Writers != 8 && p.Writers != 32 || p.Facts <= uint64(p.Writers) || p.CrossPercent != 0 && p.CrossPercent != 10 && p.CrossPercent != 50 && p.CrossPercent != 100 || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	name, ok := strings.CutPrefix(c.Graph, graphFeedRetainedNamespace)
	if !ok {
		return false
	}
	run, ok := strings.CutPrefix(name, "kv-")
	if !ok {
		run, ok = strings.CutPrefix(name, "columns-")
	}
	n := p.nodes()
	return ok && transportHex(run, 16) && c.Nodes == [2]uint64{n[0].Node, n[2].Node} && c.Limits == graphFeedRetainedLimit(p)
}

// Version3 extends only the fresh collected feed's owner admission. Catalog2
// remains mandatory: copied source/feed recipes cannot issue IDs or intents.
func (p graphOwnerTransferPolicy) validateFeedRetained() error {
	if p.Version != 3 || !validGraphFeedRetainedConfig(p.Config) || p.Admission == nil || p.Admission.Version != 3 || p.Admission.validate() != nil || p.Census == nil || p.Census.validate() != nil || p.Run == [16]byte{} || !strings.HasSuffix(p.Config.Graph, hex.EncodeToString(p.Run[:])) || p.BudgetSeconds < 1 || p.BudgetSeconds > graphTransportMaxSeconds || p.AdmittedUnixNanos <= 0 || p.AdmittedUnixNanos > math.MaxInt64-int64(p.BudgetSeconds)*int64(time.Second) || p.DeadlineUnixNanos != p.AdmittedUnixNanos+int64(p.BudgetSeconds)*int64(time.Second) || p.DeadlineUnixNanos > math.MaxInt64-int64(30*time.Second) || p.CleanupMillis != 30000 || p.MaxCleanupCalls != 8 || p.MaxAttempts != 64 || p.MaxSubmissions != 8 || p.MaxCallbacks != 16 || p.MaxProposalBytes != 32<<10 || p.MaxGrants < 4 || p.MaxGrants > 64 || p.MaxBlock < 8 || p.MaxBlock > graphOwnerMaxBlock || p.MaxBlock%2 != 0 {
		return errInvalid
	}
	a := p.Admission
	if a.Spec.Version != 3 || a.Spec.Family != graphFeedWorkloadFamily || a.Run != p.Run || a.BudgetSeconds != p.BudgetSeconds || a.AdmittedUnixNanos != p.AdmittedUnixNanos || a.DeadlineUnixNanos != p.DeadlineUnixNanos || graphFeedRetainedConfig(a.Spec, hex.EncodeToString(a.Run[:])) != p.Config {
		return errMismatch
	}
	n := p.Config.Cost.Facts
	// Both request/ID intents for every possible distinct attempt, all issued
	// IDs, grant/slot/index/cleanup records, and fixed catalog records. Charge
	// exact immutable bytes; mutable controls keep native full-width prepay.
	minimum := 2*(3+2*n)*p.MaxAttempts + 2*n + 4*p.MaxGrants + 64
	grants := (2*n+6+p.MaxBlock-1)/p.MaxBlock + (2+p.MaxBlock-1)/p.MaxBlock
	if grants > p.MaxGrants || p.MaxRecords < minimum || p.MaxRecords > 1<<32 || p.MaxControlBytes < 512<<10 || p.MaxControlBytes > 1<<50 || p.MaxRecords > p.MaxControlBytes/128 || p.Census.MaxRows < 33+21*n {
		return errInvalid
	}
	return nil
}

// The complete owner/control grant is chosen by the controller before helpers,
// SDK open or effects. It is never enlarged in response to actual exhaustion.
func graphFeedRetainedPolicy(g graphTransferGrant, a graphTransportPolicy) (graphOwnerTransferPolicy, error) {
	p := graphOwnerTransferPolicy{Version: 3, Config: graphFeedRetainedConfig(a.Spec, hex.EncodeToString(a.Run[:])), Run: a.Run, BudgetSeconds: a.BudgetSeconds, AdmittedUnixNanos: a.AdmittedUnixNanos, DeadlineUnixNanos: a.DeadlineUnixNanos,
		MaxRecords: g.MaxRecords, MaxControlBytes: g.MaxControlBytes, MaxProposalBytes: 32 << 10, MaxGrants: g.MaxGrants, MaxBlock: g.MaxBlock, MaxAttempts: 64, MaxSubmissions: 8, MaxCallbacks: 16, CleanupMillis: 30000, MaxCleanupCalls: 8, Admission: new(a), Census: new(g.Census)}
	return p, p.validate()
}
func graphFeedRetainedEnvironment() (graphTransportPolicy, graphOwnerTransferPolicy, error) {
	a, err := graphTransportFromEnv()
	if err != nil || a == nil || a.Version != 3 {
		return graphTransportPolicy{}, graphOwnerTransferPolicy{}, errors.Join(errInvalid, err)
	}
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 {
		return *a, graphOwnerTransferPolicy{}, errInvalid
	}
	s, err := decode[graphFeedWorkloadSpec](body)
	if err != nil || s != a.Spec {
		return *a, graphOwnerTransferPolicy{}, errors.Join(errMismatch, err)
	}
	body = []byte(os.Getenv("RHO_FDB_FEED_RETAINED_GRANT"))
	if len(body) == 0 || len(body) > 16384 {
		return *a, graphOwnerTransferPolicy{}, errInvalid
	}
	g, err := decode[graphTransferGrant](body)
	if err != nil {
		return *a, graphOwnerTransferPolicy{}, err
	}
	p, err := graphFeedRetainedPolicy(g, *a)
	return *a, p, err
}

type graphFeedRetainedBlock struct {
	Receipt graphOwnerAllocationReceipt
	Cursor  *graphOwnerAllocationCursor
	Session graphOwnerAllocationSession
}
type graphFeedRetainedDriver struct {
	ctx           context.Context
	h             *harness
	catalog       *graphOwnerCatalog
	clients       [2]*graphOwnerClient
	blocks        []graphFeedRetainedBlock // Actual <=64 grants, never a fact-sized map.
	cleanups      []graphTransferCleanup
	cursorsClosed bool
	a             *graphFeedRetainedArtifacts
}

func (d *graphFeedRetainedDriver) claims(x transaction) ([]graphOwnerAllocatedID, error) {
	ids, err := ownerCommandIDs(x)
	if err != nil {
		return nil, err
	}
	claims := make([]graphOwnerAllocatedID, 0, len(ids))
	for _, id := range ids {
		var found bool
		for _, b := range d.blocks {
			g := b.Receipt.Grant
			if id >= g.First && id <= g.Last {
				claims = append(claims, graphOwnerAllocatedID{id, id - g.First, g.Request, b.Session.Request})
				found = true
				break
			}
		}
		if !found {
			return nil, errUnavailable
		}
	}
	return claims, nil
}
func (d *graphFeedRetainedDriver) issue() error {
	n := d.h.config.Cost.Facts
	remaining := 2*n + 6
	for bucket := uint8(0); bucket < 2; bucket++ {
		count := remaining
		if bucket == 1 {
			count = 2
		}
		for count > 0 {
			size := min(count, d.catalog.binding.Transfer.MaxBlock)
			name := fmt.Sprintf("retained-block-%d", len(d.blocks))
			secret, err := newGraphFeedSecret()
			if err != nil {
				return err
			}
			cleanup := len(d.cleanups)
			d.cleanups = append(d.cleanups, graphTransferCleanup{Owner: bucket, Grant: name, Secret: secret})
			if err = d.a.event(struct {
				Type, Request string
				Bucket        uint8
				Count         uint64
				Secret        [32]byte
			}{"grant-original", name, bucket, size, secret}); err != nil {
				return err
			}
			r, err := d.clients[0].ReserveIDs(d.ctx, name, secret, size, bucket)
			if err != nil {
				return err
			}
			sessionSecret, err := newGraphFeedSecret()
			if err != nil {
				return err
			}
			s, err := d.clients[bucket].AllocationSession(d.ctx, name, "recipient-"+name, secret, sessionSecret)
			if err != nil {
				return err
			}
			d.cleanups[cleanup].Session = new(s)
			cursor, installed, err := d.clients[bucket].InstallIDs(d.ctx, s)
			if err != nil {
				return err
			}
			d.blocks = append(d.blocks, graphFeedRetainedBlock{installed, cursor, s})
			if err = d.a.event(struct {
				Type                   string
				Reservation, Installed graphOwnerAllocationReceipt
				Session                graphOwnerAllocationSession
			}{"grant", r, installed, s}); err != nil {
				return err
			}
			count -= size
		}
	}
	for i, node := range d.h.config.Cost.nodes() {
		cursor, err := d.cursor(node.Node, 2)
		if err != nil {
			return err
		}
		actual, claims, err := cursor.Node()
		if err != nil || actual != node {
			return errors.Join(errMismatch, err)
		}
		if err = d.a.event(struct {
			Type   string
			Node   endpoint
			Claims [2]graphOwnerAllocatedID
		}{"next-node", actual, claims}); err != nil {
			return err
		}
		d.a.end.Issued += 2
		if i == 1 {
			for want := uint64(5); want <= 6; want++ {
				claim, err := cursor.Next()
				if err != nil || claim.ID != want {
					return errors.Join(errMismatch, err)
				}
				if err = d.a.event(struct {
					Type  string
					Claim graphOwnerAllocatedID
				}{"next-value", claim}); err != nil {
					return err
				}
				d.a.end.Issued++
			}
		}
	}
	return nil
}
func (d *graphFeedRetainedDriver) cursor(first, count uint64) (*graphOwnerAllocationCursor, error) {
	for _, b := range d.blocks {
		if first >= b.Receipt.Grant.First && first+count-1 <= b.Receipt.Grant.Last {
			return b.Cursor, nil
		}
	}
	return nil, errUnavailable
}
func (d *graphFeedRetainedDriver) operation(phase string, fact uint64, client int) error {
	return d.operationAt(phase, fact, client, time.Now())
}
func (d *graphFeedRetainedDriver) operationAt(phase string, fact uint64, client int, started time.Time) error {
	p := d.h.config.Cost
	base := p
	base.Version = 1
	op := graphCostOperationID(base, phase, fact)
	command := graphCommand{Kind: "edge-" + phase, Edge: p.edge(fact, 5)}
	if phase == "update" {
		command.Edge.Value = 6
	}
	if phase == "setup" {
		command = graphCommand{Kind: "node-create", Node: p.nodes()[fact]}
	}
	var previous string
	for attempt := 1; attempt <= 64; attempt++ {
		before, callbacks, err := graphCostParallelRead(d.ctx, d.h.db, func(tr fdb.ReadTransaction) (snapshot, error) {
			if err := d.h.graphTransportCheckIn(tr); err != nil {
				return snapshot{}, err
			}
			return d.h.graphCostPointSnapshotIn(tr, command)
		})
		if err != nil {
			return err
		}
		coordinator := uint8(0)
		if phase == "setup" {
			coordinator = command.Node.Owner
		}
		name := fmt.Sprintf("retained-%s-%d-c%d-a%d", phase, fact, client, attempt)
		x := d.h.graphTransaction(name, coordinator, command, before)
		m := lifecycleMetadata{Provenance: op.Ordinal}
		if phase != "setup" {
			id := uint64(5)
			if phase == "update" {
				id = 6
			}
			m.EdgeLife, m.Definition = command.Edge.ID+1, &lifecycleDefinition{ID: id, Kind: "i64", I64: command.Edge.Value}
		}
		x.Lifecycle = &m
		claims, err := d.claims(x)
		if err != nil {
			return err
		}
		body, err := wire(x)
		if err != nil {
			return err
		}
		intent, err := d.clients[coordinator].PrepareAllocated(d.ctx, body, claims)
		if err != nil {
			return err
		}
		if err = d.a.event(struct {
			Type                              string
			Operation                         graphCostOperation
			Client, Attempt, CaptureCallbacks int
			Previous                          string
			Intent                            graphOwnerIntent
		}{"prepared", op, client, attempt, callbacks, previous, intent}); err != nil {
			return err
		}
		var decided bool
		for same := 1; same <= 8; same++ {
			o, calls, submitErr := d.clients[coordinator].Submit(d.ctx, intent)
			proof, reads, originalErr := d.clients[coordinator].Original(d.ctx, intent)
			if err = d.a.event(struct {
				Type                           string
				Request                        string
				Same, Callbacks, ReadCallbacks int
				Outcome                        outcome
				Proof                          graphCostParallelProof
				SubmitError, OriginalError     string
			}{"submission", x.Request, same, calls, reads, o, proof, graphFeedRetainedError(submitErr), graphFeedRetainedError(originalErr)}); err != nil {
				return err
			}
			if originalErr != nil {
				if same == 8 {
					return errors.Join(errUnknown, submitErr, originalErr)
				}
				continue
			}
			if proof.Outcome.ID != x.ID || proof.Outcome.Request != x.Request || proof.Outcome.Digest != sha256.Sum256(body) || submitErr == nil && o != proof.Outcome {
				return errMismatch
			}
			d.a.mu.Lock()
			d.a.end.Attempts++
			if proof.Outcome.Commit {
				d.a.end.Committed++
				if phase != "setup" {
					d.a.end.Clients[client]++
				}
			} else {
				d.a.end.Aborted++
			}
			d.a.mu.Unlock()
			if err = d.a.event(struct {
				Type                  string
				Operation             graphCostOperation
				Client, Attempt       int
				Previous              string
				Intent                graphOwnerIntent
				Proof                 graphCostParallelProof
				StartNanos, StopNanos int64
			}{"terminal", op, client, attempt, previous, intent, proof, started.Sub(d.a.started).Nanoseconds(), time.Since(d.a.started).Nanoseconds()}); err != nil {
				return err
			}
			if proof.Outcome.Commit {
				return nil
			}
			if !graphCostParallelReplan(proof.Outcome) {
				return errMismatch
			}
			previous = x.Request
			decided = true
			break
		}
		if !decided {
			return errUnknown
		}
	}
	return errLimit
}
func graphFeedRetainedError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func (d *graphFeedRetainedDriver) phase(phase string, first, last uint64) error {
	if first >= last || last > d.h.config.Cost.Facts {
		return errInvalid
	}
	start := time.Now()
	writers := d.h.config.Cost.Writers
	type job struct {
		fact    uint64
		started time.Time
	}
	queues := make([]chan job, writers)
	failures := make(chan error, writers)
	var workers sync.WaitGroup
	for client := range writers {
		queues[client] = make(chan job, 1)
		workers.Go(func() {
			var failure error
			for q := range queues[client] {
				if failure == nil {
					failure = d.operationAt(phase, q.fact, client, q.started)
				}
			}
			failures <- failure
		})
	}
	var produceErr error
	for fact := first; fact < last; fact++ {
		queued := time.Now()
		if phase == "create" {
			edge := d.h.config.Cost.edge(fact, 5)
			cursor, err := d.cursor(edge.ID, 2)
			if err != nil {
				produceErr = err
				break
			}
			actual, life, claims, err := cursor.Edge(edge.Source, edge.Target, 5)
			if err != nil || actual != edge || life != edge.ID+1 {
				produceErr = errors.Join(errMismatch, err)
				break
			}
			produceErr = d.a.event(struct {
				Type   string
				Fact   uint64
				Edge   graphEdge
				Life   uint64
				Claims [2]graphOwnerAllocatedID
			}{"next-edge", fact, actual, life, claims})
			if produceErr != nil {
				break
			}
			d.a.end.Issued += 2
		}
		select {
		case queues[fact%uint64(writers)] <- job{fact, queued}:
		case <-d.ctx.Done():
			produceErr = d.ctx.Err()
		}
		if produceErr != nil {
			break
		}
	}
	for _, q := range queues {
		close(q)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		produceErr = errors.Join(produceErr, err)
	}
	if produceErr != nil {
		return produceErr
	}
	stop := time.Now()
	wall := graphFeedBusinessPhase{Phase: phase, StartNanos: start.Sub(d.a.started).Nanoseconds(), StopNanos: stop.Sub(d.a.started).Nanoseconds(), WallNanos: stop.Sub(start).Nanoseconds(), Useful: last - first}
	d.a.end.Business = append(d.a.end.Business, wall)
	d.a.end.BusinessNanos += wall.WallNanos
	return d.a.event(struct {
		Type        string
		First, Last uint64
		Phase       graphFeedBusinessPhase
	}{"business-phase", first, last, wall})
}

func runGraphFeedRetainedWorkload(db fdb.Database) (err error) {
	admission, policy, err := graphFeedRetainedEnvironment()
	if err != nil {
		return err
	}
	ctx, cancel := graphTransportContext(&admission)
	defer cancel()
	h, err := newHarnessContext(ctx, db, policy.Config)
	if err != nil {
		return err
	}
	stored, err := h.graphTransportInstall(ctx, admission)
	if err != nil {
		return err
	}
	initial, err := graphTransportFresh(ctx, h)
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, initial, graphFeedWorkloadPolicy(h.config).History)
	if err != nil {
		return err
	}
	var authorities [2]graphOwnerAuthority
	for i := range authorities {
		secret, e := newGraphFeedSecret()
		if e != nil {
			return e
		}
		authorities[i] = graphOwnerAuthority{ID: fmt.Sprintf("retained-owner-%d", i), Secret: secret}
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
	a, err := newGraphFeedRetainedArtifacts(ctx, h, root, admission.Spec, catalog, graphTransportRecord{admission, stored}, initial)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	d := &graphFeedRetainedDriver{ctx: ctx, h: h, catalog: catalog, a: a}
	for i, authority := range authorities {
		d.clients[i], err = catalog.Client(ctx, authority)
		if err != nil {
			return err
		}
	}
	// Every acquired cursor keeps its funded exact Close, even on failure. A
	// source/process loss never reinstalls its consumed range as a new cursor.
	defer func() {
		if d.cursorsClosed {
			return
		}
		for _, c := range d.cleanups {
			var closeErr error
			if c.Session == nil {
				_, closeErr = d.clients[c.Owner].CloseReservedIDs(context.WithoutCancel(ctx), c.Grant, c.Secret)
			} else {
				_, closeErr = d.clients[c.Owner].CloseAllocationSession(context.WithoutCancel(ctx), *c.Session)
			}
			err = errors.Join(err, closeErr)
		}
	}()
	setup := time.Now()
	if err = d.issue(); err != nil {
		return err
	}
	for i := uint64(0); i < 3; i++ {
		if err = d.operation("setup", i, 0); err != nil {
			return err
		}
	}
	a.end.SetupNanos = time.Since(setup).Nanoseconds()
	if err = d.phase("create", 0, h.config.Cost.Facts); err != nil {
		return err
	}
	// No factory cursor remains live past its actual issuance. Later update
	// factories authenticate retained claims against genuine first-use rows.
	for _, b := range d.blocks {
		r, e := b.Cursor.Close(ctx)
		if e != nil {
			return e
		}
		if e = a.event(struct {
			Type    string
			Receipt graphOwnerAllocationReceipt
		}{"cursor-closed", r}); e != nil {
			return e
		}
	}
	d.cursorsClosed = true
	store, err := newGraphRetainedFeedStore(ctx, catalog, graphFeedPolicyFor(h.config.Cost))
	if err != nil {
		return err
	}
	defer store.stopNormal()
	secret, err := newGraphFeedSecret()
	if err != nil {
		return err
	}
	original, err := store.Begin(ctx, "retained-workload", secret)
	if err != nil {
		return err
	}
	snapshot, token, err := store.Publish(ctx, original)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	a.end.Original, a.end.Token, a.end.Snapshot = original, token, original.Cut
	if err = a.event(struct {
		Type     string
		Original graphFeedOriginal
		Token    graphFeedToken
	}{"published", original, token}); err != nil {
		return err
	}
	var columns *graphColumnStore
	var pins *graphColumnPins
	if admission.Spec.Layout == "columns" {
		columns, err = newGraphColumnStoreContext(ctx, h, graphFeedRetainedColumnPolicy(h.config.Cost))
		if err != nil {
			return err
		}
		pins, err = newGraphColumnPins(columns, graphColumnPinPolicy{MaxRecords: 64, MaxBytes: 1 << 20, PageChunks: 16, PageBytes: 2 << 20, MaxWorkChunks: 4096, MaxWorkBytes: 64 << 20, MaxOutputBytes: 4096})
		if err != nil {
			return err
		}
		if err = a.columns(columns, "snapshot-columns", "snapshot-native", initial, cut{original.Cut}, nil); err != nil {
			return err
		}
	}
	if err = a.native("snapshot"); err != nil {
		return err
	}
	n := h.config.Cost.Facts
	half := n / 2
	if err = d.phase("update", 0, half); err != nil {
		return err
	}
	// This full EOF is AFTER mutation: returning latest6 for the held C5 read
	// cannot pass its independent raw-row join. No scalar-only success marker.
	if err = a.snapshot("snapshot-after-update", snapshot); err != nil {
		return err
	}
	beforeGC, err := graphTransportFresh(ctx, h)
	if err != nil {
		return err
	}
	a.end.BeforeGC = beforeGC.record
	physical, err := store.Open(ctx, token, "pre-gc-physical")
	if err != nil {
		return err
	}
	page, err := physical.Page(ctx, "pre-gc-page", beforeGC.record)
	if err != nil {
		return errors.Join(err, physical.Close())
	}
	ack, err := physical.Ack(ctx, "pre-gc-page", sha256.Sum256(page.Record.Value))
	if err != nil {
		return errors.Join(err, physical.Close())
	}
	if page.Page.EOF || len(page.Page.Rows) == 0 {
		return errors.Join(errMismatch, physical.Close())
	}
	a.end.Pages++
	a.end.FeedRows += uint64(len(page.Page.Rows))
	if err = a.event(struct {
		Type     string
		Delivery graphFeedDelivery
		Ack      graphFeedAcknowledgement
	}{"feed-page", page, ack}); err != nil {
		return errors.Join(err, physical.Close())
	}
	if err = physical.Close(); err != nil {
		return err
	}
	if err = snapshot.Close(); err != nil {
		return err
	}
	if columns != nil {
		for calls := uint64(0); ; calls++ {
			if calls > 2*n+16 {
				return errLimit
			}
			r, e := pins.collect(ctx, "snapshot-columns", graphColumnCollectBudget{Chunks: 4096, Bytes: 64 << 20, OutputBytes: 4096})
			if e != nil {
				return e
			}
			if e = a.event(struct {
				Type     string
				Progress graphColumnCollectResult
			}{"columns-retire", r}); e != nil {
				return e
			}
			if r.Done {
				break
			}
		}
	}
	if err = a.native("before-gc"); err != nil {
		return err
	}
	transition, err := store.ToRetainedOnly(ctx, token, "retained-only")
	if err != nil {
		return err
	}
	again, err := store.ToRetainedOnly(ctx, token, "retained-only")
	if err != nil || !bytes.Equal(again.Value, transition.Value) {
		return errors.Join(errMismatch, err)
	}
	a.end.Transition = transition
	if err = a.event(struct {
		Type   string
		Record graphCostStoredRecord
	}{"retained-only", transition}); err != nil {
		return err
	}
	if err = source.beginContext(ctx, "retained-gc", beforeGC); err != nil {
		return err
	}
	for calls := uint64(0); ; calls++ {
		if calls > 32*n+4096 {
			return errLimit
		}
		r, e := source.collectContext(ctx, "retained-gc", graphHistoryBudget{Rows: 32, Bytes: 4 << 20, OutputBytes: 4096})
		if e != nil {
			return e
		}
		if e = a.event(struct {
			Type     string
			Progress graphHistoryPage
		}{"source-collect", r}); e != nil {
			return e
		}
		if r.Done {
			a.end.Retired = r.Seal
			break
		}
	}
	if a.end.Retired.Deleted[0]+a.end.Retired.Deleted[1] == 0 {
		return errMismatch
	}
	if err = a.native("after-gc"); err != nil {
		return err
	}
	_, _, openErr := store.Publish(ctx, original)
	if !errors.Is(openErr, errHistoryExpired) {
		return errors.Join(errMismatch, openErr)
	}
	if err = a.event(struct {
		Type, Door string
		Expired    bool
	}{"refusal", "token-snapshot", true}); err != nil {
		return err
	}
	_, openErr = source.OpenGraphSnapshot(ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: original.Cut}, graphFeedWorkloadPolicy(h.config).Read)
	if !errors.Is(openErr, errHistoryExpired) {
		return errors.Join(errMismatch, openErr)
	}
	if err = a.event(struct {
		Type, Door string
		Expired    bool
	}{"refusal", "generic-at-C", true}); err != nil {
		return err
	}
	// The first genuine post-GC write is the new physical column checkpoint.
	// It is not a refreshed certificate for C or an invented business round.
	if err = d.phase("update", half, half+1); err != nil {
		return err
	}
	checkpoint, err := graphTransportFresh(ctx, h)
	if err != nil {
		return err
	}
	a.end.Checkpoint = checkpoint.record
	if err = a.native("checkpoint"); err != nil {
		return err
	}
	child, err := a.replace(graphFeedRetainedChildInput{Catalog: catalog.binding, Authorities: authorities, Policy: store.binding.Policy, Token: token, Boundary: checkpoint.record, Reader: "retained-process-reader", Page: "retained-process-page"})
	if err != nil {
		return err
	}
	a.end.Pages++
	a.end.FeedRows += uint64(len(child.Delivery.Page.Rows))
	reader, err := store.OpenRetained(ctx, token, "tail-reader")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	drain := func(boundary cut, stage string) error {
		for i := uint64(0); i <= 2*n+8; i++ {
			name := fmt.Sprintf("%s-%d", stage, i)
			delivery, e := reader.Page(ctx, name, boundary.record)
			if e != nil {
				return e
			}
			ack, e := reader.Ack(ctx, name, sha256.Sum256(delivery.Record.Value))
			if e != nil {
				return e
			}
			replay, e := reader.Ack(ctx, name, sha256.Sum256(delivery.Record.Value))
			if e != nil || !graphColumnSame(replay.Ack, ack.Ack) {
				return errors.Join(errMismatch, e)
			}
			a.end.Pages++
			a.end.FeedRows += uint64(len(delivery.Page.Rows))
			if e = a.event(struct {
				Type           string
				Delivery       graphFeedDelivery
				Ack, ReplayAck graphFeedAcknowledgement
			}{"feed-page", delivery, ack, replay}); e != nil {
				return e
			}
			if delivery.Page.EOF {
				return nil
			}
		}
		return errLimit
	}
	if child.Delivery.Page.EOF {
		if !sameCertificate(child.Ack.Cursor.Completed, checkpoint.record) {
			return errMismatch
		}
	} else if err = drain(checkpoint, "checkpoint-tail"); err != nil {
		return err
	}
	if err = d.phase("update", half+1, n); err != nil {
		return err
	}
	final, err := graphTransportFresh(ctx, h)
	if err != nil {
		return err
	}
	a.end.Final = final.record
	if err = drain(final, "final-tail"); err != nil {
		return err
	}
	if a.end.FeedRows != n {
		return errMismatch
	}
	current, err := source.OpenGraphSnapshot(ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: final.record}, graphFeedWorkloadPolicy(h.config).Read)
	if err != nil {
		return err
	}
	if err = a.snapshot("current", current); err != nil {
		return errors.Join(err, current.Close())
	}
	if err = current.Close(); err != nil {
		return err
	}
	if columns != nil {
		if err = a.columns(columns, "checkpoint-columns", "checkpoint-native", checkpoint, final, nil); err != nil {
			return err
		}
		if err = root.Mkdir("retained-column-cache", 0700); err != nil {
			return err
		}
		cache, err := root.OpenRoot("retained-column-cache")
		if err != nil {
			return err
		}
		if err = graphFeedRetainedFillCache(ctx, columns, "checkpoint-columns", cache, a); err != nil {
			return errors.Join(err, cache.Close())
		}
		if err = a.columns(columns, "checkpoint-columns", "checkpoint-cache", checkpoint, final, cache); err != nil {
			return errors.Join(err, cache.Close())
		}
		reopened, e := newGraphColumnStoreContext(ctx, h, columns.binding.Policy)
		if e != nil {
			return errors.Join(e, cache.Close())
		}
		if err = a.columns(reopened, "checkpoint-columns", "checkpoint-reopened", checkpoint, final, cache); err != nil {
			return errors.Join(err, cache.Close())
		}
		if err = cache.Close(); err != nil {
			return err
		}
	}
	if err = reader.Close(); err != nil {
		return err
	}
	if err = store.Finish(ctx, token, "retained-expire", true); err != nil {
		return err
	}
	_, openErr = store.OpenRetained(ctx, token, "expired-reader")
	if !errors.Is(openErr, errHistoryExpired) {
		return errors.Join(errMismatch, openErr)
	}
	if err = a.event(struct {
		Type, Door string
		Expired    bool
	}{"refusal", "retained-reader-after-terminal", true}); err != nil {
		return err
	}
	if err = a.native("final"); err != nil {
		return err
	}
	if a.end.Committed != 3+2*n || a.end.Issued != 2*n+8 {
		return errMismatch
	}
	return a.finish()
}

// Old policy2 keeps its exact census gate. Fresh3 must authenticate the whole
// new agreement before inheriting it; no marker or copied packet opts in.
func graphOwnerTransferCensus(p *graphOwnerTransferPolicy) bool {
	return p != nil && (p.Version == 2 || p.Version == 3 && p.validateFeedRetained() == nil)
}
