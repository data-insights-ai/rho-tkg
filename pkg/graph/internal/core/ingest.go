package core

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The ingest pipeline (ADR-0006 stage 1, Lanes:1) is a prepare-parallel /
// apply-sequential write door that sits BESIDE the interactive transaction door
// (g.Tx()) on the same L1 core. Producer SESSIONS validate, build property
// slices, precompute content hashes, and mint snowflake IDs on the CALLER
// thread (fully parallel — no shared-state mutation beyond the mutex-guarded ID
// generator and RLock-only registry lookups); a single APPLIER goroutine drains
// prepared intents in commit groups and applies each group through the existing
// batch machinery (Batch.Execute) — one c.txMu + c.mu.Lock acquisition, one
// TxFrom stamp from the shared monotonic clock, one co-committed change-log LSN
// run, one buffered PublishBatch, one flush per group. The applier is
// "replica-apply, but as the primary": it reuses the tested single-writer apply
// path rather than building a second write path (feasibility §6d).
//
// Lanes:1 strong mode (ADR §14): the group holds c.txMu + c.mu.Lock, so the
// whole apply of one prepared group is atomic against concurrent readers, and
// the pipeline serializes against the interactive door at GROUP granularity
// (never per insert — §4.3).

var (
	// ErrIngestClosed is returned by a session method after the graph (and the
	// applier) has been closed.
	ErrIngestClosed = errors.New("graph: ingest pipeline closed")
	// ErrNilSession is returned by a method on a nil *Session.
	ErrNilSession = errors.New("graph: nil ingest session")
)

const (
	// defaultIngestGroupSize is the target number of prepared intents the
	// applier coalesces into one commit group before flushing (group commit,
	// §4.6). Larger amortizes the fsync further; smaller lowers latency.
	defaultIngestGroupSize = 4096
	// defaultIngestQueueBound is the default capacity of the prepare→apply
	// queue in submitted groups (§4.8). A full queue BLOCKS the producer
	// (synchronous stall), never drops.
	defaultIngestQueueBound = 256
	// ingestFailureCap bounds the retained per-token apply-failure records (the
	// async WaitApplied truth channel — C2). Failures are rare; prune-on-read
	// keeps the map tiny in normal operation, and this cap bounds the
	// pathological "fire-and-forget async producer, many failures, nobody reads"
	// case: at the cap the oldest record is evicted (counted in failureDrops)
	// and a WaitApplied for an evicted token returns nil.
	ingestFailureCap = 8192
)

// IngestOptions configures a producer session (ADR-0006 §4.5/§4.6/§4.8).
type IngestOptions struct {
	// Sync selects the §4.6 freshness contract. true: Submit blocks until the
	// group has been applied AND is visible (ack ⇒ any subsequent read on any
	// goroutine observes the write). false: Submit returns a (lane, seq) token
	// immediately; a reader achieves read-your-writes via WaitApplied(token).
	Sync bool
	// DeclareLabels / DeclareRelTypes pre-register an ingestion job's vocabulary
	// up front (§4.4) so every prepare thread does lookup-only token resolution
	// with zero apply-side registry work. Undeclared names still work via the
	// probe-token fallback, at a serial cost proportional to distinct new names.
	DeclareLabels   []string
	DeclareRelTypes []string
	// QueueBound bounds the prepare→apply queue (§4.8). 0 = default. At the
	// bound a Submit BLOCKS the producer until the applier drains a slot.
	QueueBound int
	// Concurrent switches the session from the prepare-parallel /
	// apply-sequential pipeline (one strong-mode applier; each group atomic
	// against readers) to a SELF-APPLYING session under the standalone
	// concurrency discipline (§14 concurrent mode — the Lanes:N write door):
	// Submit applies the group on the CALLER thread under the shared read lock
	// + entity/value locks, so N concurrent sessions apply genuinely in
	// parallel. Semantics: Submit is always synchronous and returns the group's
	// apply outcome directly (Sync is implied; WaitApplied on the returned
	// token — Lane != 0 marks it — is already resolved and returns nil); a
	// group is NOT atomic against concurrent readers (per-entity atomicity
	// only); change-log records emit eagerly per store door (gapless and
	// replica-convergent — see store.TxChangeLogScope's concurrency position);
	// QueueBound is ignored (there is no queue). Cross-session TxFrom stamps
	// are only ±ε ordered; per-entity monotonicity holds via entity locks.
	// Labels/rel-types are declared on prepare (an unseen name is registered
	// and persisted during AddNode/AddRelationship), so DeclareLabels /
	// DeclareRelTypes remain a pure warm-up optimization here.
	Concurrent bool
}

// SubmitToken is the (lane, seq) watermark a Submit returns. For an async
// session a reader compares it against AppliedSeq / WaitApplied to achieve
// read-your-writes; for a sync session the ack already guarantees visibility.
type SubmitToken struct {
	Lane uint16
	Seq  uint64
}

// ingestGroup is one submit's worth of prepared intents handed to the applier.
type ingestGroup struct {
	nodes        []pendingNode
	rels         []pendingRel
	nodeUpdates  []pendingNodeUpdate
	relUpdates   []pendingRelUpdate
	nodeDeletes  []types.NodeID
	relDeletes   []types.RelID
	nodeCascades []pendingNodeCascade
	relCascades  []pendingRelCascade
	seqHi        uint64
	// result signals a sync submitter after the group's flush returns. nil for
	// an async submitter (which does not block on apply).
	result chan error
}

func (g *ingestGroup) count() int {
	return len(g.nodes) + len(g.rels) + len(g.nodeUpdates) + len(g.relUpdates) +
		len(g.nodeDeletes) + len(g.relDeletes) + len(g.nodeCascades) + len(g.relCascades)
}

func (g *ingestGroup) empty() bool { return g.count() == 0 }

// ingestApplier is the single-writer apply stage. One per Core, started lazily.
type ingestApplier struct {
	c         *Core
	ch        chan *ingestGroup
	groupSize int

	// enqueueMu is BOTH the seq/order serializer AND the enqueue↔stop fence
	// (C1). An enqueue holds it across seq mint AND the (possibly
	// backpressure-blocking) channel send, so seq order == the single applier's
	// apply order == LSN order. stop() must acquire it EXCLUSIVELY to set
	// `stopping`, so any send that won held the lock strictly BEFORE the fence
	// and is therefore already in the channel when drainRemaining runs — no
	// accepted intent is dropped, and there is no select whose random choice
	// could send-after-close (the F1 bug). Holding the lock across the send is
	// safe precisely because stop() closes quit only AFTER the fence, so the
	// applier keeps draining the channel until every in-flight send lands.
	enqueueMu sync.Mutex
	stopping  bool // guarded by enqueueMu; set by stop() so later enqueues reject
	seqCtr    atomic.Uint64

	quit     chan struct{}
	quitOnce sync.Once
	exited   chan struct{}

	mu         sync.Mutex
	cond       *sync.Cond
	appliedSeq uint64
	stopped    bool
	// failures records, per group watermark (SubmitToken.Seq), the apply error
	// of a REJECTED intent so an async WaitApplied(token) returns the truth
	// instead of nil (C2). Pruned on read (the first WaitApplied for a token
	// consumes it); capped at ingestFailureCap with the oldest evicted
	// (failureDrops counts evictions) so a flood of never-read async failures
	// stays bounded. Guarded by mu.
	failures     map[uint64]error
	failureDrops uint64
}

func newIngestApplier(c *Core, groupSize, queueBound int) *ingestApplier {
	if groupSize <= 0 {
		groupSize = defaultIngestGroupSize
	}
	if queueBound <= 0 {
		queueBound = defaultIngestQueueBound
	}
	a := &ingestApplier{
		c:         c,
		ch:        make(chan *ingestGroup, queueBound),
		groupSize: groupSize,
		quit:      make(chan struct{}),
		exited:    make(chan struct{}),
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// run is the applier loop. It blocks for the first group, coalesces further
// immediately-available groups up to groupSize intents, applies the coalesced
// commit group via one Batch.Execute, then advances the applied watermark.
func (a *ingestApplier) run() {
	defer close(a.exited)
	for {
		select {
		case <-a.quit:
			a.drainRemaining()
			return
		case g := <-a.ch:
			batch := []*ingestGroup{g}
			total := g.count()
			for total < a.groupSize {
				select {
				case g2 := <-a.ch:
					batch = append(batch, g2)
					total += g2.count()
				default:
					goto apply
				}
			}
		apply:
			a.applyCommitGroup(batch)
		}
	}
}

// drainRemaining applies every group still in the channel and then marks the
// applier stopped. By the time run() reaches here, stop() has already set the
// enqueue fence (`stopping`, under enqueueMu) and only then closed quit, so no
// producer can send after this point and every send that WON landed before the
// fence — draining the channel therefore applies all of them (§4.8: accepted
// entity writes are never dropped on shutdown).
func (a *ingestApplier) drainRemaining() {
	for {
		select {
		case g := <-a.ch:
			a.applyCommitGroup([]*ingestGroup{g})
		default:
			a.mu.Lock()
			a.stopped = true
			a.cond.Broadcast()
			a.mu.Unlock()
			return
		}
	}
}

// applyCommitGroup merges the coalesced groups into one BatchBuilder and runs
// the tested apply path (Batch.Execute) — one txMu+mu.Lock, one flush, one
// contiguous LSN run, one PublishBatch. Then it advances the applied watermark
// and acks sync submitters. Reuses the batch applier verbatim rather than
// building a second write path (feasibility §6d).
func (a *ingestApplier) applyCommitGroup(batch []*ingestGroup) {
	bb := &BatchBuilder{g: a.c}
	var maxSeq uint64
	// idToGroup maps each intent's entity ID to its owning group so a per-op
	// Batch.Execute failure (partitionBatchNodesByUnique rejects offenders while
	// committing survivors) maps back to the SubmitToken that carried it (C2a).
	// Entity IDs are unique snowflakes and the node/rel namespaces are disjoint
	// (even vs odd node field), so one map is safe.
	idToGroup := make(map[types.EntityID]*ingestGroup)
	for _, g := range batch {
		bb.nodes = append(bb.nodes, g.nodes...)
		bb.rels = append(bb.rels, g.rels...)
		bb.nodeUpdates = append(bb.nodeUpdates, g.nodeUpdates...)
		bb.relUpdates = append(bb.relUpdates, g.relUpdates...)
		bb.nodeDeletes = append(bb.nodeDeletes, g.nodeDeletes...)
		bb.relDeletes = append(bb.relDeletes, g.relDeletes...)
		bb.nodeCascades = append(bb.nodeCascades, g.nodeCascades...)
		bb.relCascades = append(bb.relCascades, g.relCascades...)
		if g.seqHi > maxSeq {
			maxSeq = g.seqHi
		}
		for _, pn := range g.nodes {
			idToGroup[types.EntityID(pn.node.ID())] = g
		}
		for _, pr := range g.rels {
			idToGroup[types.EntityID(pr.rel.ID())] = g
		}
		for _, pu := range g.nodeUpdates {
			idToGroup[types.EntityID(pu.id)] = g
		}
		for _, pu := range g.relUpdates {
			idToGroup[types.EntityID(pu.id)] = g
		}
		for _, id := range g.nodeDeletes {
			idToGroup[types.EntityID(id)] = g
		}
		for _, id := range g.relDeletes {
			idToGroup[types.EntityID(id)] = g
		}
		for _, pc := range g.nodeCascades {
			idToGroup[types.EntityID(pc.id)] = g
		}
		for _, pc := range g.relCascades {
			idToGroup[types.EntityID(pc.id)] = g
		}
	}

	var result *BatchResult
	var applyErr error
	if bb.nodes != nil || bb.rels != nil || bb.nodeUpdates != nil || bb.relUpdates != nil ||
		bb.nodeDeletes != nil || bb.relDeletes != nil || bb.nodeCascades != nil || bb.relCascades != nil {
		result, applyErr = bb.Execute()
	}

	// Attribute the apply outcome to each group (C2): a rejected intent's error
	// surfaces to ITS submit token, while survivors in the same coalesced batch
	// get nil.
	gerrs := make([]error, len(batch))
	for i, g := range batch {
		gerrs[i] = groupApplyError(g, applyErr, result, idToGroup)
	}

	// Advance the applied watermark strictly AFTER Execute (flush-before-
	// watermark, §9 item 7): a reader that observes AppliedSeq ≥ its token has
	// its write in the committed/overlay-visible state — or, if it was rejected,
	// a recorded failure that WaitApplied returns. The watermark advances even
	// for a rejected group so waiters behind it are never wedged (C2).
	a.mu.Lock()
	if maxSeq > a.appliedSeq {
		a.appliedSeq = maxSeq
	}
	for i, g := range batch {
		if g.result == nil && gerrs[i] != nil {
			a.recordFailureLocked(g.seqHi, gerrs[i])
		}
	}
	a.cond.Broadcast()
	a.mu.Unlock()

	// Ack sync submitters with THEIR group's outcome (nil if their own intents
	// all committed, even when a sibling group in the coalesced batch failed).
	for i, g := range batch {
		if g.result != nil {
			g.result <- gerrs[i]
		}
	}
}

// groupApplyError returns the apply error attributable to one group g. A per-op
// BatchError whose entity ID maps to g is g's error; a group-level BatchError
// (no mappable ID — e.g. a commit-change-log failure) fails EVERY group; a
// whole-batch failure (Execute returned err with no result) fails every group.
// A group whose own intents all committed gets nil, even inside a partially
// failed coalesced batch.
func groupApplyError(g *ingestGroup, applyErr error, result *BatchResult, idToGroup map[types.EntityID]*ingestGroup) error {
	if applyErr == nil {
		return nil
	}
	if result == nil {
		return applyErr
	}
	for _, be := range result.Errors {
		if owner, ok := idToGroup[be.ID]; !ok || owner == g {
			return be.Err
		}
	}
	return nil
}

// recordFailureLocked stores an apply failure keyed by the group's watermark
// (SubmitToken.Seq) for a later async WaitApplied. Caller holds a.mu. Keeps the
// FIRST error for a token; at the cap it evicts the oldest (smallest seq).
func (a *ingestApplier) recordFailureLocked(seq uint64, err error) {
	if err == nil {
		return
	}
	if a.failures == nil {
		a.failures = make(map[uint64]error)
	}
	if _, exists := a.failures[seq]; exists {
		return
	}
	if len(a.failures) >= ingestFailureCap {
		oldest := seq
		first := true
		for s := range a.failures {
			if first || s < oldest {
				oldest, first = s, false
			}
		}
		delete(a.failures, oldest)
		a.failureDrops++
	}
	a.failures[seq] = err
}

// takeFailureLocked returns and REMOVES a recorded failure for seq (prune on
// read: a second WaitApplied for the same token returns nil). Caller holds a.mu.
func (a *ingestApplier) takeFailureLocked(seq uint64) error {
	if a.failures == nil {
		return nil
	}
	if err, ok := a.failures[seq]; ok {
		delete(a.failures, seq)
		return err
	}
	return nil
}

// enqueue mints the group's seq (== the order the single applier processes ==
// LSN order) and hands it to the applier. It is the ONE seam where the
// enqueue↔stop race is resolved (C1): `stopping` is read under enqueueMu — the
// SAME lock stop() takes EXCLUSIVELY to set it — and the channel send is
// UNCONDITIONAL (no select whose random choice could pick the send after
// close). So either
//
//	(a) this enqueue observed stopping==false, minted a seq, and completed its
//	    send while HOLDING enqueueMu — strictly BEFORE stop() could acquire
//	    enqueueMu to fence, hence the group is in the channel before
//	    drainRemaining drains it (applied, never accepted-then-dropped); or
//	(b) it observed stopping==true and rejects CLEANLY with ErrIngestClosed
//	    WITHOUT minting a seq (no seq gap).
//
// A full queue BLOCKS the send here (§4.8 backpressure); because quit is still
// open until stop() has fenced, the applier keeps draining, so a blocked send
// always lands (the producer is woken — applied, not hung). Returns the
// assigned seqHi.
func (a *ingestApplier) enqueue(g *ingestGroup, n int) (uint64, error) {
	a.enqueueMu.Lock()
	defer a.enqueueMu.Unlock()
	if a.stopping {
		return 0, ErrIngestClosed
	}
	seqHi := a.seqCtr.Add(uint64(n)) // #nosec G115 — n is a non-negative intent count
	g.seqHi = seqHi
	a.ch <- g
	return seqHi, nil
}

// currentAppliedSeq returns the highest submit-token sequence the applier has
// PROCESSED (attempted to commit) — not necessarily committed (a rejected group
// still advances it; see AppliedSeq).
func (a *ingestApplier) currentAppliedSeq() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.appliedSeq
}

// waitApplied blocks until the applier has PROCESSED up to seq, then returns
// that token's apply outcome: nil if the group committed, or the intent's real
// apply error if it was REJECTED (C2 truth channel), pruned on read. If the
// applier stops before seq is reached (should not happen for an accepted token —
// C1 applies every accepted intent), it returns a recorded failure if any, else
// ErrIngestClosed.
func (a *ingestApplier) waitApplied(seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.appliedSeq < seq {
		if a.stopped {
			if ferr := a.takeFailureLocked(seq); ferr != nil {
				return ferr
			}
			return ErrIngestClosed
		}
		a.cond.Wait()
	}
	return a.takeFailureLocked(seq)
}

// stop fences new enqueues, lets in-flight sends land, then drains and joins the
// applier (C1). Ordering:
//
//	(1) acquire enqueueMu — BLOCKS until any in-flight enqueue releases it; since
//	    quit is still open, the applier keeps draining the channel, so a
//	    backpressure-blocked send completes and its producer is woken (applied);
//	(2) set stopping so every LATER enqueue rejects cleanly with ErrIngestClosed;
//	(3) release enqueueMu;
//	(4) close quit and join (drainRemaining applies whatever is still buffered).
//
// Steps 1–3 guarantee no send can BEGIN after the fence and every send that WON
// is already in the channel, so drainRemaining applies all of them — no accepted
// intent is dropped, and no producer hangs. Idempotent (quitOnce; a second stop
// re-sets stopping harmlessly and returns once the applier has exited).
func (a *ingestApplier) stop() {
	a.enqueueMu.Lock()
	a.stopping = true
	a.enqueueMu.Unlock()
	a.quitOnce.Do(func() { close(a.quit) })
	<-a.exited
}

// =============================================================================
// Core wiring
// =============================================================================

// ensureIngestApplier lazily starts the applier on first session creation. It
// refuses (ErrGraphClosed) once the graph is closed OR once stopIngestApplier
// has begun (ingestClosing) — both checks under ingestMu, which serializes
// against stopIngestApplier so a session racing Close can never leave an
// orphaned applier running behind the shutdown sweep (C1 lifecycle race).
func (c *Core) ensureIngestApplier(groupSize, queueBound int) (*ingestApplier, error) {
	c.ingestMu.Lock()
	defer c.ingestMu.Unlock()
	if c.closed.Load() || c.ingestClosing {
		return nil, ErrGraphClosed
	}
	if c.ingest == nil {
		a := newIngestApplier(c, groupSize, queueBound)
		c.ingest = a
		go a.run()
	}
	return c.ingest, nil
}

// stopIngestApplier drains and stops the applier. Called at the START of Close
// (before c.closed is set) so every accepted intent is applied while the graph
// is still open (never dropped). Setting ingestClosing under ingestMu — the SAME
// lock ensureIngestApplier holds across its create — is the ordering witness
// that closes the orphaned-applier window: whichever of the two runs second
// under ingestMu sees the other's effect (either this sweep observes the
// concurrently-created applier in c.ingest and stops it, or ensureIngestApplier
// observes ingestClosing and refuses to create one).
func (c *Core) stopIngestApplier() {
	c.ingestMu.Lock()
	c.ingestClosing = true
	a := c.ingest
	c.ingestMu.Unlock()
	if a != nil {
		a.stop()
	}
}

// predeclareVocabulary allocates and persists the given label + rel-type names
// up front (§4.4 pre-declaration) so prepare threads resolve them lookup-only.
func (c *Core) predeclareVocabulary(labels, relTypes []string) error {
	if len(labels) == 0 && len(relTypes) == 0 {
		return nil
	}
	c.registryMu.Lock()
	for _, l := range labels {
		if _, err := c.labels.GetOrCreate(l); err != nil {
			c.registryMu.Unlock()
			return err
		}
	}
	for _, t := range relTypes {
		if _, err := c.relTypes.GetOrCreate(t); err != nil {
			c.registryMu.Unlock()
			return err
		}
	}
	err := c.persistRegistriesIfDirtyLockedPanicSafe()
	c.registryMu.Unlock()
	return err
}

// =============================================================================
// IngestOps — the sub-Core surface behind g.Ingest()
// =============================================================================

// NewSession creates a producer session for the ingest pipeline. Sessions are
// goroutine-parallel: each prepares on its own caller thread. The first session
// lazily starts the single applier goroutine.
func (i *IngestOps) NewSession(opts IngestOptions) (*Session, error) {
	if i == nil || i.c == nil {
		return nil, ErrNilGraph
	}
	c := i.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := c.predeclareVocabulary(opts.DeclareLabels, opts.DeclareRelTypes); err != nil {
		return nil, err
	}
	var (
		a    *ingestApplier
		lane uint16
	)
	if opts.Concurrent {
		// Concurrent mode has no applier: the session self-applies on Submit
		// (§14 concurrent mode). Lane is a nonzero session identifier so tokens
		// are distinguishable from the strong-mode applier's lane 0.
		lane = c.nextIngestLane()
	} else {
		var err error
		a, err = c.ensureIngestApplier(defaultIngestGroupSize, opts.QueueBound)
		if err != nil {
			return nil, err
		}
	}
	b, err := NewBatchBuilder(c)
	if err != nil {
		return nil, err
	}
	// Enable the §4.5 prepare-side pre-encode ONLY on the ingest path — each
	// producer session pre-encodes its node-create wires on its own thread so the
	// applier (strong mode) or the session itself (concurrent mode) patches the
	// temporal tail instead of a second msgpack pass. The plain g.Batch() door
	// never sets this and pays zero new cost. The fast path activates only when
	// the native store implements PreEncodedPutCapability (memory/badger);
	// tiered/wrappers fall back to encode-at-flush.
	b.preEncode = c.preEncodedPut != nil
	return &Session{c: c, a: a, opts: opts, b: b, lane: lane}, nil
}

// nextIngestLane mints a nonzero concurrent-session lane identifier. Lane 0 is
// the strong-mode applier; concurrent lanes wrap within [1, 65535] — the lane
// is an ordering NAMESPACE tag ((epoch, lane, seq) per the ADR), not a scarce
// resource, so reuse after 65535 sessions is harmless.
func (c *Core) nextIngestLane() uint16 {
	for {
		if lane := uint16(c.ingestLaneCtr.Add(1)); lane != 0 {
			return lane
		}
	}
}

// AppliedSeq returns the highest submit-token sequence the applier has PROCESSED
// (attempted to commit). AppliedSeq() ≥ token.Seq means the group has been
// PROCESSED — NOT that it necessarily committed: a rejected intent (e.g. a
// unique-constraint violation) still advances the watermark so it never wedges
// later waiters. For a KNOWN-GOOD async write, comparing AppliedSeq() ≥
// token.Seq is a valid read-your-writes signal (§4.6 at Lanes:1); for a write
// that can be REJECTED, WaitApplied is the truth channel — it returns the
// intent's apply error.
func (i *IngestOps) AppliedSeq() uint64 {
	if i == nil || i.c == nil {
		return 0
	}
	c := i.c
	c.ingestMu.Lock()
	a := c.ingest
	c.ingestMu.Unlock()
	if a == nil {
		return 0
	}
	return a.currentAppliedSeq()
}

// WaitApplied blocks until the applier has PROCESSED the given submit token (its
// lane's appliedSeq ≥ token.Seq), then returns that token's apply outcome: nil
// if the group committed, or the intent's real apply error if it was REJECTED
// (e.g. ErrUniqueViolation — the async failure truth channel, §9 item 7). The
// failure record is retained until the FIRST WaitApplied for its token (pruned
// on read: a second WaitApplied for the same token returns nil) or until Close;
// under a flood of never-read async failures the retention map is capped, so a
// WaitApplied for an evicted token may return nil. Returns ErrIngestClosed if
// the pipeline is closed before the token is reached. A no-op returning nil for
// a zero token.
func (i *IngestOps) WaitApplied(token SubmitToken) error {
	if i == nil || i.c == nil {
		return ErrNilGraph
	}
	c := i.c
	if token.Seq == 0 {
		return nil
	}
	if token.Lane != 0 {
		// Concurrent-session token (§14): Submit applied the group synchronously
		// and returned its outcome — the token is already resolved.
		return nil
	}
	c.ingestMu.Lock()
	a := c.ingest
	c.ingestMu.Unlock()
	if a == nil {
		return ErrIngestClosed
	}
	return a.waitApplied(token.Seq)
}

// =============================================================================
// Session — the producer-side prepare surface
// =============================================================================

// Session is a producer-side handle. Its prepare methods (AddNode, …) validate,
// build property slices, precompute content hashes, and mint snowflake IDs on
// the CALLER thread and accumulate prepared intents; Submit hands the
// accumulated group to the single applier. A session is intended to be driven
// by one goroutine; different sessions prepare in parallel.
type Session struct {
	c    *Core
	a    *ingestApplier // nil in concurrent mode (the session self-applies)
	opts IngestOptions
	lane uint16 // 0 = strong mode (shared applier); nonzero = concurrent session

	mu  sync.Mutex
	b   *BatchBuilder // accumulates prepared intents until Submit
	seq uint64        // concurrent mode: per-session submit counter (under mu)
}

func (s *Session) lockOpen() error {
	if s == nil {
		return ErrNilSession
	}
	s.mu.Lock()
	if s.b == nil {
		s.mu.Unlock()
		return ErrIngestClosed
	}
	return nil
}

// AddNode prepares a node create on the caller thread (validate, build property
// slice, precompute hash, mint ID, resolve/probe label tokens) and accumulates
// it. Returns the prepared node so it can be used as a relationship endpoint.
func (s *Session) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if err := s.lockOpen(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if s.opts.Concurrent {
		// Declare-on-prepare: concurrent apply has no probe-restamp step (that
		// runs under the strong applier's exclusive lock), so unseen labels are
		// registered and persisted NOW — queue-time Lookup then always yields
		// real tokens and the §4.5 pre-encoded buffer stays valid at apply.
		if err := s.ensureDeclaredLabels(labels); err != nil {
			return nil, err
		}
	}
	return s.b.AddNode(labels, props)
}

// ensureDeclaredLabels registers any not-yet-known label names (idempotent,
// persisted). Steady state is lookup-only — the registry write lock is taken
// only when a genuinely new name appears.
func (s *Session) ensureDeclaredLabels(labels []string) error {
	var missing []string
	for _, l := range labels {
		if _, ok := s.c.labels.Lookup(l); !ok {
			missing = append(missing, l)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return s.c.predeclareVocabulary(missing, nil)
}

// AddRelationship prepares a relationship create on the caller thread and
// accumulates it. The endpoint-hash ladder + TxFrom stamp remain apply-side.
func (s *Session) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := s.lockOpen(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if s.opts.Concurrent {
		// Declare-on-prepare (the create kernel would also allocate the type at
		// apply, but declaring here keeps prepare lookup-only in steady state).
		if _, ok := s.c.relTypes.Lookup(typeName); !ok {
			if err := s.c.predeclareVocabulary(nil, []string{typeName}); err != nil {
				return nil, err
			}
		}
	}
	return s.b.AddRelationship(typeName, startNode, endNode, props)
}

// UpdateNode accumulates a node property-delta update (APPLY-dominant: the
// version bump + PrevHash splice happen at apply, §4.2 step 3).
func (s *Session) UpdateNode(id types.NodeID, updates map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.UpdateNode(id, updates)
}

// UpdateRelationship accumulates a relationship property-delta update.
func (s *Session) UpdateRelationship(id types.RelID, updates map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.UpdateRelationship(id, updates)
}

// DeleteNode accumulates a cascade node delete.
func (s *Session) DeleteNode(id types.NodeID) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.DeleteNode(id)
}

// DeleteRelationship accumulates a relationship delete.
func (s *Session) DeleteRelationship(id types.RelID) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.DeleteRelationship(id)
}

// Pending reports the number of prepared-but-not-submitted intents.
func (s *Session) Pending() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.b == nil {
		return 0
	}
	return batchIntentCount(s.b)
}

// Submit hands the accumulated intents to the applier as one group and resets
// the session for further prepares. A Sync session blocks until the group has
// been applied and is visible (ack ⇒ read-your-writes on any goroutine); an
// async session returns immediately with the (lane, seq) watermark. Submitting
// an empty session is a no-op returning a zero token.
func (s *Session) Submit() (SubmitToken, error) {
	if err := s.lockOpen(); err != nil {
		return SubmitToken{}, err
	}
	b := s.b
	g := &ingestGroup{
		nodes:        b.nodes,
		rels:         b.rels,
		nodeUpdates:  b.nodeUpdates,
		relUpdates:   b.relUpdates,
		nodeDeletes:  b.nodeDeletes,
		relDeletes:   b.relDeletes,
		nodeCascades: b.nodeCascades,
		relCascades:  b.relCascades,
	}
	if g.empty() {
		s.mu.Unlock()
		return SubmitToken{}, nil
	}
	// Reset the session's builder for the next batch of prepares by clearing its
	// slices IN PLACE. We deliberately do NOT call NewBatchBuilder here: it
	// re-checks the graph and would surface ErrGraphClosed on a Close race
	// BEFORE the group reaches enqueue — silently dropping the captured group
	// instead of letting enqueue make the single applied-or-ErrIngestClosed
	// decision (C1). The captured g already aliases the OLD backing arrays;
	// assigning fresh nil slices here means later prepares append into brand-new
	// arrays and never realloc into g's — the same isolation NewBatchBuilder
	// provided, without the close-check.
	b.nodes = nil
	b.rels = nil
	b.nodeUpdates = nil
	b.relUpdates = nil
	b.nodeDeletes = nil
	b.relDeletes = nil
	b.nodeCascades = nil
	b.relCascades = nil

	if s.opts.Concurrent {
		// Concurrent mode (§14): self-apply on the caller thread under the
		// standalone concurrency discipline. Always synchronous — the returned
		// token is already resolved (WaitApplied on it returns nil); the apply
		// outcome IS the return value. N sessions apply genuinely in parallel.
		s.seq++
		token := SubmitToken{Lane: s.lane, Seq: s.seq}
		s.mu.Unlock()
		return token, s.c.applyIngestGroupConcurrent(g)
	}

	sync := s.opts.Sync
	s.mu.Unlock()

	n := g.count()
	if sync {
		g.result = make(chan error, 1)
	}
	seq, err := s.a.enqueue(g, n)
	if err != nil {
		return SubmitToken{}, err
	}
	token := SubmitToken{Lane: 0, Seq: seq}
	if sync {
		if applyErr := <-g.result; applyErr != nil {
			return token, applyErr
		}
	}
	return token, nil
}

// Close submits any accumulated intents (respecting the session's sync mode)
// and releases the session. It does NOT stop the shared applier — that lives
// with the graph and is stopped by Graph.Close.
func (s *Session) Close() error {
	if s == nil {
		return ErrNilSession
	}
	s.mu.Lock()
	empty := s.b == nil || batchIntentCount(s.b) == 0
	s.mu.Unlock()
	var err error
	if !empty {
		_, err = s.Submit()
	}
	s.mu.Lock()
	s.b = nil
	s.mu.Unlock()
	return err
}

// batchIntentCount counts prepared intents in a builder (session-side only).
func batchIntentCount(b *BatchBuilder) int {
	return len(b.nodes) + len(b.rels) + len(b.nodeUpdates) + len(b.relUpdates) +
		len(b.nodeDeletes) + len(b.relDeletes) + len(b.nodeCascades) + len(b.relCascades)
}
