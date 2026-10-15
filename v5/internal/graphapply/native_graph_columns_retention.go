package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeColumnTerminalAttempts = 4

const (
	nativeColumnPreparing byte = iota + 1
	nativeColumnPinPending
	nativeColumnPinLive
	nativeColumnRetiring
	nativeColumnRetired
)

// This journal is an owned recovery recipe, never native pin authority. Every
// reopen authenticates the exact original through its original logical keepers.
// The reserved mutable row includes all later token and terminal states.
type nativeColumnRetentionRecord struct {
	Version               byte
	State                 byte
	Generation            nativeColumnGeneration
	Seed, Original, Token []byte
	TerminalAttempts      uint8
	Feed                  [2]uint64
	Dependencies          nativeColumnDependencyProgress `json:",omitzero"` // fresh policy only; old journal bytes unchanged
}

// An owned crash-recovery cursor, never a native pin or proof of a current
// issuer. Every resumed page still uses the exact authenticated native cuts.
type nativeColumnDependencyProgress struct {
	Version                        byte
	Seed                           [32]byte
	Positions, Feed                [2]uint64
	AttemptedPages, CompletedPages uint64
	Frames                         uint64
	EOF                            bool
}

// The actual source context classifies a constructor failure BEFORE Close.
// A shorter RPC deadline or explicit cancellation is not a full TTL expiry.
// This is diagnostic accounting, not replacement source/pin authority.
type nativeColumnCreationFailure struct {
	Failure                            *nativeColumnSourceFailure
	IssuedUnixNanos, DeadlineUnixNanos int64
	FullTTL, Expired                   bool
}

func (e *nativeColumnCreationFailure) Error() string {
	return "native-column: interrupted fresh retained creation"
}
func (e *nativeColumnCreationFailure) Unwrap() error {
	if e == nil || e.Failure == nil {
		return nil
	}
	return e.Failure
}

func nativeColumnCreationInterruption(_ context.Context, source *nativeColumnSource) *nativeColumnCreationFailure {
	if source == nil || source.binding.Policy.Version != 1 {
		return nil
	}
	deadline, bounded := source.ctx.Deadline()
	// The actual constructor captures issuance and whether its immutable TTL
	// preceded its parent. A later caller cannot infer or replace that lifetime.
	f := &nativeColumnCreationFailure{IssuedUnixNanos: source.issued.UnixNano(), FullTTL: source.fullTTL, Expired: errors.Is(source.ctx.Err(), context.DeadlineExceeded)}
	if bounded {
		f.DeadlineUnixNanos = deadline.UnixNano()
	}
	return f
}

type nativeColumnRetentionLease struct {
	store       *nativeColumnStore
	id          [16]byte
	digest      [32]byte
	generations [2]uint64 // original genuine capture, not the current bank number
}

func (r *nativeColumnRetentionLease) detach(owner [16]byte) {
	if r == nil || r.store == nil {
		return
	}
	r.store.mu.Lock()
	delete(r.store.retained, owner)
	r.store.mu.Unlock()
}

func (s *nativeColumnSource) retainedBinding() error {
	if s == nil || s.coordinator == nil || s.coordinator.host == nil || s.coordinator.host.machine == nil || !durableColumnGraphAgreement(s.coordinator.host.machine.SemanticContractID()) {
		return errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(s.ctx); err != nil {
		return err
	}
	s.binding.Version = 2
	s.binding.TerminalAttempts = nativeColumnTerminalAttempts
	s.binding.Agreement = sha256.Sum256([]byte("rho-tkg:V2:durable-native-change-columns:v2\x00exact-initial-final;retained-original-keeper;dependency-min;explicit-generation-retirement\x00"))
	for g := range 2 {
		// A configured voter is a route, not the logical keeper's identity. V1 keeps
		// its existing voter-bound encoding. V2 still authenticates the actual route
		// and complete scope/configuration on every capture.
		scope, err := encodeAllocationScope(s.initial[g].scope)
		if err != nil {
			return err
		}
		s.binding.Peers[g], err = boundedEncoding(256, func(w *boundedWriter) { w.add([]byte("NCP2")); w.field(scope) })
		if err != nil {
			return err
		}
	}
	wire, err := nativeColumnMetadata(s.binding)
	if err == nil {
		s.digest = sha256.Sum256(wire)
	}
	return err
}

func nativeColumnRetentionKey(id [16]byte) []byte { return nativeColumnKey(id, "retention") }

func (s *nativeColumnStore) retentionRecord(id [16]byte) (nativeColumnRetentionRecord, error) {
	var r nativeColumnRetentionRecord
	if s == nil || s.binding.Version != 2 || s.binding.TerminalAttempts != nativeColumnTerminalAttempts || id == ([16]byte{}) {
		return r, errInvalid
	}
	wire, err := s.get(nativeColumnRetentionKey(id), nativeColumnMetadataBytes)
	if err != nil {
		return r, err
	}
	if err := nativeColumnDecodeMetadata(wire, &r); err != nil {
		return r, err
	}
	if r.Version != 1 || r.TerminalAttempts > s.binding.TerminalAttempts || r.State < nativeColumnPreparing || r.State > nativeColumnRetired || r.Generation.ID != id || r.Generation.Binding != s.digest || r.Generation.Coverage != s.binding.Coverage {
		return r, errCorrupt
	}
	seed, err := decodeGraphPinManifest(r.Seed)
	if err != nil || len(seed.parts) != 2 {
		return r, errors.Join(errCorrupt, err)
	}
	for g, boundary := range r.Generation.Sources {
		first, err := decodeGraphCertificate(boundary.Initial)
		if err != nil {
			return r, err
		}
		final, err := decodeGraphCertificate(boundary.Final)
		if err != nil || boundary.Generation == 0 || first != seed.parts[g].certificate || first.scope != final.scope || first.configuration != final.configuration || first.sourceIndex > final.sourceIndex || first.round > final.round {
			return r, errors.Join(errCorrupt, err)
		}
		if r.State != nativeColumnPreparing && (r.Feed[g] == 0 || r.Feed[g] > first.sourceIndex+1) {
			return r, errCorrupt
		}
	}
	if err := s.validateDependencies(r); err != nil {
		return r, err
	}
	if r.State == nativeColumnPreparing && len(r.Original) == 0 {
		if len(r.Token) != 0 || r.Generation.Pin != ([32]byte{}) || r.Feed != ([2]uint64{}) {
			return r, errCorrupt
		}
		return r, nil
	}
	original, err := decodeGraphPinManifest(r.Original)
	if err != nil || original.id() != seed.id() || original.publisher != seed.publisher || original.issuer != seed.issuer || original.request != seed.request || original.epoch != seed.epoch || original.expires != seed.expires || len(original.parts) != 2 || sha256.Sum256(r.Original) != r.Generation.Pin {
		return r, errors.Join(errCorrupt, err)
	}
	for g, part := range original.parts {
		if part.certificate != seed.parts[g].certificate || part.feedStart != r.Feed[g] {
			return r, errCorrupt
		}
	}
	if len(r.Token) > 0 {
		token, err := decodeRetainedPinToken(r.Token)
		if err != nil || !bytes.Equal(token.manifest, r.Original) {
			return r, errors.Join(errCorrupt, err)
		}
	} else if r.State == nativeColumnPinLive {
		return r, errCorrupt
	}
	return r, nil
}

func (s *nativeColumnStore) validateDependencies(r nativeColumnRetentionRecord) error {
	d := r.Dependencies
	if s.binding.Policy.Version == 0 {
		if d != (nativeColumnDependencyProgress{}) {
			return errCorrupt
		}
		return nil
	}
	if s.binding.Policy.Version != 1 || d.Version != 1 || d.Seed != sha256.Sum256(r.Seed) || d.CompletedPages > d.AttemptedPages || d.AttemptedPages > s.binding.Policy.MaxWorkRecords || d.CompletedPages > s.binding.Policy.MaxChunks+1 {
		return errCorrupt
	}
	var frames uint64
	for g, boundary := range r.Generation.Sources {
		first, err := decodeGraphCertificate(boundary.Initial)
		if err != nil {
			return err
		}
		final, err := decodeGraphCertificate(boundary.Final)
		if err != nil {
			return err
		}
		end := first.sourceIndex
		if r.Generation.Coverage&(1<<g) != 0 {
			end = final.sourceIndex
		}
		if d.Positions[g] < first.sourceIndex || d.Positions[g] > end || d.Feed[g] == 0 || d.Feed[g] > first.sourceIndex+1 || !nativeColumnAdd(frames, d.Positions[g]-first.sourceIndex) {
			return errCorrupt
		}
		frames += d.Positions[g] - first.sourceIndex
		if r.Generation.Coverage&(1<<g) == 0 && d.Feed[g] != first.sourceIndex+1 {
			return errCorrupt
		}
		if d.EOF && d.Positions[g] != end {
			return errCorrupt
		}
		if (r.State != nativeColumnPreparing || len(r.Original) != 0) && r.State < nativeColumnRetiring && (!d.EOF || d.Feed[g] != r.Feed[g]) {
			return errCorrupt
		}
	}
	if d.CompletedPages > ^uint64(0)/uint64(s.binding.Policy.ChunkRecords) || frames > d.CompletedPages*uint64(s.binding.Policy.ChunkRecords) || frames != d.Frames || d.CompletedPages == 0 && frames != 0 || d.CompletedPages > 0 && d.Frames == 0 && !d.EOF || d.EOF && d.CompletedPages == 0 || d.EOF && d.CompletedPages-1 > frames {
		return errCorrupt
	}
	return nil
}

func (s *nativeColumnStore) saveRetention(r nativeColumnRetentionRecord, progress *nativeColumnProgress) error {
	wire, err := nativeColumnMetadata(r)
	if err != nil {
		return err
	}
	writes := []nativeColumnPut{{nativeColumnRetentionKey(r.Generation.ID), wire}}
	if progress != nil {
		p, err := nativeColumnMetadata(*progress)
		if err != nil {
			return err
		}
		writes = append(writes, nativeColumnPut{nativeColumnKey(r.Generation.ID, "progress"), p})
	}
	return s.commit(writes)
}

func (s *nativeColumnStore) prepareRetention(id [16]byte, source *nativeColumnSource, seed []byte) (nativeColumnRetentionRecord, error) {
	if s == nil || source == nil || s.binding.Version != 2 || source.digest != s.digest {
		return nativeColumnRetentionRecord{}, errInvalid
	}
	if prior, err := s.retentionRecord(id); err == nil {
		return prior, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return prior, err
	}
	generation, err := source.generation(id)
	if err != nil {
		return nativeColumnRetentionRecord{}, err
	}
	r := nativeColumnRetentionRecord{Version: 1, State: nativeColumnPreparing, Generation: generation, Seed: seed}
	if s.binding.Policy.Version == 1 {
		r.Dependencies = nativeColumnDependencyProgress{Version: 1, Seed: sha256.Sum256(seed), Positions: [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}, Feed: [2]uint64{source.initial[0].sourceIndex + 1, source.initial[1].sourceIndex + 1}}
	}
	ledger, err := s.ledger()
	if err != nil {
		return r, err
	}
	// Prepay progress, manifest AND the largest mutable owner/token/terminal row.
	// No terminal transition needs further derived-store capacity.
	reserved := uint64(3*nativeColumnMetadataBytes + len(nativeColumnRetentionKey(id)) + len(nativeColumnKey(id, "progress")) + len(nativeColumnKey(id, "manifest")))
	if ledger.Generations == s.binding.Policy.MaxGenerations || reserved > s.binding.Policy.MaxRetainedBytes-ledger.Bytes {
		return r, errLimit
	}
	p := nativeColumnProgress{Generation: generation, Positions: [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}}
	terminal, err := nativeColumnRetentionWork(source.coordinator.host.machine.limits).scaled(uint64(s.binding.TerminalAttempts))
	if err != nil || !source.charged.admit(terminal, s.binding.Policy) {
		return r, errors.Join(errLimit, err)
	}
	p.Work, p.Owner = source.charged.add(terminal), source.owner
	ledger.Bytes += reserved
	ledger.Generations++
	a, err := nativeColumnMetadata(r)
	if err != nil {
		return r, err
	}
	b, err := nativeColumnMetadata(p)
	if err != nil {
		return r, err
	}
	c, err := nativeColumnMetadata(ledger)
	if err != nil {
		return r, err
	}
	err = s.commit([]nativeColumnPut{{nativeColumnRetentionKey(id), a}, {nativeColumnKey(id, "progress"), b}, {[]byte("ledger"), c}})
	return r, err
}

// Scan the genuine complete prefix while its source views remain registered.
// Each page is durably charged BEFORE native reads, including failed attempts.
// Replay originals are hydrated by source.frame; an already collected original
// refuses here rather than becoming an inferred no-op or unprotected column.
func (s *nativeColumnStore) retentionDependencies(ctx context.Context, id [16]byte, source *nativeColumnSource) ([2]uint64, error) {
	if s == nil || source == nil || ctx == nil {
		return [2]uint64{}, errInvalid
	}
	if s.binding.Policy.Version == 1 {
		for {
			feed, complete, err := s.retentionDependencyPage(ctx, id, source)
			if err != nil || complete {
				return feed, err
			}
		}
	}
	feed := [2]uint64{source.initial[0].sourceIndex + 1, source.initial[1].sourceIndex + 1}
	positions := [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}
	for pages := uint64(0); pages <= s.binding.Policy.MaxChunks; pages++ {
		p, err := s.progress(id)
		if err != nil {
			return feed, err
		}
		if err := s.reserve(&p, source); err != nil {
			return feed, err
		}
		frames, next, err := source.page(ctx, positions)
		if err != nil {
			return feed, err
		}
		for _, frame := range frames {
			group := source.frameGroup(frame.Partition)
			if group < 0 {
				return feed, errCorrupt
			}
			if len(frame.OriginalOutcome) > 0 {
				index, err := nativeColumnOriginalIndex(frame.OriginalOutcome, source.hosts[group].machine.ns)
				if err != nil || index == 0 || index > frame.Index {
					return feed, errors.Join(errCorrupt, err)
				}
				feed[group] = min(feed[group], index)
			}
		}
		if len(frames) == 0 {
			if next != source.finalPositions() {
				return feed, errCorrupt
			}
			return feed, nil
		}
		if next == positions {
			return feed, errCorrupt
		}
		positions = next
	}
	return feed, errLimit
}

// One whole native page is charged durably before any source reads. Only a
// successful complete hydration advances the dependency cursor/minimum. An
// expired constructor can therefore close and reopen the SAME generation and
// continue this prefix rather than replaying the entire prepass from zero.
func (s *nativeColumnStore) retentionDependencyPage(ctx context.Context, id [16]byte, source *nativeColumnSource) ([2]uint64, bool, error) {
	if s == nil || source == nil || ctx == nil || s.binding.Policy.Version != 1 || source.digest != s.digest || source.binding.Version != 2 {
		return [2]uint64{}, false, errInvalid
	}
	r, err := s.retentionRecord(id)
	if err != nil {
		return [2]uint64{}, false, err
	}
	d := r.Dependencies
	if r.State != nativeColumnPreparing || len(r.Original) != 0 {
		return d.Feed, false, errInvalid
	}
	for g := range 2 {
		first, err := encodeGraphCertificate(source.initial[g])
		if err != nil {
			return d.Feed, false, err
		}
		final, err := encodeGraphCertificate(source.final[g])
		if err != nil || !bytes.Equal(first, r.Generation.Sources[g].Initial) || !bytes.Equal(final, r.Generation.Sources[g].Final) {
			return d.Feed, false, errors.Join(errInvalid, err)
		}
	}
	if d.EOF {
		return d.Feed, true, nil
	}
	if d.CompletedPages > s.binding.Policy.MaxChunks || d.AttemptedPages == ^uint64(0) {
		return d.Feed, false, errLimit
	}
	p, err := s.progress(id)
	if err != nil {
		return d.Feed, false, err
	}
	grant := source.grant
	if p.Owner != source.owner {
		grant, err = grant.plus(source.openWork)
		if err != nil {
			return d.Feed, false, err
		}
	}
	if !p.Work.admit(grant, s.binding.Policy) {
		return d.Feed, false, errLimit
	}
	p.Work, p.Owner = p.Work.add(grant), source.owner
	r.Dependencies.AttemptedPages++
	if err := s.saveRetention(r, &p); err != nil {
		return d.Feed, false, err
	}
	frames, next, err := source.page(ctx, d.Positions)
	if err != nil {
		return d.Feed, false, err
	}
	for _, frame := range frames {
		group := source.frameGroup(frame.Partition)
		if group < 0 {
			return d.Feed, false, errCorrupt
		}
		if len(frame.OriginalOutcome) > 0 {
			index, err := nativeColumnOriginalIndex(frame.OriginalOutcome, source.hosts[group].machine.ns)
			if err != nil || index == 0 || index > frame.Index {
				return d.Feed, false, errors.Join(errCorrupt, err)
			}
			d.Feed[group] = min(d.Feed[group], index)
		}
	}
	if len(frames) == 0 {
		if next != source.finalPositions() {
			return d.Feed, false, errCorrupt
		}
		d.EOF = true
	} else if next == d.Positions {
		return d.Feed, false, errCorrupt
	}
	if !nativeColumnAdd(d.Frames, uint64(len(frames))) || d.CompletedPages == ^uint64(0) {
		return d.Feed, false, errLimit
	}
	d.Frames += uint64(len(frames)) // #nosec G115 -- bounded nonnegative page count, checked addition.
	d.Positions, d.CompletedPages, d.AttemptedPages = next, d.CompletedPages+1, r.Dependencies.AttemptedPages
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return d.Feed, false, err
	}
	r.Dependencies = d
	if err := s.validateDependencies(r); err != nil {
		return d.Feed, false, err
	}
	if err := s.saveRetention(r, nil); err != nil {
		return d.Feed, false, err
	}
	return d.Feed, d.EOF, nil
}

func nativeColumnOriginalIndex(wire []byte, ns namespace) (uint64, error) {
	if bytes.HasPrefix(wire, []byte("GFO\x01")) {
		r, e := decodeRetainedFeedResult(wire)
		return r.index, e
	}
	if bytes.HasPrefix(wire, []byte("GPO\x01")) {
		r, e := decodeGraphPinResult(wire)
		return r.index, e
	}
	if bytes.HasPrefix(wire, []byte("GGO\x01")) {
		r, e := decodeGraphRetentionResult(wire)
		return r.index, e
	}
	if bytes.HasPrefix(wire, []byte("GCS\x01")) {
		r, err := decodeCrossStatus(wire)
		if err != nil || r.ns != ns {
			return 0, errors.Join(errCorrupt, err)
		}
		return r.index, nil
	}
	r, err := decodePartitionGraphOutcome(wire, ns)
	if err != nil {
		return 0, err
	}
	return r.index, nil
}

func (s *nativeColumnStore) liveRetention(id [16]byte, source *nativeColumnSource) error {
	if s.binding.Version != 2 {
		return nil
	}
	if source == nil || source.retention == nil || source.retention.store != s || source.retention.id != id {
		return errInvalid
	}
	r, err := s.retentionRecord(id)
	if err != nil {
		return err
	}
	if r.State >= nativeColumnRetiring {
		return errGraphPinReleased
	}
	if r.State != nativeColumnPinLive {
		return errRetainedPinPending
	}
	if source.retention.digest != r.Generation.Pin {
		return errInvalid
	}
	s.mu.Lock()
	registered := s.retained[source.owner] == id
	s.mu.Unlock()
	if !registered {
		return errInvalid
	}
	return nil
}

// The registered hydration handle supplies local lifetime exclusion, not pin
// authority. Authentication/publication must already have completed before this
// association is made. The original generation's physical capture coordinates
// remain descriptive across an authentic native bank/snapshot reactivation.
func (s *nativeColumnStore) attachRetention(source *nativeColumnSource, r nativeColumnRetentionRecord) error {
	if source == nil || r.State != nativeColumnPinLive || r.Generation.Binding != source.digest {
		return errInvalid
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if err := source.check(source.ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.poison != nil {
		return errors.Join(errInvalid, s.poison)
	}
	if s.active {
		return errLimit
	}
	actual, err := s.retentionRecord(r.Generation.ID)
	if err != nil {
		return err
	}
	if actual.State != nativeColumnPinLive || !bytes.Equal(actual.Token, r.Token) || actual.Generation.Pin != r.Generation.Pin {
		return errGraphPinReleased
	}
	if s.retained == nil {
		s.retained = make(map[[16]byte][16]byte)
	}
	if _, exists := s.retained[source.owner]; exists {
		return errInvalid
	}
	source.retention = &nativeColumnRetentionLease{store: s, id: r.Generation.ID, digest: r.Generation.Pin, generations: [2]uint64{r.Generation.Sources[0].Generation, r.Generation.Sources[1].Generation}}
	s.retained[source.owner] = r.Generation.ID
	return nil
}

// The caller holds the store's begin/end gate through the complete attempt.
func (s *nativeColumnStore) prepareRetirement(id [16]byte) (nativeColumnRetentionRecord, error) {
	r, err := s.retentionRecord(id)
	if err != nil {
		return r, err
	}
	s.mu.Lock()
	held := false
	for _, generation := range s.retained {
		if generation == id {
			held = true
			break
		}
	}
	s.mu.Unlock()
	if held {
		return r, errLimit
	}
	if r.State == nativeColumnRetired || r.State == nativeColumnRetiring {
		return r, nil
	}
	if r.State == nativeColumnPreparing && len(r.Original) == 0 {
		r.Original = bytes.Clone(r.Seed)
		r.Generation.Pin = sha256.Sum256(r.Original)
		seed, err := decodeGraphPinManifest(r.Seed)
		if err != nil {
			return r, err
		}
		for g, part := range seed.parts {
			r.Feed[g] = part.feedStart
		}
	}
	r.State = nativeColumnRetiring
	return r, s.saveRetention(r, nil)
}

// Each pin invocation has its ordinary finite remote ledger. Its complete
// possible source/output/RPC allowance is PREPAID from this generation before
// it is created; fresh invocations never reset the durable work counters.
func nativeColumnRetentionWork(l materializerLimits) nativeColumnWork {
	return nativeColumnWork{Records: uint64(l.sourceRows), Bytes: uint64(l.sourceBytes), OutputBytes: uint64(l.outputBytes), RPCs: 1024, WireBytes: 2 * 1024 * uint64(graphSourceFrameBytes)}
}
func (s *nativeColumnStore) fundRetention(id [16]byte, source *nativeColumnSource, l materializerLimits) error {
	grant := nativeColumnRetentionWork(l)
	p, err := s.progress(id)
	if err != nil {
		return err
	}
	if source != nil {
		source.mu.Lock()
		defer source.mu.Unlock()
		if err := source.check(source.ctx); err != nil {
			return err
		}
		if !source.charged.admit(grant, source.binding.Policy) {
			return errLimit
		}
	}
	if !p.Work.admit(grant, s.binding.Policy) {
		return errLimit
	}
	p.Work = p.Work.add(grant)
	wire, err := nativeColumnMetadata(p)
	if err != nil {
		return err
	}
	if err := s.commit([]nativeColumnPut{{nativeColumnKey(id, "progress"), wire}}); err != nil {
		return err
	}
	if source != nil {
		source.charged = source.charged.add(grant)
		if source.meter != nil {
			return source.meter.fund(grant)
		}
	}
	return nil
}

// A pending local generation is reserved before the dependency prepass. No pin
// Publish is attempted until its exact issued original is durably journaled.
func createDurablePartitionNativeColumns(ctx context.Context, dir string, fs vfs.FS, c *crossProcessCoordinator, id [16]byte, initial, final graphCut, policy nativeColumnPolicy, expires uint64) (store *nativeColumnStore, source *nativeColumnSource, err error) {
	if ctx == nil || id == ([16]byte{}) || c == nil || c.host == nil || c.host.machine == nil || !durableColumnGraphAgreement(c.host.machine.SemanticContractID()) {
		return nil, nil, errInvalid
	}
	source, err = newPartitionNativeColumnSource(ctx, c, initial, final, policy)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			creation := nativeColumnCreationInterruption(ctx, source)
			source.mu.Lock()
			charged := source.charged
			source.mu.Unlock()
			err = errors.Join(err, source.Close())
			if store != nil {
				err = errors.Join(err, store.Close())
			}
			failure := &nativeColumnSourceFailure{Charged: charged, Cause: err}
			if source.meter != nil {
				failure.RPCs, failure.RequestBytes, failure.ReplyBytes = source.meter.snapshot()
			}
			err = failure
			if creation != nil {
				creation.Failure = failure
				err = creation
			}
			store, source = nil, nil
		}
	}()
	if err = source.retainedBinding(); err != nil {
		return store, source, err
	}
	publisher, err := newRetainedPinPublisher(source.coordinator)
	if err != nil {
		return store, source, err
	}
	// Constructor work is explicitly bounded before effects. Successful creation
	// records it below; failures retain it in the source failure receipt.
	grant := nativeColumnRetentionWork(c.host.machine.limits)
	if !source.charged.admit(grant, policy) {
		return store, source, errLimit
	}
	source.charged = source.charged.add(grant)
	if err = source.meter.fund(grant); err != nil {
		return store, source, err
	}
	// Generation IDs are directory-local. Independent stores on this same
	// Host/session must never share native ownership or each other's Release.
	pinRequest, err := graphReadNonce()
	if err != nil {
		return store, source, err
	}
	request, err := publisher.Begin(ctx, requestID(pinRequest), graphReadRequest{mode: graphReadAt, cut: initial}, expires)
	if err != nil {
		return store, source, err
	}
	seed, err := request.Original()
	if err != nil {
		return store, source, err
	}
	store, err = openNativeColumnStore(dir, fs, true, source)
	if err != nil {
		return store, source, err
	}
	r, err := store.prepareRetention(id, source, seed)
	if err != nil {
		return store, source, err
	}
	if err = store.completeRetentionCreation(ctx, source, publisher, request, &r); err != nil {
		return store, source, err
	}
	return store, source, nil
}

func (s *nativeColumnStore) completeRetentionCreation(ctx context.Context, source *nativeColumnSource, publisher *retainedPinPublisher, request *retainedPinRequest, r *nativeColumnRetentionRecord) error {
	if r.State == nativeColumnPreparing {
		feed, err := s.retentionDependencies(ctx, r.Generation.ID, source)
		if err != nil {
			return err
		}
		if s.binding.Policy.Version == 1 {
			latest, err := s.retentionRecord(r.Generation.ID)
			if err != nil || latest.State != nativeColumnPreparing || len(latest.Original) != 0 || !bytes.Equal(latest.Seed, r.Seed) || !nativeColumnSame(latest.Generation, r.Generation) || !latest.Dependencies.EOF || latest.Dependencies.Feed != feed {
				return errors.Join(errCorrupt, err)
			}
			*r = latest
		}
		if err := request.columnFeeds(feed); err != nil {
			return err
		}
		original, err := request.Original()
		if err != nil {
			return err
		}
		r.Original, r.Feed, r.Generation.Pin = original, feed, sha256.Sum256(original)
		// Preserve the exact original body before any uncertain native issuance call.
		// Preparing may carry this recipe, but it is never called a live generation.
		p, err := s.progress(r.Generation.ID)
		if err != nil {
			return err
		}
		p.Generation = r.Generation
		if err := s.saveRetention(*r, &p); err != nil {
			return err
		}
		if err := s.fundRetention(r.Generation.ID, source, publisher.coordinator.host.machine.limits); err != nil {
			return err
		}
		if err := request.Issue(ctx, publisher); err != nil {
			return err
		}
		r.State = nativeColumnPinPending
		if err := s.saveRetention(*r, nil); err != nil {
			return err
		}
	}
	if r.State != nativeColumnPinPending {
		return errInvalid
	}
	if err := s.fundRetention(r.Generation.ID, source, publisher.coordinator.host.machine.limits); err != nil {
		return err
	}
	token, err := request.Publish(ctx)
	if err != nil {
		return err
	}
	wire, err := encodeRetainedPinToken(token)
	if err != nil {
		return err
	}
	r.Token, r.State = wire, nativeColumnPinLive
	if err := s.saveRetention(*r, nil); err != nil {
		return err
	}
	return s.attachRetention(source, *r)
}

// Local metadata opens before hydration, but authentic original keepers—not the
// journal, token, manifest or cache—must recover publication and admit the pin.
func reopenDurablePartitionNativeColumns(ctx context.Context, dir string, fs vfs.FS, c *crossProcessCoordinator, id [16]byte, policy nativeColumnPolicy) (store *nativeColumnStore, source *nativeColumnSource, err error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || !durableColumnGraphAgreement(c.host.machine.SemanticContractID()) || id == ([16]byte{}) {
		return nil, nil, errInvalid
	}
	store, err = openNativeColumnStoreBinding(dir, fs, false, nativeColumnBinding{Policy: policy}, [32]byte{})
	if err != nil {
		return nil, nil, err
	}
	return reopenDurablePartitionNativeColumnsStore(ctx, store, c, id, policy, true)
}

// The fresh live-query owner can renew only its transient source while keeping
// the SAME dirlocked store, durable original and genuine private read cursor.
// The caller exclusively owns this store through construction or its query
// gate. No consumer callback or Host mutex spans the authenticated native calls.
// Constructor ownership closes the store on error; an existing query retains
// its already owned store and attempted-work journal for bounded recovery.
func reopenDurablePartitionNativeColumnsStore(ctx context.Context, store *nativeColumnStore, c *crossProcessCoordinator, id [16]byte, policy nativeColumnPolicy, constructor bool) (result *nativeColumnStore, source *nativeColumnSource, err error) {
	if ctx == nil || store == nil || c == nil || c.host == nil || c.host.machine == nil || !durableColumnGraphAgreement(c.host.machine.SemanticContractID()) || id == ([16]byte{}) || !nativeColumnSame(store.binding.Policy, policy) {
		return nil, nil, errInvalid
	}
	defer func() {
		if err != nil {
			creation := nativeColumnCreationInterruption(ctx, source)
			var charged nativeColumnWork
			if source != nil {
				source.mu.Lock()
				charged = source.charged
				source.mu.Unlock()
				err = errors.Join(err, source.Close())
			}
			if constructor {
				err = errors.Join(err, store.Close())
			}
			if creation != nil {
				creation.Failure = &nativeColumnSourceFailure{Charged: charged, Cause: err}
				if source.meter != nil {
					creation.Failure.RPCs, creation.Failure.RequestBytes, creation.Failure.ReplyBytes = source.meter.snapshot()
				}
				err = creation
			}
			result, source = nil, nil
		}
	}()
	r, err := store.retentionRecord(id)
	if err != nil {
		return store, nil, err
	}
	if r.State >= nativeColumnRetiring {
		return store, nil, errGraphPinReleased
	}
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		return store, nil, err
	}
	if err := store.fundRetention(id, nil, c.host.machine.limits); err != nil {
		return store, nil, err
	}
	original := r.Original
	if len(original) == 0 {
		original = r.Seed
	}
	request, err := publisher.Resume(ctx, original)
	if err != nil {
		return store, nil, err
	}
	if r.State == nativeColumnPreparing && len(r.Original) > 0 {
		if err := request.Issue(ctx, publisher); err != nil {
			return store, nil, err
		}
		r.State = nativeColumnPinPending
		if err := store.saveRetention(r, nil); err != nil {
			return store, nil, err
		}
		if err := store.fundRetention(id, nil, c.host.machine.limits); err != nil {
			return store, nil, err
		}
	}
	var held *certifiedGraphRead
	if r.State != nativeColumnPreparing {
		// Native issuance closes the all-Publish-absent crash window. Publish then
		// recovers immutable originals and performs its real common-live pass.
		token, e := request.Publish(ctx)
		if e != nil {
			return store, nil, e
		}
		tokenWire, e := encodeRetainedPinToken(token)
		if e != nil {
			return store, nil, e
		}
		if len(r.Token) > 0 && !bytes.Equal(r.Token, tokenWire) {
			return store, nil, errInvalid
		}
		if e := store.fundRetention(id, nil, c.host.machine.limits); e != nil {
			return store, nil, e
		}
		held, e = publisher.Open(ctx, token)
		if e != nil {
			return store, nil, e
		}
		defer func() {
			if held != nil {
				err = errors.Join(err, held.Close())
			}
		}()
		r.Token, r.State = tokenWire, nativeColumnPinLive
		if err := store.saveRetention(r, nil); err != nil {
			return store, nil, err
		}
	}
	var initial, final graphCut
	for _, boundary := range r.Generation.Sources {
		a, e := decodeGraphCertificate(boundary.Initial)
		if e != nil {
			return store, nil, e
		}
		b, e := decodeGraphCertificate(boundary.Final)
		if e != nil {
			return store, nil, e
		}
		initial.certificates = append(initial.certificates, a)
		final.certificates = append(final.certificates, b)
	}
	// Capture is prepaid on the existing durable work ledger BEFORE it runs.
	p, err := store.progress(id)
	if err != nil {
		return store, nil, err
	}
	open, _, err := nativeColumnSourceAllowance([2]*allocationHost{c.host, c.host}, policy)
	if err != nil {
		return store, nil, err
	}
	open, err = nativeColumnPeerOpening(open, c.host.machine.limits)
	if err != nil || !p.Work.admit(open, policy) {
		return store, nil, errors.Join(errLimit, err)
	}
	p.Work = p.Work.add(open)
	pWire, err := nativeColumnMetadata(p)
	if err != nil {
		return store, nil, err
	}
	if err := store.commit([]nativeColumnPut{{nativeColumnKey(id, "progress"), pWire}}); err != nil {
		return store, nil, err
	}
	source, err = newPartitionNativeColumnSource(ctx, c, initial, final, policy)
	if err != nil {
		return store, nil, err
	}
	if err := source.retainedBinding(); err != nil {
		return store, source, err
	}
	if !nativeColumnSame(source.binding, store.binding) || source.digest != store.digest {
		return store, source, errInvalid
	}
	// Reassociation does not refund work. Native snapshot/bank generations may
	// differ; the authentic original certificate/root must still match exactly.
	p.Owner = source.owner
	if err := store.saveRetention(r, &p); err != nil {
		return store, source, err
	}
	if r.State == nativeColumnPreparing {
		if err := request.bindUnissuedColumnSource(source); err != nil {
			return store, source, err
		}
		if err := store.completeRetentionCreation(ctx, source, publisher, request, &r); err != nil {
			return store, source, err
		}
	} else if err := store.attachRetention(source, r); err != nil {
		return store, source, err
	}
	return store, source, nil
}

// Retire is explicit durable lifecycle termination. Transient source/process or
// DB Close never calls it. Binding.TerminalAttempts is the persisted finite
// exchange policy (v2:4), prepaid at creation. Every uncertain attempt is spent;
// exhaustion returns errLimit and preserves the retiring recipe/native originals.
// Uncertain/unfunded fences leave the recipe retiring;
// subsequent calls recover the SAME originals and never reactivate generation.
func (s *nativeColumnStore) Retire(ctx context.Context, id [16]byte, publisher *retainedPinPublisher) error {
	if s == nil || ctx == nil || !publisher.valid() {
		return errInvalid
	}
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.end()
	r, err := s.prepareRetirement(id)
	if err != nil || r.State == nativeColumnRetired {
		return err
	}
	if r.TerminalAttempts == s.binding.TerminalAttempts {
		return errLimit
	}
	r.TerminalAttempts++
	if err := s.saveRetention(r, nil); err != nil {
		return err
	}
	request, err := publisher.Resume(ctx, r.Original)
	if err != nil {
		return err
	}
	if err := request.CancelIssue(ctx); err != nil {
		return err
	}
	r.State = nativeColumnRetired
	return s.saveRetention(r, nil)
}

// A metadata frame is not a business change envelope. Its actual native row,
// scoped canonical immutable original, and unchanged graph image are all
// required; an unknown/malformed prefix is never silently folded into no-op.
func (s *nativeColumnSource) retainedMetadataFrame(ctx context.Context, group int, out nativeColumnFrame) (nativeColumnFrame, error) {
	if s.hosts[group].machine.SemanticContractID() == retainedFeedSemanticContractID() {
		return s.retainedFeedMetadataFrame(ctx, group, out)
	}
	h, view := s.hosts[group], s.views[group]
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return out, err
	}
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: s.roots[group], limits: l.allocation, arena: arena}
	previous, err := view.RootAt(ctx, out.Index-1, 172)
	if err != nil || !bytes.Equal(previous.Image, out.Root[:]) {
		return out, errors.Join(errCorrupt, err)
	}
	var originalIndex uint64
	var prefix []byte
	var accept func(raftlog.ApplicationControlRecord) (bool, error)
	if bytes.HasPrefix(out.Outcome, []byte("GPO\x01")) {
		result, err := decodeGraphPinResult(out.Outcome)
		if err != nil || result.index > out.Index {
			return out, errors.Join(errCorrupt, err)
		}
		originalIndex, prefix = result.index, recordKey(q.ns, 24)
		if len(out.CDC) > 0 && (result.index != out.Index || result.status != pinApplied || !bytes.Equal(out.CDC, out.Outcome)) {
			return out, errCorrupt
		}
		if result.status == pinPending && result.index == out.Index {
			if len(out.CDC) != 0 {
				return out, errCorrupt
			}
			return out, nil // this actual native Stage observation deliberately has no GPX
		}
		accept = func(row raftlog.ApplicationControlRecord) (bool, error) {
			if len(row.Key) != len(prefix)+32 || row.Index != result.index {
				return false, nil
			}
			body, err := graphBody(row.Value, "GPX\x01", 512)
			if err != nil || len(body) < 32 {
				return false, errors.Join(errCorrupt, err)
			}
			if !bytes.Equal(body[32:], out.Outcome) {
				return false, nil
			}
			if bytes.Equal(body[:32], make([]byte, 32)) || !bytes.Equal(row.Key[len(prefix)+16:], result.action[:]) {
				return false, errCorrupt
			}
			return true, nil
		}
	} else {
		result, err := decodeGraphRetentionResult(out.Outcome)
		if err != nil || result.index > out.Index || len(out.CDC) != 0 {
			return out, errors.Join(errCorrupt, err)
		}
		if result.index == out.Index && (result.source != out.Index-1 || result.sourceHash != sha256.Sum256(out.Root[:])) {
			return out, errCorrupt
		}
		originalIndex, prefix = result.index, recordKey(q.ns, 25)
		accept = func(row raftlog.ApplicationControlRecord) (bool, error) {
			command, actual, wire, found, err := graphRetentionRecord(&q, row.Key)
			if err != nil {
				return false, err
			}
			if !found || actual != result {
				return false, nil
			}
			if command.scope != h.machine.scope() || command.configuration != s.final[group].configuration || !bytes.Equal(row.Key, graphRetentionOriginalKey(q.ns, command.issuer, command.request)) {
				return false, errCorrupt
			}
			if result.status == graphRetentionApplied {
				_, plan, planWire, found, err := graphRetentionRecord(&q, graphRetentionPlanKey(q.ns, result.id))
				if err != nil || !found || plan != result || !bytes.Equal(planWire, wire) {
					return false, errors.Join(errCorrupt, err)
				}
			}
			return true, nil
		}
	}
	after := prefix
	matched := false
	for pages := 0; pages < q.limits.readRows; pages++ {
		remaining := min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes, arena.Remaining())
		rows := min(16, q.limits.readRows-q.rows, q.view.ReadLimits().Rows)
		if rows < 2 || remaining < 4096 {
			return out, errLimit
		}
		page, err := view.ControlPage(ctx, after, raftlog.ReadBudget{Rows: rows, Bytes: remaining})
		q.rows += page.Work.Rows
		q.bytes += page.Work.Bytes
		charge := reserveComposer(&q, page.Work.Bytes)
		if err != nil || charge != nil {
			return out, errors.Join(err, charge)
		}
		stop := false
		for _, row := range page.Records {
			if !bytes.HasPrefix(row.Key, prefix) {
				stop = true
				break
			}
			same, err := accept(row)
			if err != nil {
				return out, err
			}
			if same {
				if matched {
					return out, errCorrupt
				}
				matched = true
				if err := reserveComposer(&q, 1024+4*(len(row.Key)+len(row.Value))); err != nil {
					return out, err
				}
				out.Registration, err = boundedEncoding(s.binding.Policy.FrameBytes, func(w *boundedWriter) { w.add([]byte("NCO1")); w.field(row.Key); w.u64(row.Index); w.field(row.Value) })
				if err != nil {
					return out, err
				}
			}
		}
		if matched || stop || page.Complete {
			break
		}
		if len(page.After) == 0 || bytes.Compare(page.After, after) <= 0 {
			return out, errCorrupt
		}
		after = page.After
	}
	if !matched {
		return out, errCorrupt
	}
	if originalIndex < out.Index {
		if len(out.CDC) != 0 {
			return out, errCorrupt
		}
		if err := s.replay(ctx, group, &out, originalIndex); err != nil {
			return out, err
		}
		if !bytes.Equal(out.OriginalOutcome, out.Outcome) {
			return out, errCorrupt
		}
	}
	return out, nil
}

// Recovery of a retiring generation never hydrates or reactivates its source.
func retireDurablePartitionNativeColumns(ctx context.Context, dir string, fs vfs.FS, c *crossProcessCoordinator, id [16]byte, policy nativeColumnPolicy) (err error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || !durableColumnGraphAgreement(c.host.machine.SemanticContractID()) {
		return errInvalid
	}
	store, err := openNativeColumnStoreBinding(dir, fs, false, nativeColumnBinding{Policy: policy}, [32]byte{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		return err
	}
	return store.Retire(ctx, id, publisher)
}
