package idalloc

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
)

func fixture(t testing.TB) (State, Request) {
	t.Helper()
	a, err := NewAuthority([16]byte{2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewState(GraphID{1}, a, 100)
	if err != nil {
		t.Fatal(err)
	}
	return s, Request{Graph: GraphID{1}, Authority: a, Sequence: 1, Count: 4}
}

func requireError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error %v; want errors.Is(%v)", got, want)
	}
}

func TestAuthorityStateAndDigest(t *testing.T) {
	s, r := fixture(t)
	if r.Authority.Owner() != [16]byte{2} || r.Authority.Epoch() != 1 {
		t.Fatal("authority not preserved")
	}
	v, err := Inspect(&s)
	if err != nil || v != (View{Graph: r.Graph, Authority: r.Authority, MaxBlock: 100}) {
		t.Fatalf("view %+v: %v", v, err)
	}
	for _, a := range []Authority{{}, {owner: [16]byte{1}}, {epoch: 1}} {
		_, err := NewAuthority(a.owner, a.epoch)
		requireError(t, err, ErrInvalid)
	}
	for _, bad := range []State{{}, {graph: r.Graph, authority: r.Authority}, {graph: r.Graph, authority: r.Authority, maxBlock: MaxBlockSize + 1}} {
		_, err := NewState(bad.graph, bad.authority, bad.maxBlock)
		requireError(t, err, ErrInvalid)
		_, err = Inspect(&bad)
		requireError(t, err, ErrInvalid)
	}
	for _, change := range []func(*Request){
		func(r *Request) { r.Graph[0]++ },
		func(r *Request) { r.Authority.owner[0]++ },
		func(r *Request) { r.Authority.epoch++ },
		func(r *Request) { r.Sequence++ },
		func(r *Request) { r.Count++ },
	} {
		other := r
		change(&other)
		if other.Digest() == r.Digest() {
			t.Fatal("digest failed to bind field")
		}
	}
}

func TestReserveExactRangesRetryAndOrder(t *testing.T) {
	s, r := fixture(t)
	original := s
	next, block, replay, err := Reserve(&s, r)
	if err != nil || replay || block != (Reservation{Request: r, First: 1, Last: 4}) || s != original {
		t.Fatalf("reserve %+v %v %v", block, replay, err)
	}
	v, err := Inspect(&next)
	if err != nil || v.HighWater != 4 || v.LastSequence != 1 || v.Graph != r.Graph || v.Authority != r.Authority || v.MaxBlock != 100 {
		t.Fatalf("noninitial view=%+v %v", v, err)
	}
	retry, same, replay, err := Reserve(&next, r)
	if err != nil || !replay || same != block || retry != next {
		t.Fatal("retry changed receipt/state")
	}
	different := r
	different.Count++
	_, _, _, err = Reserve(&next, different)
	requireError(t, err, ErrPayloadMismatch)
	skipped := r
	skipped.Sequence = 3
	_, _, _, err = Reserve(&next, skipped)
	requireError(t, err, ErrRequestOrder)
	r2 := r
	r2.Sequence++
	r2.Count = 2
	last, b2, replay, err := Reserve(&next, r2)
	if err != nil || replay || b2.First != 5 || b2.Last != 6 || last.high != 6 {
		t.Fatal("second block incorrect")
	}
	_, _, _, err = Reserve(&last, r)
	requireError(t, err, ErrExpired)
	// Reordered stale commands cannot change high-water or replace the receipt.
	if last.high != 6 || last.sequence != 2 {
		t.Fatal("expired request changed state")
	}
	for _, mutate := range []func(*Request){
		func(r *Request) { r.Graph = GraphID{} },
		func(r *Request) { r.Graph[0]++ },
		func(r *Request) { r.Authority = Authority{} },
		func(r *Request) { r.Count = 0 },
		func(r *Request) { r.Count = MaxBlockSize + 1 },
		func(r *Request) { r.Count = 101 },
		func(r *Request) { r.Sequence = 0 },
	} {
		bad := r
		mutate(&bad)
		_, _, _, err := Reserve(&s, bad)
		requireError(t, err, ErrInvalid)
	}
	wrong := r
	wrong.Authority.owner[0]++
	_, _, _, err = Reserve(&s, wrong)
	requireError(t, err, ErrStaleAuthority)
}

// testEmbedding separates durable image from reducer/cursor state. It models
// the Committer obligations without claiming disk, quorum or Raft fault coverage.
type testEmbedding struct {
	mu    sync.Mutex
	s     State
	image []byte
	fault string
	calls int
}

func embedding(t testing.TB, s State) *testEmbedding {
	t.Helper()
	image, err := MarshalCheckpoint(&s)
	if err != nil {
		t.Fatal(err)
	}
	return &testEmbedding{s: s, image: image}
}

func (e *testEmbedding) commit(_ context.Context, r Request) (Reservation, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	next, block, replay, err := Reserve(&e.s, r)
	if err != nil {
		if errors.Is(err, ErrInvalid) || errors.Is(err, ErrRequestOrder) || errors.Is(err, ErrExhausted) || errors.Is(err, ErrStaleAuthority) {
			return Reservation{}, false, errors.Join(ErrUnreserved, err)
		}
		return Reservation{}, false, err
	}
	if e.fault == "before" {
		return Reservation{}, false, ErrUnknown
	}
	image, err := MarshalCheckpoint(&next)
	if err != nil {
		return Reservation{}, false, err
	}
	// Embedding's modeled atomic durable commit precedes successful delivery.
	e.image, e.s = image, next
	if e.fault == "after" {
		return Reservation{}, false, ErrUnknown
	}
	return block, replay, nil
}

func (e *testEmbedding) reopen(t *testing.T) {
	t.Helper()
	s, err := DecodeCheckpoint(e.image)
	if err != nil {
		t.Fatal(err)
	}
	e.s, e.fault = s, ""
}

func issuer(t testing.TB, r Request) *Issuer {
	t.Helper()
	i, err := NewIssuer(r.Graph, r.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func TestCrashReservationAndAbandonedPartialBlock(t *testing.T) {
	for _, fault := range []string{"before", "after"} {
		t.Run(fault, func(t *testing.T) {
			s, r := fixture(t)
			e := embedding(t, s)
			e.fault = fault
			i := issuer(t, r)
			cursor, err := i.Acquire(t.Context(), r, e.commit)
			if cursor != nil {
				t.Fatal("unknown outcome gave cursor")
			}
			requireError(t, err, ErrUnknown)
			_, err = i.Acquire(t.Context(), r, e.commit)
			requireError(t, err, ErrAlreadyReserved)
			e.reopen(t)
			// Resolve the exact request without opening a recovered cursor.
			_, _, replay, err := Reserve(&e.s, r)
			if err != nil || replay != (fault == "after") {
				t.Fatalf("resolved replay=%v: %v", replay, err)
			}
			if fault == "after" {
				wrongPayload := r
				wrongPayload.Count++
				_, _, _, err := Reserve(&e.s, wrongPayload)
				requireError(t, err, ErrPayloadMismatch)
			}
			// Unknown recovery activates a new committed epoch even if the old
			// command never committed; an old in-flight command is now fenced.
			oldRequest := r
			replacement, err := NewAuthority([16]byte{3}, 2)
			if err != nil {
				t.Fatal(err)
			}
			next, err := Transfer(&e.s, r.Authority, replacement)
			if err != nil {
				t.Fatal(err)
			}
			e = embedding(t, next)
			e.reopen(t)
			r.Authority = replacement
			_, _, _, err = Reserve(&e.s, oldRequest)
			requireError(t, err, ErrStaleAuthority)
			cursor, err = issuer(t, r).Acquire(t.Context(), r, e.commit)
			if err != nil {
				t.Fatal(err)
			}
			first := uint64(1)
			if fault == "after" {
				first = 5
			}
			for want := first; want < first+4; want++ {
				id, err := cursor.Next()
				if err != nil || id != want {
					t.Fatalf("id %d want %d: %v", id, want, err)
				}
			}
			_, err = cursor.Next()
			requireError(t, err, ErrExhausted)
		})
	}
	// Crash after issuing only one ID abandons the remaining three forever.
	s, r := fixture(t)
	e := embedding(t, s)
	c, err := issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.Next()
	if err != nil || id != 1 {
		t.Fatal("partial mint")
	}
	e.reopen(t)
	replacement, err := NewAuthority([16]byte{3}, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, err := Transfer(&e.s, r.Authority, replacement)
	if err != nil {
		t.Fatal(err)
	}
	e = embedding(t, next)
	r.Authority = replacement
	c, err = issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	id, err = c.Next()
	if err != nil || id != 5 {
		t.Fatalf("abandoned tail reused: %d %v", id, err)
	}
}

func TestOneTimeGrantConcurrentRetriesAndReplay(t *testing.T) {
	s, r := fixture(t)
	e := embedding(t, s)
	i := issuer(t, r)
	var wg sync.WaitGroup
	results := make(chan *Cursor, 32)
	errs := make(chan error, 32)
	for range 32 {
		wg.Go(func() {
			c, err := i.Acquire(t.Context(), r, e.commit)
			if err != nil {
				errs <- err
			} else {
				results <- c
			}
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	if len(results) != 1 || len(errs) != 31 || e.calls != 1 {
		t.Fatalf("cursors=%d errors=%d commits=%d", len(results), len(errs), e.calls)
	}
	for err := range errs {
		requireError(t, err, ErrAlreadyReserved)
	}
	c := <-results
	b, err := c.Block()
	if err != nil || b.First != 1 || b.Last != 4 || b.Request != r {
		t.Fatal("bad cursor metadata")
	}
	// Even a mistakenly repeated fresh response cannot reopen this session.
	_, err = i.Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) { return b, false, nil })
	requireError(t, err, ErrAlreadyReserved)
	// A correctly implemented durable callback marks ALL restarted deliveries replay.
	_, err = issuer(t, r).Acquire(t.Context(), r, e.commit)
	requireError(t, err, ErrAlreadyReserved)
	if e.s.high != 4 {
		t.Fatal("replay allocated replacement block")
	}
}

func TestConcurrentMintExactUniqueSet(t *testing.T) {
	s, r := fixture(t)
	r.Count = 100
	e := embedding(t, s)
	c, err := issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	ids := make(chan uint64, 100)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 10 {
				id, err := c.Next()
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				ids <- id
			}
		})
	}
	wg.Wait()
	close(ids)
	if len(errs) != 0 {
		t.Fatal(<-errs)
	}
	seen := make(map[uint64]bool)
	for id := range ids {
		if seen[id] || id < 1 || id > 100 {
			t.Fatalf("duplicate/phantom id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != 100 {
		t.Fatalf("exact set has %d IDs", len(seen))
	}
	_, err = c.Next()
	requireError(t, err, ErrExhausted)
	if e.calls != 1 {
		t.Fatalf("mint used %d commits", e.calls)
	}
}

func TestTransferPreservesHighWaterAndFencesLateGrant(t *testing.T) {
	s, r := fixture(t)
	e := embedding(t, s)
	old, err := issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	id, err := old.Next()
	if err != nil || id != 1 {
		t.Fatal("old first mint")
	}
	replacement, err := NewAuthority([16]byte{3}, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, err := Transfer(&e.s, r.Authority, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if next.high != 4 || next.sequence != 0 || next.authority != replacement {
		t.Fatal("transfer reused reserved range")
	}
	v, err := Inspect(&next)
	if err != nil || v.HighWater != 4 || v.LastSequence != 0 || v.Authority != replacement {
		t.Fatalf("handoff view=%+v %v", v, err)
	}
	e = embedding(t, next)
	e.reopen(t)
	_, _, _, err = Reserve(&e.s, r)
	requireError(t, err, ErrStaleAuthority)
	_, err = Transfer(&e.s, r.Authority, replacement)
	requireError(t, err, ErrStaleAuthority)
	// Delayed former-authority delivery is refused by the embedding authority fence.
	_, err = issuer(t, r).Acquire(t.Context(), r, e.commit)
	requireError(t, err, ErrStaleAuthority)
	r.Authority = replacement
	fresh, err := issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	id, err = fresh.Next()
	if err != nil || id != 5 {
		t.Fatalf("new authority id %d %v", id, err)
	}
	// Local minting cannot know handoff. IDs stay unique; graph commit is fenced.
	id, err = old.Next()
	if err != nil || id != 2 {
		t.Fatal("old cursor nonreuse")
	}
	oldBlock, err := old.Block()
	if err != nil {
		t.Fatal(err)
	}
	commitGraph := func(block Reservation) error {
		if block.Request.Authority != e.s.authority {
			return ErrStaleAuthority
		}
		return nil
	}
	requireError(t, commitGraph(oldBlock), ErrStaleAuthority)
	newBlock, err := fresh.Block()
	if err != nil {
		t.Fatal(err)
	}
	if err := commitGraph(newBlock); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Authority{{}, {owner: [16]byte{3}, epoch: 4}, {owner: [16]byte{3}, epoch: 2}} {
		_, err := Transfer(&e.s, replacement, bad)
		requireError(t, err, ErrInvalid)
	}
	_, err = Transfer(&e.s, Authority{}, replacement)
	requireError(t, err, ErrInvalid)
	e.s.authority.epoch = math.MaxUint64
	e.s.sequence, e.s.count, e.s.first, e.s.digest = 0, 0, 0, [32]byte{}
	_, err = Transfer(&e.s, e.s.authority, replacement)
	requireError(t, err, ErrExhausted)
}

func TestFinalUint64AndSequenceNoWrap(t *testing.T) {
	s, r := fixture(t)
	s.high = math.MaxUint64 - 2
	r.Count = 2
	e := embedding(t, s)
	c, err := issuer(t, r).Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		id, err := c.Next()
		if err != nil || id != want {
			t.Fatalf("last id %d want %d %v", id, want, err)
		}
	}
	_, err = c.Next()
	requireError(t, err, ErrExhausted)
	e.reopen(t)
	r.Sequence++
	r.Count = 1
	_, _, _, err = Reserve(&e.s, r)
	requireError(t, err, ErrExhausted)
	s, r = fixture(t)
	s.high = math.MaxUint64 - 1
	_, _, _, err = Reserve(&s, r)
	requireError(t, err, ErrExhausted)
	// Max request sequence cannot wrap into a fresh zero sequence.
	s, r = fixture(t)
	r.Sequence = math.MaxUint64
	r.Count = 1
	s.sequence = r.Sequence
	s.count = r.Count
	s.first = math.MaxUint64
	s.high = math.MaxUint64
	s.digest = r.Digest()
	_, _, replay, err := Reserve(&s, r)
	if err != nil || !replay {
		t.Fatal("final sequence replay failed")
	}
	r.Sequence = 0
	_, _, _, err = Reserve(&s, r)
	requireError(t, err, ErrInvalid)
}

func TestNilAndMalformedPublicInputs(t *testing.T) {
	s, r := fixture(t)
	_, _, _, err := Reserve(nil, r)
	requireError(t, err, ErrInvalid)
	_, err = Transfer(nil, r.Authority, r.Authority)
	requireError(t, err, ErrInvalid)
	_, err = Inspect(nil)
	requireError(t, err, ErrInvalid)
	_, err = MarshalCheckpoint(nil)
	requireError(t, err, ErrInvalid)
	_, err = DecodeCheckpoint(nil)
	requireError(t, err, ErrCorrupt)
	_, err = NewIssuer(GraphID{}, r.Authority)
	requireError(t, err, ErrInvalid)
	_, err = NewIssuer(r.Graph, Authority{})
	requireError(t, err, ErrInvalid)
	var i *Issuer
	_, err = i.Acquire(t.Context(), r, nil)
	requireError(t, err, ErrInvalid)
	i = issuer(t, r)
	e := embedding(t, s)
	_, err = i.Acquire(nil, r, e.commit) //nolint:staticcheck // Directly test the required nil-input rejection.
	requireError(t, err, ErrInvalid)
	_, err = i.Acquire(t.Context(), r, nil)
	requireError(t, err, ErrInvalid)
	_, err = i.Acquire(t.Context(), Request{}, e.commit)
	requireError(t, err, ErrInvalid)
	bad := r
	bad.Graph[0]++
	_, err = i.Acquire(t.Context(), bad, e.commit)
	requireError(t, err, ErrInvalid)
	bad = r
	bad.Authority.epoch++
	_, err = i.Acquire(t.Context(), bad, e.commit)
	requireError(t, err, ErrStaleAuthority)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = i.Acquire(ctx, r, e.commit)
	requireError(t, err, context.Canceled)
	if e.calls != 0 {
		t.Fatal("canceled input entered commit")
	}
	// Pre-submission cancellation did not burn this still-unreserved sequence.
	if _, err := i.Acquire(t.Context(), r, e.commit); err != nil {
		t.Fatal(err)
	}
	_, err = issuer(t, r).Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) {
		return Reservation{Request: r, First: 0, Last: 3}, false, nil
	})
	requireError(t, err, ErrPayloadMismatch)
	var c *Cursor
	_, err = c.Next()
	requireError(t, err, ErrInvalid)
	_, err = c.Block()
	requireError(t, err, ErrInvalid)
	c = &Cursor{}
	_, err = c.Next()
	requireError(t, err, ErrInvalid)
	_, err = c.Block()
	requireError(t, err, ErrInvalid)
}

func TestDefinitiveRejectionProgressAndAmbiguousOutcomeBurn(t *testing.T) {
	s, r := fixture(t)
	e := embedding(t, s)
	i := issuer(t, r)
	bad := r
	bad.Count = 101 // locally valid, exceeds this graph's configured block bound
	_, err := i.Acquire(t.Context(), bad, e.commit)
	requireError(t, err, ErrInvalid)
	requireError(t, err, ErrUnreserved)
	if _, err := i.Acquire(t.Context(), r, e.commit); err != nil {
		t.Fatalf("definitive rejection stranded authority: %v", err)
	}
	for _, contradiction := range []struct {
		name   string
		block  Reservation
		replay bool
		err    error
	}{
		{name: "unknown", err: errors.Join(ErrUnreserved, ErrUnknown)},
		{name: "reserved", err: errors.Join(ErrUnreserved, ErrAlreadyReserved)},
		{name: "expired", err: errors.Join(ErrUnreserved, ErrExpired)},
		{name: "mismatch", err: errors.Join(ErrUnreserved, ErrPayloadMismatch)},
		{name: "receipt", block: Reservation{Request: r, First: 1, Last: 4}, err: ErrUnreserved},
		{name: "replay", replay: true, err: ErrUnreserved},
	} {
		t.Run(contradiction.name, func(t *testing.T) {
			i := issuer(t, r)
			_, err := i.Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) {
				return contradiction.block, contradiction.replay, contradiction.err
			})
			requireError(t, err, ErrUnreserved)
			_, err = i.Acquire(t.Context(), r, e.commit)
			requireError(t, err, ErrAlreadyReserved)
		})
	}
}

func TestDelayedFreshReplyAfterRestartEpochCannotOpenCursor(t *testing.T) {
	s, oldRequest := fixture(t)
	e := embedding(t, s)
	oldGrant, _, err := e.commit(t.Context(), oldRequest)
	if err != nil {
		t.Fatal(err)
	}
	e.reopen(t)
	replacement, err := NewAuthority([16]byte{3}, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, err := Transfer(&e.s, oldRequest.Authority, replacement)
	if err != nil {
		t.Fatal(err)
	}
	e = embedding(t, next)
	request := oldRequest
	request.Authority = replacement
	i := issuer(t, request)
	// The durable epoch handoff, not attempted, fences old-session traffic.
	_, err = i.Acquire(t.Context(), oldRequest, e.commit)
	requireError(t, err, ErrStaleAuthority)
	// Even misrouted "fresh=false/replay=false" old response cannot satisfy the
	// new request digest/epoch. It burns the uncertain attempt, never opens a cursor.
	_, err = i.Acquire(t.Context(), request, func(context.Context, Request) (Reservation, bool, error) { return oldGrant, false, nil })
	requireError(t, err, ErrPayloadMismatch)
	_, err = i.Acquire(t.Context(), request, e.commit)
	requireError(t, err, ErrAlreadyReserved)
	if e.s.high != 4 {
		t.Fatal("delayed reply changed reservation high-water")
	}
}

func TestMintAllocationsAndDurableBoundaryBytes(t *testing.T) {
	s, r := fixture(t)
	r.Count = 1
	e := embedding(t, s)
	i := issuer(t, r)
	c, err := i.Acquire(t.Context(), r, e.commit)
	if err != nil {
		t.Fatal(err)
	}
	// Reset is test-only: production exposes no cursor restore/rewind.
	allocs := testing.AllocsPerRun(1000, func() {
		c.next = 1
		c.done = false
		if _, err := c.Next(); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("mint allocates %g objects/ID", allocs)
	}
	if len(e.image) != CheckpointSize || e.calls != 1 {
		t.Fatal("reservation boundary accounting")
	}
	t.Logf("in-memory mint: %g allocs/ID; modeled durable reserve checkpoint: %d bytes/block (no network/fsync cost claim)", allocs, len(e.image))
}
