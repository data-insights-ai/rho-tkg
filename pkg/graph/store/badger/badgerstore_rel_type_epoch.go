package badger

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Per-rel-type column invalidation.
//
// relEpoch is GLOBAL and contracted that way: RelMutationEpoch() is public and the
// node-side expand path's Gate-2 depends on it advancing on EVERY edge write. It is
// not touched here. What is added beside it is a striped counter so a rel-type
// column snapshot can survive a write to an UNRELATED type — the same thing
// BACKLOG 4b did for labels once the global node counter proved too coarse.
//
// THE DEFAULT IS OVER-INVALIDATION, and that inversion is the whole safety
// argument. There are ten relationship-mutation sites. Classifying all ten as
// "knows its type" or not is an audit whose failure mode is a STALE ANSWER, which
// is exactly the kind of sweep the phase-2 plan wrote a STOP condition against.
//
// So: plain bumpRelEpoch() keeps invalidating EVERY type, byte-for-byte today's
// behaviour. Only a site that explicitly calls bumpRelEpochForType opts into
// precision. A site left unconverted — today's, or one added next year — is SLOWER
// than it could be and never wrong. Omission cannot cost correctness.
//
// Only the three insert/update sites are converted, because they hold the
// *types.Relationship and so know its type without a read. Deletes hold only an ID
// and stay coarse; they are rarer, and reading the row just to narrow an
// invalidation would trade a certainty for a maybe.

// relTypeEpochStripes matches the node side's stripe count: a power of two so
// token%stripes is a mask, and wide enough that collisions are rare while the array
// stays trivial (2KB).
const relTypeEpochStripes = 256

// relTypeEpoch is the freshness stamp for one relationship type's column snapshot:
// its own stripe plus the coarse counter that every unconverted site bumps.
// Monotonic, so a snapshot is fresh iff this has not advanced.
//
// A stripe collision between two types over-invalidates, which is safe. The coarse
// term is what makes an unconverted mutation site correct by default.
func (bs *Store) relTypeEpoch(token uint16) uint64 {
	return bs.relTypeEpochs[token%relTypeEpochStripes].Load() + bs.relEpochCoarse.Load()
}

// bumpRelEpochForType is the PRECISE invalidation: it advances the global relEpoch
// (whose every-write contract is unchanged) and only this type's stripe, leaving
// every other type's cached columns valid.
//
// Callers must know the type is right. Passing token 0 — an unregistered type — is
// treated as "unknown" and falls back to the coarse bump rather than striping into
// bucket 0, which would silently under-invalidate every type sharing that bucket.
func (bs *Store) bumpRelEpochForType(token uint16) {
	if token == 0 {
		bs.bumpRelEpoch()
		return
	}
	bs.relEpoch.Add(1)
	bs.relTypeEpochs[token%relTypeEpochStripes].Add(1)
	bs.poisonRelType(token)
}

// --- append-delta for relationship types ---
//
// Measured worth: rebuilding a 10,000-relationship column costs 3,621,267 ns and
// 30,060 allocations; extending it by 100 costs 21,217 ns and 21 — 171x, and the
// ratio GROWS with the type's size, because a rebuild re-reads every relationship
// individually (bulkRelGetters, unlike the node side's single bulk scan) while an
// extend reads only the appended ones.
//
// POISON REMAINS THE DEFAULT, exactly as with the epochs above. bumpRelEpoch
// poisons every type and bumpRelEpochForType poisons its own; only the two INSERT
// sites call the append variants. An update (replaceRelationshipRouted) keeps the
// poisoning door, because extending across an update would carry the relationship's
// old value forward.

// relAppendDelta is one type's record of pure inserts since its snapshot was built.
type relAppendDelta struct {
	ids      []types.RelID
	epoch    uint64 // relTypeEpoch immediately after the last recorded append
	poisoned bool
}

// maxRelAppendDeltaIDs bounds a type's pending appends; past it, extending is no
// longer clearly cheaper than rebuilding.
const maxRelAppendDeltaIDs = 50_000

// bumpRelEpochAppend is the INSERT door: it advances the type's stripe and records
// the new relationship as an append, so the next read can extend instead of
// rebuilding. Only a site that is genuinely inserting may call it.
func (bs *Store) bumpRelEpochAppend(r *types.Relationship) {
	if r == nil {
		bs.bumpRelEpoch()
		return
	}
	tok := uint16(r.TypeToken())
	bs.bumpRelEpochForTypeNoPoison(tok)
	bs.recordRelAppend(tok, r.ID())
}

// bumpRelEpochAppendBatch is the batch INSERT door.
func (bs *Store) bumpRelEpochAppendBatch(rels []*types.Relationship) {
	if len(rels) == 0 {
		bs.bumpRelEpoch()
		return
	}
	for _, r := range rels {
		if r == nil || uint16(r.TypeToken()) == 0 {
			bs.bumpRelEpoch() // unknown type — degrade the whole batch
			return
		}
	}
	bs.relEpoch.Add(1)
	for _, r := range rels {
		tok := uint16(r.TypeToken())
		bs.relTypeEpochs[tok%relTypeEpochStripes].Add(1)
	}
	for _, r := range rels {
		bs.recordRelAppend(uint16(r.TypeToken()), r.ID())
	}
}

// bumpRelEpochForTypeNoPoison advances a type's stripe WITHOUT poisoning it — the
// append doors' primitive. Everything else must use bumpRelEpochForType.
func (bs *Store) bumpRelEpochForTypeNoPoison(token uint16) {
	if token == 0 {
		bs.bumpRelEpoch()
		return
	}
	bs.relEpoch.Add(1)
	bs.relTypeEpochs[token%relTypeEpochStripes].Add(1)
}

func (bs *Store) recordRelAppend(token uint16, id types.RelID) {
	bs.relAppendMu.Lock()
	defer bs.relAppendMu.Unlock()
	if bs.relAppendDeltas == nil {
		bs.relAppendDeltas = make(map[uint16]*relAppendDelta)
	}
	d := bs.relAppendDeltas[token]
	if d == nil {
		d = &relAppendDelta{}
		bs.relAppendDeltas[token] = d
	}
	if d.poisoned {
		return
	}
	if len(d.ids) >= maxRelAppendDeltaIDs {
		d.poisoned, d.ids = true, nil
		return
	}
	d.ids = append(d.ids, id)
	d.epoch = bs.relTypeEpoch(token)
}

// poisonRelType voids one type's append record.
func (bs *Store) poisonRelType(token uint16) {
	bs.relAppendMu.Lock()
	defer bs.relAppendMu.Unlock()
	if bs.relAppendDeltas == nil {
		bs.relAppendDeltas = make(map[uint16]*relAppendDelta)
	}
	d := bs.relAppendDeltas[token]
	if d == nil {
		d = &relAppendDelta{}
		bs.relAppendDeltas[token] = d
	}
	d.poisoned, d.ids = true, nil
}

// poisonAllRelTypes voids every type's append record — the default door's partner.
func (bs *Store) poisonAllRelTypes() {
	bs.relAppendMu.Lock()
	bs.relAppendDeltas = nil
	bs.relAppendMu.Unlock()
}

// takeRelAppendDelta returns the IDs appended to a type since its snapshot was
// built, or ok=false if a rebuild is required.
//
// THE GUARD IS AN ACCOUNTING IDENTITY, not just a matching stamp:
//
//	gen - snapshotEpoch == len(recorded ids)
//
// Every recorded append bumped this type's epoch exactly once, so if the numbers
// balance then EVERY bump since the snapshot was built was a recorded append.
//
// A stamp check alone is not enough, and this is not hypothetical — it shipped
// broken and a probe caught it. poisonAllRelTypes drops the whole map, so the very
// next insert creates a FRESH, un-poisoned record stamped at the current epoch. A
// stamp check passes and the stale snapshot (still holding the deleted row) gets
// extended. The identity fails it: a delete plus an insert is two bumps against one
// recorded ID.
func (bs *Store) takeRelAppendDelta(token uint16, gen, snapshotEpoch uint64) ([]types.RelID, bool) {
	bs.relAppendMu.Lock()
	defer bs.relAppendMu.Unlock()
	d := bs.relAppendDeltas[token]
	if d == nil || d.poisoned || len(d.ids) == 0 || d.epoch != gen {
		return nil, false
	}
	if gen < snapshotEpoch || gen-snapshotEpoch != uint64(len(d.ids)) {
		return nil, false // some bump since the snapshot was not a recorded append
	}
	out := make([]types.RelID, len(d.ids))
	copy(out, d.ids)
	return out, true
}

func (bs *Store) clearRelAppendDelta(token uint16) {
	bs.relAppendMu.Lock()
	delete(bs.relAppendDeltas, token)
	bs.relAppendMu.Unlock()
}
