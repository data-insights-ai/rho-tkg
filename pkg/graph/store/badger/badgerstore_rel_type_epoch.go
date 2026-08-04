package badger

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
}

// bumpRelEpochForRels is the batch form: one coarse-free bump per distinct type in
// the batch. A batch spanning several types still leaves every OTHER type valid.
func (bs *Store) bumpRelEpochForRels(toks []uint16) {
	if len(toks) == 0 {
		bs.bumpRelEpoch()
		return
	}
	seen := make(map[uint16]struct{}, len(toks))
	for _, t := range toks {
		if t == 0 { // unknown type in the batch — degrade the WHOLE batch to coarse
			bs.bumpRelEpoch()
			return
		}
		seen[t] = struct{}{}
	}
	bs.relEpoch.Add(1)
	for t := range seen {
		bs.relTypeEpochs[t%relTypeEpochStripes].Add(1)
	}
}
