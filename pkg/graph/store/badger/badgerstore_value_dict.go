package badger

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unique"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/valuedict"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Value dictionaries (DICT, store.ValueDictionary). Each store keeps the
// entries its own rows use, one token space per store:
//
//	meta "vdict_names"           msgpack []string, a dictionary name per id
//	meta "vdict/" + token (4 BE) uvarint(name id) + the value's bytes
//
// A new value's entry (and the names list when it grew, and the marker
// raise to ValueDictStoreFormatVersion) is appended to the pending batch
// under vd.mu, before the encode returns the token, so it precedes in
// pending every row that uses the token: no flush commits such a row
// without its entry, staged or not. Readers resolve tokens without a lock
// (vals is append-only and published atomically); the entries load on the
// first coded row read or written. A store without a plan still decodes
// coded rows: the plan only decides what new rows code.
const (
	vdictNamesMeta  = "vdict_names"
	vdictEntryMeta  = "vdict/"
	vdictMaxEntries = math.MaxUint32 - 1
)

// dictSnap is what a reader resolves against: vals is append-only (a later
// snapshot shares its prefix), gaps are tokens with no entry on disk (set at
// load, never written after).
type dictSnap struct {
	vals []string // token-1 -> value
	gaps map[uint32]struct{}
}

type valueDict struct {
	plan *valuedict.Plan // nil: code nothing

	loadMu sync.Mutex
	loaded atomic.Bool

	snap atomic.Pointer[dictSnap] // published under mu; read lock-free

	mu      sync.Mutex // assignment; nests wbMu
	handles []unique.Handle[string]
	names   []string // persisted names, index = name id
	nameOf  []int    // plan index -> name id, -1 when not yet persisted
	byValue []map[string]uint32
	size    []int // plan index -> entries of that dictionary in this store
}

func newValueDict(plan *valuedict.Plan) *valueDict {
	vd := &valueDict{plan: plan}
	vd.resetLocked()
	return vd
}

// resetLocked empties the in-memory state and marks it unloaded. Caller holds
// loadMu and mu, or owns vd.
func (vd *valueDict) resetLocked() {
	vd.snap.Store(&dictSnap{})
	vd.handles = nil
	vd.names = nil
	n := 0
	if vd.plan != nil {
		n = vd.plan.Len()
	}
	vd.nameOf = make([]int, n)
	for i := range vd.nameOf {
		vd.nameOf[i] = -1
	}
	vd.byValue = make([]map[string]uint32, n)
	for i := range vd.byValue {
		vd.byValue[i] = map[string]uint32{}
	}
	vd.size = make([]int, n)
	vd.loaded.Store(false)
}

// reappend puts every loaded entry, the names and the marker raise back into
// the pending batch: Clear wipes the batch and the stored entries, but a
// token handed out before the wipe may still reach a row written after it,
// so the in-memory dictionary never shrinks and its entries are stored
// again. Values no row uses any more keep counting toward the caps.
func (vd *valueDict) reappend(bs *Store) {
	vd.mu.Lock()
	defer vd.mu.Unlock()
	if !vd.loaded.Load() || len(vd.names) == 0 {
		return
	}
	data, err := msgpack.Marshal(vd.names)
	if err != nil {
		return
	}
	ops := []writeOp{{opType: writeOpSet, key: storepkg.MetaKey(vdictNamesMeta), value: data}}
	sn := vd.snap.Load()
	nameOfTok := make(map[uint32]int, len(sn.vals))
	if vd.plan != nil {
		for i := 0; i < vd.plan.Len(); i++ {
			for _, tok := range vd.byValue[i] {
				nameOfTok[tok] = vd.nameOf[i]
			}
		}
	}
	for i, v := range sn.vals {
		tok := uint32(i + 1) // #nosec G115 -- bounded by vdictMaxEntries
		id, ok := nameOfTok[tok]
		if _, gap := sn.gaps[tok]; gap || !ok {
			continue
		}
		ops = append(ops, vdictEntryOp(tok, id, v))
	}
	bs.appendOpsRaising(storepkg.ValueDictStoreFormatVersion, ops...)
}

// vdictEntryOp is the pending write of entry tok.
func vdictEntryOp(tok uint32, nameID int, v string) writeOp {
	val := binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen64+len(v)), uint64(nameID)) // #nosec G115 -- a name id is a small index
	val = append(val, v...)
	key := storepkg.MetaKey(vdictEntryMeta + "    ")
	binary.BigEndian.PutUint32(key[len(key)-4:], tok)
	return writeOp{opType: writeOpSet, key: key, value: val}
}

// ensureLoaded reads the store's entries once. It takes no lock a writer
// holds while waiting for wbMu, so a decode under wbMu cannot deadlock.
func (vd *valueDict) ensureLoaded(db *badgerv4.DB) error {
	if vd.loaded.Load() {
		return nil
	}
	vd.loadMu.Lock()
	defer vd.loadMu.Unlock()
	if vd.loaded.Load() {
		return nil
	}
	var names []string
	byTok := map[uint32]struct {
		name uint64
		val  string
	}{}
	var maxTok uint32
	err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.MetaKey(vdictNamesMeta))
		switch {
		case err == badgerv4.ErrKeyNotFound:
		case err != nil:
			return err
		default:
			if err := item.Value(func(v []byte) error { return storepkg.SafeUnmarshal(v, &names) }); err != nil {
				return err
			}
		}
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = storepkg.MetaKey(vdictEntryMeta)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) != len(opts.Prefix)+4 {
				return fmt.Errorf("value dictionary entry key %x: %w", k, storecontract.ErrCorruptWire)
			}
			tok := binary.BigEndian.Uint32(k[len(opts.Prefix):])
			if err := it.Item().Value(func(v []byte) error {
				id, n := binary.Uvarint(v)
				if n <= 0 || tok == 0 {
					return fmt.Errorf("value dictionary entry %d: %w", tok, storecontract.ErrCorruptWire)
				}
				byTok[tok] = struct {
					name uint64
					val  string
				}{id, string(v[n:])}
				return nil
			}); err != nil {
				return err
			}
			maxTok = max(maxTok, tok)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: load value dictionaries: %w", err)
	}
	vd.mu.Lock()
	defer vd.mu.Unlock()
	vd.resetLocked()
	vd.names = names
	if vd.plan != nil {
		for id, name := range names {
			if i, ok := vd.plan.Index(name); ok {
				vd.nameOf[i] = id
			}
		}
	}
	vals := make([]string, maxTok)
	var gaps map[uint32]struct{}
	vd.handles = make([]unique.Handle[string], maxTok)
	for tok := uint32(1); tok <= maxTok; tok++ {
		e, ok := byTok[tok]
		if !ok || e.name >= uint64(len(names)) {
			if gaps == nil {
				gaps = map[uint32]struct{}{}
			}
			gaps[tok] = struct{}{}
			continue
		}
		h := unique.Make(e.val)
		vd.handles[tok-1], vals[tok-1] = h, h.Value()
		if vd.plan != nil {
			if i, ok := vd.plan.Index(names[e.name]); ok {
				vd.byValue[i][vals[tok-1]] = tok
				vd.size[i]++
			}
		}
	}
	vd.snap.Store(&dictSnap{vals: vals, gaps: gaps})
	vd.loaded.Store(true)
	return nil
}

// ValueOf resolves a token (storeutil.ValueTokens). Lock-free; the caller
// has loaded the dictionary.
func (vd *valueDict) ValueOf(tok uint32) (string, bool) {
	sn := vd.snap.Load()
	if tok == 0 || uint64(tok) > uint64(len(sn.vals)) {
		return "", false
	}
	if _, gap := sn.gaps[tok]; gap {
		return "", false
	}
	return sn.vals[tok-1], true
}

// token returns s's token in dictionary d, adding the entry when the
// dictionary has room; false leaves the value inline. A new entry reaches
// the pending batch before this returns.
func (vd *valueDict) token(bs *Store, d int, s string) (uint32, bool) {
	vd.mu.Lock()
	defer vd.mu.Unlock()
	if tok, ok := vd.byValue[d][s]; ok {
		vd.plan.NoteCoded(d)
		return tok, true
	}
	sn := vd.snap.Load()
	if vd.size[d] >= vd.plan.Cap(d) || len(sn.vals) >= vdictMaxEntries {
		vd.plan.NoteInlineFull(d)
		return 0, false
	}
	var ops []writeOp
	if vd.nameOf[d] < 0 {
		names := append(vd.names[:len(vd.names):len(vd.names)], vd.plan.Name(d))
		data, err := msgpack.Marshal(names)
		if err != nil {
			return 0, false // a []string always marshals; stay inline if not
		}
		vd.names, vd.nameOf[d] = names, len(names)-1
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.MetaKey(vdictNamesMeta), value: data})
	}
	tok := uint32(len(sn.vals) + 1) // #nosec G115 -- bounded by vdictMaxEntries above
	h := unique.Make(s)
	ops = append(ops, vdictEntryOp(tok, vd.nameOf[d], s))
	vd.snap.Store(&dictSnap{vals: append(sn.vals, h.Value()), gaps: sn.gaps})
	vd.handles = append(vd.handles, h)
	vd.byValue[d][s] = tok
	vd.size[d]++
	vd.plan.NoteNewEntry(d)
	vd.plan.NoteCoded(d)
	bs.appendOpsRaising(storepkg.ValueDictStoreFormatVersion, ops...)
	return tok, true
}

// codeValues replaces each member property's string value with its token.
// props carry plain keys (before key tokenization); dictOf maps a property
// key to its dictionary.
func (bs *Store) codeValues(props []storepkg.PropertyWire, dictOf func(key string) (int, bool)) error {
	for i := range props {
		p := &props[i]
		s, ok := storepkg.CodableString(p, valuedict.MinLen)
		if !ok {
			continue
		}
		d, ok := dictOf(p.Key)
		if !ok {
			continue
		}
		if err := bs.vdict.ensureLoaded(bs.db); err != nil {
			return err
		}
		if tok, ok := bs.vdict.token(bs, d, s); ok {
			p.Value, p.DictToken = nil, tok
		}
	}
	return nil
}

// coding reports whether this store codes new rows.
func (bs *Store) coding() bool { return bs.vdict.plan != nil && !bs.readOnly }

// nodeWire is the stored wire for n: values coded (when this store codes),
// then keys tokenized.
func (bs *Store) nodeWire(n *types.Node) (storepkg.NodeWire, error) {
	if !bs.coding() {
		return storepkg.NodeToWireCheckedWithKeys(n, bs.propKeyReg.Load())
	}
	w, err := storepkg.NodeToWireChecked(n)
	if err != nil {
		return storepkg.NodeWire{}, err
	}
	if labels := bs.labelNames.Load(); labels != nil {
		plan := bs.vdict.plan
		var names []string // the row's label names, primary first; resolved once
		if err := bs.codeValues(w.Properties, func(key string) (int, bool) {
			if !plan.NodeProperty(key) {
				return 0, false
			}
			if names == nil {
				names = append(names, labels.Resolve(uint16(w.PrimaryLabel))) // #nosec G115 -- a validated label token
				for _, l := range w.ExtraLabels {
					names = append(names, labels.Resolve(uint16(l))) // #nosec G115 -- a validated label token
				}
			}
			for _, name := range names {
				if d, ok := plan.NodeDict(name, key); ok {
					return d, true
				}
			}
			return 0, false
		}); err != nil {
			return storepkg.NodeWire{}, err
		}
	}
	storepkg.ApplyPropertyKeyTokens(w.Properties, bs.propKeyReg.Load())
	return w, nil
}

// relWire is nodeWire for a relationship.
func (bs *Store) relWire(r *types.Relationship) (storepkg.RelWire, error) {
	if !bs.coding() {
		return storepkg.RelToWireCheckedWithKeys(r, bs.propKeyReg.Load())
	}
	w, err := storepkg.RelToWireChecked(r)
	if err != nil {
		return storepkg.RelWire{}, err
	}
	if relTypes := bs.relTypeNames.Load(); relTypes != nil {
		plan := bs.vdict.plan
		relType := ""
		if err := bs.codeValues(w.Properties, func(key string) (int, bool) {
			if !plan.RelProperty(key) {
				return 0, false
			}
			if relType == "" {
				relType = relTypes.Resolve(uint16(w.RelType)) // #nosec G115 -- a validated type token
			}
			return plan.RelDict(relType, key)
		}); err != nil {
			return storepkg.RelWire{}, err
		}
	}
	storepkg.ApplyPropertyKeyTokens(w.Properties, bs.propKeyReg.Load())
	return w, nil
}

// codesNodeProperties reports whether a node row may carry a token, so a
// body pre-encoded without the store's dictionary must be re-encoded.
func (bs *Store) codesNodeProperties() bool {
	return bs.coding() && bs.vdict.plan.NodeMembers()
}

// resolveValueTokens resolves a decoded row's value tokens (rows of format
// ValueDictWireFormatVersion and up), loading the dictionary on first use.
func (bs *Store) resolveValueTokens(props []storepkg.PropertyWire, fv uint8) error {
	if fv < storepkg.ValueDictWireFormatVersion {
		return storepkg.ResolvePropertyValueTokens(props, fv, nil)
	}
	if err := bs.vdict.ensureLoaded(bs.db); err != nil {
		return err
	}
	return storepkg.ResolvePropertyValueTokens(props, fv, bs.vdict)
}

// SetNameRegistries installs the label and relationship-type registries the
// value dictionaries' members are named in (the graph installs its own at
// open). Until both are set, no value is coded.
func (bs *Store) SetNameRegistries(labels *registrypkg.LabelRegistry, relTypes *registrypkg.RelTypeRegistry) {
	if labels != nil {
		bs.labelNames.Store(labels)
	}
	if relTypes != nil {
		bs.relTypeNames.Store(relTypes)
	}
}

// ValueDictionaryStats returns each configured value dictionary's counters
// since open (shared by every shard of a tiered store); nil without
// dictionaries.
func (bs *Store) ValueDictionaryStats() []storecontract.ValueDictionaryStats {
	return bs.vdict.plan.Stats()
}
