package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const columnFaultFormat = "fdb-column-publication-faults-v1"

var errColumnFaultSeam = errors.New("column publication fault: observer stopped before manifest")

type columnFaultOriginal struct {
	Proposal  []byte
	Outcome   outcome
	Originals [2]graphCostStoredRecord
	CDC       graphCostStoredRecord
}
type columnFaultStage struct {
	Refusal  string `json:",omitempty"`
	Name     string
	Records  []graphCostStoredRecord
	Rows     []graphColumnRow
	Manifest graphColumnManifest
	Missing  []byte
}
type columnFaultPage struct {
	Budget  graphColumnCollectBudget
	Result  graphColumnCollectResult
	Records []graphCostStoredRecord
	Missing graphCostStoredRecord
}
type columnFaultView struct {
	Cut    certificate
	Values [2]map[string]value
	View   graphView
}
type columnFaultState struct {
	Version                    uint8
	Format, Case               string
	Coordinator                uint8
	Config                     config
	Columns                    graphColumnPolicy
	Pins                       graphColumnPinPolicy
	Initial, Old, Final        certificate
	OldManifest, FinalManifest graphColumnManifest
	Token                      graphColumnPinToken
	Owner                      graphColumnOwnerID
	Reference                  graphColumnReference
	Originals                  []columnFaultOriginal
	Pending                    *columnFaultOriginal  `json:",omitempty"`
	Failure                    *graphPlatformAttempt `json:",omitempty"`
	Stages                     []columnFaultStage
	Pages                      []columnFaultPage
	Sources                    [3]graphCostStreamEnd
	Views                      []columnFaultView
}

func columnFaultCase(name string) error {
	if !slices.Contains([]string{"chunk", "source-eof", "manifest-issued"}, name) {
		return errInvalid
	}
	return nil
}
func columnFaultSettings() (config, string, uint8, error) {
	graph, name, coord := os.Getenv("FDB_COLUMN_FAULT_GRAPH"), os.Getenv("FDB_COLUMN_FAULT_CASE"), os.Getenv("FDB_COLUMN_FAULT_COORDINATOR")
	if !validName(graph) || !strings.HasPrefix(graph, "column-publication-v1-") || columnFaultCase(name) != nil || coord != "A" && coord != "B" {
		return config{}, "", 0, errInvalid
	}
	return graphCostConfig(graph, graphCostProfile{Version: 1, Facts: 1, CrossPercent: 100, Writers: 1}), name, uint8(strings.Index("AB", coord)), nil // #nosec G115 -- admitted exact A/B.
}
func columnFaultPolicies() (graphColumnPolicy, graphColumnPinPolicy) {
	return graphColumnPolicy{ChunkRows: 1, ChunkBytes: 32 << 10, MaxChunks: 9, MaxGenerations: 2, MaxRetainedBytes: 4 << 20, MaxWorkRows: 5, MaxWorkBytes: 8 << 20},
		graphColumnPinPolicy{MaxRecords: 16, MaxBytes: 128 << 10, PageChunks: 1, PageBytes: 64 << 10, MaxWorkChunks: 1, MaxWorkBytes: 64 << 10, MaxOutputBytes: 1024}
}
func columnFaultRoot() (*os.Root, error) {
	if os.Getenv("FDB_COLUMN_FAULT_ARTIFACT") == "" {
		return nil, errInvalid
	}
	return os.OpenRoot(os.Getenv("FDB_COLUMN_FAULT_ARTIFACT"))
}
func columnFaultSave(root *os.Root, r columnFaultState) (err error) {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(body) > graphPlatformBytes {
		return errLimit
	}
	f, err := root.OpenFile("state.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(body)
	if err = errors.Join(writeErr, f.Close()); err != nil {
		return errors.Join(err, root.Remove("state.tmp"))
	}
	return root.Rename("state.tmp", "state.json")
}
func columnFaultLoad(root *os.Root) (r columnFaultState, err error) {
	f, err := root.Open("state.json")
	if err != nil {
		return r, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	body, err := io.ReadAll(io.LimitReader(f, graphPlatformBytes+1))
	if err != nil {
		return r, err
	}
	if len(body) == 0 || len(body) > graphPlatformBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, body) {
		return r, errInvalid
	}
	c, name, coord, err := columnFaultSettings()
	p, pins := columnFaultPolicies()
	if err != nil || r.Version != 1 || r.Format != columnFaultFormat || r.Config != c || r.Case != name || r.Coordinator != coord || r.Columns != p || r.Pins != pins || len(r.Originals) < 4 || len(r.Originals) > 5 || len(r.Stages) > 16 || len(r.Pages) > 4 || r.Pending != nil {
		return r, errInvalid
	}
	return r, nil
}
func columnFaultStores(h *harness, r columnFaultState) (*graphColumnStore, *graphColumnPins, error) {
	s, err := newGraphColumnStore(h, r.Columns)
	if err != nil {
		return nil, nil, err
	}
	pins, err := newGraphColumnPins(s, r.Pins)
	return s, pins, err
}
func columnFaultOwner(s *graphColumnStore, r columnFaultState) (*graphColumnRead, error) {
	if columnValidateOwner(r.Owner) != nil || r.Owner.Generation != "old" || columnValidatePin(s, r.Token) != nil || r.Token.Generation != r.Owner.Generation {
		return nil, errInvalid
	}
	// Recreate only the local handle from its genuine previously created intent.
	// open/check/Close below are the SAME production reducers and authenticate
	// its durable active record. No owner is minted or directly written here.
	token := r.Token
	token.Cut.Scope = slices.Clone(token.Cut.Scope)
	return &graphColumnRead{s: s, id: r.Owner, pin: &token}, nil
}
func columnFaultRecords(s *graphColumnStore, g graphColumnGeneration, keys []fdb.Key) ([]graphCostStoredRecord, error) {
	if len(keys) < 1 || len(keys) > 24 {
		return nil, errInvalid
	}
	result, err := s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if err := s.check(tr, g); err != nil {
			return nil, err
		}
		rows := make([]graphCostStoredRecord, 0, len(keys))
		for _, key := range keys {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(key) > 1024 || len(raw) > 64<<10 {
				return nil, errLimit
			}
			rows = append(rows, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]graphCostStoredRecord), nil
}
func columnFaultControls(s *graphColumnStore, pins *graphColumnPins, r columnFaultState) ([]graphCostStoredRecord, error) {
	keys := []fdb.Key{s.key("binding"), s.key("ledger"), pins.key("binding"), pins.key("ledger"), s.h.key(0, "meta", "config"), s.h.key(1, "meta", "config"), s.h.key(0, "meta", "state"), s.h.key(1, "meta", "state")}
	for _, id := range []string{"old", "final"} {
		for _, kind := range []string{"manifest", "progress", "owners"} {
			keys = append(keys, s.generationKey(id, kind))
		}
	}
	keys = append(keys, columnPinKey(s, r.Token.Request), pins.recordKey("reference", "head"), pins.recordKey("reference-request", "head-old"), pins.recordKey("reference-request", "head-final"), s.key("read-owner/"+hex.EncodeToString([]byte(r.Owner.Request))))
	for _, c := range []certificate{r.Initial, r.Old, r.Final} {
		if c.Graph != "" {
			body, err := wire(c)
			if err != nil {
				return nil, err
			}
			keys = append(keys, s.h.key(0, "cut", hex.EncodeToString(body)))
		}
	}
	return columnFaultRecords(s, r.OldManifest.Generation, keys)
}
func columnFaultStageAt(s *graphColumnStore, pins *graphColumnPins, r *columnFaultState, name string, owner *graphColumnRead, at cut) error {
	if len(r.Stages) >= 16 {
		return errLimit
	}
	rows, err := columnFaultControls(s, pins, *r)
	if err != nil {
		return err
	}
	stage := columnFaultStage{Name: name, Records: rows}
	if owner != nil {
		stage.Manifest, err = owner.walk(context.Background(), at, nil, func(row graphColumnRow) error {
			if len(stage.Rows) >= 5 {
				return errLimit
			}
			stage.Rows = append(stage.Rows, row)
			return nil
		})
		if err != nil {
			return err
		}
		want := int64(5)
		if owner.id.Generation == "final" {
			want = 6
		}
		found := false
		for _, row := range stage.Rows {
			if row.Owner == 7 {
				found = true
				if row.Value != want {
					return errMismatch
				}
			}
		}
		if !found || uint64(len(stage.Rows)) != stage.Manifest.Rows {
			return errMismatch
		}
	}
	r.Stages = append(r.Stages, stage)
	return nil
}
func columnFaultApply(root *os.Root, h *harness, r *columnFaultState, phase string, i uint64) error {
	command := graphCommand{Kind: "edge-" + phase, Edge: h.config.Cost.edge(i, 5)}
	provenance := 4 + i
	coord := r.Coordinator
	if phase == "setup" {
		command = graphCommand{Kind: "node-create", Node: h.config.Cost.nodes()[i]}
		provenance = i + 1
		coord = command.Node.Owner
	}
	if phase == "update" {
		command.Edge.Value = 6
		provenance = 5 + i
	}
	before, _, err := h.graphCostPointSnapshot(command)
	if err != nil {
		return err
	}
	x := h.graphTransaction(fmt.Sprintf("publication-%s-%d", phase, i), coord, command, before)
	x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
	if phase != "setup" {
		x.Lifecycle.EdgeLife = command.Edge.ID + 1
		x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
	} // #nosec G115 -- exact5/6 fixture values.
	body, err := wire(x)
	if err != nil {
		return err
	}
	r.Pending = &columnFaultOriginal{Proposal: body}
	if err = columnFaultSave(root, *r); err != nil {
		return err
	}
	o, callbacks, err := h.graphCostSubmit(x)
	if err != nil {
		failure := graphPlatformAttempt{Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(body), SubmitInvocations: 1, SDKCallbacks: callbacks}
		graphPlatformError(&failure, err)
		r.Failure = &failure
		return errors.Join(err, columnFaultSave(root, *r))
	}
	if !o.Commit {
		return errUnavailable
	}
	proof, _, err := h.graphCostReadParallelOriginal(context.Background(), x, body)
	if err != nil || proof.Outcome != o {
		return errors.Join(errMismatch, err)
	}
	r.Originals = append(r.Originals, columnFaultOriginal{body, o, proof.Pair, proof.Original})
	r.Pending = nil
	return columnFaultSave(root, *r)
}

// Tiny fixed source observations come from the ordinary retained At/graph
// doors, not a replayed graph model. At most six physical current keys.
func columnFaultSourceView(h *harness, c certificate) (columnFaultView, error) {
	snapshot, err := h.at(cut{c}, 1, nil)
	if err != nil {
		return columnFaultView{}, err
	}
	if len(snapshot.Values[0])+len(snapshot.Values[1]) > 6 {
		return columnFaultView{}, errLimit
	}
	view, err := h.graphAt(cut{c})
	if err != nil {
		return columnFaultView{}, err
	}
	return columnFaultView{c, snapshot.Values, view}, nil
}
func columnFaultCheckSource(h *harness, r columnFaultState) error {
	if len(r.Views) != 3 || len(r.Originals) != 5 || r.Final.Graph != h.config.Graph || r.Final.Groups[0]+r.Final.Groups[1] != 5 {
		return errInvalid
	}
	for i, c := range []certificate{r.Initial, r.Old, r.Final} {
		observed, err := columnFaultSourceView(h, c)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(observed, r.Views[i]) {
			return errMismatch
		}
	}
	groups, err := h.changes(cut{r.Initial}, cut{r.Final}, 1)
	if err != nil {
		return err
	}
	if len(groups) != 5 {
		return errMismatch
	}
	// CDC enumeration is genuine round/coordinator/request order, independent
	// of the separately captured setup provenance execution order1,2,3.
	for _, original := range r.Originals {
		x, err := decode[transaction](original.Proposal)
		if err != nil {
			return err
		}
		proof, _, err := h.graphCostReadParallelOriginal(context.Background(), x, original.Proposal)
		if err != nil {
			return err
		}
		if proof.Outcome != original.Outcome || !reflect.DeepEqual(proof.Pair, original.Originals) || !reflect.DeepEqual(proof.Original, original.CDC) {
			return errMismatch
		}
		matches := 0
		for _, g := range groups {
			if g.Tx.ID == x.ID && g.Tx.Coordinator == x.Coordinator {
				matches++
				if !reflect.DeepEqual(g, proof.Group) {
					return errMismatch
				}
			}
		}
		if matches != 1 {
			return errMismatch
		}
	}
	for _, check := range []struct {
		c     certificate
		value int64
	}{{r.Old, 5}, {r.Final, 6}} {
		view, err := h.graphAt(cut{check.c})
		if err != nil {
			return err
		}
		if err = graphCostCheckView(view, h.config.Cost, check.value); err != nil {
			return err
		}
	}
	current, err := h.current()
	if err != nil {
		return err
	}
	view, err := h.projectGraph(current)
	if err != nil {
		return err
	}
	return graphCostCheckView(view, h.config.Cost, 6)
}
func columnFaultMarker(root *os.Root, s *graphColumnStore, pins *graphColumnPins, r *columnFaultState, stage string, g graphColumnGeneration, ordinal uint64, chunkHash [32]byte) error {
	rows, err := columnFaultControls(s, pins, *r)
	if err != nil {
		return err
	}
	event := columnFaultStage{Name: stage, Records: rows}
	if stage != "native-manifest-commit-issued" {
		key := s.generationKey(g.ID, "manifest")
		got, err := columnFaultRecords(s, g, []fdb.Key{key, s.generationKey(g.ID, "progress"), s.key("ledger")})
		if err != nil {
			return err
		}
		if got[0].Value != nil {
			return errMismatch
		}
		event.Missing = bytes.Clone(key)
		delivered := 0
		eof, refused := s.walk(context.Background(), g.ID, cut{g.Cut}, nil, func(graphColumnRow) error { delivered++; return nil })
		if !errors.Is(refused, errUnavailable) || !graphColumnSame(eof, graphColumnManifest{}) || delivered != 0 {
			return errors.Join(errMismatch, refused)
		}
		event.Refusal = "manifest-unavailable-no-eof"
		progress, err := decode[graphColumnProgress](got[1].Value)
		if err != nil {
			return err
		}
		if stage == "source-eof" && !graphColumnComplete(progress) {
			return errUnavailable
		}
		if stage == "chunk" {
			key = s.chunkKey(g.ID, ordinal)
			chunk, err := columnFaultRecords(s, g, []fdb.Key{key})
			if err != nil {
				return err
			}
			if sha256.Sum256(chunk[0].Value) != chunkHash || progress.Next != ordinal+1 {
				return errMismatch
			}
			event.Records = append(event.Records, chunk...)
		}
	}
	r.Stages = append(r.Stages, event)
	if err = columnFaultSave(root, *r); err != nil {
		return err
	}
	saved, err := root.Open("state.json")
	if err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(saved, graphPlatformBytes+1))
	if err = errors.Join(err, saved.Close()); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Ready, Format, Case string
		Coordinator         uint8
		Generation          graphColumnGeneration
		StateSHA256         [32]byte
	}{stage, columnFaultFormat, r.Case, r.Coordinator, g, sha256.Sum256(body)})
}
func columnFaultCache(root *os.Root, s *graphColumnStore, owner *graphColumnRead, m graphColumnManifest) (cache *os.Root, err error) {
	name := "cache-" + m.Generation.ID
	if err = root.Mkdir(name, 0700); err != nil {
		return nil, err
	}
	cache, err = root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, cache.Close())
			cache = nil
		}
	}()
	// Copy only authentic bounded SDK bytes while the actual retained read owner
	// conflicts with collection. walk below performs all original/census checks.
	for n := uint64(0); n < m.Chunks; n++ {
		got, err := s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			if err := owner.check(tr); err != nil {
				return nil, err
			}
			return tr.Get(s.chunkKey(m.Generation.ID, n)).Get()
		})
		if err != nil {
			return nil, err
		}
		raw := got.([]byte)
		if len(raw) == 0 || len(raw) > s.binding.Policy.ChunkBytes {
			return nil, errUnavailable
		}
		f, err := cache.OpenFile(graphColumnCacheName(m.Generation, n), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, writeErr := f.Write(raw)
		if err = errors.Join(writeErr, f.Close()); err != nil {
			return nil, err
		}
	}
	return cache, nil
}
func columnFaultCached(s *graphColumnStore, owner *graphColumnRead, at cut, cache *os.Root) (columnFaultStage, error) {
	event := columnFaultStage{Name: "cached-" + owner.id.Generation}
	m, err := owner.walk(context.Background(), at, cache, func(row graphColumnRow) error {
		if len(event.Rows) >= 5 {
			return errLimit
		}
		event.Rows = append(event.Rows, row)
		return nil
	})
	if err != nil {
		return columnFaultStage{}, err
	}
	event.Manifest = m
	if uint64(len(event.Rows)) != m.Rows {
		return columnFaultStage{}, errMismatch
	}
	return event, nil
}
func columnFaultCollect(s *graphColumnStore, pins *graphColumnPins, r *columnFaultState) (columnFaultPage, error) {
	b := graphColumnCollectBudget{Chunks: 1, Bytes: 64 << 10, OutputBytes: 1024}
	result, err := pins.collect(context.Background(), "old", b)
	if err != nil {
		return columnFaultPage{}, err
	}
	if result.WorkChunks != 1 || result.Cursor != uint64(len(r.Pages)+1) || result.Done != (result.Cursor == r.OldManifest.Chunks) {
		return columnFaultPage{}, errMismatch
	}
	key := s.chunkKey("old", result.Cursor-1)
	raw, err := columnFaultRecords(s, r.OldManifest.Generation, []fdb.Key{key, s.generationKey("old", "owners"), s.key("ledger")})
	if err != nil {
		return columnFaultPage{}, err
	}
	if raw[0].Value != nil {
		return columnFaultPage{}, errMismatch
	}
	page := columnFaultPage{b, result, raw[1:], raw[0]}
	r.Pages = append(r.Pages, page)
	return page, nil
}
func runGraphColumnFault(db fdb.Database, mode string) (err error) {
	c, name, coord, err := columnFaultSettings()
	if err != nil {
		return err
	}
	root, err := columnFaultRoot()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	h := &harness{db: db, config: c}
	if mode == "column-fault-seed" {
		h, err = newHarness(db, c)
		if err != nil {
			return err
		}
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		p, pinPolicy := columnFaultPolicies()
		r := columnFaultState{Version: 1, Format: columnFaultFormat, Case: name, Coordinator: coord, Config: c, Columns: p, Pins: pinPolicy, Initial: initial.record}
		first, err := columnFaultSourceView(h, r.Initial)
		if err != nil {
			return err
		}
		if len(first.Values[0])+len(first.Values[1]) != 0 {
			return errMismatch
		}
		r.Views = append(r.Views, first)
		for i := range 3 {
			if err = columnFaultApply(root, h, &r, "setup", uint64(i)); err != nil {
				return err
			}
		}
		if err = columnFaultApply(root, h, &r, "create", 0); err != nil {
			return err
		}
		old, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r.Old = old.record
		oldView, err := columnFaultSourceView(h, r.Old)
		if err != nil {
			return err
		}
		r.Views = append(r.Views, oldView)
		s, pins, err := columnFaultStores(h, r)
		if err != nil {
			return err
		}
		r.OldManifest, err = s.publish(context.Background(), "old", initial, old)
		if err != nil {
			return err
		}
		r.Reference, err = pins.reference(context.Background(), "head-old", "head", "old", 0)
		if err != nil {
			return err
		}
		r.Token, err = pins.pin(context.Background(), "held-old", "old", old)
		if err != nil {
			return err
		}
		owner, err := newGraphColumnRead(s, "old", &r.Token)
		if err != nil {
			return err
		}
		r.Owner = owner.id
		// Persist caller-established identity BEFORE native acquisition. Only the
		// child opens it; process death can neither erase nor replace this intent.
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	r, err := columnFaultLoad(root)
	if err != nil {
		return err
	}
	s, pins, err := columnFaultStores(h, r)
	if err != nil {
		return err
	}
	switch mode {
	case "column-fault-publication-child":
		if len(r.Originals) != 4 || len(r.Stages) != 0 {
			return errInvalid
		}
		owner, err := columnFaultOwner(s, r)
		if err != nil {
			return err
		}
		if err = owner.open(context.Background()); err != nil {
			return err
		}
		if err = columnFaultStageAt(s, pins, &r, "owner-active-before-update", owner, cut{r.Old}); err != nil {
			return err
		}
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		if err = columnFaultApply(root, h, &r, "update", 0); err != nil {
			return err
		}
		final, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r.Final = final.record
		finalView, err := columnFaultSourceView(h, r.Final)
		if err != nil {
			return err
		}
		r.Views = append(r.Views, finalView)
		if err = columnFaultCheckSource(h, r); err != nil {
			return err
		}
		if err = columnFaultStageAt(s, pins, &r, "held-old-after-update", owner, cut{r.Old}); err != nil {
			return err
		}
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		observer := func(e graphColumnPublicationEvent) error {
			if r.Case == "chunk" && e.Stage == "chunk-upload-returned" && e.Ordinal == 0 {
				if err := columnFaultMarker(root, s, pins, &r, "chunk", e.Generation, e.Ordinal, e.ChunkSHA256); err != nil {
					return err
				}
				select {}
			}
			if e.Stage == "source-eof" {
				if r.Case == "source-eof" {
					if err := columnFaultMarker(root, s, pins, &r, "source-eof", e.Generation, e.Ordinal, [32]byte{}); err != nil {
						return err
					}
					select {}
				}
				if r.Case == "manifest-issued" {
					return errColumnFaultSeam
				}
			}
			return nil
		}
		_, err = s.publishObserved(context.Background(), "final", cut{r.Initial}, final, observer)
		if r.Case != "manifest-issued" || !errors.Is(err, errColumnFaultSeam) {
			return errors.Join(errMismatch, err)
		}
		g := graphColumnGeneration{Version: 1, ID: "final", Binding: s.digest, Initial: r.Initial, Cut: r.Final}
		tr, err := db.CreateTransaction()
		if err != nil {
			return err
		}
		defer tr.Cancel()
		if err = tr.Options().SetTimeout(10000); err != nil {
			return err
		}
		tentative, err := s.sealIn(tr, g)
		if err != nil {
			return err
		}
		future := tr.Commit()
		if err = columnFaultMarker(root, s, pins, &r, "native-manifest-commit-issued", g, 0, [32]byte{}); err != nil {
			return err
		}
		nativeErr := future.Get()
		a := graphPlatformAttempt{Classification: "native-manifest-commit-returned-public-reply-withheld", SubmitInvocations: 1, SDKCallbacks: 1}
		if nativeErr != nil {
			graphPlatformError(&a, nativeErr)
		}
		if err = json.NewEncoder(os.Stderr).Encode(struct {
			Native    graphPlatformAttempt
			Tentative graphColumnManifest
		}{a, tentative}); err != nil {
			return err
		}
		select {}
	case "column-fault-recover":
		if len(r.Originals) != 5 || len(r.Stages) != 3 {
			return errInvalid
		}
		if err = columnFaultCheckSource(h, r); err != nil {
			return err
		}
		owner, err := columnFaultOwner(s, r)
		if err != nil {
			return err
		}
		if err = owner.open(context.Background()); err != nil {
			return err
		}
		r.FinalManifest, err = s.publish(context.Background(), "final", cut{r.Initial}, cut{r.Final})
		if err != nil {
			return err
		}
		r.Reference, err = pins.reference(context.Background(), "head-final", "head", "final", r.Reference.Version)
		if err != nil {
			return err
		}
		if err = pins.finish(context.Background(), "expire-old", r.Token, true); err != nil {
			return err
		}
		if _, refused := pins.open(context.Background(), r.Token); !errors.Is(refused, errColumnExpired) {
			return errors.Join(errMismatch, refused)
		}
		if _, refused := pins.collect(context.Background(), "old", graphColumnCollectBudget{Chunks: 1, Bytes: 64 << 10, OutputBytes: 1024}); !errors.Is(refused, errColumnInUse) {
			return errors.Join(errMismatch, refused)
		}
		if err = columnFaultStageAt(s, pins, &r, "expired-new-open-refused-held-owner-live", owner, cut{r.Old}); err != nil {
			return err
		}
		cache, err := columnFaultCache(root, s, owner, r.OldManifest)
		if err != nil {
			return err
		}
		cached, err := columnFaultCached(s, owner, cut{r.Old}, cache)
		if err = errors.Join(err, cache.Close()); err != nil {
			return err
		}
		r.Stages = append(r.Stages, cached)
		if err = owner.Close(); err != nil {
			return err
		}
		if err = columnFaultStageAt(s, pins, &r, "original-owner-authenticated-close", nil, cut{}); err != nil {
			return err
		}
		current, err := newGraphColumnRead(s, "final", nil)
		if err != nil {
			return err
		}
		if err = current.open(context.Background()); err != nil {
			return errors.Join(err, columnReadCleanup(current))
		}
		if err = columnFaultStageAt(s, pins, &r, "final-generation", current, cut{r.Final}); err != nil {
			return errors.Join(err, columnReadCleanup(current))
		}
		cache, err = columnFaultCache(root, s, current, r.FinalManifest)
		if err != nil {
			return errors.Join(err, columnReadCleanup(current))
		}
		cached, err = columnFaultCached(s, current, cut{r.Final}, cache)
		err = errors.Join(err, cache.Close(), columnReadCleanup(current))
		if err != nil {
			return err
		}
		r.Stages = append(r.Stages, cached)
		for _, point := range []struct {
			cut  certificate
			want int64
		}{{r.Old, 5}, {r.Final, 6}} {
			row, found, err := s.point(context.Background(), "final", cut{point.cut}, 7)
			if err != nil || !found || row.Value != point.want {
				return errors.Join(errMismatch, err)
			}
		}
		if _, found, err := s.point(context.Background(), "final", cut{r.Final}, 999); err != nil || found {
			return errors.Join(errMismatch, err)
		}
		if err = columnFaultStageAt(s, pins, &r, "collection-ready", nil, cut{}); err != nil {
			return err
		}
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "column-fault-gc-child":
		if len(r.Stages) != 9 || len(r.Pages) != 0 {
			return errInvalid
		}
		if _, err = columnFaultCollect(s, pins, &r); err != nil {
			return err
		}
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		if err = json.NewEncoder(os.Stdout).Encode(struct {
			Ready, Format, Case string
			Coordinator         uint8
			Page                columnFaultPage
		}{"derived-gc-page-committed", r.Format, r.Case, r.Coordinator, r.Pages[0]}); err != nil {
			return err
		}
		select {}
	case "column-fault-terminal":
		if len(r.Pages) != 1 {
			return errInvalid
		}
		for len(r.Pages) < int(r.OldManifest.Chunks) {
			if _, err = columnFaultCollect(s, pins, &r); err != nil {
				return err
			}
			if err = columnFaultSave(root, r); err != nil {
				return err
			}
		}
		if _, refused := s.publish(context.Background(), "old", cut{r.Initial}, cut{r.Old}); !errors.Is(refused, errColumnExpired) {
			return errors.Join(errMismatch, refused)
		}
		if _, refused := pins.collect(context.Background(), "final", graphColumnCollectBudget{Chunks: 1, Bytes: 64 << 10, OutputBytes: 1024}); !errors.Is(refused, errColumnInUse) {
			return errors.Join(errMismatch, refused)
		}
		if err = columnFaultCheckSource(h, r); err != nil {
			return err
		}
		for _, original := range r.Originals {
			x, err := decode[transaction](original.Proposal)
			if err != nil {
				return err
			}
			o, _, err := h.graphCostSubmit(x)
			if err != nil || o != original.Outcome {
				return errors.Join(errMismatch, err)
			}
		}
		if err = columnFaultCheckSource(h, r); err != nil {
			return err
		}
		r.Sources, err = h.graphCostArtifacts(root, cut{r.Initial}, cut{r.Old}, cut{r.Final}, graphCostStreamPolicy{PageRows: 8, PageBytes: 256 << 10})
		if err != nil {
			return err
		}
		if r.Sources[0].Rows[0]+r.Sources[0].Rows[1] != 9 || r.Sources[1].Rows[0]+r.Sources[1].Rows[1] != 6 || r.Sources[2].Rows[0]+r.Sources[2].Rows[1] != 5 {
			return errMismatch
		}
		if err = columnFaultStageAt(s, pins, &r, "terminal-lineage-preserved", nil, cut{}); err != nil {
			return err
		}
		if err = columnFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Format                            string
			SetupOperations, UsefulOperations int
			Record                            columnFaultState
			Scope                             string
		}{r.Format, 3, 2, r, "actual publication/owner/collector child replacement; source KV retained; finite one-fact scope, not matrix/storage/capacity acceptance"})
	default:
		return fmt.Errorf("unsupported explicit publication mode %s: %w", mode, errInvalid)
	}
}
