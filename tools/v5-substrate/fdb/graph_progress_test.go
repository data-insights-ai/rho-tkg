package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// These are real native database checks; the launcher separately supplies the
// actual zone kill and same-volume reopen. No helper test claims a server fault.
func TestNativeGraphProgressRecoversFrozenRequestWithoutDuplicateCDC(t *testing.T) {
	h, _ := faultFixture(t)
	if err := runGraphFault(h.db, "graph-fault-progress"); !errors.Is(err, errInvalid) {
		t.Fatal("progress before a captured request", err)
	}
	if err := runGraphFault(h.db, "graph-fault-prepare-progress"); !errors.Is(err, errInvalid) {
		t.Fatal("progress preparation before kill/recovery phases", err)
	}
	if err := runGraphFault(h.db, "graph-fault-recover-before"); err != nil {
		t.Fatal(err)
	}
	r, err := graphFaultLoad()
	if err != nil || r.AfterTx == nil {
		t.Fatal(r, err)
	}
	mustSubmit(t, h, *r.AfterTx)
	if err = runGraphFault(h.db, "graph-fault-recover-after"); err != nil {
		t.Fatal(err)
	}
	if err = runGraphFault(h.db, "graph-fault-prepare-progress"); err != nil {
		t.Fatal(err)
	}
	r, err = graphFaultLoad()
	if err != nil || r.ProgressTx == nil || r.Progress != nil {
		t.Fatal(r, err)
	}
	original := cloneDTO(t, *r.ProgressTx)
	// Commit without recording a caller receipt: subsequent progress must replay
	// the frozen input, even though its captured generation is now stale.
	committed := mustSubmit(t, h, original)
	if err = runGraphFault(h.db, "graph-fault-progress"); err != nil {
		t.Fatal(err)
	}
	r, err = graphFaultLoad()
	if err != nil || r.Progress == nil || *r.Progress != committed || !reflect.DeepEqual(*r.ProgressTx, original) {
		t.Fatal("unknown progress rebuilt input or changed original result", r, err)
	}
	before, err := h.current()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"graph-fault-progress", "graph-fault-prepare-progress", "graph-fault-terminal"} {
		if err = runGraphFault(h.db, mode); err != nil {
			t.Fatal(mode, err)
		}
	}
	after, err := h.current()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("progress replay changed revisions", after, err)
	}
	if err = graphFaultCheck(h, r); err != nil {
		t.Fatal("old graph/history or complete CDC changed", err)
	}
	group, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		return tr.Get(h.changeKey(1, committed.Sequence, committed.Request)).Get()
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := decode[changeGroup](group.([]byte))
	if err != nil || len(changes.Writes) != 3 {
		t.Fatal(changes, err)
	}
	changes.Writes = changes.Writes[:2]
	restore := replaceNativeRecord(t, h, h.changeKey(1, committed.Sequence, committed.Request), changes)
	if err = graphFaultCheck(h, r); !errors.Is(err, errInvalid) {
		t.Fatal("partial progress CDC accepted", err)
	}
	restore()
	bad := cloneDTO(t, r)
	bad.ProgressTx.Dependency++
	if err = graphFaultCheck(h, bad); !errors.Is(err, errInvalid) {
		t.Fatal("progress input not bound to original result", err)
	}
}

func TestNativeGraphTerminalRefusesIncompleteProgress(t *testing.T) {
	h, _ := faultFixture(t)
	if err := runGraphFault(h.db, "graph-fault-terminal"); !errors.Is(err, errInvalid) {
		t.Fatal("seed classified as completed graph faults", err)
	}
}
