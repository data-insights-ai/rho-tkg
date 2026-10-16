package graphapply

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

// Seventeen genuine facts cross the HOME grant boundary and the incident page
// boundary. Both process cohorts use the same six physical voter directories.
func TestNativeGraphCostPhysicalReopenAndIndexesSmallActualProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, cell := range []struct {
		name  string
		cross uint64
	}{{"local", 0}, {"cross", 100}} {
		t.Run(cell.name, func(t *testing.T) {
			runNativeGraphCostWithBlock(t, nativeGraphCostSpec{Version: 1, Facts: 17, Writers: 1, CrossPercent: cell.cross}, 32)
		})
	}
}

// Inert bounds/shape guards; genuine authority and complete original histories
// are asserted by the finite same-directory native process parent above.
func TestNativeGraphCostReopenIndexPagesRejectMalformedAndOversizedInput(t *testing.T) {
	page := nativeGraphCostIndexPage{Kind: 1, Endpoint: 1, Complete: true, Rows: []nativeGraphCostIndexRow{}}
	wire, err := nativeGraphCostEncodeIndexPage(page)
	if err != nil {
		t.Fatal(err)
	}
	got, err := nativeGraphCostDecodeIndexPage(wire)
	if err != nil || !reflect.DeepEqual(got, page) {
		t.Fatal(got, err)
	}
	for _, mutate := range []func(*nativeGraphCostIndexPage){
		func(p *nativeGraphCostIndexPage) { p.Kind = 0 },
		func(p *nativeGraphCostIndexPage) { p.Next = 1 },
		func(p *nativeGraphCostIndexPage) { p.Complete = false; p.Next = 1 },
		func(p *nativeGraphCostIndexPage) { p.Kind = 2 },
	} {
		bad := page
		mutate(&bad)
		wire, err := nativeGraphCostEncodeIndexPage(bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeGraphCostDecodeIndexPage(wire); !errors.Is(err, errCorrupt) {
			t.Fatal("bad index page accepted", err)
		}
	}
	for _, bad := range [][]byte{nil, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		if _, err := nativeGraphCostDecodeIndexPage(bad); !errors.Is(err, errCorrupt) {
			t.Fatal("bad index framing accepted", err)
		}
	}
	if _, err := nativeGraphCostDecodeIndexPage(make([]byte, nativeGraphCostPageBytes+1)); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	page.Rows = make([]nativeGraphCostIndexRow, 17)
	if _, err := nativeGraphCostEncodeIndexPage(page); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	for _, ctx := range []context.Context{nil, t.Context()} {
		if out, err := nativeGraphCostIndexObserve(ctx, nil, processRequest{}); !errors.Is(err, errInvalid) || out != nil {
			t.Fatal("nil cost index owner", out, err)
		}
	}
}
