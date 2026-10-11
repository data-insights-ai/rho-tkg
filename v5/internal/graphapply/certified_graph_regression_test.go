package graphapply

import (
	"testing"
)

// This regression measures the missing prerequisite in the accepted write
// seam. A publication changes snapshot-visible routing even before any entity
// is written, so an entity-write-only round map cannot reproduce that snapshot.
func TestCertifiedReadPublicationNeedsMappedRound(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	before, err := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}, h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	i := n.activate(0, [16]byte{1})
	n.acquire(0, i, 1, 16)
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	floor, err := readPartitionRoundFloor(&q, before.configuration)
	if err != nil {
		t.Fatal(err)
	}
	after, err := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{2}}, h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	if after.epoch <= before.epoch {
		t.Fatal("fixture did not install routing publication")
	}
	if floor == 0 {
		t.Fatalf("snapshot-visible publication advanced semantic epoch %d -> %d but installed round stayed 0", before.epoch, after.epoch)
	}
	wire, found, err := q.getBounded(partitionRoundMapKey(h.machine.ns, floor), 256)
	if err != nil || !found || len(wire) == 0 {
		t.Fatal("published routing has no retained round root", floor, found, err)
	}
}

// Guard: the historical write-only agreement remains a separate binding, and
// cannot be reopened using a factory that promises missing cut prerequisites.
func TestCertifiedGraphFactoryRefusesWriteOnlyBinding(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	h := n.hosts[0][0]
	if _, e := newCertifiedGraphMaterializer(h.machine.store, h.machine.ns, n.d, h.machine.limits); e == nil {
		t.Fatal("write-only binding reopened as certified")
	}
}
