package storeutil

import (
	"testing"
	"time"
)

// DECODEBOUND (tasks/evidence/DECODEBOUND/): an 80-byte node row whose nested
// map header declares ~808 million entries took 13.6 s to reject (the fuzz
// smoke counted it as hung). A trust-boundary decode must reject it at once:
// allocate and iterate by bytes delivered, never by a declared count
// (lesson 48). Mutation: a decoder that sizes or loops by the declared count.
func TestDecodeRejectsHugeDeclaredCountQuickly(t *testing.T) {
	data := []byte("\x84\xa2000\xa1p\x91\xd80\x83\xa1v\x98\x98\x98000000\xdf0000000")
	start := time.Now()
	var w NodeWire
	err := SafeUnmarshal(data, &w)
	unmarshal := time.Since(start)
	var checked time.Duration
	if err == nil {
		start = time.Now()
		_, err = WireToNodeChecked(w)
		checked = time.Since(start)
	}
	t.Logf("SafeUnmarshal %v, WireToNodeChecked %v, err %v", unmarshal, checked, err)
	if unmarshal+checked > 100*time.Millisecond {
		t.Fatalf("rejecting an 80-byte row took %v (SafeUnmarshal %v, WireToNodeChecked %v)", unmarshal+checked, unmarshal, checked)
	}
}

// TestDecodeRejectsVisibleHugeDeclaredCount: the same claim without an
// extension around it: a map32 header declaring ~808 million entries with 2
// bytes left, decoded into an interface (the property-value path). The guard
// walks every declared element and stops at the end of the bytes, so it
// rejects the claim before the decoder sizes a map or slice by it. Mutation:
// a guard that skips a container's elements instead of walking them.
func TestDecodeRejectsVisibleHugeDeclaredCount(t *testing.T) {
	for _, data := range [][]byte{
		[]byte("\xdf\x30\x30\x30\x30\x01\x01"), // map32
		[]byte("\xdd\x30\x30\x30\x30\x01\x01"), // array32
	} {
		start := time.Now()
		var v any
		err := SafeUnmarshal(data, &v)
		took := time.Since(start)
		if err == nil || took > 100*time.Millisecond {
			t.Fatalf("% x: err %v after %v; want a quick ErrCorruptWire", data[:5], err, took)
		}
	}
}
