package raftlog

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// Two disjoint physical ranges; other-bank/dormant metadata is never traversed.
type applicationIterator struct {
	it       *pebble.Iterator
	open     func(*pebble.IterOptions) (*pebble.Iterator, error)
	bank     byte
	controls bool
	part     int
	err      error
}

func newApplicationIterator(open func(*pebble.IterOptions) (*pebble.Iterator, error), bank byte, controls bool) (*applicationIterator, error) {
	a := &applicationIterator{open: open, bank: bank, controls: controls}
	err := a.reset(0)
	return a, err
}
func (a *applicationIterator) reset(part int) error {
	if a.it != nil {
		if err := a.it.Close(); err != nil {
			a.it = nil
			a.err = err
			return err
		}
	}
	a.part = part
	lo, hi := bankTag(a.bank, appDataTag), bankTag(a.bank, appOutcomeTag)+1
	if part == 1 {
		lo = controlBankTag(a.bank)
		hi = lo + 1
	}
	var err error
	a.it, err = a.open(&pebble.IterOptions{LowerBound: []byte{lo}, UpperBound: []byte{hi}})
	a.err = err
	return err
}
func (a *applicationIterator) advance(valid bool) bool {
	if valid {
		return true
	}
	if a.it != nil && a.it.Error() != nil {
		a.err = a.it.Error()
		return false
	}
	if a.part == 0 && a.controls {
		if a.reset(1) != nil {
			return false
		}
		return a.it.First()
	}
	return false
}
func (a *applicationIterator) First() bool { return a.advance(a.it.First()) }
func (a *applicationIterator) Next() bool  { return a.advance(a.it.Next()) }
func (a *applicationIterator) SeekGE(k []byte) bool {
	part := 0
	if len(k) > 0 && k[0] == controlBankTag(a.bank) {
		part = 1
	}
	if part != a.part {
		if a.reset(part) != nil {
			return false
		}
	}
	return a.advance(a.it.SeekGE(k))
}
func (a *applicationIterator) Key() []byte                  { return a.it.Key() }
func (a *applicationIterator) ValueAndErr() ([]byte, error) { return a.it.ValueAndErr() }
func (a *applicationIterator) Error() error {
	if a.err != nil {
		return a.err
	}
	return a.it.Error()
}
func (a *applicationIterator) Close() error {
	if a.it == nil {
		return a.err
	}
	return errors.Join(a.err, a.it.Close())
}
func canonicalApplicationKey(bank byte, key []byte) []byte {
	b := copyApplicationBytes(key)
	if b[0] == controlBankTag(bank) {
		b[0] = controlTag
	} else {
		b[0] -= bank * 4
	}
	return b
}
func localApplicationKey(bank byte, key []byte) []byte {
	b := copyApplicationBytes(key)
	if b[0] == controlTag {
		b[0] = controlBankTag(bank)
	} else {
		b[0] += bank * 4
	}
	return b
}
func (m ApplicationSnapshotManifest) controls() bool { return m.Version == 4 }
func (m metadata) manifestVersion() uint32 {
	if m.Controls.Config.enabled() {
		return 4
	}
	return semanticManifestVersion(m.Rep.SemanticContractID)
}
func descriptorBytesForContract(id ApplicationSemanticContractID, c ApplicationContract) int {
	n := descriptorFixedBytes(id)
	if c.Version == 2 {
		n += 16
	}
	return n
}
func sameControlLogical(previous, current []byte, limit int) bool {
	if len(previous) == 0 || previous[0] != controlTag || len(current) == 0 || current[0] != controlTag {
		return false
	}
	p, _, e := decodeControlKey(0, previous, limit)
	q, _, f := decodeControlKey(0, current, limit)
	return e == nil && f == nil && bytes.Equal(p, q)
}
