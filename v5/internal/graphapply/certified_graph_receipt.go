package graphapply

import (
	"encoding/binary"
	"errors"
)

type graphLocalReceipt struct {
	scope        allocationScope
	identity     [16]byte
	digest       [32]byte
	round, index uint64
}

func graphLocalReceiptKey(n namespace, id [16]byte) []byte { return append(recordKey(n, 16), id[:]...) }
func encodeGraphLocalReceipt(r graphLocalReceipt) []byte {
	b := appendAllocationScope([]byte{'G', 'S', 'L', 1}, r.scope)
	b = append(b, r.identity[:]...)
	b = append(b, r.digest[:]...)
	b = binary.BigEndian.AppendUint64(b, r.round)
	return seal(binary.BigEndian.AppendUint64(b, r.index))
}
func decodeGraphLocalReceipt(w []byte) (graphLocalReceipt, error) {
	body, e := graphBody(w, "GSL\x01", 256)
	if e != nil || len(body) != allocationScopeBytes-36+64 {
		return graphLocalReceipt{}, errors.Join(errCorrupt, e)
	}
	d := decoder{body}
	r := graphLocalReceipt{scope: readAllocationScope(&d), identity: d.array()}
	copy(r.digest[:], d.b[:32])
	d.b = d.b[32:]
	r.round, r.index = d.number(), d.number()
	if !r.scope.valid() || r.scope.semantic != certifiedGraphSemanticContractID() || r.identity == ([16]byte{}) || r.index == 0 {
		return graphLocalReceipt{}, errCorrupt
	}
	return r, nil
}
func (m *declaredMaterializer) writeGraphLocalReceipt(q *reader, r graphRequest, o outcome, round uint64, hash [32]byte) error {
	if !m.certifiedReads() {
		return nil
	}
	if e := reserveComposer(q, 2048); e != nil {
		return e
	}
	receipt := graphLocalReceipt{scope: m.scope(), identity: r.identity(), digest: hash, round: round, index: o.index}
	return q.put(graphLocalReceiptKey(q.ns, receipt.identity), encodeGraphLocalReceipt(receipt))
}
func graphReceiptFromLocal(p graphCutProof) (graphCommitReceipt, error) {
	if p.question.kind != 3 {
		return graphCommitReceipt{}, errInvalid
	}
	if e := checkGraphCutProof(p, p.question); e != nil {
		return graphCommitReceipt{}, e
	}
	r := p.observation.local
	if r.scope != p.observation.scope || r.identity != p.question.request || r.index > p.observation.index {
		return graphCommitReceipt{}, errInvalid
	}
	return graphCommitReceipt{local: p}, nil
}
