package importmap

import (
	"bytes"
	"encoding/json"
)

// OpsJSON is the op stream as a JSON array, one op per line. The bytes are
// stable: same source, same bytes.
func (r *Result) OpsJSON() []byte { return lines(len(r.Ops), func(i int) any { return r.Ops[i] }) }

// AmbiguityJSON is the ambiguity report as a JSON array, one row per line, sorted.
func (r *Result) AmbiguityJSON() []byte {
	return lines(len(r.Ambiguities), func(i int) any { return r.Ambiguities[i] })
}

func lines(n int, at func(int) any) []byte {
	var b bytes.Buffer
	b.WriteString("[")
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n")
		enc, err := json.Marshal(at(i))
		if err != nil {
			panic(err) // only PropValue can fail, and Map never stores an invalid one
		}
		b.Write(enc)
	}
	b.WriteString("\n]\n")
	return b.Bytes()
}
