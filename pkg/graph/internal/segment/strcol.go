package segment

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// A string column holds n strings: u32 n | u8 mode | body, where the body is
//
//	dict   u32 count | count × (u32 len | bytes), strictly ascending | int column of codes
//	plain  u32 len | int column of cumulative end offsets | blob
//	hex32  n × 32 bytes, each value the lowercase hex spelling of its 32 bytes
//
// The writer picks hex32 when every value is a 64-character lowercase hex
// string (hashes), plain when the column has more than 256 distinct values
// and more than one per 16 rows (so a dictionary never grows with the rows,
// ADR-0011 §2.2), and dict otherwise. Dictionaries are sorted, so a value or
// a range maps to a code range.
const (
	strDict   byte = 0
	strPlain  byte = 1
	strHex32  byte = 2
	strShared byte = 3 // int column of codes into the store's StringDict for the column

	dictMinDistinct = 256
	dictRowsPerCode = 16
)

func allEmpty(vals []string) bool {
	for _, v := range vals {
		if v != "" {
			return false
		}
	}
	return true
}

func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// encodeSharedStrCol writes codes into a StringDict.
func encodeSharedStrCol(codes []int64, pageRows int) []byte {
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(codes))) // #nosec G115 -- bounded by the writer
	out = append(out, strShared)
	return append(out, encodeIntCol(codes, pageRows, intEncodeCtx{allowed: allow(transformNone)})...)
}

func encodeStrCol(vals []string, pageRows int) []byte {
	n := len(vals)
	out := binary.LittleEndian.AppendUint32(nil, uint32(n)) // #nosec G115 -- bounded by the writer
	hex32 := n > 0
	for _, v := range vals {
		if !isLowerHex64(v) {
			hex32 = false
			break
		}
	}
	if hex32 {
		out = append(out, strHex32)
		for _, v := range vals {
			b, _ := hex.DecodeString(v) // checked by isLowerHex64
			out = append(out, b...)
		}
		return out
	}
	codes := make(map[string]int64)
	for _, v := range vals {
		codes[v] = 0
	}
	if len(codes) > dictMinDistinct && len(codes)*dictRowsPerCode > n {
		out = append(out, strPlain)
		ends := make([]int64, n)
		var blob []byte
		for i, v := range vals {
			blob = append(blob, v...)
			ends[i] = int64(len(blob))
		}
		col := encodeIntCol(ends, pageRows, intEncodeCtx{allowed: allow(transformNone)})
		out = binary.LittleEndian.AppendUint32(out, uint32(len(col))) // #nosec G115 -- bounded by the writer
		out = append(out, col...)
		return append(out, blob...)
	}
	dict := make([]string, 0, len(codes))
	for v := range codes {
		dict = append(dict, v)
	}
	sort.Strings(dict)
	out = append(out, strDict)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dict))) // #nosec G115 -- bounded by n
	for i, v := range dict {
		codes[v] = int64(i)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v))) // #nosec G115 -- property values are bounded by MaxPropertyValueSize
		out = append(out, v...)
	}
	cv := make([]int64, n)
	for i, v := range vals {
		cv[i] = codes[v]
	}
	return append(out, encodeIntCol(cv, pageRows, intEncodeCtx{allowed: allow(transformNone)})...)
}

// strCol is a parsed string column; the zero value reads every value as "".
type strCol struct {
	name    string
	present bool
	n       int
	mode    byte
	dict    []string
	shared  *StringDict // strShared: codes index its current snapshot
	codes   intCol
	ends    intCol
	blob    []byte
}

// parseStrCol validates a string column. The dictionary is copied into the
// heap (strings handed to callers never alias the segment bytes); its size is
// bounded by the section's own bytes. want -1 takes n from the section.
func parseStrCol(name string, b []byte, want, pageRows int, sd *StringDict) (strCol, error) {
	if len(b) < 5 {
		return strCol{}, corrupt(name, "string column header truncated")
	}
	c := strCol{name: name, present: true, n: int(binary.LittleEndian.Uint32(b)), mode: b[4]}
	if want >= 0 && c.n != want {
		return strCol{}, corrupt(name, "holds %d values, want %d", c.n, want)
	}
	body := b[5:]
	switch c.mode {
	case strHex32:
		if len(body)/32 != c.n || len(body)%32 != 0 {
			return strCol{}, corrupt(name, "hex32 body is %d bytes for %d values", len(body), c.n)
		}
		c.blob = body
	case strPlain:
		if len(body) < 4 {
			return strCol{}, corrupt(name, "plain header truncated")
		}
		l := int(binary.LittleEndian.Uint32(body))
		if l > len(body)-4 {
			return strCol{}, corrupt(name, "plain offsets truncated")
		}
		ends, err := parseIntCol(name, body[4:4+l], c.n, pageRows, allow(transformNone))
		if err != nil {
			return strCol{}, err
		}
		c.ends, c.blob = ends, body[4+l:]
	case strDict:
		if len(body) < 4 {
			return strCol{}, corrupt(name, "dictionary header truncated")
		}
		count := int(binary.LittleEndian.Uint32(body))
		body = body[4:]
		if count > len(body)/4 {
			return strCol{}, corrupt(name, "dictionary of %d entries in %d bytes", count, len(body))
		}
		c.dict = make([]string, count)
		for i := range c.dict {
			if len(body) < 4 {
				return strCol{}, corrupt(name, "dictionary entry %d truncated", i)
			}
			l := int(binary.LittleEndian.Uint32(body))
			if l > len(body)-4 {
				return strCol{}, corrupt(name, "dictionary entry %d truncated", i)
			}
			c.dict[i] = string(body[4 : 4+l])
			body = body[4+l:]
			if i > 0 && c.dict[i] <= c.dict[i-1] {
				return strCol{}, corrupt(name, "dictionary not strictly ascending at %d", i)
			}
		}
		codes, err := parseIntCol(name, body, c.n, pageRows, allow(transformNone))
		if err != nil {
			return strCol{}, err
		}
		c.codes = codes
	case strShared:
		if sd == nil {
			return strCol{}, fmt.Errorf("%w: column %s holds codes into its store's string dictionary", ErrUnsupportedVersion, name)
		}
		codes, err := parseIntCol(name, body, c.n, pageRows, allow(transformNone))
		if err != nil {
			return strCol{}, err
		}
		// Every code must name a value now (the dictionary only grows), so
		// a column consumer can index the dictionary without a check.
		limit := int64(sd.Len())
		buf := make([]int64, min(pageRows, max(c.n, 1)))
		for p := 0; p*pageRows < c.n; p++ {
			cnt := min(pageRows, c.n-p*pageRows)
			codes.decodePage(p, cnt, buf, nil, nil)
			for k, x := range buf[:cnt] {
				if x < 0 || x >= limit {
					return strCol{}, corrupt(name, "value %d has code %d of %d", p*pageRows+k, x, limit)
				}
			}
		}
		c.codes, c.shared = codes, sd
	default:
		return strCol{}, corrupt(name, "unknown string mode %d", c.mode)
	}
	return c, nil
}

func (c *strCol) at(i int) (string, error) {
	if !c.present {
		return "", nil
	}
	switch c.mode {
	case strHex32:
		return hex.EncodeToString(c.blob[32*i : 32*i+32]), nil
	case strPlain:
		lo := int64(0)
		if i > 0 {
			lo = c.ends.atNoRef(i - 1)
		}
		hi := c.ends.atNoRef(i)
		if lo < 0 || hi < lo || hi > int64(len(c.blob)) {
			return "", corrupt(c.name, "value %d spans [%d,%d) of %d bytes", i, lo, hi, len(c.blob))
		}
		return string(c.blob[lo:hi]), nil
	case strShared:
		dict := *c.shared.snap.Load()
		code := c.codes.atNoRef(i)
		if code < 0 || code >= int64(len(dict)) {
			return "", corrupt(c.name, "value %d has code %d of %d", i, code, len(dict))
		}
		return dict[code], nil
	default:
		code := c.codes.atNoRef(i)
		if code < 0 || code >= int64(len(c.dict)) {
			return "", corrupt(c.name, "value %d has code %d of %d", i, code, len(c.dict))
		}
		return c.dict[code], nil
	}
}

// A bytes column holds n byte slices, each nil or not:
//
//	u32 n | u32 len | int column of cumulative end offsets | u32 len | int column of state (0 nil, 1 set) | blob
func allNil(vals [][]byte) bool {
	for _, v := range vals {
		if v != nil {
			return false
		}
	}
	return true
}

func encodeBytesCol(vals [][]byte, pageRows int) []byte {
	n := len(vals)
	ends := make([]int64, n)
	state := make([]int64, n)
	var blob []byte
	for i, v := range vals {
		blob = append(blob, v...)
		ends[i] = int64(len(blob))
		if v != nil {
			state[i] = 1
		}
	}
	out := binary.LittleEndian.AppendUint32(nil, uint32(n)) // #nosec G115 -- bounded by the writer
	e := encodeIntCol(ends, pageRows, intEncodeCtx{allowed: allow(transformNone)})
	out = binary.LittleEndian.AppendUint32(out, uint32(len(e))) // #nosec G115 -- bounded by the writer
	out = append(out, e...)
	s := encodeIntCol(state, pageRows, intEncodeCtx{allowed: allow(transformNone)})
	out = binary.LittleEndian.AppendUint32(out, uint32(len(s))) // #nosec G115 -- bounded by the writer
	out = append(out, s...)
	return append(out, blob...)
}

type bytesCol struct {
	name    string
	present bool
	ends    intCol
	state   intCol
	blob    []byte
}

func parseBytesCol(name string, b []byte, want, pageRows int) (bytesCol, error) {
	c := bytesCol{name: name, present: true}
	if len(b) < 8 {
		return c, corrupt(name, "bytes column header truncated")
	}
	if n := int(binary.LittleEndian.Uint32(b)); n != want {
		return c, corrupt(name, "holds %d values, want %d", n, want)
	}
	b = b[4:]
	var err error
	for _, dst := range []*intCol{&c.ends, &c.state} {
		if len(b) < 4 {
			return c, corrupt(name, "bytes column truncated")
		}
		l := int(binary.LittleEndian.Uint32(b))
		if l > len(b)-4 {
			return c, corrupt(name, "bytes column truncated")
		}
		if *dst, err = parseIntCol(name, b[4:4+l], want, pageRows, allow(transformNone)); err != nil {
			return c, err
		}
		b = b[4+l:]
	}
	c.blob = b
	return c, nil
}

// at returns a copy of value i (nil when the value was nil).
func (c *bytesCol) at(i int) ([]byte, error) {
	if !c.present {
		return nil, nil
	}
	lo := int64(0)
	if i > 0 {
		lo = c.ends.atNoRef(i - 1)
	}
	hi := c.ends.atNoRef(i)
	if lo < 0 || hi < lo || hi > int64(len(c.blob)) {
		return nil, corrupt(c.name, "value %d spans [%d,%d) of %d bytes", i, lo, hi, len(c.blob))
	}
	switch c.state.atNoRef(i) {
	case 0:
		if hi != lo {
			return nil, corrupt(c.name, "nil value %d has %d bytes", i, hi-lo)
		}
		return nil, nil
	case 1:
		return append(make([]byte, 0, hi-lo), c.blob[lo:hi]...), nil
	default:
		return nil, corrupt(c.name, "value %d has state %d", i, c.state.atNoRef(i))
	}
}

// coded reports whether the column is stored as codes into a dictionary.
func (c *strCol) coded() bool {
	return c.present && (c.mode == strDict || c.mode == strShared)
}

// dictionary returns what a coded column's codes index.
func (c *strCol) dictionary() []string {
	if c.mode == strShared {
		return *c.shared.snap.Load()
	}
	return c.dict
}
