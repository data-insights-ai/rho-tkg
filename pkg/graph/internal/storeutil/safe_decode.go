package storeutil

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// maxWireDecodeDepth bounds the msgpack container-nesting depth accepted at the
// store/import trust boundary. Legitimate wire data nests shallowly: a NodeWire
// /RelWire is a map (1) whose "p" array (2) holds PropertyWire maps (3) whose
// "v" value may itself be a nested []any / map[string]any — but the property
// allowlist (types.ValidatePropertyValue) already rejects property values
// nested beyond 32 levels with ErrMaxDepthExceeded, so anything that would pass
// validation sits well under ~36 levels. 64 leaves generous headroom for legit
// data while bounding hostile recursion far below the level that overflows the
// goroutine stack.
const maxWireDecodeDepth = 64

// SafeUnmarshal decodes msgpack bytes into v like msgpack.Unmarshal, but it
// is hardened against the two ways the vmihailenco/msgpack/v5 decoder turns
// hostile or corrupt bytes into a PROCESS CRASH rather than an error:
//
//  1. A panic via reflect — e.g. a msgpack map that repeats a key bound to an
//     interface-typed struct field (the second decode of PropertyWire.Value
//     targets an unaddressable reflect.Value and panics with
//     "reflect.Value.SetString/SetInt using unaddressable value"). Recovered
//     here and returned as store.ErrCorruptWire.
//  2. A FATAL stack overflow — the decoder recurses once per nesting level when
//     decoding deeply-nested arrays/maps into an interface{} field, and a
//     ~hundreds-of-thousands-deep blob aborts the process with
//     "fatal error: stack overflow". A fatal runtime error is NOT a panic and
//     CANNOT be recovered, so it must be prevented BEFORE the decoder runs:
//     guardMsgpackDepth scans the bytes iteratively (no recursion) and rejects
//     over-deep input as store.ErrCorruptWire.
//
// Lessons 6 and 44: untrusted persisted/imported bytes are a trust boundary —
// a single corrupt or adversarial row must fail closed with an error, never
// crash the process. msgpack.Marshal is panic-free and needs no wrapper.
//
// On the normal path SafeUnmarshal returns the decoder's own error (or nil)
// unchanged, so existing errors.Is checks on decode failures are preserved;
// only an actual panic or an over-deep blob is converted to ErrCorruptWire.
func SafeUnmarshal(data []byte, v any) (err error) {
	if derr := guardMsgpackDepth(data); derr != nil {
		return derr
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: msgpack decode panicked: %v", storepkg.ErrCorruptWire, r)
		}
	}()
	return msgpack.Unmarshal(data, v)
}

// guardMsgpackDepth walks the msgpack token stream of a single top-level value
// iteratively (using an explicit pending-count stack, never recursion) and
// returns store.ErrCorruptWire if container nesting exceeds maxWireDecodeDepth.
//
// It is deliberately NOT a full validator: it only needs to (a) measure
// nesting depth without ever under-counting, and (b) keep its read cursor
// aligned by skipping scalar payloads exactly. Anything it lets through is
// still decoded by msgpack (and any decode panic is caught by SafeUnmarshal's
// recover) — so over-permissiveness is harmless, while correct container
// accounting guarantees a genuinely over-deep blob is rejected before the
// recursive decoder ever sees it.
func guardMsgpackDepth(data []byte) error {
	// pending[i] = number of values still expected inside the container open at
	// level i. Level 0 is the synthetic root, which expects exactly one value.
	var pending [maxWireDecodeDepth + 1]int
	pending[0] = 1
	sp := 1 // number of open levels

	pos := 0
	tooDeep := func() error {
		return fmt.Errorf("%w: msgpack nesting exceeds %d levels", storepkg.ErrCorruptWire, maxWireDecodeDepth)
	}
	truncated := func() error {
		return fmt.Errorf("%w: truncated msgpack while measuring depth", storepkg.ErrCorruptWire)
	}
	push := func(n int) error {
		if sp >= len(pending) {
			return tooDeep()
		}
		pending[sp] = n
		sp++
		return nil
	}

	for sp > 0 {
		if pending[sp-1] == 0 {
			sp-- // current container fully consumed
			continue
		}
		pending[sp-1]--

		if pos >= len(data) {
			return truncated()
		}
		c := data[pos]
		pos++

		switch {
		case c <= 0x7f, c >= 0xe0: // positive / negative fixint
			// scalar, no payload
		case c >= 0xa0 && c <= 0xbf: // fixstr
			pos += int(c & 0x1f)
		case c >= 0x90 && c <= 0x9f: // fixarray
			if err := push(int(c & 0x0f)); err != nil {
				return err
			}
		case c >= 0x80 && c <= 0x8f: // fixmap
			if err := push(2 * int(c&0x0f)); err != nil {
				return err
			}
		case c == 0xc0, c == 0xc2, c == 0xc3: // nil / false / true
		case c == 0xcc, c == 0xd0: // uint8 / int8
			pos++
		case c == 0xcd, c == 0xd1: // uint16 / int16
			pos += 2
		case c == 0xce, c == 0xd2, c == 0xca: // uint32 / int32 / float32
			pos += 4
		case c == 0xcf, c == 0xd3, c == 0xcb: // uint64 / int64 / float64
			pos += 8
		case c == 0xd9, c == 0xc4: // str8 / bin8
			if pos >= len(data) {
				return truncated()
			}
			pos += 1 + int(data[pos])
		case c == 0xda, c == 0xc5: // str16 / bin16
			if pos+2 > len(data) {
				return truncated()
			}
			pos += 2 + int(binary.BigEndian.Uint16(data[pos:]))
		case c == 0xdb, c == 0xc6: // str32 / bin32
			if pos+4 > len(data) {
				return truncated()
			}
			pos += 4 + int(binary.BigEndian.Uint32(data[pos:]))
		case c == 0xdc: // array16
			if pos+2 > len(data) {
				return truncated()
			}
			n := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			if err := push(n); err != nil {
				return err
			}
		case c == 0xdd: // array32
			if pos+4 > len(data) {
				return truncated()
			}
			n := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if err := push(n); err != nil {
				return err
			}
		case c == 0xde: // map16
			if pos+2 > len(data) {
				return truncated()
			}
			n := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			if err := push(2 * n); err != nil {
				return err
			}
		case c == 0xdf: // map32
			if pos+4 > len(data) {
				return truncated()
			}
			n := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if err := push(2 * n); err != nil {
				return err
			}
		case c == 0xd4: // fixext1
			pos += 1 + 1
		case c == 0xd5: // fixext2
			pos += 1 + 2
		case c == 0xd6: // fixext4
			pos += 1 + 4
		case c == 0xd7: // fixext8
			pos += 1 + 8
		case c == 0xd8: // fixext16
			pos += 1 + 16
		case c == 0xc7: // ext8
			if pos >= len(data) {
				return truncated()
			}
			pos += 1 + 1 + int(data[pos])
		case c == 0xc8: // ext16
			if pos+2 > len(data) {
				return truncated()
			}
			pos += 2 + 1 + int(binary.BigEndian.Uint16(data[pos:]))
		case c == 0xc9: // ext32
			if pos+4 > len(data) {
				return truncated()
			}
			pos += 4 + 1 + int(binary.BigEndian.Uint32(data[pos:]))
		default: // 0xc1 — never a valid msgpack type
			return fmt.Errorf("%w: invalid msgpack type byte 0x%02x", storepkg.ErrCorruptWire, c)
		}
	}
	return nil
}
