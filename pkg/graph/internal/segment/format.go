package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

const (
	headerSize  = 64
	trailerSize = 16 // footer length u32 | footer CRC u32 | magic [8]

	headerMagic  = "TKGSEG\x00\x01"
	trailerMagic = "TKGSEGFT"

	// pageIndexEntry: min/max of valid_from, valid_to, tx_from, rel_id (8 ×
	// i64) and a flags byte (bit 0: some row of the page has an open valid_to).
	pageIndexEntry = 8*8 + 1

	// flagNodeDict: endpoint identities and hashes are codes into the
	// store's NodeDict ("nodes" holds node codes, no "nodehash" or
	// "ephash.exc" section, endpoint-hash columns hold 0 or 1+hash code).
	flagNodeDict = 1
)

// Section names. Row columns hold rowCount values in segment order; node
// columns hold nodeCount values in node-ordinal order (ordinal = rank of the
// NodeID among the segment's distinct endpoints).
const (
	secNodes        = "nodes"        // int, node ordinal -> NodeID, ascending (NodeDict mode: -> node code, NodeIDs ascending)
	secNodeHash     = "nodehash"     // string, node ordinal -> the node's usual endpoint hash
	secOutCSR       = "outcsr"       // int, nodeCount+1 row offsets of each start run
	secInOff        = "incsr.off"    // int, nodeCount+1 offsets into incsr.perm
	secInPerm       = "incsr.perm"   // int, rows grouped by end ordinal
	secIDs          = "idindex.ids"  // int, rel ids ascending
	secIDRows       = "idindex.rows" // int, the row of each idindex.ids entry
	secIntegrity    = "integrity"    // raw, one SHA-256 root per integrity group
	secPageIndex    = "pageindex"    // raw, one pageIndexEntry per page
	secEndpointExc  = "ephash.exc"   // string, endpoint hashes that differ from nodehash
	secFallback     = "fallback"     // bytes, per-row properties outside the schema
	colPrefix       = "col:"
	propPrefix      = "prop:"
	presencePrefix  = "presence:"
	colEnd          = colPrefix + "end"
	colValidFrom    = colPrefix + "valid_from"
	colValidTo      = colPrefix + "valid_to"
	colTxFrom       = colPrefix + "tx_from"
	colRelID        = colPrefix + "rel_id"
	colVersion      = colPrefix + "version"
	colNoTemporal   = colPrefix + "no_temporal"
	colTxTo         = colPrefix + "tx_to"
	colCreatedAt    = colPrefix + "created_at"
	colUpdatedAt    = colPrefix + "updated_at"
	colDeletedAt    = colPrefix + "deleted_at"
	colBaseEntity   = colPrefix + "base_entity"
	colAuthLevel    = colPrefix + "auth_level"
	colFromHash     = colPrefix + "from_hash" // 0 = nodehash[start], k = ephash.exc[k-1]
	colToHash       = colPrefix + "to_hash"
	colCreatedBy    = colPrefix + "created_by"
	colUpdatedBy    = colPrefix + "updated_by"
	colPrevHash     = colPrefix + "prev_hash"
	colAuthorID     = colPrefix + "author_id"
	colAuthorizedBy = colPrefix + "authorized_by"
	colSignature    = colPrefix + "signature"
)

// sectionInfo locates one section in the segment bytes.
type sectionInfo struct {
	name           string
	offset, length int
	crc            uint32
	crcAt          int // absolute position of the section's CRC in the footer
}

type header struct {
	format, flags, typeToken, pageRows int
	rows, nodes                        int
	idMin, idMax, vfMin, vfMax         int64
}

func putHeader(h header) []byte {
	b := make([]byte, headerSize)
	copy(b, headerMagic)
	binary.LittleEndian.PutUint16(b[8:], uint16(h.format))     // #nosec G115 -- constant
	binary.LittleEndian.PutUint16(b[10:], uint16(h.flags))     // #nosec G115 -- constant
	binary.LittleEndian.PutUint16(b[12:], uint16(h.typeToken)) // #nosec G115 -- a uint16 token
	binary.LittleEndian.PutUint16(b[14:], uint16(h.pageRows))  // #nosec G115 -- <= 4096
	binary.LittleEndian.PutUint32(b[16:], uint32(h.rows))      // #nosec G115 -- <= MaxRows
	binary.LittleEndian.PutUint32(b[20:], uint32(h.nodes))     // #nosec G115 -- <= 2*MaxRows
	binary.LittleEndian.PutUint64(b[24:], uint64(h.idMin))     // #nosec G115 -- bit pattern
	binary.LittleEndian.PutUint64(b[32:], uint64(h.idMax))     // #nosec G115 -- bit pattern
	binary.LittleEndian.PutUint64(b[40:], uint64(h.vfMin))     // #nosec G115 -- bit pattern
	binary.LittleEndian.PutUint64(b[48:], uint64(h.vfMax))     // #nosec G115 -- bit pattern
	binary.LittleEndian.PutUint32(b[56:], crc32.Checksum(b[:56], castagnoli))
	return b
}

func readHeader(data []byte) (header, error) {
	if len(data) < headerSize+trailerSize {
		return header{}, corrupt("header", "segment of %d bytes is shorter than header and trailer", len(data))
	}
	if string(data[:8]) != headerMagic {
		return header{}, corrupt("header", "bad magic")
	}
	if crc32.Checksum(data[:56], castagnoli) != binary.LittleEndian.Uint32(data[56:]) {
		return header{}, corrupt("header", "CRC mismatch")
	}
	h := header{
		format:    int(binary.LittleEndian.Uint16(data[8:])),
		flags:     int(binary.LittleEndian.Uint16(data[10:])),
		typeToken: int(binary.LittleEndian.Uint16(data[12:])),
		pageRows:  int(binary.LittleEndian.Uint16(data[14:])),
		rows:      int(binary.LittleEndian.Uint32(data[16:])),
		nodes:     int(binary.LittleEndian.Uint32(data[20:])),
		idMin:     int64(binary.LittleEndian.Uint64(data[24:])), // #nosec G115 -- bit pattern
		idMax:     int64(binary.LittleEndian.Uint64(data[32:])), // #nosec G115 -- bit pattern
		vfMin:     int64(binary.LittleEndian.Uint64(data[40:])), // #nosec G115 -- bit pattern
		vfMax:     int64(binary.LittleEndian.Uint64(data[48:])), // #nosec G115 -- bit pattern
	}
	if h.format != CurrentFormatVersion {
		return header{}, fmt.Errorf("%w: segment format %d (this build reads %d)", ErrUnsupportedVersion, h.format, CurrentFormatVersion)
	}
	if h.flags&^flagNodeDict != 0 {
		return header{}, corrupt("header", "unknown flags %#x", h.flags)
	}
	if !pow2UpTo4096(h.pageRows) {
		return header{}, corrupt("header", "page size %d", h.pageRows)
	}
	if h.typeToken == 0 {
		return header{}, corrupt("header", "type token 0")
	}
	if (h.rows == 0) != (h.nodes == 0) || h.nodes > 2*h.rows || h.rows > MaxRows {
		return header{}, corrupt("header", "%d rows over %d nodes", h.rows, h.nodes)
	}
	return h, nil
}

// pow2UpTo4096 reports whether v is a power of two in [1, 4096] — the range of
// both the page size and IntegrityBlockRows.
func pow2UpTo4096(v int) bool { return v >= 1 && v <= 4096 && v&(v-1) == 0 }

type footer struct {
	version   int
	blockRows int
	typeName  string
	columns   []Column
	root      [32]byte
	sections  []sectionInfo
}

func putFooter(f footer) []byte {
	b := binary.LittleEndian.AppendUint16(nil, uint16(f.version)) // #nosec G115 -- constant
	b = binary.LittleEndian.AppendUint16(b, uint16(f.blockRows))  // #nosec G115 -- <= 4096
	b = appendStr16(b, f.typeName)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(f.columns))) // #nosec G115 -- validated <= 65535
	for _, c := range f.columns {
		b = appendStr16(b, c.Name)
		b = append(b, byte(c.Kind))
	}
	b = append(b, f.root[:]...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(f.sections))) // #nosec G115 -- a few dozen
	for _, s := range f.sections {
		b = appendStr16(b, s.name)
		b = binary.LittleEndian.AppendUint64(b, uint64(s.offset)) // #nosec G115 -- non-negative
		b = binary.LittleEndian.AppendUint64(b, uint64(s.length)) // #nosec G115 -- non-negative
		b = binary.LittleEndian.AppendUint32(b, s.crc)
	}
	return b
}

func appendStr16(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s))) // #nosec G115 -- names are validated <= 65535 bytes
	return append(b, s...)
}

// cursor reads the footer with every length checked against what is left.
type cursor struct {
	b   []byte
	off int
	err error
}

func (c *cursor) need(n int) bool {
	if c.err != nil {
		return false
	}
	if n < 0 || n > len(c.b)-c.off {
		c.err = corrupt("footer", "truncated at byte %d (need %d more)", c.off, n)
		return false
	}
	return true
}

func (c *cursor) u16() int {
	if !c.need(2) {
		return 0
	}
	v := int(binary.LittleEndian.Uint16(c.b[c.off:]))
	c.off += 2
	return v
}

func (c *cursor) u32() uint32 {
	if !c.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v
}

func (c *cursor) u64() uint64 {
	if !c.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(c.b[c.off:])
	c.off += 8
	return v
}

func (c *cursor) bytes(n int) []byte {
	if !c.need(n) {
		return nil
	}
	v := c.b[c.off : c.off+n]
	c.off += n
	return v
}

func (c *cursor) str16() string { return string(c.bytes(c.u16())) }

// locateFooter checks the trailer and returns the footer's bounds, after
// verifying the footer CRC.
func locateFooter(data []byte) (start, end int, err error) {
	n := len(data)
	if n < headerSize+trailerSize {
		return 0, 0, corrupt("trailer", "segment of %d bytes", n)
	}
	t := data[n-trailerSize:]
	if string(t[8:]) != trailerMagic {
		return 0, 0, corrupt("trailer", "bad magic")
	}
	flen := int(binary.LittleEndian.Uint32(t))
	if flen > n-headerSize-trailerSize {
		return 0, 0, corrupt("trailer", "footer length %d exceeds the segment", flen)
	}
	start, end = n-trailerSize-flen, n-trailerSize
	if crc32.Checksum(data[start:end], castagnoli) != binary.LittleEndian.Uint32(t[4:]) {
		return 0, 0, corrupt("footer", "CRC mismatch")
	}
	return start, end, nil
}

// readFooter parses the footer (CRC already verified) and validates the
// section directory: known names, no duplicates, and sections that tile the
// bytes between the header and the footer exactly, in order.
func readFooter(data []byte, start, end int) (footer, error) {
	c := &cursor{b: data[:end], off: start}
	var f footer
	f.version = c.u16()
	if c.err == nil && f.version != CurrentFooterVersion {
		return footer{}, fmt.Errorf("%w: footer version %d (this build reads %d)", ErrUnsupportedVersion, f.version, CurrentFooterVersion)
	}
	f.blockRows = c.u16()
	f.typeName = c.str16()
	ncols := c.u16()
	if c.err == nil && ncols > (end-c.off)/3 {
		return footer{}, corrupt("footer", "%d columns in %d bytes", ncols, end-c.off)
	}
	if c.err == nil && ncols > 0 {
		f.columns = make([]Column, ncols)
		for i := range f.columns {
			f.columns[i].Name = c.str16()
			if b := c.bytes(1); b != nil {
				f.columns[i].Kind = Kind(b[0])
			}
		}
	}
	copy(f.root[:], c.bytes(32))
	nsec := c.u16()
	if c.err == nil && nsec > (end-c.off)/22 {
		return footer{}, corrupt("footer", "%d sections in %d bytes", nsec, end-c.off)
	}
	if c.err == nil {
		f.sections = make([]sectionInfo, 0, nsec)
	}
	for i := 0; i < nsec && c.err == nil; i++ {
		var s sectionInfo
		s.name = c.str16()
		off, ln := c.u64(), c.u64()
		s.crcAt = c.off
		s.crc = c.u32()
		if off > uint64(len(data)) || ln > uint64(len(data)) { // #nosec G115 -- len is non-negative
			return footer{}, corrupt("footer", "section %q out of range", s.name)
		}
		s.offset, s.length = int(off), int(ln) // #nosec G115 -- bounded by len(data) above
		f.sections = append(f.sections, s)
	}
	if c.err != nil {
		return footer{}, c.err
	}
	if c.off != end {
		return footer{}, corrupt("footer", "%d trailing bytes", end-c.off)
	}
	if !pow2UpTo4096(f.blockRows) {
		return footer{}, corrupt("footer", "integrity block size %d", f.blockRows)
	}
	if err := (Schema{TypeName: f.typeName, TypeToken: 1, Columns: f.columns}).validate(); err != nil {
		return footer{}, corrupt("footer", "schema: %v", err)
	}
	pos := headerSize
	seen := make(map[string]struct{}, len(f.sections))
	for _, s := range f.sections {
		if _, dup := seen[s.name]; dup {
			return footer{}, corrupt("footer", "section %q twice", s.name)
		}
		seen[s.name] = struct{}{}
		if s.offset != pos || s.length > start-pos || s.length == 0 {
			return footer{}, corrupt("footer", "section %q at [%d,+%d) does not follow byte %d", s.name, s.offset, s.length, pos)
		}
		pos += s.length
	}
	if pos != start {
		return footer{}, corrupt("footer", "sections end at %d, footer starts at %d", pos, start)
	}
	return f, nil
}

// sections returns the section directory of data without checking section
// CRCs (diagnostics and tests).
func sections(data []byte) ([]sectionInfo, error) {
	start, end, err := locateFooter(data)
	if err != nil {
		return nil, err
	}
	f, err := readFooter(data, start, end)
	if err != nil {
		return nil, err
	}
	out := append([]sectionInfo(nil), f.sections...)
	sort.Slice(out, func(i, j int) bool { return out[i].offset < out[j].offset })
	return out, nil
}

// validate checks the declared schema.
func (s Schema) validate() error {
	if s.TypeName == "" || len(s.TypeName) > 1<<16-1 {
		return fmt.Errorf("%w: type name of %d bytes", ErrInvalidSchema, len(s.TypeName))
	}
	if s.TypeToken == 0 {
		return fmt.Errorf("%w: type token 0 is reserved", ErrInvalidSchema)
	}
	if len(s.Columns) > MaxColumns {
		return fmt.Errorf("%w: %d columns (at most %d)", ErrInvalidSchema, len(s.Columns), MaxColumns)
	}
	seen := make(map[string]struct{}, len(s.Columns))
	for _, c := range s.Columns {
		if c.Name == "" || len(c.Name) > 1<<16-1-len(presencePrefix) {
			return fmt.Errorf("%w: column name of %d bytes", ErrInvalidSchema, len(c.Name))
		}
		if strings.HasPrefix(c.Name, "tkg_") {
			return fmt.Errorf("%w: column %q uses the reserved tkg_ prefix", ErrInvalidSchema, c.Name)
		}
		if c.Kind < KindBool || c.Kind > KindString {
			return fmt.Errorf("%w: column %q has unsupported kind %d", ErrInvalidSchema, c.Name, c.Kind)
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("%w: column %q declared twice", ErrInvalidSchema, c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	return nil
}
