package segment

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

var maxRows = MaxRows

var errStub = errors.New("stub")

type Segment struct{ secs map[string]sectionInfo }
type Batch struct {
	Base, N                                                            int
	IDs, StartIDs, EndIDs, Revisions, ValidFrom, ValidTo, TxFrom, TxTo []int64
}
type TimeCol uint8

const (
	TimeValidFrom TimeCol = iota
	TimeValidTo
	TimeTxFrom
	TimeTxTo
)

func (b *Batch) Coord(c TimeCol, k int) Coord                    { return Coord{} }
func (b *Batch) Row(k int) (*Row, error)                         { return nil, errStub }
func (b *Batch) Columns() []Column                               { return nil }
func (b *Batch) IntColumn(c int) ([]int64, func(int) bool, bool) { return nil, nil, false }
func (b *Batch) StringColumn(c int) ([]int64, []string, func(int) bool, bool) {
	return nil, nil, nil, false
}
func Encode(s Schema, rows []Row, o Options) ([]byte, error)                       { return nil, errStub }
func encode(s Schema, rows []Row, o Options, pr int) ([]byte, error)               { return nil, errStub }
func verifySealed(data []byte, rows []Row, order []int) error                      { return errStub }
func verifySealedHash(data []byte, typeName string, rows []Row, order []int) error { return errStub }
func Open(data []byte) (*Segment, error)                                           { return nil, errStub }
func (s *Segment) Len() int                                                        { return 0 }
func (s *Segment) Schema() Schema                                                  { return Schema{} }
func (s *Segment) IntegrityBlockRows() int                                         { return 0 }
func (s *Segment) Root() [32]byte                                                  { return [32]byte{} }
func (s *Segment) Verify() error                                                   { return errStub }
func (s *Segment) VerifyGroup(int) error                                           { return errStub }
func (s *Segment) sectionLen(string) int                                           { return 0 }
func (s *Segment) Scan(func(int, *Row) bool) error                                 { return errStub }
func (s *Segment) ScanRange(int, int, func(int, *Row) bool) error                  { return errStub }
func (s *Segment) ScanBatches(func(*Batch) bool) error                             { return errStub }
func (s *Segment) Row(int) (*Row, error)                                           { return nil, errStub }
func (s *Segment) Lookup(types.RelID) ([]int, error)                               { return nil, errStub }
func (s *Segment) OutRows(types.NodeID) (int, int, error)                          { return 0, 0, errStub }
func (s *Segment) InRows(types.NodeID) ([]int, error)                              { return nil, errStub }
func (s *Segment) ForEachID(func(types.RelID, int) bool) error                     { return errStub }
func (s *Segment) IDAt(int) (types.RelID, error)                                   { return 0, errStub }
func (s *Segment) IDRange() (lo, hi types.RelID)                                   { return 0, 0 }
func (s *Segment) SectionBytes() map[string]int                                    { return nil }
