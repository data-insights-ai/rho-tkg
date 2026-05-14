package resolve

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestAPINilReceiversReturnZeroOrErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if got, ok := nilAPI.NodeProperty(nil, "name"); got != nil || ok {
		t.Fatalf("nil NodeProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if got, ok := nilAPI.RelProperty(nil, "weight"); got != nil || ok {
		t.Fatalf("nil RelProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if got, err := nilAPI.LabelToken("Person"); got != 0 || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil LabelToken = (%d, %v), want (0, ErrNilGraph)", got, err)
	}
	if got, err := nilAPI.RelTypeToken("KNOWS"); got != 0 || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil RelTypeToken = (%d, %v), want (0, ErrNilGraph)", got, err)
	}
	if got, ok := nilAPI.LookupLabel("Person"); got != 0 || ok {
		t.Fatalf("nil LookupLabel = (%d, %v), want (0, false)", got, ok)
	}
	if got, ok := nilAPI.LookupRelType("KNOWS"); got != 0 || ok {
		t.Fatalf("nil LookupRelType = (%d, %v), want (0, false)", got, ok)
	}

	api := New((*resolveOpsSpy)(nil))
	if got, ok := api.NodeProperty(nil, "name"); got != nil || ok {
		t.Fatalf("typed-nil NodeProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if got, err := api.LabelToken("Person"); got != 0 || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil LabelToken = (%d, %v), want (0, ErrNilGraph)", got, err)
	}
	if got, ok := api.LookupRelType("KNOWS"); got != 0 || ok {
		t.Fatalf("typed-nil LookupRelType = (%d, %v), want (0, false)", got, ok)
	}
}

func TestAPIForwardsMethods(t *testing.T) {
	t.Parallel()

	ops := &resolveOpsSpy{
		nodePropertyValue: "alice",
		nodePropertyOK:    true,
		relPropertyValue:  int64(7),
		relPropertyOK:     true,
		labelToken:        11,
		relTypeToken:      12,
		lookupLabelToken:  13,
		lookupLabelOK:     true,
		lookupRelToken:    14,
		lookupRelOK:       true,
	}
	api := New(ops)
	node := &types.Node{}
	rel := &types.Relationship{}

	if got, ok := api.NodeProperty(node, "name"); got != "alice" || !ok {
		t.Fatalf("NodeProperty = (%v, %v), want (alice, true)", got, ok)
	}
	if got, ok := api.RelProperty(rel, "weight"); got != int64(7) || !ok {
		t.Fatalf("RelProperty = (%v, %v), want (7, true)", got, ok)
	}
	if got, err := api.LabelToken("Person"); got != 11 || err != nil {
		t.Fatalf("LabelToken = (%d, %v), want (11, nil)", got, err)
	}
	if got, err := api.RelTypeToken("KNOWS"); got != 12 || err != nil {
		t.Fatalf("RelTypeToken = (%d, %v), want (12, nil)", got, err)
	}
	if got, ok := api.LookupLabel("ExistingPerson"); got != 13 || !ok {
		t.Fatalf("LookupLabel = (%d, %v), want (13, true)", got, ok)
	}
	if got, ok := api.LookupRelType("EXISTING_KNOWS"); got != 14 || !ok {
		t.Fatalf("LookupRelType = (%d, %v), want (14, true)", got, ok)
	}

	if ops.nodePropertyNode != node || ops.relPropertyRel != rel {
		t.Fatalf("forwarded pointers = node %p rel %p", ops.nodePropertyNode, ops.relPropertyRel)
	}
	if ops.nodePropertyKey != "name" || ops.relPropertyKey != "weight" ||
		ops.labelName != "Person" || ops.relTypeName != "KNOWS" ||
		ops.lookupLabelName != "ExistingPerson" || ops.lookupRelTypeName != "EXISTING_KNOWS" {
		t.Fatalf("forwarded names = %+v", ops)
	}
}

func TestAPITokenErrorsPropagate(t *testing.T) {
	t.Parallel()

	labelErr := errors.New("label failed")
	relTypeErr := errors.New("reltype failed")
	api := New(&resolveOpsSpy{labelErr: labelErr, relTypeErr: relTypeErr})

	if got, err := api.LabelToken("Person"); got != 0 || !errors.Is(err, labelErr) {
		t.Fatalf("LabelToken error = (%d, %v), want (0, %v)", got, err, labelErr)
	}
	if got, err := api.RelTypeToken("KNOWS"); got != 0 || !errors.Is(err, relTypeErr) {
		t.Fatalf("RelTypeToken error = (%d, %v), want (0, %v)", got, err, relTypeErr)
	}
}

type resolveOpsSpy struct {
	nodePropertyValue any
	nodePropertyOK    bool
	relPropertyValue  any
	relPropertyOK     bool
	labelToken        uint16
	relTypeToken      uint16
	lookupLabelToken  uint16
	lookupLabelOK     bool
	lookupRelToken    uint16
	lookupRelOK       bool
	labelErr          error
	relTypeErr        error

	nodePropertyNode  *types.Node
	relPropertyRel    *types.Relationship
	nodePropertyKey   string
	relPropertyKey    string
	labelName         string
	relTypeName       string
	lookupLabelName   string
	lookupRelTypeName string
}

func (s *resolveOpsSpy) NodeProperty(n *types.Node, key string) (any, bool) {
	s.nodePropertyNode = n
	s.nodePropertyKey = key
	return s.nodePropertyValue, s.nodePropertyOK
}

func (s *resolveOpsSpy) RelProperty(r *types.Relationship, key string) (any, bool) {
	s.relPropertyRel = r
	s.relPropertyKey = key
	return s.relPropertyValue, s.relPropertyOK
}

func (s *resolveOpsSpy) GetOrCreateLabel(name string) (uint16, error) {
	s.labelName = name
	if s.labelErr != nil {
		return 0, s.labelErr
	}
	return s.labelToken, nil
}

func (s *resolveOpsSpy) GetOrCreateRelType(name string) (uint16, error) {
	s.relTypeName = name
	if s.relTypeErr != nil {
		return 0, s.relTypeErr
	}
	return s.relTypeToken, nil
}

func (s *resolveOpsSpy) LookupLabel(name string) (uint16, bool) {
	s.lookupLabelName = name
	return s.lookupLabelToken, s.lookupLabelOK
}

func (s *resolveOpsSpy) LookupRelType(name string) (uint16, bool) {
	s.lookupRelTypeName = name
	return s.lookupRelToken, s.lookupRelOK
}
