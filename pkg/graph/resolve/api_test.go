package resolve

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPINilReceiversReturnZeroOrFalse(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if got, ok := nilAPI.NodeProperty(nil, "name"); got != nil || ok {
		t.Fatalf("nil NodeProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if got, ok := nilAPI.RelProperty(nil, "weight"); got != nil || ok {
		t.Fatalf("nil RelProperty = (%v, %v), want (nil, false)", got, ok)
	}

	api := New((*resolveOpsSpy)(nil))
	if got, ok := api.NodeProperty(nil, "name"); got != nil || ok {
		t.Fatalf("typed-nil NodeProperty = (%v, %v), want (nil, false)", got, ok)
	}
}

func TestAPIForwardsMethods(t *testing.T) {
	t.Parallel()

	ops := &resolveOpsSpy{
		nodePropertyValue: "alice",
		nodePropertyOK:    true,
		relPropertyValue:  int64(7),
		relPropertyOK:     true,
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

	if ops.nodePropertyNode != node || ops.relPropertyRel != rel {
		t.Fatalf("forwarded pointers = node %p rel %p", ops.nodePropertyNode, ops.relPropertyRel)
	}
	if ops.nodePropertyKey != "name" || ops.relPropertyKey != "weight" {
		t.Fatalf("forwarded keys = node %q rel %q", ops.nodePropertyKey, ops.relPropertyKey)
	}
}

type resolveOpsSpy struct {
	nodePropertyValue any
	nodePropertyOK    bool
	relPropertyValue  any
	relPropertyOK     bool

	nodePropertyNode *types.Node
	relPropertyRel   *types.Relationship
	nodePropertyKey  string
	relPropertyKey   string
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
