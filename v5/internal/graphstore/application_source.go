package graphstore

import (
	"context"
	"errors"
	"reflect"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// ApplicationReadSource supplies immutable application data, never control,
// initialization, installation, recovery or quorum authority. Root is a bounded
// compatibility call: remote providers must use their captured root ceiling and
// charge its complete output to the same operation allowance as Get and Scan.
// The caller owns its lifetime; catalog/graph handles only borrow it.
type ApplicationReadSource interface {
	Root() (raftlog.ApplicationRoot, error)
	RootBounded(context.Context, int) (raftlog.ApplicationRoot, error)
	ApplicationBinding(context.Context) (raftlog.ApplicationBinding, error)
	ReadLimits() raftlog.ReadBudget
	Get(context.Context, []byte, int) (raftlog.KV, bool, error)
	Scan(context.Context, []byte, []byte, []byte, raftlog.ReadBudget) (raftlog.ApplicationPage, error)
}

// ErrSourceUnavailable distinguishes an incomplete source operation from graph
// absence, bad caller input and stored semantic corruption.
var ErrSourceUnavailable = errors.New("graphstore: immutable source unavailable")

// Nil checking is once at provider admission, never reflective read dispatch.
func nilApplicationSource(source ApplicationReadSource) bool {
	if source == nil {
		return true
	}
	v := reflect.ValueOf(source)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (c *Catalog) initializationView() (*raftlog.ApplicationView, error) {
	if c == nil {
		return nil, ErrInvalid
	}
	if c.view == nil {
		return nil, ErrTopologyUnsupported
	}
	return c.view, nil
}

type applicationSources struct {
	native []*raftlog.ApplicationView
	data   []ApplicationReadSource
}

func (s applicationSources) len() int {
	if s.native != nil {
		return len(s.native)
	}
	return len(s.data)
}
func (s applicationSources) at(i int) ApplicationReadSource {
	if s.native != nil {
		return s.native[i]
	}
	return s.data[i]
}

var _ ApplicationReadSource = (*raftlog.ApplicationView)(nil)

func (c *Catalog) source() ApplicationReadSource {
	if c.data != nil {
		return c.data
	}
	return c.view
}
