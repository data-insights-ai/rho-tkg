package graph_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	hashpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/hash"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/index"
	ingestpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	nodespkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/nodes"
	relspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/rels"
	replicationpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	resolvepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/resolve"
	statspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/stats"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	tierpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/tier"
)

// TestSubAPINilAndZeroReceiversFailClosed calls every exported method of every
// sub-API on a nil and on a zero-value receiver, with zero-valued arguments
// (a live context): a method returning an error must return ErrNilGraph, a
// method without one must return zero values, and none may panic. It is
// reflection-driven, so a new door is covered the day it is added.
func TestSubAPINilAndZeroReceiversFailClosed(t *testing.T) {
	t.Parallel()
	apis := []any{
		(*adminpkg.API)(nil), (*constraintspkg.API)(nil), (*eventspkg.API)(nil), (*hashpkg.API)(nil),
		(*indexpkg.API)(nil), (*ingestpkg.API)(nil), (*tkgio.API)(nil), (*nodespkg.API)(nil),
		(*relspkg.API)(nil), (*replicationpkg.API)(nil), (*resolvepkg.API)(nil), (*statspkg.API)(nil),
		(*temporalpkg.API)(nil), (*tierpkg.API)(nil),
	}
	ctxType := reflect.TypeFor[context.Context]()
	errType := reflect.TypeFor[error]()
	methods := 0
	for _, api := range apis {
		ptr := reflect.TypeOf(api)
		receivers := map[string]reflect.Value{
			"nil":  reflect.Zero(ptr),
			"zero": reflect.New(ptr.Elem()),
		}
		for i := 0; i < ptr.NumMethod(); i++ {
			m := ptr.Method(i)
			methods++
			for rname, recv := range receivers {
				name := ptr.Elem().PkgPath()[strings.LastIndex(ptr.Elem().PkgPath(), "/")+1:] + "." + m.Name + "/" + rname
				t.Run(name, func(t *testing.T) {
					args := []reflect.Value{recv}
					for j := 1; j < m.Type.NumIn(); j++ {
						in := m.Type.In(j)
						if j == m.Type.NumIn()-1 && m.Type.IsVariadic() {
							args = append(args, reflect.Zero(in))
							continue
						}
						if in == ctxType {
							args = append(args, reflect.ValueOf(context.Background()))
							continue
						}
						args = append(args, reflect.Zero(in))
					}
					var out []reflect.Value
					func() {
						defer func() {
							if r := recover(); r != nil {
								t.Fatalf("panicked: %v", r)
							}
						}()
						if m.Type.IsVariadic() {
							out = m.Func.CallSlice(args)
						} else {
							out = m.Func.Call(args)
						}
					}()
					checkFailClosed(t, out, errType)
				})
			}
		}
	}
	if methods < 300 {
		t.Fatalf("enumerated only %d sub-API methods; the reflection walk lost its targets", methods)
	}
}

// checkFailClosed requires ErrNilGraph from a trailing error result and zero
// values everywhere else; an iterator result must yield nothing.
func checkFailClosed(t *testing.T, out []reflect.Value, errType reflect.Type) {
	t.Helper()
	for k, v := range out {
		if k == len(out)-1 && v.Type() == errType {
			err, _ := v.Interface().(error)
			if !errors.Is(err, graphpkg.ErrNilGraph) {
				t.Fatalf("error = %v, want ErrNilGraph", err)
			}
			continue
		}
		if v.Kind() == reflect.Func && !v.IsNil() {
			requireEmptyIterator(t, v)
			continue
		}
		if !v.IsZero() {
			t.Fatalf("result %d = %v, want the zero value", k, v.Interface())
		}
	}
}

// requireEmptyIterator ranges an iter.Seq / iter.Seq2 result: it may yield
// nothing, or (for an iter.Seq2[T, error]) one zero value paired with
// ErrNilGraph.
func requireEmptyIterator(t *testing.T, seq reflect.Value) {
	t.Helper()
	if seq.Type().NumIn() != 1 || seq.Type().In(0).Kind() != reflect.Func {
		t.Fatalf("non-nil func result %v is not an iterator", seq.Type())
	}
	yield := reflect.MakeFunc(seq.Type().In(0), func(args []reflect.Value) []reflect.Value {
		last := args[len(args)-1]
		err, isErr := last.Interface().(error)
		if len(args) != 2 || !isErr || !errors.Is(err, graphpkg.ErrNilGraph) || !args[0].IsZero() {
			t.Fatalf("iterator from a nil sub-API yielded %v, want nothing or (zero, ErrNilGraph)", args)
		}
		return []reflect.Value{reflect.ValueOf(false)}
	})
	seq.Call([]reflect.Value{yield})
}
