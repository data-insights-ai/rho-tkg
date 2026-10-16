package graph

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/internal/convert"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// Storage layout of one compat entity on v5 (compat-only, documented):
//
//   - a user property k of kind t is the v5 property "k~t" (t = i f s b). v4 keys
//     are dynamically typed, v5 keys are monotyped after auto-declare; the kind
//     switch strategy of WP-11 is one shadow key per kind, so integer 1 and string
//     "1" under one key are two v5 properties and never collide. A write of kind t
//     nulls the other-kind shadows of the same key in that scope. Reads union the
//     shadows; equality lookups probe the matching kinds (int and float together,
//     one numeric rule). User keys must not contain '~'.
//   - reserved "~" properties carry the v4 version record over the version scope:
//     ~ver (dense version), ~tf (TxFrom), ~vf and ~vt (the version's own valid
//     range, 0 = unset/open); entity-level: ~dt (delete or retract instant), ~dr
//     (1 = retracted), ~xl (excise log "a,b,tx;...").
const (
	kVer = "~ver"
	kTf  = "~tf"
	kVf  = "~vf"
	kVt  = "~vt"
	kDt  = "~dt"
	kDr  = "~dr"
	kXl  = "~xl"
)

func kindTag(k v5.ValueKind) string {
	switch k {
	case v5.KindInt:
		return "i"
	case v5.KindFloat:
		return "f"
	case v5.KindBool:
		return "b"
	}
	return "s"
}

func shadow(key string, v v5.Value) string { return key + "~" + kindTag(v.Kind()) }

func checkKey(k string) error {
	if k == "" {
		return ErrEmptyPropertyKey
	}
	if strings.Contains(k, "~") {
		return fmt.Errorf("%w: property key %q contains '~' (reserved by v5compat)", ErrEmptyPropertyKey, k)
	}
	return nil
}

// userProps converts v4 properties to shadow-keyed Values, dropping the two
// valid-time keys into (from, to). A nil value is returned as a Null entry under
// every kind (see nullOthers).
type propSet struct {
	vals     map[string]v5.Value // shadow name -> value (non-null)
	clear    []string            // user keys set to nil
	vf, vt   int64
	hasValid bool
}

func toProps(in map[string]any, allowValid bool) (propSet, error) {
	ps := propSet{vals: map[string]v5.Value{}}
	for k, x := range in {
		if allowValid && (k == "tkg_valid_from" || k == "tkg_valid_to") {
			n, err := instantOf(x)
			if err != nil {
				return ps, err
			}
			ps.hasValid = true
			if k == "tkg_valid_from" {
				ps.vf = n
			} else {
				ps.vt = n
			}
			continue
		}
		if err := checkKey(k); err != nil {
			return ps, err
		}
		v, err := convert.AnyToValue(x)
		if err != nil {
			return ps, err
		}
		if v.IsNull() {
			ps.clear = append(ps.clear, k)
			continue
		}
		ps.vals[shadow(k, v)] = v
	}
	slices.Sort(ps.clear)
	return ps, nil
}

func instantOf(x any) (int64, error) {
	if t, ok := x.(types.Instant); ok {
		return int64(t), nil
	}
	v, err := convert.AnyToValue(x)
	if err != nil {
		return 0, err
	}
	n, ok := v.AsInt()
	if !ok {
		return 0, fmt.Errorf("%w: valid-time property must be an integer instant", convert.ErrUnsupportedValue)
	}
	return n, nil
}

// splitShadow undoes shadow(); ok is false for reserved names.
func splitShadow(name string) (key string, ok bool) {
	if strings.HasPrefix(name, "~") {
		return "", false
	}
	i := strings.LastIndexByte(name, '~')
	if i < 0 {
		return "", false
	}
	return name[:i], true
}

// row is one decoded v5 row (one valid-time segment of one revision).
type row struct {
	id       int64
	rel      bool
	labels   []string
	relType  string
	from, to int64
	vals     map[string]v5.Value // shadow name -> value (non-null)
	ver      int64
	tf       int64
	vf, vt   int64
	dt       int64
	dr       bool
	xl       string
	sp       span
	revision uint64
}

func (a axes) decode(e v5.Entity) row {
	r := row{rel: e.Ref.IsRel(), labels: slices.Clone(e.Labels), relType: e.RelType, from: int64(e.From), to: int64(e.To),
		vals: map[string]v5.Value{}, revision: e.Revision, sp: a.spanOf(e.Valid)}
	if r.rel {
		r.id = int64(e.Ref.Rel())
	} else {
		r.id = int64(e.Ref.Node())
	}
	for name, v := range e.Props {
		if v.IsNull() {
			continue
		}
		i, _ := v.AsInt()
		switch name {
		case kVer:
			r.ver = i
		case kTf:
			r.tf = i
		case kVf:
			r.vf = i
		case kVt:
			r.vt = i
		case kDt:
			r.dt = i
		case kDr:
			r.dr = i == 1
		case kXl:
			r.xl, _ = v.AsString()
		default:
			if _, ok := splitShadow(name); ok {
				r.vals[name] = v
			}
		}
	}
	return r
}

// userMap collapses shadow values into the v4 property map (int64, float64,
// string, bool). If a key somehow holds several kinds, the order i f b s decides.
func userMap(vals map[string]v5.Value) map[string]any {
	out := make(map[string]any, len(vals))
	rank := func(name string) int {
		switch name[strings.LastIndexByte(name, '~')+1:] {
		case "i":
			return 0
		case "f":
			return 1
		case "b":
			return 2
		}
		return 3
	}
	best := map[string]int{}
	for name, v := range vals {
		k, ok := splitShadow(name)
		if !ok {
			continue
		}
		if r, seen := best[k]; seen && r <= rank(name) {
			continue
		}
		best[k] = rank(name)
		out[k] = convert.ValueToAny(v)
	}
	return out
}

// voidRec is one entry of the excise log.
type voidRec struct{ a, b, tx int64 }

func parseXl(s string) []voidRec {
	var out []voidRec
	for _, part := range strings.Split(s, ";") {
		f := strings.Split(part, ",")
		if len(f) != 3 {
			continue
		}
		a, e1 := strconv.ParseInt(f[0], 10, 64)
		b, e2 := strconv.ParseInt(f[1], 10, 64)
		tx, e3 := strconv.ParseInt(f[2], 10, 64)
		if e1 == nil && e2 == nil && e3 == nil {
			out = append(out, voidRec{a, b, tx})
		}
	}
	return out
}

func formatXl(v []voidRec) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = fmt.Sprintf("%d,%d,%d", x.a, x.b, x.tx)
	}
	return strings.Join(parts, ";")
}
