package graphstate

import (
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestOutputBudgetPortableSlotsCoverEveryRetainedTypedLayout(t *testing.T) {
	for _, entry := range []struct {
		name    string
		actual  uintptr
		charged int
	}{
		{"Dependency", unsafe.Sizeof(Dependency{}), outputDependencySlotBytes},
		{"Entity", unsafe.Sizeof(EntityRecord{}), outputEntitySlotBytes},
		{"Life", unsafe.Sizeof(LifeRecord{}), outputLifeSlotBytes},
		{"Value", unsafe.Sizeof(ValueWrite{}), outputValueSlotBytes},
		{"Patch", unsafe.Sizeof(ComponentPatch{}), outputPatchSlotBytes},
		{"Page", unsafe.Sizeof(ComponentPage{}), outputPageSlotBytes},
		{"Piece", unsafe.Sizeof(state.Piece{}), outputPieceSlotBytes},
		{"Change", unsafe.Sizeof(state.Change{}), outputChangeSlotBytes},
		{"ActiveValue", unsafe.Sizeof(activeValue{}), outputActiveSlotBytes},
		{"ScopeHeader", unsafe.Sizeof(temporal.Scope{}), outputScopeHeaderBytes},
		{"ScopeParts", unsafe.Sizeof(temporal.Scope{}) + 2*unsafe.Sizeof(temporal.Bound{}), outputScopePartsBytes},
		{"Key", unsafe.Sizeof(ComponentKey{}), outputKeySlotBytes},
		{"Claim", unsafe.Sizeof(UniqueClaim{}), outputClaimSlotBytes},
		{"EntityID", unsafe.Sizeof(EntityID(0)), outputIDSlotBytes},
		// Sixteen bytes cover portable control/alignment slots; old table backing
		// is charged afresh at every growth and never refunded.
		{"EntityMap", unsafe.Sizeof(EntityID(0)) + unsafe.Sizeof(EntityRecord{}) + 16, outputEntityMapSlotBytes},
		{"LifeMap", unsafe.Sizeof(lifeKey{}) + unsafe.Sizeof(LifeRecord{}) + 16, outputLifeMapSlotBytes},
		{"ValueMap", unsafe.Sizeof(ValueID(0)) + unsafe.Sizeof(Scalar{}) + 16, outputValueMapSlotBytes},
		{"IdentityMap", unsafe.Sizeof("") + unsafe.Sizeof(ValueID(0)) + 16, outputIdentityMapSlotBytes},
		{"TargetMap", unsafe.Sizeof(lifeKey{}) + unsafe.Sizeof([]temporal.Scope{}) + 16, outputTargetMapSlotBytes},
		{"ClaimMap", unsafe.Sizeof(UniqueClaim{}) + 16, outputClaimMapSlotBytes},
		{"EntityIDMap", unsafe.Sizeof(EntityID(0)) + 16, outputIDMapSlotBytes},
		{"CursorMap", unsafe.Sizeof(Cursor(0)) + 16, outputCursorMapSlotBytes},
		{"KeyMap", unsafe.Sizeof(ComponentKey{}) + 16, outputKeyMapSlotBytes},
	} {
		t.Run(entry.name, func(t *testing.T) {
			if entry.actual > uintptr(entry.charged) {
				t.Fatalf("actual typed layout=%d exceeds admitted slot=%d", entry.actual, entry.charged)
			}
		})
	}
}
func TestOutputBudgetIntervalBackingDoesNotHideBehindSharedWireHeader(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	point, _ := temporal.Point(testPosition(t, v.axis, 0))
	parts := make([]temporal.Scope, 128)
	for i := range parts {
		parts[i], _ = temporal.Point(testPosition(t, v.axis, int64(2*i)))
	}
	region, err := temporal.Region(v.axis, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []temporal.Scope{all, point, region} {
		info, err := scope.EncodingBounds(temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		required := 2 * unsafe.Sizeof(temporal.Bound{}) * uintptr(info.Parts)
		admitted := outputIntervalBytes*info.Parts + 4*info.WireBytes
		if uintptr(admitted) < required {
			t.Fatalf("parts=%d wire=%d two-Bound backing=%d admission=%d", info.Parts, info.WireBytes, required, admitted)
		}
		t.Logf("parts=%d wire=%d perinterval=%d admitted=%d", info.Parts, info.WireBytes, 2*unsafe.Sizeof(temporal.Bound{}), admitted)
	}
}
