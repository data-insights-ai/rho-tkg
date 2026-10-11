package graphstate

// Portable conservative typed representation slots. Independent tests compare
// actual layouts against these charges; runtime allocator metadata is excluded.
const (
	outputDependencySlotBytes  = 1024
	outputEntitySlotBytes      = 256
	outputLifeSlotBytes        = 64
	outputValueSlotBytes       = 256
	outputPatchSlotBytes       = 512
	outputPageSlotBytes        = 512
	outputPieceSlotBytes       = 256
	outputChangeSlotBytes      = 384
	outputActiveSlotBytes      = 384
	outputScopeHeaderBytes     = 128
	outputScopePartsBytes      = 512
	outputIntervalBytes        = 768
	outputKeySlotBytes         = 128
	outputClaimSlotBytes       = 192
	outputIDSlotBytes          = 16
	outputEntityMapSlotBytes   = 512
	outputLifeMapSlotBytes     = 128
	outputValueMapSlotBytes    = 256
	outputIdentityMapSlotBytes = 128
	outputTargetMapSlotBytes   = 128
	outputClaimMapSlotBytes    = 192
	outputIDMapSlotBytes       = 64
	outputCursorMapSlotBytes   = 64
	outputKeyMapSlotBytes      = 128
)
