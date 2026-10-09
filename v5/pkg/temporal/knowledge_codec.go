package temporal

import (
	"bytes"
	"errors"
	"slices"
)

// Provisional TK v1: axis-bound scalar-style header, knowledge kind, nominal
// flag. Optional nominal and kind-specific support/confidence-level/opaque data are
// uint32-sized nested canonical payloads. Existing codecs retain their headers;
// the byte budget counts them all. This is not a stored per-fact row layout.
const knowledgeCodecVersion = 1
const knowledgeHeaderBytes = positionHeaderBytes + 2

func knowledgeWireBytes(k PointKnowledge, l Limits) (int, error) {
	size := knowledgeHeaderBytes
	if k.hasNominal {
		size += 4 + positionWireBytes(k.nominal)
	}
	switch k.kind {
	case PointKnowledgeHardSupport, PointKnowledgeConfidenceRegion:
		size += 4 + scopeWireBytes(k.support)
		if k.kind == PointKnowledgeConfidenceRegion {
			size += 4 + integerWireBytes(k.confidenceLevel.num) + integerWireBytes(k.confidenceLevel.Denominator())
		}
	case PointKnowledgeOpaqueConstraint:
		descriptor, err := descriptorWireBytes(k.constraint.spec, l)
		if err != nil {
			return 0, err
		}
		size += 4 + descriptor
	}
	return size, nil
}

// AppendPointKnowledge preserves every evidence distinction and optional
// nominal. All child canonical codecs finish before caller bytes are mutated;
// budgets account the complete value and referenced descriptor metadata.
func AppendPointKnowledge(dst []byte, k PointKnowledge, l Limits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return dst, err
	}
	if err := k.validate(l); err != nil {
		return dst, err
	}
	size, err := knowledgeWireBytes(k, l)
	if err != nil {
		return dst, err
	}
	if size > int(^uint(0)>>1)-len(dst) {
		return dst, ErrResourceLimit
	}
	var nominal, support, confidenceLevel, descriptor []byte
	if k.hasNominal {
		nominal, err = AppendPosition(nil, k.nominal, l)
		if err != nil {
			return dst, err
		}
	}
	switch k.kind {
	case PointKnowledgeHardSupport, PointKnowledgeConfidenceRegion:
		support, err = AppendScope(nil, k.support, l)
		if err != nil {
			return dst, err
		}
		if k.kind == PointKnowledgeConfidenceRegion {
			confidenceLevel = appendRational(nil, k.confidenceLevel)
		}
	case PointKnowledgeOpaqueConstraint:
		descriptor, err = AppendOpaqueDescriptor(nil, k.constraint, l)
		if err != nil {
			return dst, err
		}
	}
	dst = slices.Grow(dst, size)
	dst = append(dst, 'T', 'K', knowledgeCodecVersion, byte(k.axis.desc.Profile))
	dst = append(dst, k.axis.desc.ID[:]...)
	dst = append(dst, k.axis.digest[:]...)
	flag := byte(0)
	if k.hasNominal {
		flag = 1
	}
	dst = append(dst, byte(k.kind), flag)
	if k.hasNominal {
		dst = appendDescriptorField(dst, nominal)
	}
	if support != nil {
		dst = appendDescriptorField(dst, support)
	}
	if confidenceLevel != nil {
		dst = appendDescriptorField(dst, confidenceLevel)
	}
	if descriptor != nil {
		dst = appendDescriptorField(dst, descriptor)
	}
	return dst, nil
}

type knowledgeWireView struct {
	kind                                          PointKnowledgeKind
	nominal, support, confidenceLevel, descriptor []byte
}

func inspectKnowledgeEnvelope(src []byte, axis Axis, l Limits) (knowledgeWireView, error) {
	if len(src) > l.MaxInputBytes || len(src) > l.MaxValueBytes-axisDescriptorBytes(axis) {
		return knowledgeWireView{}, ErrResourceLimit
	}
	if len(src) < knowledgeHeaderBytes || src[0] != 'T' || src[1] != 'K' {
		return knowledgeWireView{}, ErrInvalidEncoding
	}
	if src[2] != knowledgeCodecVersion {
		return knowledgeWireView{}, errors.Join(ErrInvalidEncoding, ErrUnknownVersion)
	}
	profile := Profile(src[3])
	if !validProfile(profile) {
		return knowledgeWireView{}, errors.Join(ErrInvalidEncoding, ErrUnknownProfile)
	}
	if profile != axis.desc.Profile || !bytes.Equal(src[4:20], axis.desc.ID[:]) || !bytes.Equal(src[20:52], axis.digest[:]) {
		return knowledgeWireView{}, ErrAxisMismatch
	}
	view := knowledgeWireView{kind: PointKnowledgeKind(src[52])}
	flag := src[53]
	if flag > 1 || view.kind < PointKnowledgeUnspecified || view.kind > PointKnowledgeOpaqueConstraint || view.kind == PointKnowledgeUnspecified && flag != 0 || view.kind == PointKnowledgeNominalOnly && flag != 1 {
		return knowledgeWireView{}, ErrInvalidEncoding
	}
	c := envelopeCursor{src: src[knowledgeHeaderBytes:]}
	var err error
	if flag == 1 {
		view.nominal, err = c.sized()
		if err != nil {
			return knowledgeWireView{}, err
		}
		if len(view.nominal) == 0 {
			return knowledgeWireView{}, ErrInvalidEncoding
		}
	}
	switch view.kind {
	case PointKnowledgeHardSupport, PointKnowledgeConfidenceRegion:
		view.support, err = c.sized()
		if err != nil {
			return knowledgeWireView{}, err
		}
		if len(view.support) == 0 {
			return knowledgeWireView{}, ErrInvalidEncoding
		}
		if view.kind == PointKnowledgeConfidenceRegion {
			view.confidenceLevel, err = c.sized()
			if err != nil {
				return knowledgeWireView{}, err
			}
			if len(view.confidenceLevel) == 0 {
				return knowledgeWireView{}, ErrInvalidEncoding
			}
		}
	case PointKnowledgeOpaqueConstraint:
		view.descriptor, err = c.sized()
		if err != nil {
			return knowledgeWireView{}, err
		}
		if len(view.descriptor) == 0 {
			return knowledgeWireView{}, ErrInvalidEncoding
		}
		if len(view.descriptor) > l.MaxDescriptorBytes-axisDescriptorBytes(axis) {
			return knowledgeWireView{}, ErrResourceLimit
		}
	}
	if len(c.src) != 0 {
		return knowledgeWireView{}, ErrInvalidEncoding
	}
	return view, nil
}

// DecodePointKnowledge checks an explicitly supplied axis and owns all decoded
// data. Unknown tags/versions and malformed nested values decline; confidence,
// unspecified quality and opaque constraints never decode as hard support.
func DecodePointKnowledge(src []byte, axis Axis, l Limits) (PointKnowledge, error) {
	l, err := l.resolved()
	if err != nil {
		return PointKnowledge{}, err
	}
	if err := axis.validate(); err != nil {
		return PointKnowledge{}, err
	}
	if err := axis.validateDescriptorBudget(l); err != nil {
		return PointKnowledge{}, err
	}
	view, err := inspectKnowledgeEnvelope(src, axis, l)
	if err != nil {
		return PointKnowledge{}, err
	}
	k := PointKnowledge{axis: axis, kind: view.kind}
	if view.nominal != nil {
		k.nominal, err = DecodePosition(view.nominal, axis, l)
		if err != nil {
			return PointKnowledge{}, err
		}
		k.hasNominal = true
	}
	switch view.kind {
	case PointKnowledgeHardSupport, PointKnowledgeConfidenceRegion:
		k.support, err = DecodeScope(view.support, axis, l)
		if err != nil {
			return PointKnowledge{}, err
		}
		if view.kind == PointKnowledgeConfidenceRegion {
			d := positionDecoder{src: view.confidenceLevel, limits: l}
			k.confidenceLevel, err = d.rational()
			if err != nil {
				return PointKnowledge{}, err
			}
			if len(d.src) != 0 {
				return PointKnowledge{}, ErrInvalidEncoding
			}
		}
	case PointKnowledgeOpaqueConstraint:
		k.constraint, err = DecodeOpaqueDescriptor(view.descriptor, l)
		if err != nil {
			return PointKnowledge{}, err
		}
	}
	if err := k.validate(l); err != nil {
		if errors.Is(err, ErrResourceLimit) {
			return PointKnowledge{}, err
		}
		return PointKnowledge{}, errors.Join(ErrInvalidEncoding, err)
	}
	return k, nil
}
