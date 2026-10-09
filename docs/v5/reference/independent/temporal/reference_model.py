"""Independent mathematical oracle; no production imports or codecs.

Closed integer endpoint normal form deliberately differs from Go's half-open
normal form. Snapshots are immutable values, not database cuts or MVCC storage.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from fractions import Fraction
from itertools import combinations
import json
from typing import Any


class ModelError(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class Limits:
    coordinate_bits: int = 256
    region_fragments: int = 256
    component_fragments: int = 1024
    entities: int = 128
    operations: int = 128
    input_json_bytes: int = 65536
    snapshot_json_bytes: int = 262144
    change_json_bytes: int = 262144
    total_fragments: int = 4096

    def __post_init__(self):
        if any(v <= 0 for v in self.__dict__.values()):
            raise ModelError("INVALID_LIMITS")


@dataclass(frozen=True)
class Axis:
    identity: str
    profile: str
    reference: str = "abstract-test-reference-v1"
    unit: str = "abstract-position"
    version: int = 1

    def __post_init__(self):
        if not self.identity or not self.reference or not self.unit:
            raise ModelError("INVALID_AXIS")
        if self.profile not in ("Q", "Z", "QN"):
            raise ModelError("UNSUPPORTED_PROFILE")
        if self.version != 1:
            raise ModelError("UNSUPPORTED_VERSION")


# Extended ordered coordinates: (-1,0,0)=-infinity; (1,0,0)=+infinity.
# All finite coordinates use rank 0; QN retains model time and microstep.
NEG = (-1, Fraction(0), 0)
POS = (1, Fraction(0), 0)


def coord(axis: Axis, value: Any, limits: Limits = Limits()):
    if axis.profile == "QN":
        if not isinstance(value, (tuple, list)) or len(value) != 2:
            raise ModelError("INCOMPATIBLE_DOMAIN")
        if type(value[0]) is bool or isinstance(value[0], (tuple, list, dict, float)):
            raise ModelError("INVALID_COORDINATE")
        q, m = exact_fraction(value[0], limits), value[1]
        if type(m) is not int or m < 0:
            raise ModelError("INVALID_MICROSTEP")
    else:
        if type(value) is bool or isinstance(value, (tuple, list, dict, float)):
            raise ModelError("INVALID_COORDINATE")
        q, m = exact_fraction(value, limits), 0
        if axis.profile == "Z" and q.denominator != 1:
            raise ModelError("INCOMPATIBLE_DOMAIN")
    if max(abs(q.numerator).bit_length(), q.denominator.bit_length(), m.bit_length()) > limits.coordinate_bits:
        raise ModelError("RESOURCE_LIMIT")
    return (0, q, m)


def exact_fraction(value, limits=Limits()):
    if type(value) is bool or not isinstance(value, (str, int, Fraction)):
        raise ModelError("INVALID_COORDINATE")
    if isinstance(value, str) and len(value.encode("utf-8")) > limits.input_json_bytes:
        raise ModelError("RESOURCE_LIMIT")
    try:
        result = Fraction(value)
    except (ValueError, ZeroDivisionError):
        raise ModelError("INVALID_COORDINATE") from None
    if max(abs(result.numerator).bit_length(), result.denominator.bit_length()) > limits.coordinate_bits:
        raise ModelError("RESOURCE_LIMIT")
    return result


@dataclass(frozen=True)
class Interval:
    lo: tuple
    hi: tuple
    lc: bool = True
    hc: bool = False

    def normalized(self, axis: Axis, limits: Limits):
        lo, hi, lc, hc = self.lo, self.hi, self.lc, self.hc
        if lo[0] == 1 or hi[0] == -1 or (lo[0] and lc) or (hi[0] and hc):
            raise ModelError("INVALID_BOUND")
        for c in (lo, hi):
            if c[0] == 0:
                raw = (c[1], c[2]) if axis.profile == "QN" else c[1]
                coord(axis, raw, limits)
        # Empty raw ranges require no successor/predecessor construction.
        if lo > hi or lo == hi and not (lc and hc):
            return None
        if axis.profile == "Z":
            if lo[0] == 0 and not lc:
                lo, lc = (0, lo[1] + 1, 0), True
            if hi[0] == 0 and not hc:
                hi, hc = (0, hi[1] - 1, 0), True
        elif axis.profile == "QN":
            if lo[0] == 0 and not lc:
                lo, lc = (0, lo[1], lo[2] + 1), True
            if hi[0] == 0 and not hc and hi[2] > 0:
                hi, hc = (0, hi[1], hi[2] - 1), True
        for c in (lo, hi):
            if c[0] == 0:
                raw = (c[1], c[2]) if axis.profile == "QN" else c[1]
                coord(axis, raw, limits)
        if lo > hi or lo == hi and not (lc and hc):
            return None
        return Interval(lo, hi, lc, hc)

    def contains(self, c):
        return (c > self.lo or c == self.lo and self.lc) and (c < self.hi or c == self.hi and self.hc)


def common(a: Interval, b: Interval):
    lo, hi = max(a.lo, b.lo), min(a.hi, b.hi)
    lc = (a.lc if a.lo == lo else True) and (b.lc if b.lo == lo else True)
    hc = (a.hc if a.hi == hi else True) and (b.hc if b.hi == hi else True)
    return Interval(lo, hi, lc, hc)


@dataclass(frozen=True)
class Region:
    axis: Axis
    parts: tuple[Interval, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, "parts", tuple(self.parts))

    @classmethod
    def build(cls, axis, intervals=(), limits=Limits()):
        if len(intervals) > limits.region_fragments:
            raise ModelError("RESOURCE_LIMIT")
        ordered = []
        for raw in intervals:
            p = raw.normalized(axis, limits)
            if p is not None:
                ordered.append(p)
        ordered.sort(key=lambda p: (p.lo, not p.lc, p.hi, p.hc))
        out = []
        for p in ordered:
            if out:
                a = out[-1]
                # There is no missing domain position iff this complement gap is empty.
                gap = Interval(a.hi, p.lo, not a.hc, not p.lc)
                overlap = a.hi > p.lo or a.hi == p.lo and (a.hc or p.lc)
                gap_empty = overlap or gap.normalized(axis, limits) is None
                if gap_empty:
                    hi = max(a.hi, p.hi)
                    hc = (a.hc if a.hi == hi else False) or (p.hc if p.hi == hi else False)
                    out[-1] = Interval(a.lo, hi, a.lc, hc)
                    continue
            out.append(p)
        if len(out) > limits.region_fragments:
            raise ModelError("RESOURCE_LIMIT")
        return cls(axis, tuple(out))

    @classmethod
    def span(cls, axis, lo, hi, lc=True, hc=False, limits=Limits()):
        a = NEG if lo == "-inf" else coord(axis, lo, limits)
        b = POS if hi == "+inf" else coord(axis, hi, limits)
        return cls.build(axis, [Interval(a, b, lc, hc)], limits)

    def checked_pair(self, other):
        if self.axis != other.axis:
            raise ModelError("AXIS_MISMATCH")

    def union(self, other, limits=Limits()):
        self.checked_pair(other)
        return Region.build(self.axis, self.parts + other.parts, limits)

    def intersection(self, other, limits=Limits()):
        self.checked_pair(other)
        parts = []
        for a in self.parts:
            for b in other.parts:
                p = common(a, b).normalized(self.axis, limits)
                if p is not None:
                    parts.append(p)
                    if len(parts) > limits.region_fragments:
                        raise ModelError("RESOURCE_LIMIT")
        return Region.build(self.axis, parts, limits)

    def difference(self, other, limits=Limits()):
        self.checked_pair(other)
        remaining = list(self.parts)
        for cut in other.parts:
            out = []
            for a in remaining:
                c = common(a, cut).normalized(self.axis, limits)
                if c is None:
                    out.append(a)
                else:
                    if c.lo != NEG:
                        left = Interval(a.lo, c.lo, a.lc, not c.lc).normalized(self.axis, limits)
                        if left is not None:
                            out.append(left)
                    if c.hi != POS:
                        right = Interval(c.hi, a.hi, not c.hc, a.hc).normalized(self.axis, limits)
                        if right is not None:
                            out.append(right)
                if len(out) > limits.region_fragments:
                    raise ModelError("RESOURCE_LIMIT")
            remaining = out
        return Region.build(self.axis, remaining, limits)

    def contains(self, value, limits=Limits()):
        c = coord(self.axis, value, limits)
        return any(p.contains(c) for p in self.parts)

    def subset(self, other, limits=Limits()):
        return not self.difference(other, limits).parts

    def to_json(self):
        def value(c):
            if c == NEG:
                return "-inf"
            if c == POS:
                return "+inf"
            return [str(c[1]), c[2]] if self.axis.profile == "QN" else str(c[1])
        return {"axis": self.axis.__dict__, "pieces": [
            {"lower": value(p.lo), "upper": value(p.hi), "lower_closed": p.lc, "upper_closed": p.hc}
            for p in self.parts]}


@dataclass(frozen=True)
class OwnedJSON:
    canonical: str


def own(value):
    if isinstance(value, float):
        raise ModelError("UNSUPPORTED_VALUE")
    if isinstance(value, (dict, list, tuple)):
        return OwnedJSON(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return value


def unown(value):
    return json.loads(value.canonical) if isinstance(value, OwnedJSON) else value


@dataclass(frozen=True, eq=False)
class Cell:
    present: bool = False
    value: Any = None
    revision: str | None = None
    provenance: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "value", own(self.value))

    def __eq__(self, other):
        if not isinstance(other, Cell):
            return NotImplemented
        return (self.present == other.present and type(self.value) is type(other.value)
                and self.value == other.value and self.revision == other.revision
                and self.provenance == other.provenance)

    def __hash__(self):
        return hash((self.present, type(self.value), self.value, self.revision, self.provenance))

    def to_json(self):
        return {"present": self.present, "value": unown(self.value),
                "revision": self.revision, "provenance": self.provenance}


ABSENT = Cell()


@dataclass(frozen=True)
class TypedMember:
    tag: str
    value: Any


@dataclass(frozen=True)
class Assertion:
    region: Region
    cell: Cell


@dataclass(frozen=True)
class Component:
    axis: Axis
    assertions: tuple[Assertion, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, "assertions", tuple(self.assertions))

    def at(self, value, limits=Limits()):
        coord(self.axis, value, limits)
        found = [a.cell for a in self.assertions if a.region.contains(value, limits)]
        if len(found) > 1:
            raise AssertionError("component assertions overlap")
        return found[0] if found else ABSENT

    def replace(self, scope: Region, cell: Cell, limits=Limits()):
        if self.axis != scope.axis:
            raise ModelError("AXIS_MISMATCH")
        if not cell.revision:
            raise ModelError("REVISION_REQUIRED")
        if metadata_bytes(cell) > limits.input_json_bytes:
            raise ModelError("RESOURCE_LIMIT")
        # Algebra controls may add one discrete boundary bit. Work and returned
        # limits are separate; fitting coalesced output must not fail because
        # a transient complement used a successor absent from final support.
        working = replace(limits, coordinate_bits=limits.coordinate_bits + 1,
                          region_fragments=2 * (limits.component_fragments + limits.region_fragments))
        # Independent region-per-full-cell representation, not production atomic runs.
        before = []
        untouched = []
        covered = Region(self.axis)
        for a in self.assertions:
            shared = a.region.intersection(scope, working)
            if shared.parts:
                before.append((shared, a.cell, cell))
                covered = covered.union(shared, working)
            tail = a.region.difference(scope, working)
            if tail.parts:
                untouched.append(Assertion(tail, a.cell))
        missing = scope.difference(covered, working)
        if missing.parts:
            before.append((missing, ABSENT, cell))
        if scope.parts:
            untouched.append(Assertion(scope, cell))
        # Merge by COMPLETE cell equality; revision/provenance remain independent.
        merged = []
        for a in untouched:
            match = next((i for i, b in enumerate(merged) if b.cell == a.cell), None)
            if match is None:
                merged.append(a)
            else:
                b = merged[match]
                merged[match] = Assertion(b.region.union(a.region, working), a.cell)
        # Revalidate actual returned support under the caller's final policy.
        merged = [Assertion(Region.build(self.axis, a.region.parts, limits), a.cell) for a in merged]
        if sum(len(a.region.parts) for a in merged) > limits.component_fragments:
            raise ModelError("RESOURCE_LIMIT")
        return Component(self.axis, tuple(merged)), tuple(x for x in before if x[1] != x[2])

    def present_region(self, limits=Limits()):
        out = Region(self.axis)
        for a in self.assertions:
            if a.cell.present:
                out = out.union(a.region, limits)
        return out


@dataclass(frozen=True)
class Life:
    identity: int
    presence: Component
    # Components are immutable key/value tuples. Set members have their own key.
    components: tuple[tuple[tuple, Component], ...] = ()
    endpoint_lives: tuple[int, int] | None = None

    def __post_init__(self):
        if type(self.identity) is not int or not 0 < self.identity < (1 << 64):
            raise ModelError("INVALID_LIFE_ID")
        object.__setattr__(self, "components", tuple((tuple(k), c) for k, c in self.components))
        if self.endpoint_lives is not None:
            object.__setattr__(self, "endpoint_lives", tuple(self.endpoint_lives))

    def component(self, key):
        return dict(self.components).get(key, Component(self.presence.axis))

    def with_component(self, key, component):
        values = dict(self.components)
        values[key] = component
        return replace(self, components=tuple(values.items()))


@dataclass(frozen=True)
class Entity:
    identity: str
    kind: str
    lives: tuple[Life, ...]
    source: str | None = None
    target: str | None = None
    type_name: str | None = None
    reference_mode: str = "life_bound"
    interpretation: str | None = None
    temporal_role: str | None = None

    def __post_init__(self):
        if type(self.identity) is not str or not self.identity:
            raise ModelError("INVALID_ENTITY_ID")
        object.__setattr__(self, "lives", tuple(self.lives))


@dataclass(frozen=True)
class Snapshot:
    graph: str
    axis: Axis
    entities: tuple[Entity, ...] = ()

    def __post_init__(self):
        if type(self.graph) is not str or not self.graph:
            raise ModelError("INVALID_GRAPH_ID")
        object.__setattr__(self, "entities", tuple(self.entities))

    def entity(self, identity):
        found = next((e for e in self.entities if e.identity == identity), None)
        if found is None:
            raise ModelError("NOT_FOUND")
        return found

    def put(self, entity):
        return replace(self, entities=tuple(e for e in self.entities if e.identity != entity.identity) + (entity,))

    def active_life(self, entity, value, limits=Limits()):
        found = [life for life in entity.lives if life.presence.at(value, limits).present]
        if len(found) > 1:
            raise AssertionError("lifecycles overlap")
        return found[0] if found else None

    def effective_region(self, entity, life, limits=Limits()):
        region = life.presence.present_region(limits)
        if entity.kind == "edge" and entity.reference_mode == "life_bound":
            for owner, bound in zip((entity.source, entity.target), life.endpoint_lives):
                endpoint = self.entity(owner)
                endpoint_life = next(l for l in endpoint.lives if l.identity == bound)
                region = region.intersection(endpoint_life.presence.present_region(limits), limits)
        return region

    def project(self, value, mode="effective", lifecycle_status=False, limits=Limits()):
        coord(self.axis, value, limits)
        if mode not in ("effective", "declared"):
            raise ModelError("UNSUPPORTED_VIEW")
        result = {"nodes": {}, "edges": {}}
        for entity in sorted(self.entities, key=lambda e: e.identity):
            life = self.active_life(entity, value, limits)
            if life is None:
                continue
            if mode == "effective" and not self.effective_region(entity, life, limits).contains(value, limits):
                continue
            properties, labels, sets = {}, [], {}
            for key, component in life.components:
                cell = component.at(value, limits)
                if not cell.present:
                    continue
                kind, name, *member = key
                if kind == "label":
                    labels.append(name)
                elif kind == "set":
                    sets.setdefault(name, []).append(unown(member[0].value))
                else:
                    properties[name] = unown(cell.value)
            properties.update({k: sorted(v, key=lambda x: (type(x).__name__, json.dumps(x, sort_keys=True))) for k, v in sets.items()})
            row = {"life": life.identity, "properties": properties}
            if entity.interpretation is not None:
                row["interpretation"] = entity.interpretation
            if entity.temporal_role is not None:
                row["temporal_role"] = entity.temporal_role
            if entity.kind == "node":
                row["labels"] = sorted(labels)
                result["nodes"][entity.identity] = row
            else:
                row.update(source=entity.source, target=entity.target, type=entity.type_name)
                if lifecycle_status:
                    statuses = []
                    for i, owner in enumerate((entity.source, entity.target)):
                        endpoint = self.entity(owner)
                        active = self.active_life(endpoint, value, limits)
                        bound = life.endpoint_lives[i] if life.endpoint_lives else None
                        statuses.append({"identity": owner, "exists": True,
                                         "active_life": active.identity if active else None,
                                         "bound_life": bound,
                                         "bound_life_active": active is not None and active.identity == bound})
                    row["endpoint_status"] = statuses
                    row["reference_mode"] = entity.reference_mode
                result["edges"][entity.identity] = row
        return result

    def apply(self, operations, revision, provenance=None, schema=None, limits=Limits()):
        if len(operations) > limits.operations or len(json.dumps(operations, sort_keys=True).encode()) > limits.input_json_bytes:
            raise ModelError("RESOURCE_LIMIT")
        next_snapshot = self
        changes = []
        for i, op in enumerate(operations):
            next_snapshot, local = next_snapshot._operation(op, op.get("revision", revision), op.get("provenance", provenance), schema, limits)
            changes.extend(local)
        next_snapshot._check_unique(schema, limits)
        if len(next_snapshot.entities) > limits.entities:
            raise ModelError("RESOURCE_LIMIT")
        fragments = sum(len(a.region.parts) for e in next_snapshot.entities for life in e.lives
                        for component in (life.presence,) + tuple(c for _, c in life.components)
                        for a in component.assertions)
        if fragments > limits.total_fragments or metadata_bytes(next_snapshot) > limits.snapshot_json_bytes or metadata_bytes(changes) > limits.change_json_bytes:
            raise ModelError("RESOURCE_LIMIT")
        return next_snapshot, tuple(changes)

    def _scope(self, op, limits):
        if "graph" in op and op["graph"] != self.graph:
            raise ModelError("GRAPH_MISMATCH")
        if "axis" in op and op["axis"] != self.axis.identity:
            raise ModelError("AXIS_MISMATCH")
        if "valid" not in op:
            raise ModelError("VALIDITY_REQUIRED")
        if op["valid"] == "unplaced":
            raise ModelError("UNSUPPORTED_PLACEMENT")
        if isinstance(op["valid"], dict):
            if op["valid"].get("kind") != "region":
                raise ModelError("UNSUPPORTED_PLACEMENT")
            parts = op["valid"].get("pieces", [])
            if len(parts) > limits.region_fragments:
                raise ModelError("RESOURCE_LIMIT")
            intervals = []
            for part in parts:
                if part.get("axis", self.axis.identity) != self.axis.identity:
                    raise ModelError("AXIS_MISMATCH")
                atomic = Region.span(self.axis, part["lower"], part["upper"],
                                     part.get("lower_closed", True), part.get("upper_closed", False), limits)
                intervals.extend(atomic.parts)
            region = Region.build(self.axis, intervals, limits)
        else:
            lo, hi = op["valid"]
            region = Region.span(self.axis, lo, hi, op.get("lower_closed", True), op.get("upper_closed", False), limits)
        if not region.parts:
            raise ModelError("INVALID_INTERVAL")
        return region

    def _operation(self, op, revision, provenance, schema, limits):
        scope = self._scope(op, limits)
        kind = op["op"]
        cell = Cell(True, None, revision, provenance)
        if kind in ("create_vertex", "create_edge"):
            if any(e.identity == op["id"] for e in self.entities):
                raise ModelError("IDENTITY_EXISTS")
            endpoint_lives = None
            mode = op.get("reference_mode", "life_bound")
            if kind == "create_edge":
                if mode not in ("life_bound", "identity_reference"):
                    raise ModelError("UNSUPPORTED_REFERENCE")
                endpoints = [self.entity(op[k]) for k in ("source", "target")]
                if any(e.kind != "node" for e in endpoints):
                    raise ModelError("ENDPOINT_KIND")
                if mode == "life_bound":
                    bound = []
                    for endpoint in endpoints:
                        covered = [l for l in endpoint.lives if scope.subset(l.presence.present_region(limits), limits)]
                        if len(covered) != 1:
                            raise ModelError("OWNER_VALIDITY")
                        bound.append(covered[0].identity)
                    endpoint_lives = tuple(bound)
            presence, delta = Component(self.axis).replace(scope, cell, limits)
            life = Life(op.get("life", 1), presence, endpoint_lives=endpoint_lives)
            entity = Entity(op["id"], "node" if kind == "create_vertex" else "edge", (life,),
                            op.get("source"), op.get("target"), op.get("type"), mode,
                            op.get("interpretation"), op.get("temporal_role"))
            next_snapshot = self.put(entity)
            changes = list(delta)
            placement = {k: op[k] for k in ("valid", "lower_closed", "upper_closed") if k in op}
            for key, value in op.get("properties", {}).items():
                next_snapshot, delta = next_snapshot._operation({"op": "set", "owner": entity.identity,
                    **placement, "key": key, "value": value}, revision, provenance, schema, limits)
                changes.extend(delta)
            for label in op.get("labels", []):
                next_snapshot, delta = next_snapshot._operation({"op": "add_label", "owner": entity.identity,
                    **placement, "label": label}, revision, provenance, schema, limits)
                changes.extend(delta)
            return next_snapshot, changes
        entity = self.entity(op["owner"])
        if kind == "reopen":
            identity = op.get("life", max(l.identity for l in entity.lives) + 1)
            if any(l.identity == identity for l in entity.lives):
                raise ModelError("LIFE_EXISTS")
            if any(scope.intersection(l.presence.present_region(limits), limits).parts for l in entity.lives):
                raise ModelError("LIFECYCLE_OVERLAP")
            presence, changes = Component(self.axis).replace(scope, cell, limits)
            endpoint_lives = None
            if entity.kind == "edge" and entity.reference_mode == "life_bound":
                endpoint_lives = tuple(self._covered_life(self.entity(owner), scope, limits).identity for owner in (entity.source, entity.target))
            life = Life(identity, presence, endpoint_lives=endpoint_lives)
            return self.put(replace(entity, lives=entity.lives + (life,))), changes
        identity = op.get("life")
        if identity is not None:
            life = next((l for l in entity.lives if l.identity == identity), None)
            if life is None:
                raise ModelError("NOT_FOUND")
        elif kind in ("correct", "close"):
            # Historical fixture's implicit correction selects its first life;
            # explicit new corpus always supplies LifeID where ambiguity matters.
            life = entity.lives[0] if kind == "correct" else self._covered_life(entity, scope, limits)
        else:
            life = self._covered_life(entity, scope, limits)
        if kind in ("correct", "close"):
            present = op.get("present", kind != "close")
            if present and any(l.identity != life.identity and scope.intersection(l.presence.present_region(limits), limits).parts for l in entity.lives):
                raise ModelError("LIFECYCLE_OVERLAP")
            if present and entity.kind == "edge" and entity.reference_mode == "life_bound":
                # Presence correction is a new strict placement claim. Check
                # the originally bound LifeIDs, never whichever life is active
                # now. Declared property edits below remain legal while masked.
                for owner, bound in zip((entity.source, entity.target), life.endpoint_lives):
                    endpoint = self.entity(owner)
                    endpoint_life = next((l for l in endpoint.lives if l.identity == bound), None)
                    if endpoint_life is None or not scope.subset(endpoint_life.presence.present_region(limits), limits):
                        raise ModelError("OWNER_VALIDITY")
            presence, changes = life.presence.replace(scope, Cell(present, None, revision, provenance), limits)
            life = replace(life, presence=presence)
        elif kind in ("set", "unset", "add", "remove", "add_label", "remove_label"):
            if not scope.subset(life.presence.present_region(limits), limits):
                raise ModelError("OWNER_VALIDITY")
            if kind.endswith("label"):
                key = ("label", op["label"])
            else:
                definition = schema.get("properties", {}).get(op["key"]) if schema is not None else None
                cardinality = "set" if kind in ("add", "remove") else "scalar"
                if schema is not None and (definition is None or definition["owner"] != ("vertex" if entity.kind == "node" else "edge") or definition["cardinality"] != cardinality):
                    raise ModelError("TYPE_MISMATCH")
                value = op.get("value")
                if definition is not None and kind in ("set", "add", "remove"):
                    self._validate_value(value, definition["type"], cardinality)
                if cardinality == "set" and value is None:
                    raise ModelError("NULL_SET_MEMBER_UNSUPPORTED")
                key = ("set", op["key"], TypedMember(type(value).__name__, own(value))) if cardinality == "set" else ("property", op["key"])
            present = kind in ("set", "add", "add_label")
            value = op.get("value") if present and kind == "set" else None
            component, changes = life.component(key).replace(scope, Cell(present, value, revision, provenance), limits)
            life = life.with_component(key, component)
        else:
            raise ModelError("UNSUPPORTED_OPERATION")
        return self.put(replace(entity, lives=tuple(life if l.identity == life.identity else l for l in entity.lives))), changes

    def _covered_life(self, entity, scope, limits):
        found = [l for l in entity.lives if scope.subset(l.presence.present_region(limits), limits)]
        if len(found) != 1:
            raise ModelError("OWNER_VALIDITY")
        return found[0]

    @staticmethod
    def _validate_value(value, kind, cardinality):
        if value is None and cardinality == "scalar":
            return
        valid = {"String": type(value) is str, "Bool": type(value) is bool,
                 "I64": type(value) is int and -(1 << 63) <= value < (1 << 63)}
        if not valid.get(kind, False):
            raise ModelError("TYPE_MISMATCH")

    def _check_unique(self, schema, limits):
        if schema is None:
            return
        for name, definition in schema.get("properties", {}).items():
            if not definition.get("unique"):
                continue
            if definition["cardinality"] != "scalar":
                raise ModelError("UNSUPPORTED_UNIQUENESS")
            claims = []
            for entity in self.entities:
                for life in entity.lives:
                    active = self.effective_region(entity, life, limits)
                    component = life.component(("property", name))
                    for assertion in component.assertions:
                        cell = assertion.cell
                        if cell.present and cell.value is not None:
                            support = active.intersection(assertion.region, limits)
                            if support.parts:
                                claims.append((entity.identity, life.identity, cell.value, support))
            for a, b in combinations(claims, 2):
                if (a[0], a[1]) != (b[0], b[1]) and type(a[2]) is type(b[2]) and a[2] == b[2] and a[3].intersection(b[3], limits).parts:
                    raise ModelError("UNIQUE_OVERLAP")


def knowledge_predicate(kind, support, window, predicate="possible", limits=Limits()):
    support.checked_pair(window)
    if kind != "hard_support":
        raise ModelError("UNSUPPORTED_PREDICATE")
    if predicate not in ("possible", "definite"):
        raise ModelError("UNSUPPORTED_PREDICATE")
    if not support.parts:
        return {"holds": False, "consistency": "inconsistent"}
    holds = bool(support.intersection(window, limits).parts) if predicate == "possible" else support.subset(window, limits)
    return {"holds": holds, "consistency": "consistent"}


def convert_units(value, source_unit, target_axis, limits=Limits()):
    """Explicit unit conversion only; no cross-axis identity inference."""
    if source_unit == target_axis.unit:
        factor = Fraction(1)
    elif (source_unit, target_axis.unit) == ("microsecond", "millisecond"):
        factor = Fraction(1, 1000)
    elif (source_unit, target_axis.unit) == ("millisecond", "microsecond"):
        factor = Fraction(1000)
    else:
        raise ModelError("EXPLICIT_MAPPING_REQUIRED")
    converted = exact_fraction(value, limits) * factor
    coord(target_axis, converted, limits)
    return converted


def instant_ms_codec(value, limits=Limits()):
    """Codec range is separate from the axis's unbounded mathematical domain."""
    n = exact_fraction(value, limits)
    if n.denominator != 1:
        raise ModelError("NONINTEGRAL_INSTANT_CODEC")
    if not -(1 << 63) <= n < (1 << 63):
        raise ModelError("INSTANT_CODEC_RANGE")
    return n.numerator


def metadata_bytes(value):
    """Oracle JSON envelope accounting, deliberately NOT production wire/heap."""
    def encode(obj):
        if isinstance(obj, Fraction):
            return str(obj)
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        raise TypeError(type(obj))
    return len(json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def run_historical(path):
    document = json.loads(path.read_text())
    assertions = 0
    outcomes = []
    for case in document["cases"]:
        axis = Axis("historical-us", "Z", "UTC-test", "microsecond-test-instant")
        current = Snapshot(case["id"], axis)
        snapshots = {}
        for number, step in enumerate(case["steps"]):
            if "operations" in step:
                try:
                    candidate, _ = current.apply(step["operations"], f"{step['system']}:revision", schema=case["schema"])
                except ModelError as error:
                    if error.code != step.get("expect_error"):
                        raise AssertionError((case["id"], number, error.code, step.get("expect_error")))
                    assertions += 1
                    outcomes.append({"case": case["id"], "step": number, "error": error.code})
                else:
                    if "expect_error" in step:
                        raise AssertionError((case["id"], number, "missing expected failure"))
                    current = candidate
                    snapshots[step["system"]] = current
            else:
                actual = snapshots[step["system"]].project(step["valid"])
                if actual != step["expect"]:
                    raise AssertionError((case["id"], number, actual, step["expect"]))
                assertions += 1
                outcomes.append({"case": case["id"], "step": number, "projection": actual})
    return {"cases": len(document["cases"]), "assertions": assertions, "outcomes": outcomes}
