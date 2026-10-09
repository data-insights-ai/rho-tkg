"""Small executable semantic oracle. Full-copy storage is only for testing."""
from __future__ import annotations
from copy import deepcopy
from enum import Enum
import math
from typing import Any


class Missing(Enum):
    VALUE = 0

ABSENT = Missing.VALUE

class ModelError(Exception):
    pass


def bound(x: Any) -> float | int:
    if x == '-inf': return -math.inf
    if x == '+inf': return math.inf
    if isinstance(x, bool) or not isinstance(x, int):
        raise ModelError('INVALID_INTERVAL')
    return x


def span(raw: Any) -> tuple[float | int, float | int]:
    if raw is None:
        raise ModelError('VALIDITY_REQUIRED')
    if not isinstance(raw, list) or len(raw) != 2:
        raise ModelError('INVALID_INTERVAL')
    lo, hi = map(bound, raw)
    if lo >= hi:
        raise ModelError('INVALID_INTERVAL')
    return lo, hi


def at(runs: list, t: float | int) -> Any:
    for lo, hi, value in runs:
        if lo <= t < hi:
            return value
    return ABSENT


def canonical(runs: list) -> list:
    out = []
    for lo, hi, value in sorted(runs, key=lambda r: r[0]):
        if lo >= hi or value is ABSENT:
            continue
        if out and out[-1][1] > lo:
            raise AssertionError('overlapping runs')
        if out and out[-1][1] == lo and out[-1][2] == value:
            out[-1] = (out[-1][0], hi, value)
        else:
            out.append((lo, hi, deepcopy(value)))
    return out


def assign(runs: list, lo: float | int, hi: float | int, value: Any) -> list:
    if lo >= hi:
        raise ModelError('INVALID_INTERVAL')
    out = []
    for a, b, old in runs:
        if b <= lo or a >= hi:
            out.append((a,b,old))
        else:
            if a < lo: out.append((a,lo,old))
            if b > hi: out.append((hi,b,old))
    if value is not ABSENT:
        out.append((lo,hi,value))
    return canonical(out)


def covered(runs: list, lo: float | int, hi: float | int, value: Any) -> bool:
    pos = lo
    for a,b,x in runs:
        if b <= pos: continue
        if a > pos or x != value: return False
        pos = min(b,hi)
        if pos == hi: return True
    return False


def vacant(runs: list, lo: float | int, hi: float | int, same=ABSENT) -> bool:
    return all(b <= lo or a >= hi or x == same for a,b,x in runs)


def set_change(runs: list, lo: float | int, hi: float | int, value: Any, add: bool) -> list:
    points = sorted({lo,hi} | {x for a,b,_ in runs for x in (a,b) if lo < x < hi})
    out = runs
    for a,b in zip(points,points[1:]):
        old = at(runs,a)
        members = set() if old is ABSENT else set(old)
        if add: members.add(value)
        else: members.discard(value)
        out = assign(out,a,b,frozenset(members) if members else ABSENT)
    return out


def _new_object(valid: list) -> dict:
    a,b = span(valid)
    return {'presence':[(a,b,1)], 'next_life':2, 'props':{1:{}}, 'labels':{1:[]}}


def _check_type(value: Any, definition: dict) -> None:
    if value is None: return
    kind = definition.get('type','String')
    okay = {
        'String': lambda: isinstance(value,str),
        'Bool': lambda: isinstance(value,bool),
        'I64': lambda: isinstance(value,int) and not isinstance(value,bool),
        'F64': lambda: isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value),
    }.get(kind, lambda: False)()
    if not okay: raise ModelError('TYPE_MISMATCH')


class TemporalOracle:
    def __init__(self, schema: dict):
        self.schema = schema
        self.current = {'nodes':{}, 'edges':{}}
        self.history = {'EMPTY':deepcopy(self.current)}
        self.commits: list[str] = []

    @staticmethod
    def _owner(state: dict, ident: str) -> tuple[dict,str]:
        if ident in state['nodes']: return state['nodes'][ident], 'vertex'
        if ident in state['edges']: return state['edges'][ident], 'edge'
        raise ModelError('NOT_FOUND')

    def mutate(self, ops: list[dict], system: str) -> None:
        candidate = deepcopy(self.current)
        for op in ops: self._apply(candidate,op)
        self._validate_unique(candidate)
        if system in self.history: raise ValueError('duplicate system label')
        self.current = candidate
        self.history[system] = deepcopy(candidate)
        self.commits.append(system)

    def _initial(self, state: dict, ident: str, life: int, op: dict) -> None:
        for k,v in op.get('properties',{}).items():
            self._apply(state,{'op':'set','owner':ident,'life':life,'key':k,'value':v,'valid':op['valid']})
        for label in op.get('labels',[]):
            self._apply(state,{'op':'add_label','owner':ident,'life':life,'label':label,'valid':op['valid']})

    def _apply(self, state: dict, op: dict) -> None:
        kind = op['op']
        a,b = span(op.get('valid'))
        if kind in ('create_vertex','create_edge'):
            ident = op['id']
            if ident in state['nodes'] or ident in state['edges']: raise ModelError('ALREADY_EXISTS')
            obj = _new_object(op['valid'])
            if kind == 'create_vertex':
                state['nodes'][ident] = obj
            else:
                src, dst = op['source'],op['target']
                sl,tl = op.get('source_life',1),op.get('target_life',1)
                for n,l in ((src,sl),(dst,tl)):
                    if n not in state['nodes']: raise ModelError('NOT_FOUND')
                    if not covered(state['nodes'][n]['presence'],a,b,l): raise ModelError('OWNER_VALIDITY')
                obj.update(source=src,target=dst,type=op.get('type','LINK'),bindings={1:(sl,tl)})
                state['edges'][ident] = obj
            self._initial(state,ident,1,op)
            return
        ident = op.get('owner',op.get('id'))
        obj, owner_kind = self._owner(state,ident)
        life = op.get('life',1)
        if kind == 'reopen':
            if not vacant(obj['presence'],a,b): raise ModelError('LIFECYCLE_OVERLAP')
            life = obj['next_life']
            if owner_kind == 'edge':
                sl,tl = op['source_life'],op['target_life']
                for n,l in ((obj['source'],sl),(obj['target'],tl)):
                    if not covered(state['nodes'][n]['presence'],a,b,l): raise ModelError('OWNER_VALIDITY')
                obj['bindings'][life] = (sl,tl)
            obj['next_life'] += 1
            obj['props'][life] = {}; obj['labels'][life] = []
            obj['presence'] = assign(obj['presence'],a,b,life)
            self._initial(state,ident,life,op)
            return
        if life not in obj['props']: raise ModelError('NOT_FOUND')
        if kind in ('close','correct'):
            present = kind == 'correct' and op['present']
            if not vacant(obj['presence'],a,b,same=life): raise ModelError('LIFECYCLE_OVERLAP')
            if present and owner_kind == 'edge':
                sl,tl = obj['bindings'][life]
                for n,l in ((obj['source'],sl),(obj['target'],tl)):
                    if not covered(state['nodes'][n]['presence'],a,b,l): raise ModelError('OWNER_VALIDITY')
            obj['presence'] = assign(obj['presence'],a,b,life if present else ABSENT)
            return
        if not covered(obj['presence'],a,b,life): raise ModelError('OWNER_VALIDITY')
        if kind in ('add_label','remove_label'):
            if owner_kind != 'vertex': raise ModelError('TYPE_MISMATCH')
            obj['labels'][life] = set_change(obj['labels'][life],a,b,op['label'],kind == 'add_label')
            return
        key = op['key']; definition = self.schema['properties'].get(key)
        if definition is None: raise ModelError('SCHEMA_MISMATCH')
        if definition.get('owner',owner_kind) != owner_kind: raise ModelError('TYPE_MISMATCH')
        props = obj['props'][life]; runs = props.get(key,[])
        card = definition.get('cardinality','scalar')
        if kind == 'unset': props[key] = assign(runs,a,b,ABSENT)
        elif kind == 'set':
            if card != 'scalar': raise ModelError('TYPE_MISMATCH')
            _check_type(op['value'],definition)
            props[key] = assign(runs,a,b,op['value'])
        elif kind in ('add','remove'):
            if card != 'set': raise ModelError('TYPE_MISMATCH')
            _check_type(op['value'],definition)
            props[key] = set_change(runs,a,b,op['value'],kind == 'add')
        else: raise ModelError('INVALID_VALUE')

    def _validate_unique(self, state: dict) -> None:
        for key,definition in self.schema.get('properties',{}).items():
            if not definition.get('unique',False): continue
            points = set()
            for obj in state['nodes'].values():
                points.update(x for a,b,_ in obj['presence'] for x in (a,b))
                for props in obj['props'].values():
                    points.update(x for a,b,_ in props.get(key,[]) for x in (a,b))
            for t in sorted(points):
                if t == math.inf: continue
                claimed = {}
                for ident,obj in state['nodes'].items():
                    life = at(obj['presence'],t)
                    if life is ABSENT: continue
                    val = at(obj['props'][life].get(key,[]),t)
                    if val is ABSENT or val is None: continue
                    if val in claimed and claimed[val] != ident: raise ModelError('UNIQUE_OVERLAP')
                    claimed[val] = ident

    def read(self, valid: int, system: str) -> dict:
        state = self.history[system]
        nodes,edges = {},{}
        def props(obj,life):
            out = {}
            for key,runs in obj['props'][life].items():
                v = at(runs,valid)
                if v is not ABSENT: out[key] = sorted(v) if isinstance(v,frozenset) else v
            return out
        for ident,obj in sorted(state['nodes'].items()):
            life = at(obj['presence'],valid)
            if life is ABSENT: continue
            labels = at(obj['labels'][life],valid)
            nodes[ident] = {'life':life,'properties':props(obj,life),'labels':[] if labels is ABSENT else sorted(labels)}
        for ident,obj in sorted(state['edges'].items()):
            life = at(obj['presence'],valid)
            if life is ABSENT: continue
            sl,tl = obj['bindings'][life]
            if nodes.get(obj['source'],{}).get('life') != sl: continue
            if nodes.get(obj['target'],{}).get('life') != tl: continue
            edges[ident] = {'life':life,'source':obj['source'],'target':obj['target'],'type':obj['type'],'properties':props(obj,life)}
        return {'nodes':nodes,'edges':edges}


def run_fixture(case: dict) -> int:
    oracle = TemporalOracle(case['schema'])
    checked = 0
    for step in case['steps']:
        if 'operations' in step:
            expected = step.get('expect_error')
            before = deepcopy(oracle.current)
            try:
                oracle.mutate(step['operations'],step['system'])
            except ModelError as e:
                if str(e) != expected:
                    raise AssertionError((case['id'],str(e),expected)) from e
                assert oracle.current == before
                checked += 1
            else:
                if expected: raise AssertionError((case['id'],'expected error',expected))
        else:
            actual = oracle.read(step['valid'],step['system'])
            if actual != step['expect']:
                raise AssertionError((case['id'],step,actual))
            checked += 1
    return checked
