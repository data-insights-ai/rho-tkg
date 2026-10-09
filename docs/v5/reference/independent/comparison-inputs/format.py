"""Versioned portable cells and rows; no graph generation or query algorithms."""
import json
import math
import re
import struct

MAPPING_VERSION = "basic-graph-v1"
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1

class ContractError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("duplicate JSON key: " + key)
        result[key] = value
    return result


def parse(text):
    return json.loads(text, object_pairs_hook=_pairs, parse_constant=lambda value: (_ for _ in ()).throw(ContractError("nonfinite JSON: " + value)))


def fields(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ContractError("unexpected/missing fields")


def text(value):
    if type(value) is not str or not value:
        raise ContractError("nonempty text required")
    value.encode("utf-8", "strict")
    return value


def external_id(value, kind):
    if type(value) is not str or not re.fullmatch(("n" if kind == "node" else "e") + r":[0-9a-f]{16}", value):
        raise ContractError("invalid external ID")
    return value


def i64(value):
    if type(value) is int:
        integer = value
    elif type(value) is str and re.fullmatch(r"0|-?[1-9][0-9]*", value):
        integer = int(value)
    else:
        raise ContractError("i64 must be exact integer or canonical decimal text; never float/bool")
    if not I64_MIN <= integer <= I64_MAX:
        raise ContractError("i64 outside signed 64-bit range")
    return integer


def int_key(value):
    # Lexical TEXT ordering is exact over the entire signed int64 domain.
    return f"{i64(value) + (1 << 63):016x}"


def f64_cell(value):
    if type(value) is not float or not math.isfinite(value):
        raise ContractError("finite binary64 required")
    if value == 0.0:
        value = 0.0  # Common numeric-zero semantics; source sign bits are separate.
    return {"type": "f64", "bits": struct.pack(">d", value).hex()}


def float_value(cell):
    return struct.unpack(">d", bytes.fromhex(cell["bits"]))[0]


def cell(value):
    if type(value) is not dict or "type" not in value:
        raise ContractError("typed cell required")
    kind = value["type"]
    if kind == "i64":
        fields(value, ["type", "value"])
        return {"type": kind, "value": str(i64(value["value"]))}
    if kind == "bool":
        fields(value, ["type", "value"])
        if type(value["value"]) is not bool:
            raise ContractError("boolean type lost")
        return dict(value)
    if kind == "text":
        fields(value, ["type", "value"])
        if type(value["value"]) is not str:
            raise ContractError("text type lost")
        value["value"].encode("utf-8", "strict")
        return dict(value)
    if kind == "f64":
        fields(value, ["type", "bits"])
        if type(value["bits"]) is not str or not re.fullmatch(r"[0-9a-f]{16}", value["bits"]):
            raise ContractError("binary64 bits must be exactly 16 lower-case hex digits")
        return f64_cell(float_value(value))
    raise ContractError("unsupported basic scalar type; feature lane required")


def payload_bytes(value):
    value = cell(value)
    return len(value["value"].encode("utf-8")) if value["type"] == "text" else 1 if value["type"] == "bool" else 8


def entity(value):
    if type(value) is not dict or value.get("kind") not in ("node", "edge"):
        raise ContractError("entity kind required")
    kind = value["kind"]
    fields(value, ["kind", "id", "properties", "labels"] if kind == "node" else ["kind", "id", "properties", "source", "target", "type"])
    result = {"kind": kind, "id": external_id(value["id"], kind)}
    if kind == "node":
        labels = value["labels"]
        if type(labels) is not list or any(type(label) is not str for label in labels) or len(set(labels)) != len(labels):
            raise ContractError("unique text labels required")
        result["labels"] = sorted(text(label) for label in labels)
    else:
        result.update(source=external_id(value["source"], "node"), target=external_id(value["target"], "node"), type=text(value["type"]))
    if type(value["properties"]) is not dict:
        raise ContractError("property object required")
    result["properties"] = {text(key): cell(item) for key, item in value["properties"].items()}
    return result


def result_row(value, shape):
    if shape == "entity":
        return entity(value)
    if shape == "id":
        fields(value, ["kind", "id"])
        if value["kind"] not in ("node", "edge"):
            raise ContractError("wrong result kind")
        external_id(value["id"], value["kind"])
        return value
    if shape == "adjacency":
        fields(value, ["id", "source", "target", "type"])
        external_id(value["id"], "edge")
        external_id(value["source"], "node")
        external_id(value["target"], "node")
        text(value["type"])
        return value
    if shape == "projection":
        fields(value, ["kind", "id", "columns"])
        if value["kind"] not in ("node", "edge") or type(value["columns"]) is not dict:
            raise ContractError("invalid projection")
        external_id(value["id"], value["kind"])
        columns = {}
        for key, item in value["columns"].items():
            text(key)
            if item == {"presence": "absent"}:
                columns[key] = item
            else:
                fields(item, ["presence", "value"])
                if item["presence"] != "present":
                    raise ContractError("presence state lost")
                columns[key] = {"presence": "present", "value": cell(item["value"])}
        return {"kind": value["kind"], "id": value["id"], "columns": columns}
    if shape == "walk":
        fields(value, ["start", "depth", "edge_ids", "node_ids"])
        if type(value["depth"]) is not int or value["depth"] < 1 or type(value["edge_ids"]) is not list or type(value["node_ids"]) is not list or len(value["edge_ids"]) != value["depth"] or len(value["node_ids"]) != value["depth"] + 1:
            raise ContractError("incomplete bounded walk")
        external_id(value["start"], "node")
        if value["node_ids"][0] != value["start"]:
            raise ContractError("walk start mismatch")
        for identifier in value["node_ids"]:
            external_id(identifier, "node")
        for identifier in value["edge_ids"]:
            external_id(identifier, "edge")
        return value
    raise ContractError("unknown output shape")
