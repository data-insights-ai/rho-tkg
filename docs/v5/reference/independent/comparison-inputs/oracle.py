"""Disk-backed independent graph reducer/query oracle; imports no generator."""
import json
import sqlite3
from collections import Counter
from pathlib import Path

from format import ContractError, canonical, cell, entity, float_value, int_key, parse, payload_bytes


class Oracle:
    def __init__(self, path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path)
        self.db.executescript("""
        PRAGMA journal_mode=DELETE;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=-4096;
        PRAGMA synchronous=OFF;
        CREATE TABLE entities(kind TEXT NOT NULL, id TEXT NOT NULL, row TEXT,
          deleted INTEGER NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(kind,id));
        CREATE TABLE labels(id TEXT NOT NULL, label TEXT NOT NULL, PRIMARY KEY(id,label));
        CREATE TABLE edges(id TEXT PRIMARY KEY, source TEXT NOT NULL, target TEXT NOT NULL, type TEXT NOT NULL);
        CREATE INDEX outgoing ON edges(source,id);
        CREATE INDEX incoming ON edges(target,id);
        CREATE INDEX edge_types ON edges(type,id);
        CREATE TABLE properties(kind TEXT NOT NULL,id TEXT NOT NULL,key TEXT NOT NULL,
          type TEXT NOT NULL,value TEXT NOT NULL,int_order TEXT,real_value REAL,
          text_value TEXT,bool_value INTEGER,PRIMARY KEY(kind,id,key));
        CREATE INDEX property_integer ON properties(kind,key,type,int_order,id);
        CREATE INDEX property_float ON properties(kind,key,type,real_value,id);
        CREATE INDEX property_text ON properties(kind,key,type,text_value,id);
        CREATE TABLE revisions(revision INTEGER NOT NULL,kind TEXT NOT NULL,id TEXT NOT NULL,
          deleted INTEGER NOT NULL,row TEXT,changes TEXT NOT NULL,PRIMARY KEY(revision,kind,id));
        """)
        self.high_revision = 0

    def close(self):
        self.db.close()

    def exists(self, kind, identifier):
        return self.db.execute("SELECT 1 FROM entities WHERE kind=? AND id=? AND deleted=0", (kind, identifier)).fetchone() is not None

    def _install(self, row, revision, changes, allow_existing=False):
        row = entity(row)
        kind, identifier = row["kind"], row["id"]
        previous = self.db.execute("SELECT deleted,row FROM entities WHERE kind=? AND id=?", (kind, identifier)).fetchone()
        if previous is not None and (not allow_existing or previous[0]):
            raise ContractError("duplicate/recreated external identity")
        if kind == "edge" and previous is not None:
            earlier = parse(previous[1])
            if any(earlier[key] != row[key] for key in ("source", "target", "type")):
                raise ContractError("edge endpoint/type rewrites are outside the basic workload")
        if kind == "edge" and any(not self.exists("node", row[key]) for key in ("source", "target")):
            raise ContractError("dangling edge")
        wire = canonical(row)
        self.db.execute("INSERT INTO entities VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET row=excluded.row,deleted=0,revision=excluded.revision", (kind, identifier, wire, 0, revision))
        self.db.execute("DELETE FROM properties WHERE kind=? AND id=?", (kind, identifier))
        if kind == "node":
            self.db.execute("DELETE FROM labels WHERE id=?", (identifier,))
            self.db.executemany("INSERT INTO labels VALUES(?,?)", ((identifier, label) for label in row["labels"]))
        else:
            self.db.execute("INSERT INTO edges VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET source=excluded.source,target=excluded.target,type=excluded.type", (identifier, row["source"], row["target"], row["type"]))
        for key, value in row["properties"].items():
            typed = cell(value)
            typename = typed["type"]
            self.db.execute("INSERT INTO properties VALUES(?,?,?,?,?,?,?,?,?)", (
                kind, identifier, key, typename, canonical(typed),
                int_key(typed["value"]) if typename == "i64" else None,
                float_value(typed) if typename == "f64" else None,
                typed["value"] if typename == "text" else None,
                int(typed["value"]) if typename == "bool" else None))
        self.db.execute("INSERT INTO revisions VALUES(?,?,?,?,?,?)", (revision, kind, identifier, 0, wire, canonical(changes)))

    def load(self, nodes, edges):
        for path, kind in ((nodes, "node"), (edges, "edge")):
            with Path(path).open(encoding="utf-8") as source:
                for line in source:
                    row = parse(line)
                    if row.get("kind") != kind:
                        raise ContractError("wrong input entity kind")
                    self._install(row, 0, {"created": True})
        self.db.commit()

    def apply(self, event):
        self.db.execute("SAVEPOINT event")
        previous_revision = self.high_revision
        try:
            self._apply(event)
        except Exception:
            self.db.execute("ROLLBACK TO event")
            self.db.execute("RELEASE event")
            self.high_revision = previous_revision
            raise
        self.db.execute("RELEASE event")

    def _apply(self, event):
        if type(event) is not dict or type(event.get("revision")) is not int or event["revision"] < 1 or event["revision"] < self.high_revision:
            raise ContractError("revision ordering lost")
        revision = event["revision"]
        operation = event.get("op")
        if operation in ("append", "upsert"):
            if set(event) != {"revision", "op", "row"}:
                raise ContractError("malformed row mutation")
            self._install(event["row"], revision, {operation: True}, operation == "upsert")
        elif operation == "update":
            if set(event) != {"revision", "op", "kind", "id", "set", "remove"} or type(event["set"]) is not dict or type(event["remove"]) is not list or len(set(event["remove"])) != len(event["remove"]):
                raise ContractError("malformed property update")
            previous = self.db.execute("SELECT row FROM entities WHERE kind=? AND id=? AND deleted=0", (event["kind"], event["id"])).fetchone()
            if previous is None:
                raise ContractError("update of absent/deleted identity")
            row = parse(previous[0])
            if set(event["set"]) & set(event["remove"]):
                raise ContractError("ambiguous set/remove")
            for key in event["remove"]:
                if key not in row["properties"]:
                    raise ContractError("remove must identify an actually present property")
                del row["properties"][key]
            row["properties"].update({key: cell(value) for key, value in event["set"].items()})
            self._install(row, revision, {"set": event["set"], "remove": event["remove"]}, True)
        elif operation == "delete":
            if set(event) != {"revision", "op", "kind", "id"} or not self.exists(event["kind"], event["id"]):
                raise ContractError("delete of absent/deleted identity")
            kind, identifier = event["kind"], event["id"]
            if kind == "node" and self.db.execute("SELECT 1 FROM edges WHERE source=? OR target=? LIMIT 1", (identifier, identifier)).fetchone():
                raise ContractError("node deletion requires explicit incident-edge deletions")
            self.db.execute("UPDATE entities SET row=NULL,deleted=1,revision=? WHERE kind=? AND id=?", (revision, kind, identifier))
            self.db.execute("DELETE FROM properties WHERE kind=? AND id=?", (kind, identifier))
            self.db.execute("DELETE FROM labels WHERE id=?", (identifier,))
            self.db.execute("DELETE FROM edges WHERE id=?", (identifier,))
            self.db.execute("INSERT INTO revisions VALUES(?,?,?,?,?,?)", (revision, kind, identifier, 1, None, canonical({"entity_deleted": True})))
        else:
            raise ContractError("unsupported mutation")
        self.high_revision = revision

    def commit(self):
        self.db.commit()

    def _ids(self, sql, arguments, kind):
        for (identifier,) in self.db.execute(sql, arguments):
            yield {"kind": kind, "id": identifier}

    def answer(self, request):
        operation = request["op"]
        kind = request.get("kind")
        if operation == "lookup":
            for (wire,) in self.db.execute("SELECT row FROM entities WHERE kind=? AND id=? AND deleted=0", (kind, request["id"])):
                yield parse(wire)
        elif operation == "scan":
            for (wire,) in self.db.execute("SELECT row FROM entities WHERE kind=? AND deleted=0 ORDER BY id", (kind,)):
                yield parse(wire)
        elif operation == "label":
            yield from self._ids("SELECT id FROM labels WHERE label=? ORDER BY id", (request["label"],), "node")
        elif operation == "type":
            yield from self._ids("SELECT id FROM edges WHERE type=? ORDER BY id", (request["type"],), "edge")
        elif operation in ("equality", "range"):
            key = request["key"]
            if operation == "equality":
                value = cell(request["value"])
                # All basic f64 zeros share a canonical numeric-zero key.
                sql = "SELECT id FROM properties WHERE kind=? AND key=? AND type=? AND value=? ORDER BY id"
                arguments = (kind, key, value["type"], canonical(value))
            else:
                low, high = cell(request["low"]), cell(request["high"])
                if low["type"] != high["type"] or low["type"] not in ("i64", "f64"):
                    raise ContractError("unsupported/mixed range type")
                column = "int_order" if low["type"] == "i64" else "real_value"
                lower, upper = (int_key(low["value"]), int_key(high["value"])) if low["type"] == "i64" else (float_value(low), float_value(high))
                if lower > upper:
                    raise ContractError("reversed range")
                sql = f"SELECT id FROM properties WHERE kind=? AND key=? AND type=? AND {column}>=? AND {column}<=? ORDER BY id"
                arguments = (kind, key, low["type"], lower, upper)
            yield from self._ids(sql, arguments, kind)
        elif operation == "adjacency":
            column = "source" if request["direction"] == "out" else "target" if request["direction"] == "in" else None
            if column is None:
                raise ContractError("invalid adjacency direction")
            for identifier, source, target, typename in self.db.execute(f"SELECT id,source,target,type FROM edges WHERE {column}=? ORDER BY id", (request["id"],)):
                yield {"id": identifier, "source": source, "target": target, "type": typename}
        elif operation == "projection":
            for identifier, wire in self.db.execute("SELECT id,row FROM entities WHERE kind=? AND deleted=0 ORDER BY id", (kind,)):
                row = parse(wire)
                yield {"kind": kind, "id": identifier, "columns": {key: {"presence": "present", "value": row["properties"][key]} if key in row["properties"] else {"presence": "absent"} for key in request["keys"]}}
        elif operation == "expand":
            depth = request["max_depth"]
            if type(depth) is not int or not 1 <= depth <= 4:
                raise ContractError("expansion depth must be 1..4")
            start = request["id"]
            if self.exists("node", start):
                yield from self._walks(start, start, (), (start,), depth)
        elif operation == "history":
            for revision, entity_kind, identifier, deleted, wire, changes in self.db.execute("SELECT revision,kind,id,deleted,row,changes FROM revisions ORDER BY revision,kind,id"):
                yield {"revision": revision, "kind": entity_kind, "id": identifier, "deleted": bool(deleted), "row": parse(wire) if wire is not None else None, "property_changes": parse(changes)}
        else:
            raise ContractError("unknown query operation")

    def _walks(self, start, current, edge_ids, node_ids, remaining):
        if remaining == 0:
            return
        for identifier, target in self.db.execute("SELECT id,target FROM edges WHERE source=? ORDER BY id", (current,)):
            next_edges, next_nodes = (*edge_ids, identifier), (*node_ids, target)
            yield {"start": start, "depth": len(next_edges), "edge_ids": list(next_edges), "node_ids": list(next_nodes)}
            yield from self._walks(start, target, next_edges, next_nodes, remaining - 1)

    def ledger(self):
        result = {"current_nodes": 0, "current_edges": 0, "label_memberships": 0, "type_memberships": 0, "self_edges": 0, "property_entries": {"node": {}, "edge": {}}, "property_payload_bytes": {"node": {}, "edge": {}}, "property_key_utf8_bytes": {"node": 0, "edge": 0}, "label_type_utf8_bytes": 0}
        entries = {kind: Counter() for kind in ("node", "edge")}
        payload = {kind: Counter() for kind in ("node", "edge")}
        for kind, wire in self.db.execute("SELECT kind,row FROM entities WHERE deleted=0 ORDER BY kind,id"):
            row = parse(wire)
            result["current_nodes" if kind == "node" else "current_edges"] += 1
            if kind == "node":
                result["label_memberships"] += len(row["labels"])
                result["label_type_utf8_bytes"] += sum(len(label.encode("utf-8")) for label in row["labels"])
            else:
                result["type_memberships"] += 1
                result["self_edges"] += row["source"] == row["target"]
                result["label_type_utf8_bytes"] += len(row["type"].encode("utf-8"))
            for key, value in row["properties"].items():
                entries[kind][value["type"]] += 1
                payload[kind][value["type"]] += payload_bytes(value)
                result["property_key_utf8_bytes"][kind] += len(key.encode("utf-8"))
        result["property_entries"] = {kind: dict(sorted(value.items())) for kind, value in entries.items()}
        result["property_payload_bytes"] = {kind: dict(sorted(value.items())) for kind, value in payload.items()}
        result["label_memberships_by_label"] = {label: count for label, count in self.db.execute("SELECT label,COUNT(*) FROM labels GROUP BY label ORDER BY label")}
        result["edge_identities_by_type"] = {typename: count for typename, count in self.db.execute("SELECT type,COUNT(*) FROM edges GROUP BY type ORDER BY type")}
        result["parallel_edge_excess"] = self.db.execute("SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM edges GROUP BY source,target,type HAVING COUNT(*)>1)").fetchone()[0]
        result["retained_revisions"] = self.db.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
        result["retained_revisions_by_kind"] = {kind: count for kind, count in self.db.execute("SELECT kind,COUNT(*) FROM revisions GROUP BY kind ORDER BY kind")}
        result["current_assertions"] = result["current_nodes"] + result["current_edges"]
        result["retained_deleted_revisions"] = self.db.execute("SELECT COUNT(*) FROM revisions WHERE deleted=1").fetchone()[0]
        result["deleted_node_identities"] = self.db.execute("SELECT COUNT(*) FROM entities WHERE kind='node' AND deleted=1").fetchone()[0]
        result["deleted_edge_identities"] = self.db.execute("SELECT COUNT(*) FROM entities WHERE kind='edge' AND deleted=1").fetchone()[0]
        retained_entries = {kind: Counter() for kind in ("node", "edge")}
        retained_payload = {kind: Counter() for kind in ("node", "edge")}
        for (wire,) in self.db.execute("SELECT row FROM revisions WHERE deleted=0"):
            row = parse(wire)
            for value in row["properties"].values():
                retained_entries[row["kind"]][value["type"]] += 1
                retained_payload[row["kind"]][value["type"]] += payload_bytes(value)
        result["retained_property_entries"] = {kind: dict(sorted(counts.items())) for kind, counts in retained_entries.items()}
        result["retained_property_payload_bytes"] = {kind: dict(sorted(counts.items())) for kind, counts in retained_payload.items()}
        return result
