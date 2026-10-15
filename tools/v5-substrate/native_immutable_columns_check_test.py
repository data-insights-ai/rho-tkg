#!/usr/bin/env python3
"""Authored finite guards; synthetic bytes are not native execution evidence."""
import base64
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import native_immutable_columns_check as check
import native_immutable_columns_cell as cell
from comparison_spool import Policy,Spool


class NativeImmutableColumnsGuards(unittest.TestCase):
    def test_fresh_family_original_deadline_and_exclusive_leaf(self):
        s={"version":1,"family":check.FAMILY,"facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in check.TRANSPORTS:cell.preflight(s,transport,1800,4096)
        for change in ({"family":"partition-local-create-update"},{"version":True},{"facts":20}):
            with self.assertRaises(ValueError):check.shape(s|change)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):cell.preflight(s,transport,1800,4096,"paired")
        with tempfile.TemporaryDirectory() as directory:
            parent=Path(directory).resolve();target=parent/"absent";linked=parent/"linked";linked.symlink_to(target,target_is_directory=True)
            with self.assertRaises(ValueError):cell.common.owned_output(linked)
            self.assertFalse(target.exists())
            binary,watchdog=parent/"binary",parent/"watchdog";binary.write_bytes(b"supplied");watchdog.write_bytes(b"supplied")
            class BeforeNative(Exception):pass
            def launch(command,*,env,stdout,stderr,check):
                self.assertEqual(env["RHO_GRAPH_NATIVE_WORKLOAD"],"native-immutable-columns-workload-v1")
                self.assertIn("-test.run=^TestNativeGraphImmutableColumnsWorkloadCell$",command)
                self.assertIn(env["RHO_GRAPH_WORKLOAD_DEADLINE"],command)
                self.assertFalse(Path(env["RHO_GRAPH_WORKLOAD_OUTPUT"]).exists())
                raise BeforeNative
            with patch.object(cell.common,"executables",return_value=(binary,watchdog)),patch.object(cell.common.subprocess,"run",side_effect=launch):
                with self.assertRaises(BeforeNative):cell.execute(s,"local",parent/"cell",binary,watchdog,1800,4096,Policy(4<<20,65536,4096,check.LINE),run="01"*16)

    def test_exact_predecessor_probe_and_complete_native_eof_only(self):
        n=40;end={"spec":{"version":1,"family":check.FAMILY,"facts":n,"crossPercent":50,"writers":32,"layout":"kv"},"AdmissionSHA256":[1]*32}
        summary={"Version":1,"Access":check.ACCESS,"Publications":0,"Scans":0,"ScanPoints":0,"HeldRetireRefusals":0,"SHA256":[0]*32,"DerivedRetention":check.DERIVED}
        header={"Type":"header","Format":check.FORMAT,"Spec":end["spec"],"AdmissionSHA256":end["AdmissionSHA256"]}
        payload=check.encoded(header)+b"\n";summary["SHA256"]=list(hashlib.sha256(payload).digest());end["ImmutableColumns"]=summary
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);stream=root/"column-observations.jsonl";scratch=root/"scratch";scratch.mkdir()
            stream.write_bytes(payload+check.encoded({"Type":"footer","End":summary})+b"\n")
            with Spool(scratch,Policy(4<<20,65536,4096,check.LINE)) as spool:
                self.assertEqual(check.observations(root,end,spool)["columnTiming"],[])
            for body in (payload,payload+payload,payload+check.encoded({"Type":"Scan","Stage":"final-seal/native"})+b"\n"):
                stream.write_bytes(body)
                with Spool(scratch,Policy(4<<20,65536,4096,check.LINE)) as spool,self.assertRaises(ValueError):check.observations(root,end,spool)
            row={"Owner":7,"Life":8,"Component":3,"Before":{"Revision":10},"After":{"Present":True,"Handle":6,"Provenance":44}}
            probes=[]
            with Spool(scratch,Policy(4<<20,65536,4096,check.LINE)) as spool:
                for owner in (7,85):
                    ordinal=4+(owner-7)//2+n
                    presence=row|{"Owner":owner,"Component":1,"After":{"Present":True,"Handle":owner+1,"Provenance":ordinal}}
                    prop=row|{"Owner":owner,"After":{"Present":True,"Handle":6,"Provenance":ordinal}}
                    for component,value in ((1,presence),(3,prop)):spool.insert(b"row",check.u64(ordinal)+check.u64(owner)+bytes([component]),check.encoded(value))
                    probes.append({"Owner":owner,"Found":True,"Presence":presence,"Property":prop})
                empty={name:0 for name in check.column.ROW if name not in ("Name","Type","Axis","Scope","Before","After")};empty.update(Name="",Type="",Axis=None,Scope=None,Before=check.codec.ZERO,After=check.codec.ZERO)
                probes.append({"Owner":5,"Found":False,"Presence":empty,"Property":empty})
                check.points(probes,end,1,spool)
                wrong=[probes[0]|{"Property":probes[0]["Property"]|{"Before":{"Revision":11}}},*probes[1:]]
                with self.assertRaises(ValueError):check.points(wrong,end,1,spool)
                with self.assertRaises(ValueError):check.points(probes[:-1],end,1,spool)
                wire=check.encoded({"EOF":True});record={"Key":base64.b64encode(b"manifest").decode(),"Value":base64.b64encode(wire+hashlib.sha256(wire).digest()).decode()}
                self.assertEqual(check.metadata(record,b"manifest"),{"EOF":True})
                with self.assertRaises(ValueError):check.metadata(record,b"foreign")
                corrupted=record|{"Value":base64.b64encode(wire+bytes(32)).decode()}
                with self.assertRaises(ValueError):check.metadata(corrupted,b"manifest")


if __name__=="__main__":unittest.main()
