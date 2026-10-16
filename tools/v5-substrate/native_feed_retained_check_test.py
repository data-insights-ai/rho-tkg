#!/usr/bin/env python3
"""Compact authored codec/join/controller guards, not captured workload evidence."""
import copy
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import native_feed_retained_check as check
import native_feed_retained_cell as cell
from comparison_spool import Policy, Spool, u64


class NativeRetainedFeedGuards(unittest.TestCase):
    def setUp(self):
        self.spec={"version":3,"family":"snapshot-to-feed","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}

    def test_fresh_dimensions_and_retention_cannot_relabel_old_or_paired(self):
        self.assertEqual(check.shape(self.spec),self.spec)
        for name in ("version","facts","crossPercent","writers"):
            with self.assertRaises(ValueError):check.shape({**self.spec,name:True})
        for label,rtt in check.TRANSPORTS.items():self.assertEqual(cell.preflight(self.spec,label,1800,4096)["targetRTTMillis"],rtt)
        for version in (1,2):
            with self.assertRaises(ValueError):check.shape({**self.spec,"version":version})
        for engine in ("foundationdb","paired"):
            with self.assertRaises(ValueError):cell.preflight(self.spec,"local",1800,4096,engine)
        for facts in (10_000,100_000,1_000_000):
            self.assertLess(check.frames({**self.spec,"facts":facts})*260+65536,1<<39)
        self.assertNotEqual(check.AGREEMENT,check.scalar.AGREEMENT)

    def test_same_body_replay_requires_actual_first_install_below_ack(self):
        image=bytes(172);first_index=5;out_body=b"\1"+b"i"*32+b"h"*32+u64(first_index)+u64(4)+u64(1)
        out=b"GGO\1"+out_body;out+=check.digest(out)
        first={"Outcome":check.scalar.base64.b64encode(out).decode(),"CDC":check.scalar.base64.b64encode(b"actual-first-sidecar").decode()}
        frame={"Partition":3,"Index":12,"Root":list(image),"Outcome":first["Outcome"],"CDC":"","OriginalOutcome":first["Outcome"],"OriginalCDC":None,"Rows":[]}
        with tempfile.TemporaryDirectory() as path:
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                with self.assertRaises(ValueError):check.original_before_ack(frame,image,spool,6)
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                spool.insert(b"frame",u64(3)+u64(first_index),check.encoded(first))
                check.original_before_ack(frame,image,spool,6)
                for bad,ack in ((frame,5),({**frame,"CDC":first["CDC"]},6),({**frame,"OriginalOutcome":None},6),({**frame,"Root":[1]+[0]*171},6)):
                    with self.assertRaises(ValueError):check.original_before_ack(bad,image,spool,ack)

    def test_checkpoint_generation_is_explicit_not_initial_relabel(self):
        for stage in check.COLUMN_STAGES:
            self.assertEqual(check.column_generation(stage),int(stage.startswith(("actual-post-GC-checkpoint-seal","reopened-checkpoint"))))
        self.assertEqual(check.column_generation("retire-checkpoint-owner"),1)
        with self.assertRaises(ValueError):check.column_generation("final-seal/native")
        end={"spec":self.spec,"CheckpointFirst":21,"SparseFrom":[10,11],"Initial":"empty","Checkpoint":"actual-after-GC","Snapshot":"C","Final":"final"}
        cert=[{"sourceIndex":12,"round":4},{"sourceIndex":13,"round":4}]
        end["ColumnManifests"]=[[{"Rows":82},{"Rows":1}],[{"Rows":38},{"Rows":0}]]
        with patch.object(check,"cut",return_value=cert),patch.object(check.scalar,"_scalar_columns",return_value="bound") as shared:
            self.assertEqual(check.columns("directory",end,"spool"),"bound")
            self.assertEqual(shared.call_args.kwargs["initial_cuts"],("empty","actual-after-GC"))
            changed=copy.deepcopy(end);changed["ColumnManifests"][1][0]["Rows"]=82
            with self.assertRaises(ValueError):check.columns("directory",changed,"spool")

    def test_real_gc_original_binds_scope_index_hash_and_floors(self):
        scope=bytes.fromhex(check.raw.GRAPH)+u64(3)+bytes(64)+check.AGREEMENT
        body=b"GGC\1"+scope+b"i"*16+b"r"*16+b"c"*32+bytes(32)+b"".join(u64(v) for v in (8,8,9,9,8,1))
        command=body+check.digest(body)
        b=b"GGO\1"+b"\1"+check.digest(command)+b"s"*32+u64(11)+u64(10)+u64(5);out=b+check.digest(b)
        self.assertEqual(check.retention_original(command,out,3)["floors"],[8,8,9,9,8,1])
        for c,o,p in ((command,out,8),(command,out[:-1]+bytes([out[-1]^1]),3),(command[:-1]+bytes([command[-1]^1]),out,3)):
            with self.assertRaises(ValueError):check.retention_original(c,o,p)

    def test_fresh_owned_output_and_complete_gc_inventory(self):
        with tempfile.TemporaryDirectory() as path:
            root=Path(path)
            with self.assertRaises(ValueError):cell.owned_output("relative")
            output,engine=cell.owned_output(root/"fresh")
            self.assertTrue(output.is_dir());self.assertFalse(engine.exists())
            with self.assertRaises(FileExistsError):cell.owned_output(output)
            link=root/"foreign";link.symlink_to(output,target_is_directory=True)
            with self.assertRaises(ValueError):cell.owned_output(link)
            with self.assertRaises(ValueError):cell.hash_artifacts(output,"columns")
            with patch.object(cell,"executables",side_effect=AssertionError("binary work must not begin")):
                with self.assertRaises(ValueError):cell.execute({**self.spec,"version":1},"local",root/"invalid","binary",None,1800,4096,Policy(1<<20,65536,4096,check.LINE))
            self.assertFalse((root/"invalid").exists())
        self.assertIn("gc.jsonl",cell.ARTIFACTS)

    def test_transport_ceiling_override_is_finite_without_legacy_shape_change(self):
        spec={**self.spec,"version":1,"family":check.codec.FAMILY}
        for ceiling in (True,0,-1,(1<<41)+1):
            with self.assertRaises(ValueError):check.codec.owned_transport([{},{}],spec,maximum_frames=ceiling)
        transport={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009}
        lane={"Admitted":2,"Completed":2,"Canceled":0,"Bytes":8,"ConfiguredNanos":0,"ObservedNanos":2,"DeviationNanos":2,"MinimumObservedNanos":1,"MaximumObservedNanos":1}
        pipe={"AttemptedFrames":2,"CompleteFrames":2,"FailedFrames":0,"PrepaidBytes":8,"ActualBytes":8}
        receipt={"Version":1,"Spec":transport,"Raft":lane,"SourceRequest":lane,"SourceReply":lane,"MaximumHeldFrames":1,"MaximumHeldBytes":32768,"MetadataBytes":32768,"PipeWrites":pipe,"PipeReads":pipe,"Scope":"not native delivery/ACK; no physical network claim"}
        receipts=[receipt,receipt]
        self.assertEqual(check.codec.owned_transport(receipts,spec),transport)
        self.assertEqual(check.codec.owned_transport(receipts,spec,maximum_frames=2),transport)
        self.assertEqual(check.codec.owned_transport(receipts,spec,maximum_frames=1<<41),transport)
        with self.assertRaises(ValueError):check.codec.owned_transport(receipts,spec,maximum_frames=1)
        # A positive bound cannot bless absent actual native counter receipts.
        for ceiling in (1,1<<41):
            with self.assertRaises((ValueError,KeyError)):check.codec.owned_transport([{},{}],spec,maximum_frames=ceiling)

    def test_unknown_reply_never_becomes_a_pin_or_completed_page(self):
        q={"Version":1,"Action":"page"}
        reply={"Version":0,"Action":"","Digest":[0]*32,"Replay":False,"EOF":False,"Next":0,"Index":0,"Nanos":0,"Page":None}
        self.assertFalse(check.reply_identity(q,reply,"unknown actual RPC"))
        for body,error in ((reply,None),({**reply,"Version":False},"unknown"),({**reply,"Page":"not-an-outcome"},"unknown"),({**reply,"Version":1,"Action":"ack"},None)):
            with self.assertRaises(ValueError):check.reply_identity(q,body,error)

    def test_legacy_column_wrapper_remains_literal(self):
        end={"spec":{"version":1,"family":"snapshot-to-feed","facts":40,"crossPercent":50,"writers":32,"layout":"columns"},"Snapshot":"C"}
        with patch.object(check.scalar,"_scalar_columns",return_value="old") as helper:
            self.assertEqual(check.scalar.columns("directory",end,"spool"),"old")
            helper.assert_called_once_with("directory",end,"spool",check.scalar.cut,check.scalar.frames(end["spec"]),"C")


if __name__=="__main__":unittest.main()
