#!/usr/bin/env python3
"""Compact authored source guards, not captured native feed evidence."""
import copy
import hashlib
import struct
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import native_feed_check as check
import native_feed_cell as cell
import fdb_feed_check as fdb
from comparison_spool import Policy, Spool, u64


class NativeFeedGuards(unittest.TestCase):
    def setUp(self):
        self.spec={"version":1,"family":"snapshot-to-feed","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}

    def test_dimensions_and_real_transport_pairing_refuse_before_effects(self):
        check.shape(self.spec)
        for name in ("version","facts","crossPercent","writers"):
            with self.assertRaises(ValueError):check.shape({**self.spec,name:True})
        for label,rtt in check.TRANSPORTS.items():self.assertEqual(cell.preflight(self.spec,label,1800,4096)["targetRTTMillis"],rtt)
        cell.preflight(self.spec,"local",1800,4096,"paired")
        for args in (("rtt-1ms",1800,"paired"),("rtt-100ms",True,"native-raft"),("local",1801,"paired")):
            with self.assertRaises(ValueError):cell.preflight(self.spec,args[0],args[1],4096,args[2])

    def test_scalar_reuse_keeps_exact_feed_wrapper_parameters(self):
        end={"spec":self.spec,"Snapshot":"actual-C"}
        with patch.object(check,"_scalar_grants",return_value="grants") as helper:
            self.assertEqual(check.grants("owned",end,"spool"),"grants")
            helper.assert_called_once_with("owned",end,"spool",check.AGREEMENT,check.cut)
        with patch.object(check,"_scalar_claims",return_value="claims") as helper:
            self.assertEqual(check.claims("owned",end,"spool"),"claims")
            helper.assert_called_once_with("owned",end,"spool")
        with patch.object(check,"_scalar_attempts",return_value="attempts") as helper:
            self.assertEqual(check.attempts("owned",end,"spool"),"attempts")
            helper.assert_called_once_with("owned",end,"spool")
        with patch.object(check,"_scalar_sources",return_value="sources") as helper:
            self.assertEqual(check.sources("owned",end,"spool"),"sources")
            helper.assert_called_once_with("owned",end,"spool",check.cut,check.frames(self.spec))
        with patch.object(check,"_scalar_columns",return_value="columns") as helper:
            self.assertEqual(check.columns("owned",end,"spool"),"columns")
            helper.assert_called_once_with("owned",end,"spool",check.cut,check.frames(self.spec),"actual-C")

    def test_explicit_column_checkpoint_parameters_preserve_legacy_wrapper(self):
        # The ordinary wrapper still supplies no fresh interval/stage overrides.
        end={"spec":self.spec,"Snapshot":"original-C"}
        with patch.object(check,"_scalar_columns",return_value="legacy") as shared:
            self.assertEqual(check.columns("directory",end,"spool"),"legacy")
            shared.assert_called_once_with("directory",end,"spool",check.cut,check.frames(self.spec),"original-C")

    def test_feed_claim_requires_actual_prior_installed_grant(self):
        # Inert join fixture; workload grants() alone authenticates this roster
        # from native reserve/install originals. No Recipient.Next is inferred.
        session=b"s"*40
        wire=b"\1"+u64(0)+u64(7)+session+u64(1)
        with tempfile.TemporaryDirectory() as path:
            # A missing raw join aborts its disposable spool by contract.
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                with self.assertRaises(ValueError):check._scalar_claim_binding(check.cursor(wire),(1,0,7),3,10,spool)
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                spool.insert(b"installed-grant",session+u64(1),check.encoded({"home":3,"first":1,"last":86,"index":9}))
                check._scalar_claim_binding(check.cursor(wire),(1,0,7),3,10,spool)
                for body,expected,home,index in ((b"\2"+wire[1:],(1,0,7),3,10),(wire,(1,0,7),8,10),(wire,(1,0,7),3,9),
                        (b"\1"+u64(0)+u64(87)+session+u64(1),(1,0,87),3,10)):
                    with self.assertRaises(ValueError):check._scalar_claim_binding(check.cursor(body),expected,home,index,spool)

    def test_allocation_cdc_binds_exact_control_and_routing_publication(self):
        def seal(body):return body+check.digest(body)
        def field(body):return struct.pack(">I",len(body))+body
        graph=bytes.fromhex(check.raw.GRAPH);scope=graph+u64(3)+bytes(96)
        config=b"c"*32;identity=b"r"*16;command_hash=b"h"*32
        def outcome(kind):return seal(b"GRO\2"+bytes((kind,))+scope[:24]+identity+command_hash+u64(9)+b"\1\0\0")
        def routing(out,*,namespace=scope[:24],cfg=config,original=None,publication=None,owner=None):
            publication=seal(b"RPB\1"+graph+struct.pack(">5Q",1,1,46,3,2)+config+u64(3)+u64(9)) if publication is None else publication
            owner=seal(b"ROF\1"+graph+struct.pack(">3Q",1,3,2)+config+b"\0") if owner is None else owner
            logical=seal(b"RCD\1"+namespace+cfg+field(out if original is None else original)+field(publication)+field(owner))
            return seal(b"GCE\1"+out[4:5]+identity+command_hash+u64(9)+field(logical))
        # Real mutating AQP controls emit their exact GRO2, not empty graph CDC.
        out=outcome(10)
        self.assertIsNone(check._scalar_allocation_cdc(out,out,scope,config))
        out=outcome(15);wire=routing(out)
        publication=check._scalar_allocation_cdc(wire,out,scope,config)
        self.assertEqual(len(publication),(140+109))
        for cdc in (b"",outcome(11),routing(out,namespace=graph+u64(8)),routing(out,cfg=b"x"*32),
                    routing(out,original=outcome(16)),routing(out,publication=b"wrong"),routing(out,owner=b"wrong")):
            with self.assertRaises(ValueError):check._scalar_allocation_cdc(cdc,out,scope,config)
        for offset in (4,5,21,53):
            body=bytearray(wire[:-32]);body[offset]^=1
            with self.assertRaises(ValueError):check._scalar_allocation_cdc(seal(body),out,scope,config)
        with self.assertRaises(ValueError):check._scalar_allocation_cdc(wire[:-1]+b"x",out,scope,config)

    def test_inline_axis_matches_only_identical_sealed_column_axis(self):
        # Actual first setup row, whose GCD5 definition is embedded unsealed.
        wire=check.base64.b64decode("HwAAAAAAAAAAAAAAAAAAAAIAAQAAAChwb3NpeC11bml4LWVwb2NoOjE5NzAtMDEtMDFUMDA6MDA6MDBaOnYxAAAAC21pbGxpc2Vjb25kOR57qCYlSq/t5hTTb+rDTZfr3y77RoDWT9CrnpEZ8vQ=",validate=True)
        definition=wire[:-32]
        check._scalar_row_axis(check.base64.b64encode(wire).decode(),definition)
        for invalid in (definition,wire+b"\0",wire[:-1]+bytes((wire[-1]^1,)),b"x"+wire[1:]):
            with self.assertRaises(ValueError):check._scalar_row_axis(check.base64.b64encode(invalid).decode(),definition)
        changed=b"x"+definition[1:]
        with self.assertRaises(ValueError):check._scalar_row_axis(check.base64.b64encode(changed+check.digest(changed)).decode(),definition)

    def test_strict_operation_and_original_request_binding(self):
        dataset=list(hashlib.sha256(b"\1"+struct.pack(">QQ",40,50)+b"graph-v2").digest())
        q={"Dataset":dataset,"Phase":"update","Fact":3,"Ordinal":47}
        self.assertEqual(check.operation(self.spec,q),("update",3,47))
        for name,value in (("Ordinal",True),("Fact",True),("Ordinal",46),("Dataset",[0]*32)):
            with self.assertRaises(ValueError):check.operation(self.spec,{**q,name:value})
        admission=b"x"*32
        self.assertNotEqual(check.request(admission,"page",1),check.request(admission,"page",2))
        self.assertNotEqual(check.request(admission,"page",1),check.request(admission,"feed-owner",1))

    def test_transfer_operation_requires_explicit_native_dataset_version(self):
        spec={**self.spec,"version":2,"family":"transfer-retained-pins"}
        q={"Dataset":list(hashlib.sha256(b"\x0c"+struct.pack(">QQ",40,50)+b"graph-v2").digest()),"Phase":"update","Fact":1,"Ordinal":45}
        self.assertEqual(check.operation(spec,q,version=12),("update",1,45))
        with self.assertRaises(ValueError):check.operation(spec,q)
        with self.assertRaises(ValueError):check.operation(self.spec,q,version=12)
        with patch.object(check,"_scalar_attempts",return_value="legacy") as helper:
            self.assertEqual(check.attempts("directory",{"spec":self.spec},"spool"),"legacy")
            helper.assert_called_once_with("directory",{"spec":self.spec},"spool")

    def test_empty_and_half_zero_business_pages_refuse(self):
        # Canonical grammar guards use only inert codec fixtures. Genuine native
        # authority/process evidence is exclusively the separate workload parent.
        for wire in (b"GFP\1"+bytes(32),b"not-a-native-page"):
            with self.assertRaises(ValueError):check.page(wire)
        self.assertNotEqual(check.AGREEMENT,check.BASE)
        self.assertLess(check.frames({**self.spec,"facts":1_000_000})*260+65536,1<<39)

    def test_shared_fdb_answer_requires_authenticated_original_and_exact_eof(self):
        # An empty scratch arena cannot manufacture a typed old/current answer.
        # This exercises the actual normalization entrypoint's required joins.
        with tempfile.TemporaryDirectory() as path:
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                with self.assertRaises(ValueError):fdb.logical_answers({"Spec":self.spec,"Config":{}},spool)

    def test_owned_inventory_and_caller_counts_cannot_be_nominal(self):
        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ValueError):cell.hash_artifacts(Path(path),"columns")
        bad=copy.deepcopy(self.spec);bad["writers"]=1;bad["facts"]=20
        check.shape(bad)
        with self.assertRaises(ValueError):check.shape({**bad,"facts":0})

    def test_historical_point_joins_original_revision_and_presence(self):
        # Inert raw-row fixtures exercise only the join. Captured source rows
        # enter this namespace exclusively after logical() authenticates CDC.
        def cell(handle,revision,provenance,kind):
            return {"present":True,"null":False,"handle":handle,"revision":revision,"provenance":provenance,"kind":kind,"i64":handle if kind=="i64" else 0}
        p={"Fact":0,"Entity":{"id":7,"found":True,"effective":True,"kind":2,"type":"edge","mode":1,
            "source":1,"target":3,"effectiveLife":8,"labels":[],"presence":cell(8,11,4,"life"),"property":cell(5,11,4,"i64")}}
        with tempfile.TemporaryDirectory() as path:
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                for component,c in ((1,p["Entity"]["presence"]),(3,p["Entity"]["property"])):
                    after=dict(zip(check.column.CELL,(True,False,c["handle"],c["i64"],c["revision"],c["provenance"])))
                    spool.insert(b"row",u64(4)+u64(7)+bytes([component]),check.encoded({"After":after}))
                self.assertEqual(check.point(self.spec,p,5,spool),0)
                nil_labels=copy.deepcopy(p);nil_labels["Entity"]["labels"]=None
                self.assertEqual(check.point(self.spec,nil_labels,5,spool),0)
                for labels in (["phantom"],False,0,{}):
                    wrong_labels=copy.deepcopy(p);wrong_labels["Entity"]["labels"]=labels
                    with self.assertRaises(ValueError):check.point(self.spec,wrong_labels,5,spool)
                for name,key,value in (("property","revision",23),("presence","revision",23),("presence","provenance",44),("presence","handle",9)):
                    changed=copy.deepcopy(p);changed["Entity"][name][key]=value
                    with self.assertRaises(ValueError):check.point(self.spec,changed,5,spool)


if __name__=="__main__":unittest.main()
