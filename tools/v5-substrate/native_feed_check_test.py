#!/usr/bin/env python3
"""Compact authored source guards, not captured native feed evidence."""
import copy
import hashlib
import struct
import tempfile
from pathlib import Path
import unittest

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

    def test_strict_operation_and_original_request_binding(self):
        dataset=list(hashlib.sha256(b"\1"+struct.pack(">QQ",40,50)+b"graph-v2").digest())
        q={"Dataset":dataset,"Phase":"update","Fact":3,"Ordinal":47}
        self.assertEqual(check.operation(self.spec,q),("update",3,47))
        for name,value in (("Ordinal",True),("Fact",True),("Ordinal",46),("Dataset",[0]*32)):
            with self.assertRaises(ValueError):check.operation(self.spec,{**q,name:value})
        admission=b"x"*32
        self.assertNotEqual(check.request(admission,"page",1),check.request(admission,"page",2))
        self.assertNotEqual(check.request(admission,"page",1),check.request(admission,"feed-owner",1))

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


if __name__=="__main__":unittest.main()
