#!/usr/bin/env python3
"""Authored finite oracle/controller guards, never captured selector evidence."""
import copy
import tempfile
from pathlib import Path
import unittest

import native_selectors_check as check
import native_selectors_cell as cell
import fdb_selectors_check as fdb
from comparison_spool import Policy, Spool, u64


class NativeSelectorsGuards(unittest.TestCase):
    def setUp(self):
        self.spec={"version":1,"family":"snapshot-selectors","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}

    def test_strict_dimensions_and_real_transport_boundary(self):
        check.shape(self.spec)
        for name in ("version","facts","crossPercent","writers"):
            with self.assertRaises(ValueError):check.shape({**self.spec,name:True})
        for name,delay in check.TRANSPORTS.items():
            self.assertEqual(cell.preflight(self.spec,name,1800,4096)["targetRTTMillis"],delay)
        cell.preflight(self.spec,"local",1800,4096,"paired")
        for name in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):cell.preflight(self.spec,name,1800,4096,"paired")
        with self.assertRaises(ValueError):cell.preflight(self.spec,"local",True,4096)
        with self.assertRaises(ValueError):cell.preflight(self.spec,"local",1801,4096,"paired")
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"must-not-exist"
            with self.assertRaises(ValueError):cell.execute({**self.spec,"family":"snapshot-to-feed"},"local",output,"/missing","/missing",180,4096,Policy(1<<20,65536,4096,check.LINE))
            self.assertFalse(output.exists())

    def test_explicit_semantic_and_canonical_cut_guards(self):
        self.assertNotEqual(check.AGREEMENT,check.scalar.AGREEMENT)
        self.assertNotEqual(check.AGREEMENT,check.scalar.BASE)
        for wire in (b"",b"GEQ\1"+bytes(100),b"not-a-cut"):
            with self.assertRaises(ValueError):check.cut(wire)
        for facts in (10_000,100_000,1_000_000):
            self.assertLess(check.frames({**self.spec,"facts":facts})*260+65536,1<<39)

    def test_exact_historical_revision_presence_and_phantom(self):
        def observed(handle,revision,provenance,kind):
            return {"present":True,"null":False,"handle":handle,"revision":revision,"provenance":provenance,"kind":kind,"i64":handle if kind=="i64" else 0}
        entity={"id":7,"found":True,"effective":True,"kind":2,"type":"edge","mode":1,"source":1,"target":3,"effectiveLife":8,"labels":[],
                "presence":observed(8,11,4,"life"),"property":observed(5,11,4,"i64")}
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(1<<20,65536,4096,check.LINE)) as spool:
                for component,c in ((1,entity["presence"]),(3,entity["property"])):
                    after=dict(zip(check.column.CELL,(True,False,c["handle"],c["i64"],c["revision"],c["provenance"])))
                    spool.insert(b"row",u64(4)+u64(7)+bytes([component]),check.encoded({"After":after}))
                check.point(self.spec,entity,5,spool)
                for name,key,value in (("property","revision",23),("presence","revision",23),("presence","provenance",44),("property","i64",6)):
                    changed=copy.deepcopy(entity);changed[name][key]=value
                    with self.assertRaises(ValueError):check.point(self.spec,changed,5,spool)
                phantom={"id":86,"found":False,"effective":False,"kind":0,"type":"","mode":0,"source":0,"target":0,"effectiveLife":0,
                         "labels":[],"presence":dict(check.EMPTY_CELL),"property":dict(check.EMPTY_CELL)}
                check.point(self.spec,phantom,6,spool)
                phantom["found"]=True
                with self.assertRaises(ValueError):check.point(self.spec,phantom,6,spool)

    def test_shared_answers_require_authenticated_full_source_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(1<<20,65536,4096,check.LINE)) as spool:
                with self.assertRaises(ValueError):fdb.logical_answers({"Spec":self.spec,"Config":{}},spool)
                with self.assertRaises(ValueError):check.logical_answers({"spec":self.spec},spool)
        self.assertEqual(len(check.STAGES),14)
        self.assertEqual(len(check.MODES),len(check.STAGES))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):cell.hash_artifacts(Path(directory),"columns")

    def test_fresh_claim_requires_exact_prior_installed_native_grant(self):
        # Inert codec/join fixture. Only grants() may populate this namespace
        # from actual AQP/APO/APG originals in workload verification.
        session=b"s"*40
        wire=b"\1"+u64(0)+u64(7)+session+u64(1)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(1<<20,65536,4096,check.LINE)) as spool:
                spool.insert(b"installed-grant",session+u64(1),check.encoded({"home":3,"first":1,"last":86,"index":9}))
                check.claim_binding(check.cursor(wire),(1,0,7),3,10,spool)
                for body,expected,home,index in ((b"\2"+wire[1:],(1,0,7),3,10),(wire,(1,0,7),8,10),(wire,(1,0,7),3,9),
                                                 (wire[:17]+b"x"*40+wire[-8:],(1,0,7),3,10),(b"\1"+u64(0)+u64(87)+session+u64(1),(1,0,87),3,10)):
                    with self.assertRaises(ValueError):check.claim_binding(check.cursor(body),expected,home,index,spool)

    def test_whole_observation_percentiles_are_not_page_sums(self):
        samples=[]
        for stage in check.STAGES:
            scoped="scope-" in stage
            samples.append({"name":stage,"OpenNanos":101,"ScanNanos":0 if scoped else 509,"PointsNanos":37,
                            "ScopeRefusalNanos":19 if scoped else 0,"Rows":0 if scoped else 40})
        result=check.selector_timings(samples)
        scans=[row for row in result["nearestRank"] if row["call"]=="Scan"]
        self.assertTrue(scans)
        self.assertTrue(all(row["p50Nanos"]==row["p95Nanos"]==row["p99Nanos"]==509 for row in scans))


if __name__=="__main__":unittest.main()
