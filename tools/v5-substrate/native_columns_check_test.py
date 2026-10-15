#!/usr/bin/env python3
"""Direct fresh move codec guards; inert fixtures never establish native authority."""
import hashlib
import struct
import tempfile
import unittest

import native_feed_check as scalar
from comparison_spool import Spool,Policy


class NativeMovedColumnGuards(unittest.TestCase):
    def status(self,result=1,failed_phase=0,failed_sequence=0,failed_hash=bytes(32)):
        body=b"GMO\3"+bytes.fromhex(scalar.raw.GRAPH)+struct.pack(">Q",8)+b"m"*16+b"i"*32+bytes((4,result))
        body+=struct.pack(">6Q",11,1,2,1,3,4)+b"d"*32+bytes((failed_phase,))+struct.pack(">Q",failed_sequence)+failed_hash
        return body+hashlib.sha256(body).digest()

    def test_pooled_status_keeps_exact_attempted_refusal_and_rejects_old_formats(self):
        c=scalar.column
        self.assertEqual(c.pooled_move_status(scalar.raw,self.status())["index"],11)
        refused=c.pooled_move_status(scalar.raw,self.status(5,4,2,b"h"*32))
        self.assertEqual((refused["phase"],refused["sequence"],refused["failedPhase"],refused["failedSequence"]),(4,1,4,2))
        for wire in (b"",self.status()[:-1],self.status(5),self.status(5,4,1,b"h"*32),self.status(1,4,2,b"h"*32)):
            with self.assertRaises(ValueError):c.pooled_move_status(scalar.raw,wire)
        body=b"GMO\2"+self.status()[4:-32]
        with self.assertRaises(ValueError):c.pooled_move_status(scalar.raw,body+hashlib.sha256(body).digest())

    def test_move_column_evidence_cannot_use_current_status_or_empty_registration(self):
        frame={"Partition":8,"Index":11,"Outcome":scalar.base64.b64encode(self.status()).decode(),"Rows":[],"Registration":""}
        with self.assertRaises(ValueError):scalar.column.pooled_move_frame(scalar.raw,frame,bytes(172),b"s"*32,None)

    def test_move_native_control_requires_exact_original_command_and_install_index(self):
        def sealed(body):return body+hashlib.sha256(body).digest()
        def field(body):return struct.pack(">I",len(body))+body
        u=scalar.struct.pack
        graph=bytes.fromhex(scalar.raw.GRAPH);semantic=b"s"*32
        source=graph+u(">Q",3)+b"g"*16+u(">QQ",1,1)+b"d"*32+semantic
        target=source[:16]+u(">Q",8)+source[24:]
        publication=sealed(b"RPB\1"+graph+u(">5Q",1,1,100,3,1)+b"c"*32+u(">QQ",3,2))
        request=b"r"*16;intent=sealed(b"GMI\3"+request+source+target+field(publication)+u(">QIII3Q",1,100,32,65536,1<<20,1<<20,1<<20))
        parsed=scalar.column.pooled_move_intent(scalar.raw,intent,semantic);move=parsed["move"]
        body=b"GMO\3"+graph+u(">Q",8)+move+hashlib.sha256(intent).digest()+bytes((4,1))+u(">6Q",11,1,2,1,3,4)+b"d"*32+b"\0"+u(">Q",0)+bytes(32)
        status=sealed(body);command=sealed(b"GMQ\3"+target+b"\4"+u(">Q",1)+field(intent)+field(b"\0")+u(">I",0))
        ik=b"GMI\1"+graph+u(">Q",8)+move;vk=b"GMV\1"+graph+u(">Q",8)+move+u(">I",0x20000001)
        def record(key,index,value):return field(key)+u(">Q",index)+field(value)
        native=sealed(b"NCM1"+record(ik,5,intent)+record(vk,11,hashlib.sha256(command).digest()+status)+record(ik,5,intent))
        encode=lambda wire:scalar.base64.b64encode(wire).decode()
        frame={"Partition":8,"Index":11,"Outcome":encode(status),"CDC":"","Registration":encode(native),"Rows":[],"Root":[0]*172}
        with tempfile.TemporaryDirectory() as tmp:
            with Spool(tmp,Policy(1<<20,65536,4096,scalar.LINE)) as spool:
                spool.insert(b"move-command",u(">QQ",8,11),command)
                actual,_=scalar.column.pooled_move_frame(scalar.raw,frame,bytes(172),semantic,spool)
                self.assertEqual(actual["index"],11)
                wrong=sealed(b"NCM1"+record(ik,5,intent)+record(vk,12,hashlib.sha256(command).digest()+status)+record(ik,5,intent))
                frame["Registration"]=encode(wrong)
                with self.assertRaises(ValueError):scalar.column.pooled_move_frame(scalar.raw,frame,bytes(172),semantic,spool)
        frame["Rows"]=[{}]
        with self.assertRaises(ValueError):scalar.column.pooled_move_frame(scalar.raw,frame,bytes(172),b"s"*32,None)


if __name__=="__main__":unittest.main()
