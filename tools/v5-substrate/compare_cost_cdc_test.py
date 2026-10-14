"""Bounded replay guards; synthetic native metadata, genuine retained FDB groups."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
GRAPH = '01000000000000000000000000000000'
SCOPE = {'kind':'all','axisProfile':'Q','axisVersion':1,'canonicalUnit':'millisecond','reference':'posix-unix-epoch:1970-01-01T00:00:00Z:v1'}

def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    out = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(out)
    return out

CELL = module('cell', 'cost-cell.py')

def sealed(body):
    return base64.b64encode(body + hashlib.sha256(body).digest()).decode()

def absent():
    return {'present':False,'null':False,'handle':0,'revision':0,'provenance':0,'kind':'absent','i64':0}

def native_bounds(rows):
    certificates=[]
    for partition in (3,8):
        last=max((r for r in rows if r['Partition']==partition),key=lambda r:r['Index'])
        index=last['Index']
        image=native_image(partition,index)
        if last['Original']['Kind']=='no-application-output':image=native_image(partition,index-1)
        certificates.append(sealed(b'GSC\1'+bytes.fromhex(GRAPH)+struct.pack('>Q',partition)+b'\1'*96+b'\2'*32+struct.pack('>QQQQ',300,index+2,200,index)+image))
    return {'from':[min(r['Index'] for r in rows if r['Partition']==p)-1 for p in (3,8)],
            'through':[max(r['Index'] for r in rows if r['Partition']==p) for p in (3,8)],
            'finalCut':{'Certificates':certificates}}

def native_image(partition,index):
    return bytes([partition])*164+index.to_bytes(8,'big')

def fdb_replay(cross):
    bundle = ROOT.parents[1] / 'tasks/evidence' / ('v2-fdb-cost-final-source-20261013' if cross else 'v2-fdb-cost-profile-20261013')
    directory = 'actual-sdk' if cross else 'local-functional'
    with tarfile.open(bundle / 'raw.tar.gz') as archive:
        groups = [json.loads(line) for line in archive.extractfile(directory + '/fdb-cost-cdc.jsonl')]
        result = json.load(archive.extractfile(directory + '/fdb-cost-result.json'))
        views = {n:json.load(archive.extractfile(directory + f'/fdb-cost-view-{n}.json')) for n in (5,6)}
    return groups, result, views

def native_fixture(groups):
    """Explicit test-only decoded originals; this is not a native runtime receipt."""
    rows, cells, indexes = [], {}, {3:10,8:20}
    for group in groups:
        tx, command = group['Tx'], group['Tx']['GraphChange']
        provenance = tx['Lifecycle']['Provenance']
        coordinator = 8 if command['Kind']=='node-create' and command['Node']['Owner']==1 else 3
        parts = [3 if p['Group']==0 else 8 for p in tx['Participants']]
        request = provenance.to_bytes(16,'big')
        namespace = bytes.fromhex(GRAPH) + struct.pack('>Q', coordinator)
        cross = len(parts)==2
        transaction = hashlib.sha256(b'rho-tkg:cross-transaction-id:v1\0'+namespace+request).digest()[:16] if cross else request
        digest = hashlib.sha256(b'fixture-original'+request).digest()
        round_ = 100 + provenance
        changes, entities, lives, values = [], [], [], []
        if command['Kind']=='node-create':
            owner, life = command['Node']['Node'], command['Node']['Life']
            components = [(owner,0,1,'',life,'life',0)]
            entities = [{'id':owner,'found':True,'kind':1,'source':0,'target':0,'mode':0,'type':'','presence':absent()|{'kind':''},'effective':False,'effectiveLife':0,'labels':None,'property':absent()|{'kind':''}}]
            lives = [{'owner':owner,'life':life,'found':True,'sourceLife':0,'targetLife':0}]
        else:
            edge=command['Edge']; owner,life=edge['ID'],tx['Lifecycle']['EdgeLife']; value=edge['Value']
            components=[]
            if command['Kind']=='edge-create':
                components.append((owner,0,1,'',life,'life',0))
                entities=[{'id':owner,'found':True,'kind':2,'source':edge['Source']['Node'],'target':edge['Target']['Node'],'mode':1,'type':'edge','presence':absent()|{'kind':''},'effective':False,'effectiveLife':0,'labels':None,'property':absent()|{'kind':''}}]
                lives=[{'owner':owner,'life':life,'found':True,'sourceLife':edge['Source']['Life'],'targetLife':edge['Target']['Life']}]
            components.append((owner,life,3,'p',value,'i64',value))
            if command['Kind']=='edge-create' and owner==7 or command['Kind']=='edge-update' and owner==7:
                values=[{'ID':value,'Kind':'i64','I64':value}]
        for owner,life,component,name,handle,kind,value in components:
            key=(owner,life,component,name)
            after={'present':True,'null':False,'handle':handle,'revision':round_,'provenance':provenance,'kind':kind,'i64':value}
            changes.append({'Owner':owner,'Life':life,'Component':component,'Name':name,'AllScope':SCOPE,'Scope':base64.b64encode(b'fixture-encoded-all-scope').decode(),'Before':copy.deepcopy(cells.get(key,absent())),'After':after})
            cells[key]=after
        for partition in parts:
            phases = (1,2,3,4) if cross and partition==coordinator else (2,4) if cross else (0,)
            for phase in phases:
                indexes[partition]+=1; index=indexes[partition]
                logical = not cross or phase==4 and partition==coordinator
                cdc=base64.b64encode(b'fixture-canonical-decoded-envelope').decode() if not cross or phase==4 else ''
                original={'Kind':'cross' if cross else 'local-graph','Graph':GRAPH,'Request':request.hex(),'Transaction':transaction.hex(),'Digest':digest.hex(),'Partition':partition,'Index':index,'Coordinator':coordinator,'Provenance':provenance,'Round':round_ if phase in (0,3,4) else 0,'Phase':phase,'Commit':phase in (0,3,4),'Reason':0,'Disposition':0 if cross else 1,'Participants':parts}
                if cross:
                    body=b'GCS\1'+bytes.fromhex(GRAPH)+struct.pack('>Q',partition)+transaction+digest+bytes([phase])+struct.pack('>QQQ',index,0,original['Round'])+bytes([3 if original['Commit'] else 1,0])
                else:
                    body=b'GRO\4\6'+bytes.fromhex(GRAPH)+struct.pack('>Q',partition)+request+digest+struct.pack('>Q',index)+b'\1\0\0'
                outcome=sealed(body)
                row={'Partition':partition,'Index':index,'Outcome':outcome,'CDC':cdc,'Changes':changes if logical else None,'Entities':entities if logical else None,'Lives':lives if logical else None,'Values':values if logical else None,'Original':original,'CDCClass':'logical' if logical else 'participant-empty' if cdc else 'protocol-empty','RawHashes':{'OutcomeSHA256':hashlib.sha256(base64.b64decode(outcome)).hexdigest(),'CDCSHA256':hashlib.sha256(base64.b64decode(cdc)).hexdigest()},'Logical':{'Entities':len(entities) if logical else 0,'Lives':len(lives) if logical else 0,'Values':len(values) if logical else 0,'Components':len(changes) if logical else 0,'Schemas':0}}
                row['SourceBoundary']={'Index':index,'Generation':1,'ImageSHA256':hashlib.sha256(native_image(partition,index)).hexdigest()}
                rows.append(row)
    # Genuine batched exporter orders records by partition within ≤16 commands.
    return sorted(rows,key=lambda r:(r['Partition'],r['Index']))

class Replay(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.groups,self.result,self.views=fdb_replay(True)
        self.native=native_fixture(self.groups)

    def tearDown(self):
        self.tmp.cleanup()

    def streams(self,native=None,fdb=None):
        paths=[]
        for name,rows in [('native.jsonl',self.native if native is None else native),('fdb.jsonl',self.groups if fdb is None else fdb)]:
            path=self.root/name; path.write_text(''.join(json.dumps(row,separators=(',',':'))+'\n' for row in rows)); paths.append(path)
        return paths

    def compare(self,native=None,fdb=None):
        checker=module('cdc','compare-cost-cdc.py')
        return checker.compare_streams(*self.streams(native,fdb),self.result['spec'],self.result['Config'],self.result['FinalCut'],native_bounds(self.native))

    def test_actual_fdb_cross_and_synthetic_native_groups_equal(self):
        result=self.compare();self.assertTrue(result['equal']);self.assertEqual(result['logicalGroups'],7);self.assertEqual(result['componentChanges'],9)

    def test_actual_fdb_local_and_synthetic_native_groups_equal(self):
        groups,result,_=fdb_replay(False)
        checker=module('cdc','compare-cost-cdc.py')
        native=native_fixture(groups)
        report=checker.compare_streams(*self.streams(native,groups),result['spec'],result['Config'],result['FinalCut'],native_bounds(native))
        self.assertEqual(report['logicalGroups'],7)

    def test_native_equal_count_wrong_effects_declarations_and_originals(self):
        def logical(rows, provenance):
            return next(r for r in rows if r['CDCClass']=='logical' and r['Original']['Provenance']==provenance)
        mutations = (
            ('after value', lambda r: logical(r, 6)['Changes'][0]['After'].__setitem__('i64', 55)),
            ('before value', lambda r: logical(r, 6)['Changes'][0]['Before'].__setitem__('i64', 55)),
            ('before revision', lambda r: logical(r, 6)['Changes'][0]['Before'].__setitem__('revision', 999)),
            ('after provenance', lambda r: logical(r, 6)['Changes'][0]['After'].__setitem__('provenance', 7)),
            ('endpoint', lambda r: logical(r, 4)['Entities'][0].__setitem__('target', 3)),
            ('life binding', lambda r: logical(r, 4)['Lives'][0].__setitem__('targetLife', 4)),
            ('typed definition', lambda r: logical(r, 4)['Values'][0].__setitem__('I64', 9)),
            ('axis', lambda r: logical(r, 4)['Changes'][0]['AllScope'].__setitem__('axisProfile', 'Z')),
            ('request', lambda r: logical(r, 4)['Original'].__setitem__('Request', 'f'*32)),
            ('local request', lambda r: logical(r, 1)['Original'].update(Request='f'*32,Transaction='f'*32)),
            ('digest', lambda r: logical(r, 4)['Original'].__setitem__('Digest', 'f'*64)),
            ('raw hash', lambda r: logical(r, 4)['RawHashes'].__setitem__('CDCSHA256', 'f'*64)),
            ('decoded count', lambda r: logical(r, 4)['Logical'].__setitem__('Components', 1)),
            ('schema', lambda r: logical(r, 4)['Logical'].__setitem__('Schemas', 1)),
            ('unknown field', lambda r: logical(r, 4).__setitem__('Extra', 1)),
            ('unknown component', lambda r: logical(r, 4)['Changes'][0].__setitem__('Component', 44)),
        )
        for name, mutate in mutations:
            with self.subTest(name=name):
                bad=copy.deepcopy(self.native); mutate(bad)
                with self.assertRaises(ValueError): self.compare(native=bad)

    def test_reused_original_request_with_coherent_local_outcome_refuses(self):
        bad=copy.deepcopy(self.native)
        first=next(r for r in bad if r['Original']['Provenance']==1)
        second=next(r for r in bad if r['Original']['Provenance']==2)
        request=first['Original']['Request']
        second['Original'].update(Request=request,Transaction=request)
        body=base64.b64decode(second['Outcome'])[:-32]
        second['Outcome']=sealed(body[:29]+bytes.fromhex(request)+body[45:])
        second['RawHashes']['OutcomeSHA256']=hashlib.sha256(base64.b64decode(second['Outcome'])).hexdigest()
        with self.assertRaisesRegex(ValueError,'original request/transaction bijection'):
            self.compare(native=bad)

    def test_missing_duplicate_reordered_native_records_and_empty_classes(self):
        empty=next(i for i,r in enumerate(self.native) if r['CDCClass']=='protocol-empty')
        variants = [self.native[1:], self.native[:-1],
                    self.native[:empty]+self.native[empty+1:],
                    self.native[:empty]+[self.native[empty]]+self.native[empty:],
                    [self.native[1],self.native[0]]+self.native[2:]]
        for rows in variants:
            with self.subTest(shape=len(rows)):
                with self.assertRaises(ValueError): self.compare(native=rows)
        for kind, classification in [('unknown','read-control'),('cross','unknown'),('raft-noop','read-control')]:
            bad=copy.deepcopy(self.native)
            bad[empty]['Original']['Kind']=kind;bad[empty]['CDCClass']=classification
            with self.assertRaises(ValueError): self.compare(native=bad)

    def test_retained_empty_and_read_control_need_named_original_and_prefix(self):
        checker=module('cdc','compare-cost-cdc.py')
        bound=native_bounds(self.native)
        index=bound['through'][0]+1
        original={'Kind':'no-application-output','Graph':GRAPH,'Request':'','Transaction':'','Digest':'','Partition':3,'Index':index,'Coordinator':0,'Provenance':0,'Round':0,'Phase':0,'Commit':False,'Reason':0,'Disposition':0,'Participants':[]}
        row={'Partition':3,'Index':index,'Outcome':'','CDC':'','Changes':None,'Entities':None,'Lives':None,'Values':None,'Original':original,'CDCClass':'read-control','RawHashes':{'OutcomeSHA256':hashlib.sha256(b'').hexdigest(),'CDCSHA256':hashlib.sha256(b'').hexdigest()},'Logical':{'Entities':0,'Lives':0,'Values':0,'Components':0,'Schemas':0},'SourceBoundary':{'Index':index,'Generation':1,'ImageSHA256':hashlib.sha256(native_image(3,index-1)).hexdigest()}}
        extended=self.native+[row];bound=native_bounds(extended)
        report=checker.compare_streams(*self.streams(extended),self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)
        self.assertEqual(report['native']['controls'],1)
        # Even loss of a zero-output final record is caught by actual prefix anchors.
        with self.assertRaisesRegex(ValueError,'retained prefix'):
            checker.compare_streams(*self.streams(self.native),self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)
        control=copy.deepcopy(row)
        control['Original'].update(Kind='read-control',Round=200,Phase=1,Disposition=1,Commit=True)
        control['Outcome']=sealed(b'GSO\1'+struct.pack('>BBQQ',1,1,200,index))
        control['RawHashes']['OutcomeSHA256']=hashlib.sha256(base64.b64decode(control['Outcome'])).hexdigest()
        control['SourceBoundary']['ImageSHA256']=hashlib.sha256(native_image(3,index)).hexdigest()
        bound=native_bounds(self.native+[control])
        checker.compare_streams(*self.streams(self.native+[control]),self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)
        control['Original']['Phase']=2
        with self.assertRaisesRegex(ValueError,'read-control original'):
            checker.compare_streams(*self.streams(self.native+[control]),self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)

    def test_fdb_equal_count_wrong_originals_effects_and_physical_postings(self):
        mutations=(
            ('after',lambda r:r[-1]['Writes'][0]['After'].__setitem__('Value',55)),
            ('before',lambda r:r[-1]['Writes'][0]['Before'].__setitem__('Value',55)),
            ('posting',lambda r:r[-1]['Writes'][-1]['After'].__setitem__('Value',55)),
            ('version',lambda r:r[-1]['Writes'][0]['After'].__setitem__('Version',1)),
            ('request',lambda r:r[-1]['Tx'].__setitem__('Request','different-original')),
            ('tx',lambda r:r[-1]['Tx'].__setitem__('ID','different-original')),
            ('provenance',lambda r:r[-1]['Tx']['Lifecycle'].__setitem__('Provenance',90)),
            ('epoch',lambda r:r[-1]['Tx']['Participants'][1].__setitem__('Epoch',99)),
            ('missing reads',lambda r:r[-1]['Tx']['Participants'][0]['Reads'].pop()),
            ('wrong owner',lambda r:r[-1]['Writes'][-1].__setitem__('Group',0)),
            ('unknown row',lambda r:r[-1].__setitem__('unknown',1)),
            ('empty group',lambda r:r[-1].__setitem__('Writes',[])),
            ('float value',lambda r:r[-1]['Writes'][0]['After'].__setitem__('Value',6.0)),
            ('bool version',lambda r:r[0]['Writes'][0]['After'].__setitem__('Version',True)),
            ('bool coordinator',lambda r:r[0]['Tx'].__setitem__('Coordinator',False)),
            ('float provenance',lambda r:r[0]['Tx']['Lifecycle'].__setitem__('Provenance',1.0)),
        )
        for name,mutate in mutations:
            with self.subTest(name=name):
                bad=copy.deepcopy(self.groups);mutate(bad)
                # Rehash edited commands: semantic guards must still refuse.
                for group in bad:
                    group['Outcome']['Digest']=list(hashlib.sha256(module('cdc','compare-cost-cdc.py').go_json(group['Tx'])).digest())
                with self.assertRaises(ValueError):self.compare(fdb=bad)

    def test_missing_duplicate_and_reordered_fdb_groups(self):
        for groups in (self.groups[:-1],self.groups+[self.groups[-1]],
                       self.groups[:-1]+[self.groups[-2]],
                       self.groups[:3]+[self.groups[4],self.groups[3]]+self.groups[5:]):
            with self.assertRaises(ValueError):self.compare(fdb=groups)

    def test_bounded_lines_duplicate_json_nonfinite_and_unknown_fields(self):
        checker=module('cdc','compare-cost-cdc.py')
        native,fdb=self.streams()
        bound=native_bounds(self.native)
        for raw in (b'{"Partition":3,"Partition":8}\n',b'{}',b'x'*(checker.LINE_BYTES+1)+b'\n',b'{"Partition":NaN}\n'):
            native.write_bytes(raw)
            with self.assertRaises(ValueError):checker.compare_streams(native,fdb,self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)

    def test_controller_refuses_equal_count_wrong_cdc(self):
        # Red against the former controller: complete point views remain correct.
        native_root=self.root/'native';fdb_root=self.root/'fdb';native_root.mkdir();fdb_root.mkdir()
        observations=[]
        for value in (5,6):
            for number,edge in enumerate(self.views[value]['View']['VisibleEdges']):
                observations.append({'found':True,'effective':True,'id':edge['ID'],'source':edge['Source']['Node'],'target':edge['Target']['Node'],'effectiveLife':edge['ID']+1,'property':{'present':True,'null':False,'kind':'i64','i64':value,'handle':value,'revision':1,'provenance':4+number+(2 if value==6 else 0)}})
            (fdb_root/f'fdb-cost-view-{value}.json').write_text(json.dumps(self.views[value]))
        (native_root/'observations.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in observations))
        (native_root/'cdc.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in self.native))
        bad=copy.deepcopy(self.groups);bad[-1]['Writes'][0]['After']['Value']=55
        (fdb_root/'fdb-cost-cdc.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in bad))
        hashes=lambda root:{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
        bounds=native_bounds(self.native)
        native={'completedCell':True,'spec':self.result['spec'],'result':{'directory':str(native_root),'CDCFrom':bounds['from'],'CDCThrough':bounds['through'],'FinalCut':bounds['finalCut']},'artifacts':hashes(native_root)}
        fdb={'completedCell':True,'spec':self.result['spec'],'result':self.result,'FDBArtifacts':str(fdb_root),'launchReceipt':{'sha256':hashes(fdb_root)}}
        receipts=[]
        for name,data in [('native',native),('fdb',fdb)]:
            p=self.root/(name+'.json');p.write_text(json.dumps(data));receipts.append(p)
        with self.assertRaises(ValueError):CELL.compare_cells(*receipts)
        # Restoring the genuine FDB group stream makes the same retained point
        # views and strict synthetic native metadata compare successfully.
        (fdb_root/'fdb-cost-cdc.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in self.groups))
        fdb['launchReceipt']['sha256']=hashes(fdb_root)
        receipts[1].write_text(json.dumps(fdb))
        report=CELL.compare_cells(*receipts)
        self.assertEqual(report['completeLogicalCDC']['logicalGroups'],7)
        self.assertEqual(report['exactLogicalComponentChanges'],9)
        # A stream outside the sealed artifact inventory has no authority.
        fdb['launchReceipt']['sha256'].pop('fdb-cost-cdc.jsonl')
        receipts[1].write_text(json.dumps(fdb))
        with self.assertRaisesRegex(ValueError,'hash-bound'):CELL.compare_cells(*receipts)

    def test_certified_prefix_and_empty_root_proofs_cannot_be_removed(self):
        checker=module('cdc','compare-cost-cdc.py')
        native,fdb=self.streams()
        for change in (
                lambda b:b['through'].__setitem__(0,b['through'][0]-1),
                lambda b:b.pop('finalCut'),
                lambda b:b['finalCut'].__setitem__('Certificates',[])):
            bound=native_bounds(self.native);change(bound)
            with self.assertRaises(ValueError):checker.compare_streams(native,fdb,self.result['spec'],self.result['Config'],self.result['FinalCut'],bound)
        for change in (
                lambda r:r[-1].pop('SourceBoundary'),
                lambda r:r[-1]['SourceBoundary'].__setitem__('ImageSHA256','0'*64),
                lambda r:r[-1]['SourceBoundary'].__setitem__('Index',0)):
            bad=copy.deepcopy(self.native);change(bad)
            with self.assertRaises(ValueError):self.compare(native=bad)

if __name__=='__main__':unittest.main()
