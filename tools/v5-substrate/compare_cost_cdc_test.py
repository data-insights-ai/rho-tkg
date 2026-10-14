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

def raw_fixture(groups, result):
    """Synthetic v2 header/footer wrappers, genuine historical group bodies.

    This exercises byte/state validation, never a new SDK acceptance receipt.
    """
    checker=module('raw-cdc','compare-cost-cdc.py')
    result=copy.deepcopy(result);result.update(version=2,oracleFormat=checker.RAW_FORMAT)
    encoded=lambda body:base64.b64encode(body).decode()
    stored=lambda key,value:{'Key':encoded(key),'Value':encoded(checker.go_json(value))}
    config=result['Config'];history,current={},{};native_bytes=[0,0];versions=[0,0];generation=[0,0];sequences=[0,0]
    max_meta={k:(1<<64)-1 for k in ('Floor','Generation','Versions','Transactions','Certificates','Bytes','Groups')}
    for g in range(2):
        native_bytes[g]=len(checker.fdb_key(config,g,'meta','config'))+len(checker.go_json(config))+len(checker.fdb_key(config,g,'meta','state'))+len(checker.go_json(max_meta))
    cuts=[stored(checker.fdb_key(config,0,'cut',checker.go_json(result[k]).hex()),result[k]) for k in ('InitialCut','OldCut','FinalCut')]
    native_bytes[0]+=sum(len(base64.b64decode(r[k])) for r in cuts for k in ('Key','Value'))
    cdc=[]
    for group in groups:
        outcome=group['Outcome'];owner=outcome['Coordinator'];sequences[owner]+=1
        row={'Group':owner,'Key':encoded((f"g{owner}/{config['Graph'].encode().hex()}/changes/{outcome['Sequence']:016x}/"+outcome['Request'].encode().hex()).encode()),'Value':encoded(checker.go_json(group)),
             'IDKey':encoded(checker.fdb_key(config,owner,'id',outcome['ID'])),'IDValue':encoded(checker.go_json(outcome)),
             'RequestKey':encoded(checker.fdb_key(config,owner,'request',outcome['Request'])),'RequestValue':encoded(checker.go_json(outcome))}
        native_bytes[owner]+=sum(len(base64.b64decode(v)) for k,v in row.items() if k!='Group');cdc.append(row)
        for part in group['Tx']['Participants']:generation[part['Group']]+=1
        for write in group['Writes']:
            g,key,after=write['Group'],write['Key'],write['After'];versions[g]+=1
            hk=(f"g{g}/{config['Graph'].encode().hex()}/history/{after['Round']:016x}/"+key.encode().hex()).encode()
            ck=checker.fdb_key(config,g,'current',key);body=checker.go_json(after)
            history[(g,hk)]={'Group':g,'Key':encoded(hk),'Value':encoded(body)}
            if (g,ck) in current:native_bytes[g]-=len(ck)+len(base64.b64decode(current[(g,ck)]['Value']))
            current[(g,ck)]={'Group':g,'Key':encoded(ck),'Value':encoded(body)}
            native_bytes[g]+=len(hk)+len(body)+len(ck)+len(body)
    metadata=[{'Floor':result['FinalCut']['Round'],'Generation':generation[g],'Versions':versions[g],'Transactions':sequences[g],'Certificates':3 if g==0 else 0,'Bytes':native_bytes[g],'Groups':sequences[g]} for g in range(2)]
    header={'Type':'header','Format':checker.RAW_FORMAT,'Stream':'','Config':config,'InitialCut':result['InitialCut'],'OldCut':result['OldCut'],'FinalCut':result['FinalCut'],'Policy':{'PageRows':1,'PageBytes':65536},'Metadata':metadata,
            'ConfigRecords':[stored(checker.fdb_key(config,g,'meta','config'),config) for g in range(2)],'MetadataRecords':[stored(checker.fdb_key(config,g,'meta','state'),metadata[g]) for g in range(2)],'CutRecords':cuts}
    cdc.sort(key=lambda r:(lambda o:(o['Round'],o['Coordinator'],o['Request']))(json.loads(base64.b64decode(r['Value']))['Outcome']))
    streams={'history':[history[k] for k in sorted(history)],'current':[current[k] for k in sorted(current)],'cdc':cdc}
    result['StreamEnds']=[]
    for name in ('history','current','cdc'):
        counts=[sum(r['Group']==g for r in streams[name]) for g in range(2)]
        result['StreamEnds'].append({'Rows':counts,'Groups':result['FinalCut']['Groups'],'Metadata':metadata})
    return result,header,streams


def raw_native_bounds(native, facts):
    bounds=native_bounds(native);certificates=[]
    for partition in (3,8):
        index=max(r['Index'] for r in native if r['Partition']==partition and r['Original']['Provenance']<=3+facts)
        certificates.append(sealed(b'GSC\1'+bytes.fromhex(GRAPH)+struct.pack('>Q',partition)+b'\1'*96+b'\2'*32+struct.pack('>QQQQ',250,index+2,200,index)+native_image(partition,index)))
    return bounds|{'oldCut':{'Certificates':certificates}}


def native_observations(native,facts):
    out=[]
    for value in (5,6):
        for i in range(facts):
            create=next(r for r in native if r['CDCClass']=='logical' and r['Original']['Provenance']==4+i)
            logical=next(r for r in native if r['CDCClass']=='logical' and r['Original']['Provenance']==4+i+(facts if value==6 else 0))
            entity=copy.deepcopy(create['Entities'][0]);entity.update(effective=True,effectiveLife=entity['id']+1,presence=create['Changes'][0]['After'],property=logical['Changes'][-1]['After']);out.append(entity)
    return out


def column_fixture(directory, result, header, groups):
    """Explicit synthetic binary/control replay, never an actual column receipt.

    Reuses the retained group schema as input; there is no second graph model.
    Actual SDK composition is authored separately in the Go direct test.
    """
    c=module('column-replay','compare-cost-cdc.py');config=result['Config'];n=result['spec']['facts']
    enc=lambda b:base64.b64encode(b).decode()
    sha=lambda v:list(hashlib.sha256(c.go_json(v)).digest())
    prefix=f"g0/{config['Graph'].encode().hex()}/columns-v1/".encode()
    key=lambda name:prefix+name.encode()
    gkey=lambda name,kind:key('generation/'+name.encode().hex()+'/'+kind)
    rkey=lambda kind,name:key('retention-v1/'+kind+'/'+name.encode().hex())
    policy={'ChunkRows':32,'ChunkBytes':65536,'MaxChunks':1000000,'MaxGenerations':2,'MaxRetainedBytes':(64<<20)+(6+3*n)*(64<<10),'MaxWorkRows':3+2*n,'MaxWorkBytes':(64<<20)+(3+2*n)*(512<<10)}
    binding={'Version':1,'Layout':c.COLUMN_LAYOUT,'Schema':'node-presence;edge-life-bound;property-p:i64;canonical-original:v1','Config':config,'Scope':SCOPE,'Policy':policy}
    pb={'Version':1,'Columns':sha(binding),'Policy':{'MaxRecords':32,'MaxBytes':256<<10,'PageChunks':1,'PageBytes':128<<10,'MaxWorkChunks':1,'MaxWorkBytes':128<<10,'MaxOutputBytes':1024}}
    def stored(k,v):return {'Key':enc(k),'Value':enc(v if type(v) is bytes else c.go_json(v))}
    records=[stored(key('binding'),binding),stored(key('retention-v1/binding'),pb),*[stored(c.fdb_key(config,g,'meta','config'),config) for g in (0,1)],stored(c.fdb_key(config,0,'cut',c.go_json(result['InitialCut']).hex()),result['InitialCut'])]
    out=[{'Type':'header','Format':c.COLUMN_FORMAT,'SourceFormat':c.RAW_FORMAT,'ReferenceLayout':'native-logical-kv-reference','Config':config,'InitialCut':result['InitialCut'],'StoragePagePolicy':{'PageRows':32,'PageBytes':32*(65536+1024)},'Records':records}]
    manifests,progress,chunks,typed={},{},{},{}
    for name,limit,cut in [('old',3+n,result['OldCut']),('final',3+2*n,result['FinalCut'])]:
        selected=sorted(groups[:limit],key=lambda g:(g['Outcome']['Round'],g['Outcome']['Coordinator'],g['Outcome']['Request']))
        generation={'Version':1,'ID':name,'Binding':sha(binding),'Initial':result['InitialCut'],'Cut':cut}
        rows=[]
        for group in selected:
            command,m,o=group['Tx']['GraphChange'],group['Tx']['Lifecycle'],group['Outcome'];node,edge=command['Node'],command['Edge'];isnode=command['Kind']=='node-create'
            rows.append({'Kind':1 if isnode else 2 if command['Kind']=='edge-create' else 3,'Owner':node['Node'] if isnode else edge['ID'],'Life':node['Life'] if isnode else m['EdgeLife'],'SourceNode':edge['Source']['Node'],'SourceLife':edge['Source']['Life'],'TargetNode':edge['Target']['Node'],'TargetLife':edge['Target']['Life'],'Value':1 if isnode else edge['Value'],'ValueID':0 if isnode else m['Definition']['ID'],'Provenance':m['Provenance'],'Round':o['Round'],'Sequence':o['Sequence'],'Coordinator':o['Coordinator'],'Original':enc(c.go_json(group))})
        total=len(rows);arena=b'';offsets=[];lengths=[]
        for row in rows:
            raw=base64.b64decode(row['Original']);offsets.append(len(arena));lengths.append(len(raw));arena+=raw
        numbers=('Owner','Life','SourceNode','SourceLife','TargetNode','TargetLife','Value','ValueID','Provenance','Round','Sequence')
        body=b'RHOCLM01'+bytes(sha(binding))+bytes(sha(generation))+struct.pack('>Q',0)+bytes(32)+struct.pack('>H',total)+bytes(r['Kind'] for r in rows)+bytes(r['Coordinator'] for r in rows)+b''.join(struct.pack('>Q',r[name]%(1<<64)) for name in numbers for r in rows)+b''.join(struct.pack('>I',v) for v in offsets+lengths)+arena
        if name=='old':
            old=copy.deepcopy(result);old['FinalCut']=old['OldCut'];_,oldheader,_=raw_fixture(groups[:limit],old);metadata=oldheader['Metadata']
            duplicate=oldheader['CutRecords'][-1];metadata[0]['Bytes']-=sum(len(base64.b64decode(duplicate[k])) for k in ('Key','Value'));metadata[0]['Certificates']=2
        else:metadata=copy.deepcopy(header['Metadata'])
        chain=list(hashlib.sha256(bytes(32)+hashlib.sha256(body).digest()).digest())
        manifest={'Generation':generation,'Metadata':metadata,'Chunks':1,'Rows':total,'Bytes':len(gkey(name,'chunk/0000000000000000'))+len(body),'History':[m['Versions'] for m in metadata],'Chain':chain}
        manifests[name]=manifest;chunks[name]=body;typed[name]=rows
        progress[name]={'Generation':generation,'Metadata':metadata,'Next':1,'Rows':total,'Bytes':manifest['Bytes'],'WorkBytes':len(body)+sum(c.column_work(g,config) for g in selected),'Sequences':cut['Groups'],'History':manifest['History'],'Chain':chain,'Previous':selected[-1]['Outcome'],'EOF':True}
    token={'Version':1,'Request':'hold-old','Generation':'old','Columns':sha(binding),'Manifest':sha(manifests['old']),'Secret':[99]+[0]*31,'Cut':result['OldCut'],'FeedStart':result['OldCut']['Groups']}
    readers=[]
    for number in range(1,9):
        name='old' if number<=4 else 'final';secret=[number]+[0]*31
        owner={'Request':'read-'+bytes(secret[:16]).hex(),'Generation':name,'Secret':secret}
        readers.append({'Owner':owner,'Manifest':sha(manifests[name]),'PinRequest':'hold-old' if number==1 else '', 'PinToken':sha(token) if number==1 else [0]*32,'State':2})
    heldkey=key('read-owner/'+readers[0]['Owner']['Request'].encode().hex());terminal={key('binding'):c.go_json(binding),key('retention-v1/binding'):c.go_json(pb)}
    def stage(index):
        names=['old'] if index==0 else ['old','final'];raw={}
        charge=len(key('binding'))+len(c.go_json(binding))+len(key('ledger'))+256
        charge+=sum(8192+len(gkey(name,'progress'))+len(gkey(name,'manifest'))+len(gkey(name,'owners'))+4096+manifests[name]['Bytes'] for name in names)
        charge+=(1 if index<=2 else 8)*(len(heldkey)+4096)
        if index>=5:charge-=manifests['old']['Bytes']
        ledger={'Bytes':charge,'Generations':len(names),'Chunks':len(names)-(1 if index>=5 else 0)}
        pinbytes=len(key('retention-v1/binding'))+len(c.go_json(pb))+len(key('retention-v1/ledger'))+256+sum(len(k)+4096 for k in (rkey('reference','head'),rkey('reference-request','head-old'),rkey('pin','hold-old')))+(len(rkey('reference-request','head-final'))+4096 if index else 0)
        pl={'Records':5 if index==0 else 6,'Bytes':pinbytes,'ReservedRecords':1,'ReservedBytes':len(prefix)+len(b'retention-v1/reference-request/')+256+4096}
        raw[key('ledger')]=ledger;raw[key('retention-v1/ledger')]=pl
        metadata=manifests['old' if index==0 else 'final']['Metadata']
        for g in (0,1):raw[c.fdb_key(config,g,'meta','state')]=metadata[g]
        for name in names:
            m=manifests[name];collected=name=='old' and index>=5
            raw[gkey(name,'manifest')]=m;raw[gkey(name,'progress')]=progress[name]
            raw[gkey(name,'owners')]={'Manifest':sha(m),'State':3 if collected else 1,'References':1 if name=='final' or index==0 else 0,'Pins':1 if name=='old' and index<=1 else 0,'Readers':1 if name=='old' and index<=3 else 0,'Cursor':1 if collected else 0,'DeletedBytes':m['Bytes'] if collected else 0,'Chain':m['Chain'] if collected else [0]*32}
            raw[c.fdb_key(config,0,'cut',c.go_json(m['Generation']['Cut']).hex())]=m['Generation']['Cut']
        raw[heldkey]=readers[0]|{'State':1 if index<=3 else 2}
        raw[rkey('pin','hold-old')]={'Token':token,'Intent':sha({'Columns':token['Columns'],'Generation':'old','Cut':result['OldCut']}),'State':1 if index<=1 else 3,'TerminalRequest':'' if index<=1 else 'expire-old','TerminalKind':0 if index<=1 else 3}
        for name,request,version in [('old','head-old',1),('final','head-final',2)]:
            if name not in names:continue
            ref={'Name':'head','Generation':name,'Version':version,'Manifest':sha(manifests[name])}
            raw[rkey('reference-request',request)]={'Intent':sha({'Name':'head','Generation':name,'Expected':version-1}),'Reference':ref}
            if name==('old' if index==0 else 'final'):raw[rkey('reference','head')]=ref
        out.append({'Type':'stage','Name':('old-held','final-head','expired-held','reads-complete','old-closed','old-collected','terminal')[index],'Records':[stored(k,v) for k,v in raw.items()]})
        for k,v in raw.items():
            if k.startswith(prefix):terminal[k]=c.go_json(v)
        return ledger,pl
    stage(0);stage(1)
    out.extend([{'Type':'refusal','Name':'expired-new-open','Error':'fdb-column: retained generation or token expired'},{'Type':'refusal','Name':'held-old-collection','Error':'fdb-column: retained generation has live owners or references'}]);stage(2)
    for name in ('old','final'):
        m=manifests[name];body=chunks[name];cachefile='column-'+hashlib.sha256(c.go_json(m['Generation'])).hexdigest()+'-0000000000000000.bin';cachedir=directory/('column-cache-'+name);cachedir.mkdir();(cachedir/cachefile).write_bytes(body)
        out.append({'Type':'chunk','Generation':name,'CacheDirectory':cachedir.name,'CacheFile':cachefile,'Ordinal':0,'Length':len(body),'Rows':m['Rows'],'SHA256':list(hashlib.sha256(body).digest()),'Record':stored(gkey(name,'chunk/0000000000000000'),body)})
        out.append({'Type':'chunks-eof','Generation':name,'Manifest':m})
        for walk in (name,name+'-cache'):
            out.extend({'Type':'walk-row','Name':walk,'Generation':name,'Row':r} for r in typed[name]);out.append({'Type':'walk-eof','Name':walk,'Generation':name,'Cut':m['Generation']['Cut'],'Rows':m['Rows'],'Manifest':m})
        for owner in (7,7+2*(n-1),5):
            row=next((r for r in reversed(typed[name]) if r['Owner']==owner),None)
            out.append({'Type':'point','Generation':name,'Owner':owner,'Found':row is not None,'Row':row if row else {k:None if k=='Original' else 0 for k in typed[name][0]}})
    stage(3);stage(4)
    m=manifests['old'];guard={'Manifest':sha(m),'State':3,'References':0,'Pins':0,'Readers':0,'Cursor':1,'DeletedBytes':m['Bytes'],'Chain':m['Chain']}
    out.append({'Type':'collect','Budget':{'Chunks':1,'Bytes':128<<10,'OutputBytes':1024},'Result':{'Generation':'old','Cursor':1,'DeletedBytes':m['Bytes'],'WorkChunks':1,'WorkBytes':m['Bytes'],'Done':True},'Absent':enc(gkey('old','chunk/0000000000000000')),'Guard':stored(gkey('old','owners'),guard)})
    out.append({'Type':'refusal','Name':'retired-publication','Error':'fdb-column: retained generation or token expired'});stage(5)
    out.append({'Type':'refusal','Name':'final-reference-collection','Error':'fdb-column: retained generation has live owners or references'});ledger,pl=stage(6)
    terminal[gkey('final','chunk/0000000000000000')]=chunks['final']
    for record in readers:terminal[key('read-owner/'+record['Owner']['Request'].encode().hex())]=c.go_json(record)
    storage={k:0 for k in 'Records Bytes LiveChunkRecords LiveChunkBytes TombstoneRecords TombstoneBytes RetiredLineageRecords RetiredLineageBytes'.split()}
    for k,v in sorted(terminal.items()):
        out.append({'Type':'storage-row','Record':stored(k,v)});size=len(k)+len(v);storage['Records']+=1;storage['Bytes']+=size
        kind='LiveChunk' if k.startswith(gkey('final','chunk/')) else 'RetiredLineage' if k.startswith(gkey('old','')) else 'Tombstone' if k.startswith(key('read-owner/')) or k==rkey('pin','hold-old') else None
        if kind:storage[kind+'Records']+=1;storage[kind+'Bytes']+=size
    end={'Format':c.COLUMN_FORMAT,'SourceFormat':c.RAW_FORMAT,'ReferenceLayout':'native-logical-kv-reference','PayloadSHA256':[0]*32,'ArtifactSHA256':[0]*32,'Old':manifests['old'],'Final':manifests['final'],'Counts':{'Chunks':2,'WalkRows':2*(manifests['old']['Rows']+manifests['final']['Rows']),'Points':6,'Stages':7,'Collections':1},'Ledger':ledger,'PinLedger':pl,'Storage':storage}
    result.update(version=3,oracleFormat=c.COLUMN_FORMAT,Columns=end)
    return out


def seal_columns(directory, result, records):
    c=module('column-seal','compare-cost-cdc.py');payload=b''.join(c.go_json(r)+b'\n' for r in records);end=result['Columns'];end['PayloadSHA256']=list(hashlib.sha256(payload).digest());end['ArtifactSHA256']=[0]*32
    body=payload+c.go_json({'Type':'footer','End':end})+b'\n';end['ArtifactSHA256']=list(hashlib.sha256(body).digest());(directory/'fdb-cost-columns.jsonl').write_bytes(body)

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

    def raw_compare(self, mutate=None, policy=None, columns=None):
        checker=module('raw-cdc','compare-cost-cdc.py')
        groups,baseline=copy.deepcopy(self.groups),copy.deepcopy(self.result)
        if columns is not None:
            baseline['Config']['Graph']='graph-cost-columns-v1'
            for group in groups:
                group['Tx']['Graph']=baseline['Config']['Graph'];group['Outcome']['Digest']=list(hashlib.sha256(checker.go_json(group['Tx'])).digest())
        result,header,streams=raw_fixture(groups,baseline)
        if mutate:mutate(result,header,streams)
        directory=self.root/'raw';directory.mkdir(exist_ok=True)
        for name,records in streams.items():
            payload=checker.go_json(header|{'Stream':name})+b'\n'+b''.join(checker.go_json({'Type':'row','Row':r})+b'\n' for r in records)
            footer={'Type':'footer','Rows':[sum(r['Group']==g for r in records) for g in range(2)],'Groups':header['FinalCut']['Groups'],'Metadata':header['Metadata'],'PayloadSHA256':hashlib.sha256(payload).hexdigest()}
            (directory/f'fdb-cost-{name}.jsonl').write_bytes(payload+checker.go_json(footer)+b'\n')
        (directory/'fdb-cost-operation-groups.jsonl').write_bytes(b''.join(checker.go_json(g)+b'\n' for g in groups))
        samples=[]
        for group in groups:
            ordinal=group['Tx']['Lifecycle']['Provenance'];phase,fact,node,edge=checker.fixture(result['spec'],ordinal)
            samples.append({'phase':phase,'fact':fact,'cross':bool(edge and edge['Target']['Owner']),'ProposalNanos':1,'AckNanos':1,'VerificationNanos':1,'ProposalAttempts':1,'CommitAttempts':1,'Outcome':group['Outcome']})
        (directory/'fdb-cost-latency.jsonl').write_bytes(b''.join(checker.go_json(s)+b'\n' for s in samples))
        native=self.root/'native.jsonl';native.write_bytes(b''.join(checker.go_json(r)+b'\n' for r in self.native))
        observations=self.root/'observations.jsonl';observations.write_bytes(b''.join(checker.go_json(r)+b'\n' for r in native_observations(self.native,result['spec']['facts'])))
        if columns is not None:
            for name in ('old','final'):
                cache=directory/('column-cache-'+name)
                if cache.exists():
                    for file in cache.iterdir():file.unlink()
                    cache.rmdir()
            records=column_fixture(directory,result,header,groups)
            columns(result,records,directory)
            seal_columns(directory,result,records)
            (directory/'fdb-cost-result.json').write_bytes(checker.go_json(result)+b'\n')
        if policy is None:policy=checker.Policy(1<<20,16<<10,4096,512<<10)
        try:
            return checker.compare_streams(native,directory/'fdb-cost-cdc.jsonl',result['spec'],result['Config'],result['FinalCut'],raw_native_bounds(self.native,result['spec']['facts']),spool_directory=self.root,spool_policy=policy,raw_result=result,observations_path=observations)
        finally:
            self.assertEqual(list(self.root.glob('.cost-spool-*')),[])

    def test_raw_original_history_current_sparse_parity_and_setup_order(self):
        report=self.raw_compare()
        self.assertTrue(report['equal']);self.assertEqual(report['logicalGroups'],7)
        self.assertEqual(report['fdb']['history']['rows'],[10,5]);self.assertEqual(report['fdb']['current']['rows'],[6,3])
        self.assertEqual(report['oracleFormat'],'fdb-graph-cost-raw-v2')
        self.assertLessEqual(report['comparisonState']['peak_file_bytes'],1<<20)
        self.assertFalse(report['comparisonState']['cache_is_process_rss_limit'])
        self.assertIn('three-entry',report['fdb']['orderContract'])

    def test_column_binary_owned_read_cache_and_exact_terminal_census(self):
        report=self.raw_compare(columns=lambda r,rows,d:None)
        proof=report['columns']
        self.assertEqual(report['oracleFormat'],'fdb-graph-cost-columns-v1')
        self.assertEqual(proof['counts'],{'Chunks':2,'WalkRows':24,'Points':6,'Stages':7,'Collections':1})
        self.assertEqual(proof['storedLogicalBytes']['Records'],23)
        self.assertEqual(proof['storedLogicalBytes']['TombstoneRecords'],9)
        self.assertEqual(proof['storedLogicalBytes']['RetiredLineageRecords'],3)
        self.assertFalse(proof['nativeColumnsCostParity'])
        self.assertIn('not performed',proof['sourceKVGC'])
        directory=self.root/'raw';checker=module('column-controller','compare-cost-cdc.py')
        result=json.loads((directory/'fdb-cost-result.json').read_bytes());bounds=raw_native_bounds(self.native,2)
        native_result={'directory':str(self.root),'CDCFrom':bounds['from'],'CDCThrough':bounds['through'],'FinalCut':bounds['finalCut'],'OldCut':bounds['oldCut']}
        (self.root/'cdc.jsonl').write_bytes((self.root/'native.jsonl').read_bytes());(self.root/'native-cost-result.json').write_bytes(checker.go_json(native_result))
        hashes=lambda root:{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file()}
        native={'completedCell':True,'spec':result['spec'],'layout':'kv','result':native_result,'artifacts':hashes(self.root)}
        fdb={'completedCell':True,'spec':result['spec'],'layout':'columns','result':result,'FDBArtifacts':str(directory),'launchReceipt':{'sha256':hashes(directory)}}
        left,right=self.root/'native-receipt.json',self.root/'fdb-receipt.json';left.write_text(json.dumps(native));right.write_text(json.dumps(fdb))
        self.assertFalse(CELL.compare_cells(left,right)['nativeColumnsCostParity'])
        for mutate in (lambda f:f['launchReceipt']['sha256'].pop('fdb-cost-columns.jsonl'),lambda f:f.__setitem__('layout','kv')):
            bad=copy.deepcopy(fdb);mutate(bad);right.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):CELL.compare_cells(left,right)

    def test_column_rehashed_wrong_values_ownership_missing_eof_and_cache_refuse(self):
        def wrong_binary(result, rows, directory):
            chunk=next(r for r in rows if r['Type']=='chunk' and r['Generation']=='old');body=bytearray(base64.b64decode(chunk['Record']['Value']));n=chunk['Rows']
            struct.pack_into('>Q',body,114+2*n+(6*n+3)*8,55)
            chunk['Record']['Value']=base64.b64encode(body).decode();chunk['SHA256']=list(hashlib.sha256(body).digest())
            (directory/chunk['CacheDirectory']/chunk['CacheFile']).write_bytes(body)
        def wrong_point(result, rows, directory):
            old=next(r for r in rows if r['Type']=='point' and r['Generation']=='old' and r['Owner']==7)
            current=next(r for r in rows if r['Type']=='point' and r['Generation']=='final' and r['Owner']==7);old['Row']=copy.deepcopy(current['Row'])
        def wrong_held(result, rows, directory):
            stage=next(r for r in rows if r['Type']=='stage' and r['Name']=='expired-held')
            record=next(r for r in stage['Records'] if b'/read-owner/' in base64.b64decode(r['Key']));body=json.loads(base64.b64decode(record['Value']));body['State']=2
            record['Value']=base64.b64encode(module('column-bad-owner','compare-cost-cdc.py').go_json(body)).decode()
        def wrong_order(result, rows, directory):
            indexes=[i for i,r in enumerate(rows) if r['Type']=='walk-row' and r['Name']=='old'];rows[indexes[0]],rows[indexes[1]]=rows[indexes[1]],rows[indexes[0]]
        def missing_eof(result, rows, directory):rows.remove(next(r for r in rows if r['Type']=='walk-eof' and r['Name']=='old-cache'))
        def missing_tombstone(result, rows, directory):rows.remove(next(r for r in rows if r['Type']=='storage-row' and b'/read-owner/' in base64.b64decode(r['Record']['Key'])))
        def wrong_cache(result, rows, directory):
            chunk=next(r for r in rows if r['Type']=='chunk');path=directory/chunk['CacheDirectory']/chunk['CacheFile'];path.write_bytes(path.read_bytes()[:-1])
        for name,mutate in (('binary value',wrong_binary),('old/current substitution',wrong_point),('held ownership',wrong_held),('walk reorder',wrong_order),('missing EOF',missing_eof),('missing tombstone',missing_tombstone),('truncated cache',wrong_cache)):
            with self.subTest(name=name),self.assertRaises(ValueError):self.raw_compare(columns=mutate)

    def test_raw_equal_count_rehashed_wrong_rows_and_foreign_bindings_refuse(self):
        def body_change(row,key,value):
            body=json.loads(base64.b64decode(row['Value']));body[key]=value
            row['Value']=base64.b64encode(module('raw-cdc','compare-cost-cdc.py').go_json(body)).decode()
        mutations=(
            ('history value',lambda r,h,s:body_change(s['history'][-1],'Value',55)),
            ('current predecessor',lambda r,h,s:body_change(s['current'][0],'Version',99)),
            ('current duplicate',lambda r,h,s:s['current'].__setitem__(1,copy.deepcopy(s['current'][0]))),
            ('history reorder',lambda r,h,s:s['history'].reverse()),
            ('original substitution',lambda r,h,s:s['cdc'][0].__setitem__('IDValue',s['cdc'][-1]['IDValue'])),
            ('raw request key',lambda r,h,s:s['cdc'][0].__setitem__('RequestKey',base64.b64encode(b'foreign').decode())),
            ('raw enumeration order',lambda r,h,s:s['cdc'].__setitem__(slice(0,3),[s['cdc'][0],s['cdc'][2],s['cdc'][1]])),
            ('cut bytes',lambda r,h,s:h['CutRecords'][1].__setitem__('Value',h['CutRecords'][2]['Value'])),
            ('metadata',lambda r,h,s:h['Metadata'][0].__setitem__('Generation',99)),
            ('missing current',lambda r,h,s:s['current'].pop()),
            ('format relabel',lambda r,h,s:r.__setitem__('version',1)),
        )
        for name,mutate in mutations:
            with self.subTest(name=name),self.assertRaises(ValueError):self.raw_compare(mutate)

    def test_raw_missing_mixed_eof_and_private_spool_quota_refuse(self):
        checker=module('raw-cdc','compare-cost-cdc.py')
        self.raw_compare()
        directory=self.root/'raw';path=directory/'fdb-cost-current.jsonl';original=path.read_bytes()
        result,_,_=raw_fixture(self.groups,self.result)
        def compare():
            return checker.compare_streams(self.root/'native.jsonl',directory/'fdb-cost-cdc.jsonl',result['spec'],result['Config'],result['FinalCut'],raw_native_bounds(self.native,2),spool_directory=self.root,raw_result=result,observations_path=self.root/'observations.jsonl')
        for raw in (b''.join(original.splitlines(keepends=True)[:-1]),original+b'{}\n',original.replace(b'"Stream":"current"',b'"Stream":"history"',1)):
            path.write_bytes(raw)
            with self.assertRaises(ValueError):compare()
            self.assertEqual(list(self.root.glob('.cost-spool-*')),[])
        path.write_bytes(original)
        with self.assertRaisesRegex(ValueError,'capacity|quota'):
            self.raw_compare(policy=checker.Policy(8192,4096,1024,4096))

    def test_controller_raw_format_requires_all_bound_artifacts(self):
        self.raw_compare();checker=module('raw-cdc','compare-cost-cdc.py')
        result,_,_=raw_fixture(self.groups,self.result);directory=self.root/'raw'
        (directory/'fdb-cost-result.json').write_bytes(checker.go_json(result)+b'\n')
        bounds=raw_native_bounds(self.native,2)
        hashes=lambda root:{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file()}
        native={'completedCell':True,'spec':result['spec'],'result':{'directory':str(self.root),'CDCFrom':bounds['from'],'CDCThrough':bounds['through'],'FinalCut':bounds['finalCut'],'OldCut':bounds['oldCut']},'artifacts':hashes(self.root)}
        # The unchanged native filename contract is cdc.jsonl.
        (self.root/'cdc.jsonl').write_bytes((self.root/'native.jsonl').read_bytes())
        (self.root/'native-cost-result.json').write_text(json.dumps(native['result']));native['artifacts']=hashes(self.root)
        fdb={'completedCell':True,'spec':result['spec'],'result':result,'FDBArtifacts':str(directory),'launchReceipt':{'sha256':hashes(directory)}}
        left,right=self.root/'native-receipt.json',self.root/'fdb-receipt.json'
        left.write_text(json.dumps(native));right.write_text(json.dumps(fdb))
        report=CELL.compare_cells(left,right);self.assertEqual(report['oracleFormat'],checker.RAW_FORMAT)
        for missing in ('fdb-cost-history.jsonl','fdb-cost-operation-groups.jsonl','fdb-cost-latency.jsonl'):
            bad=copy.deepcopy(fdb);bad['launchReceipt']['sha256'].pop(missing);right.write_text(json.dumps(bad))
            with self.assertRaisesRegex(ValueError,'hash-bound'):CELL.compare_cells(left,right)
        bad=copy.deepcopy(fdb);bad['result'].pop('oracleFormat');right.write_text(json.dumps(bad))
        with self.assertRaisesRegex(ValueError,'format'):CELL.compare_cells(left,right)
        self.assertEqual(list(self.root.glob('.cost-spool-*')),[])

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
