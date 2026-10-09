"""External bridge to unchanged oracle.serial_witness; no Evidence audit."""
from pathlib import Path
import argparse,copy,json,sys,hashlib,importlib.util,tempfile
ROOT = Path(__file__).resolve().parent
ORACLE = ROOT.parent / "protocol" / "oracle.py"
ORACLE_SHA256 = "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a"
if hashlib.sha256(ORACLE.read_bytes()).hexdigest() != ORACLE_SHA256:
    raise RuntimeError("canonical protocol oracle hash changed; review before running this bridge")
_spec = importlib.util.spec_from_file_location("reviewed_host_mathematical_oracle", ORACLE)
_oracle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_oracle)
serial_witness = _oracle.serial_witness

def gname(g):return ('A','B')[g]
def synth(g,key):return gname(g)+'/'+key

def validate_bindings(c):
 raw=c['raw_actual_evidence']
 actual_reads={(json.dumps(r['read_id'],sort_keys=True),json.dumps(r['query'],sort_keys=True)):r for r in raw if r['kind']=='completed-host-read'}
 inputs={r['submitted_input']['ID']:r for r in raw if r['kind']=='constructed-transaction'}
 commands=[r['command'] for r in raw if r['kind']=='host-submit-acceptance-only']
 for txid,spec in c['history']['transactions'].items():
  input_record=inputs[txid];tx=input_record['submitted_input'];assert txid==tx['ID']
  assert any(command.get('Tx')==tx and command['Kind'] in ('local','register') for command in commands)
  assert spec['request_key']==tx['Request'] and spec['coordinator']==gname(tx['Coordinator'])
  assert spec['participants']==[gname(p['Group']) for p in tx['Participants']]
  writes={}
  for b in input_record['bound_actual_views']:
   part=b['participant'];group=part['Group'];view=b['view'];key=(json.dumps(b['read_id'],sort_keys=True),json.dumps(b['query'],sort_keys=True))
   assert key in actual_reads and actual_reads[key]['view']==view and actual_reads[key]['query']['Kind']=='current'
   assert view['Group']==group and view['Epoch']==part['Epoch'] and view['State']['Generation']==part['Generation']
   assert part in tx['Participants']
   values=view['State']['Values']
   for read in part['Reads'] or []:assert read['Version']==values.get(read['Key'],{}).get('Version',0)
   for effect in part['Effects'] or []:
    writes[synth(group,effect['Key'])]={'op':'delete'} if effect['Delete'] else {'op':'put','value':effect['Value']}
   for read in spec['reads']:
    if not read.get('key',read.get('lo','')).startswith(gname(group)+'/'):continue
    if read['kind']=='key':
     key=read['key'].split('/',1)[1];assert read['exists']==(key in values);assert read['value']==values.get(key,{}).get('Value')
     assert any(r['Key']==key for r in part['Reads'] or [])
    else:
     want={synth(group,k):v['Value'] for k,v in values.items() if read['lo']<=synth(group,k)<read['hi']};assert read['values']==want
  assert writes==spec['writes']
 for txid,d in c['decisions'].items():
  view=d['proof_view'];tx=inputs[txid]['submitted_input'];assert view['Tx']==tx and view['Decision']['Commit']==d['commit'] and view['Decision']['Round']==d['round']
  assert any(r['kind']=='completed-host-read' and r['read_id']==d['read_id'] and r['view']==view for r in raw)
 assert c['rounds']=={txid:d['round'] for txid,d in c['decisions'].items() if d['commit']}
 assert c['committed']==sorted(c['rounds'])
 for observation in c['observations']:
  count=len(observation['scope']);entries=raw[observation['event']-count:observation['event']];values={}
  assert all(e['kind']=='host-at' for e in entries)
  for entry in entries:
   group=(entry['host']-1)//3;assert gname(group) in observation['scope'] and entry['round']==observation['round']
   certs=entry['cut_certificates'];assert sorted(gname(v['Group']) for v in certs)==sorted(observation['scope'])
   for v in certs:
    assert v['Certificate']['Round']==observation['round']
    assert any(r['kind']=='completed-host-read' and r['view']==v and r['query']['Kind']=='certified' for r in raw)
   values.update({synth(group,key):v['Value'] for key,v in entry['snapshot']['Values'].items()})
  assert values==observation['values']
 for final in c['finals']:
  entries=raw[final['event']-len(final['scope']):final['event']];values={}
  for entry in entries:
   assert entry['kind']=='completed-host-read' and entry['query']['Kind']=='current';group=entry['view']['Group'];assert gname(group) in final['scope']
   values.update({synth(group,key):v['Value'] for key,v in entry['view']['State']['Values'].items()})
  assert values==final['values']
 return {'actual_current_read_bindings_checked':len(inputs),'captured_decisions_checked':len(c['decisions']),'complete_observations':len(c['observations']),'complete_finals':len(c['finals'])}

def check(c):
 binding=validate_bindings(c)
 witness,detail=serial_witness(c['history'],set(c['committed']),c['rounds'],c['observations'],c['finals'])
 return {'name':c['name'],'accepted':witness is not None,'serial_witness':witness,'search':detail,'bindings':binding,'scope':'actual Host captures checked against mathematical map search; not full Evidence or durability acceptance'}

def negative_controls(c):
 """Mutated observed data only, explicitly NOT actual production findings."""
 result=[]
 def expect_reject(name,m):
  witness,detail=serial_witness(m['history'],set(m['committed']),m['rounds'],m['observations'],m['finals'])
  result.append({'name':name,'rejected':witness is None,'search':detail,'kind':'oracle sensitivity control; not actual Host capture'})
 if c['name']=='partial-install-old-cut-correction-outside-coordinator':
  m=copy.deepcopy(c);m['observations'][0]['values'].pop('B/in/old');expect_reject('partial-cross-partition-answer',m)
  m=copy.deepcopy(c);m['observations'][2]['values']=m['observations'][1]['values'];expect_reject('current-map-substituted-at-old-cut',m)
  m=copy.deepcopy(c);m['observations'][1]['values']['B/phantom']=99;expect_reject('extra-phantom-output-key',m)
 if c['name'] in ('absent-key-cross-write-skew','empty-range-phantom-cross-write-skew'):
  m=copy.deepcopy(c);m['committed']=['left','right'];m['rounds']={'left':1,'right':2};m['observations']=[];m['finals']=[];expect_reject('two-stale-predicate-reads-commit-cycle',m)
 return result

def main():
 parser=argparse.ArgumentParser();parser.add_argument('directory',type=Path,nargs='?',default=ROOT/'captures-race');parser.add_argument('--output',type=Path);parser.add_argument('--source-root',type=Path,help='isolated repository root for verifying freshly exported raw Go captures');args=parser.parse_args()
 if args.output is None:
  with tempfile.NamedTemporaryFile(prefix='rho-host-serial-results-',suffix='.json',delete=False) as output: args.output=Path(output.name)
 if args.output.resolve().is_relative_to(ROOT): parser.error('choose an output outside the reviewed package')
 source=json.loads((ROOT/'source-validation.json').read_text())
 manifest_sha=hashlib.sha256(json.dumps(source['tracked_files_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
 assert manifest_sha==source['tracked_files_manifest_sha256']
 assert source['unchanged_canonical_oracle']['sha256']==ORACLE_SHA256
 patch=ROOT/'independent-host-capture.patch'
 assert hashlib.sha256(patch.read_bytes()).hexdigest()==source['copied_behavioral_evidence']['adapter_patch_sha256']
 go_lines=[line[1:] for line in patch.read_text().splitlines() if line.startswith('+') and not line.startswith('+++')]
 assert hashlib.sha256(('\n'.join(go_lines)+'\n').encode()).hexdigest()==source['copied_behavioral_evidence']['adapter_go_sha256']
 if args.source_root is not None:
  for relative,expected in source['tracked_files_sha256'].items():
   candidate=args.source_root/relative
   assert candidate.is_file() and hashlib.sha256(candidate.read_bytes()).hexdigest()==expected, relative
 results=[];controls=[]
 for path in sorted(args.directory.glob('*.json')):
  capture=json.loads(path.read_text());metadata=capture.get('source_hashes')
  if metadata is None:
   if args.source_root is None: parser.error('raw Go captures require --source-root matching the pinned tracked source bytes')
  else:
   assert metadata['tracked_source_files']==101 and metadata['tracked_files_manifest_sha256']==manifest_sha
   assert metadata['oracle_sha256']==ORACLE_SHA256 and metadata['adapter_sha256']==source['copied_behavioral_evidence']['adapter_go_sha256']
   assert metadata['head_at_snapshot']==source['copied_behavioral_evidence']['head_at_snapshot']
  r=check(capture)
  r['source_validation']='fresh raw Go capture with externally verified tracked source bytes' if metadata is None else 'packaged historical capture provenance';r['capture_file']=path.name;r['capture_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();results.append(r);controls+=negative_controls(capture)
 if len(results)!=5:raise AssertionError(('expected five complete histories',len(results)))
 report={'schema':'rho-host-mathematical-serial-bridge','version':1,'oracle_sha256':ORACLE_SHA256,'all_actual_captures_accepted':all(r['accepted'] for r in results),'all_negative_controls_rejected':all(c['rejected'] for c in controls),'results':results,'negative_controls':controls,'full_evidence_audit_run':False}
 args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'output':str(args.output),'captures':len(results),'actual_accepted':report['all_actual_captures_accepted'],'synthetic_controls_rejected':report['all_negative_controls_rejected'],'full_evidence_audit_run':False},indent=2));return not(report['all_actual_captures_accepted'] and report['all_negative_controls_rejected'])
if __name__=='__main__':sys.exit(main())
