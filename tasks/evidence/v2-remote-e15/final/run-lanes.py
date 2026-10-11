import os,sys,json,hashlib,subprocess,pathlib
repo=pathlib.Path('/var/tmp/rho-v5-v2-remote-e15-20261011');e=pathlib.Path(__file__).parent
manifest=json.loads((e/'source-manifest.json').read_text());source=hashlib.sha256((e/'source-manifest.json').read_bytes()).hexdigest();plan=json.loads((e/'lanes.json').read_text());lanes=plan['race_lanes'] if sys.argv[1]=='race' else plan['lanes']
order=['graphapply-e15','graphstore-e15','graphapply-process-source','graphapply-certified','graphapply-cross','graphapply-registry','graphapply-replicated','graphapply-rest','graphstore-partition','graphstore-current','graphstore-rest'];lanes.sort(key=lambda x:order.index('graphapply-cross' if x['lane'].startswith('graphapply-cross-') else x['lane']))
env=dict(os.environ,GOTOOLCHAIN='go1.26.9',GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOFLAGS='-p=1',GOWORK='off')
def check():
 for name,h in manifest.items():
  if hashlib.sha256((repo/name).read_bytes()).hexdigest()!=h:raise RuntimeError('changed source '+name)
mode=sys.argv[1];selected=set(sys.argv[2:])
for lane in lanes:
 if selected and lane['lane'] not in selected:continue
 check();stem=e/(lane['lane']+'-'+mode)
 cmd=['python3','/var/tmp/rho-v5-rebase-main-20261010-0mdr6yaw/limit-local.py','--rss-mib','4096','--seconds','180','--report',str(stem)+'.watchdog.json','--','go','test','-short','-count=1','-json']
 if mode=='normal':cmd+=['-coverpkg=./internal/graphapply,./internal/graphstore','-coverprofile='+str(e/(lane['lane']+'.cover'))]
 elif mode=='race':cmd+=['-race']
 else:raise RuntimeError('mode')
 cmd+=['-run',lane['regex'],lane['package']]
 print('START',lane['lane'],mode,flush=True)
 with pathlib.Path(str(stem)+'.jsonl').open('w') as out:result=subprocess.run(cmd,cwd=repo/'v5',env=env,stdout=out,stderr=subprocess.STDOUT)
 check();pathlib.Path(str(stem)+'.receipt.json').write_text(json.dumps(dict(source_manifest_sha256=source,lane=lane['lane'],mode=mode,names=lane['names'],exit_code=result.returncode,command=cmd),indent=2)+'\n')
 print('DONE',lane['lane'],mode,result.returncode,flush=True)
 if result.returncode:sys.exit(result.returncode)
