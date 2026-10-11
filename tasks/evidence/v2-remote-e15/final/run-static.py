import pathlib,os,subprocess,json,hashlib,sys
repo=pathlib.Path('/var/tmp/rho-v5-v2-remote-e15-20261011');out=pathlib.Path(__file__).parent;manifest=json.loads((out/'source-manifest.json').read_text());env=dict(os.environ,GOTOOLCHAIN='go1.26.9',GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOFLAGS='-p=1',GOWORK='off')
checks={
'fmt':['python3','-c',"import pathlib,subprocess;files=sorted(str(p) for p in pathlib.Path('.').rglob('*.go'));s=subprocess.check_output(['gofmt','-l',*files]).decode();print(s,end='');assert not s"],
'vet':['go','vet','./...'],
'build':['go','build','./...']}
image=['docker','run','--rm','--memory=4g','--cpus=2','-e','GOTOOLCHAIN=go1.26.9','-e','GOMAXPROCS=2','-e','GOMEMLIMIT=2GiB','-e','GOFLAGS=-p=1','-e','GOWORK=off','-v',str(repo)+':/src','-w','/src/v5','-v','rho-tkg-gocache:/go','-v','rho-tkg-buildcache:/root/.cache/go-build','golang:1.26.9','sh','-c']
checks.update({
'lint':image+['go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 && golangci-lint version && golangci-lint run --config ../.golangci.yml --max-same-issues=0 --max-issues-per-linter=0 ./...'],
'security':image+['go install github.com/securego/gosec/v2/cmd/gosec@v2.29.0 && go version -m /go/bin/gosec && gosec -quiet ./...'],
'vuln':image+['go install golang.org/x/vuln/cmd/govulncheck@v1.7.0 && go version -m /go/bin/govulncheck && govulncheck ./...']})
for name in sys.argv[1:]:
 for path,h in manifest.items():assert hashlib.sha256((repo/path).read_bytes()).hexdigest()==h,path
 cmd=['python3','/var/tmp/rho-v5-rebase-main-20261010-0mdr6yaw/limit-local.py','--rss-mib','4096','--seconds','180','--report',str(out/(name+'.watchdog.json')),'--',*checks[name]]
 print('START',name,flush=True)
 with (out/(name+'.log')).open('w') as log:r=subprocess.run(cmd,cwd=repo/'v5',env=env,stdout=log,stderr=subprocess.STDOUT)
 (out/(name+'.receipt.json')).write_text(json.dumps(dict(source_manifest_sha256=hashlib.sha256((out/'source-manifest.json').read_bytes()).hexdigest(),name=name,command=cmd,exit_code=r.returncode,env={k:env[k] for k in ['GOTOOLCHAIN','GOMAXPROCS','GOMEMLIMIT','GOFLAGS','GOWORK']}),indent=2)+'\n')
 print('DONE',name,r.returncode,flush=True)
 if r.returncode:sys.exit(r.returncode)
