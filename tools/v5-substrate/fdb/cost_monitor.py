#!/usr/bin/env python3
"""Sample actual RSS of every explicitly owned FDB server/client process."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import tempfile
import time


def process_rows(text):
    rows = []
    for line in text.splitlines()[1:]:
        fields = line.split(None, 3)
        if len(fields) < 3:
            raise ValueError("incomplete docker top row")
        pid, parent, rss = (int(value) for value in fields[:3])
        if pid <= 0 or parent < 0 or rss < 0:
            raise ValueError("invalid actual process identity/RSS")
        rows.append({"pid": pid, "parent": parent, "rssKiB": rss, "command": fields[3] if len(fields) == 4 else ""})
    if len({row["pid"] for row in rows}) != len(rows):
        raise ValueError("duplicate process PID within container")
    return rows


def native_observation(command, deadline, directory):
    if deadline is None:
        return subprocess.run(command,capture_output=True,text=True,timeout=5,check=False)
    remaining=deadline-time.monotonic()
    if remaining<=0: raise ValueError("original resource observation deadline")
    with tempfile.TemporaryFile(dir=directory) as output:
        process=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=observation_limit)
        try: result=process.wait(timeout=min(5,remaining))
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=3)
            raise ValueError("owned resource helper timeout/reaped; engine cleanup required") from None
        output.seek(0);body=output.read((128<<10)+1)
    if len(body)>128<<10: raise ValueError("native observation output cap")
    return subprocess.CompletedProcess(command,result,body.decode(),"")


def observation_limit():
    resource.setrlimit(resource.RLIMIT_FSIZE,(128<<10,128<<10))


def sample(container, required, deadline=None, directory=None):
    start = time.monotonic_ns()
    top = native_observation(["docker", "top", container, "-eo", "pid,ppid,rss,args"],deadline,directory)
    if top.returncode == 0:
        rows = process_rows(top.stdout)
        if rows:
            result={"container": container, "startNanos": start, "endNanos": time.monotonic_ns(), "processes": rows, "state": "live"}
            if deadline is not None: result["rawTop"]=top.stdout
            return result
        # A successful top can race process exit and return just its header.
        # Only the actual terminal inspection below can classify that result.
    inspect = native_observation(["docker", "inspect", container],deadline,directory)
    inspect.check_returncode()
    records = json.loads(inspect.stdout)
    if len(records) != 1:
        raise ValueError("container identity changed")
    state = records[0]["State"]
    if required or state["Running"] or state["OOMKilled"] or state["ExitCode"] != 0:
        raise ValueError("missing/refused actual live server/client RSS: " + container)
    return {"container": container, "startNanos": start, "endNanos": time.monotonic_ns(), "processes": [], "state": "stopped", "inspection": state}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", nargs=4, required=True)
    parser.add_argument("--client", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--absolute-deadline", type=int)
    parser.add_argument("--sample-millis", type=int)
    parser.add_argument("--max-samples", type=int)
    parser.add_argument("--max-bytes", type=int)
    parser.add_argument("--terminal-marker", type=Path)
    args = parser.parse_args()
    fresh = args.absolute_deadline is not None
    if len(set(args.servers + [args.client])) != 5 or not fresh and not 1 <= args.seconds <= 1800:
        parser.error("four servers plus one distinct client; finite <=1800second watchdog required")
    if fresh and not (1 <= args.seconds <= 31_536_000 and time.time_ns() < args.absolute_deadline <= time.time_ns()+args.seconds*1_000_000_000 and
                      args.sample_millis is not None and 1000 <= args.sample_millis <= 60000 and args.max_samples is not None and
                      (args.seconds*1000+args.sample_millis-1)//args.sample_millis+2 <= args.max_samples <= 31_536_002 and
                      args.max_bytes is not None and args.max_samples*(64<<10) <= args.max_bytes <= 1<<42 and args.terminal_marker==args.output.parent/"transport-release"):
        parser.error("fresh fixed absolute lifetime and explicit finite sample/output grants")
    if not fresh and any(v is not None for v in (args.sample_millis,args.max_samples,args.max_bytes,args.terminal_marker)):
        parser.error("fresh grants require actual fixed absolute deadline")
    started, peak = time.monotonic(), 0
    deadline = started+args.seconds if not fresh else started+(args.absolute_deadline-time.time_ns())/1e9
    count, used, digest = 0, 0, hashlib.sha256()
    with args.output.open("x") as output, concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        while time.monotonic() < deadline:
            if fresh:
                futures=[pool.submit(sample,name,i<4,deadline,args.output.parent) for i,name in enumerate(args.servers+[args.client])]
            else:
                futures = [pool.submit(sample, name, True) for name in args.servers] + [pool.submit(sample, args.client, False)]
            observations = [future.result() for future in futures]
            pids = [p["pid"] for c in observations for p in c["processes"]]
            if len(set(pids)) != len(pids):
                raise ValueError("same actual host PID counted across containers")
            rss = sum(p["rssKiB"] for c in observations for p in c["processes"])
            peak = max(peak, rss)
            frame = {"elapsedSeconds": time.monotonic() - started, "rssKiB": rss,
                                     "sampledPeakRSSKiB": peak, "samplingSkewNanos": max(c["endNanos"] for c in observations) - min(c["startNanos"] for c in observations),
                                     "containers": observations}
            if fresh:
                groups=[cgroup(name,deadline,args.output.parent) for name in args.servers+[args.client]]
                frame.update({"ordinal":count,"deadlineUnixNanos":args.absolute_deadline,"cgroups":groups,
                              "hostController":host_usage()})
                frame["samplingSkewNanos"]=max(c["endNanos"] for c in observations+groups)-min(c["startNanos"] for c in observations+groups)
            body=json.dumps(frame)+"\n"
            if fresh and (count>=args.max_samples or len(body.encode())>64<<10 or len(body.encode())>args.max_bytes-used):
                raise ValueError("actual aggregate resource evidence exceeded original finite grant")
            output.write(body)
            digest.update(body.encode())
            used+=len(body.encode());count+=1
            output.flush()
            if observations[-1]["state"] == "stopped":
                if fresh:
                    footer=json.dumps({"type":"footer","samples":count,"payloadBytes":used,"payloadSHA256":digest.hexdigest(),"deadlineUnixNanos":args.absolute_deadline})+"\n"
                    if len(footer.encode())>args.max_bytes-used: raise ValueError("resource EOF not funded")
                    output.write(footer);output.flush()
                return
            if fresh: monitor_sleep(args.sample_millis/1000,deadline,args.terminal_marker)
            else: time.sleep(0.5)
    raise ValueError("cost monitor reached finite deadline before actual client completion")


def monitor_sleep(interval,deadline,terminal_marker):
    # A long admitted sampling interval cannot strand a completed client. This
    # merely wakes observation; only actual Docker terminal state ends the run.
    until=min(deadline,time.monotonic()+interval)
    while not terminal_marker.exists():
        remaining=until-time.monotonic()
        if remaining<=0: return
        time.sleep(min(0.1,remaining))


def host_usage():
    import sys
    def usage(who):
        u=resource.getrusage(who)
        return {"userSeconds":u.ru_utime,"systemSeconds":u.ru_stime,"maxRSS":u.ru_maxrss}
    return {"platform":sys.platform,"self":usage(resource.RUSAGE_SELF),"reapedChildren":usage(resource.RUSAGE_CHILDREN),"rssUnits":"bytes" if sys.platform=="darwin" else "KiB","scope":"child CPU cumulative; maxRSS largest reaped child, not simultaneous host process-tree peak"}


def cgroup(name,deadline,directory):
    # Fresh only. cgroup peak/stat include helper processes and kernel-accounted
    # memory; process RSS remains separately reported, never conflated with it.
    start=time.monotonic_ns()
    result=native_observation(["docker","exec",name,"timeout","--signal=KILL","4s","/bin/bash","-ec",'for f in memory.current memory.peak memory.stat; do echo "$f"; cat "/sys/fs/cgroup/$f"; done'],deadline,directory)
    if result.returncode != 0:
        state=native_observation(["docker","inspect","--format","{{json .State}}",name],deadline,directory)
        state.check_returncode()
        terminal=json.loads(state.stdout)
        if terminal["Running"] or terminal["OOMKilled"] or terminal["ExitCode"]!=0: raise ValueError("actual cgroup unavailable")
        return {"container":name,"state":"stopped","startNanos":start,"endNanos":time.monotonic_ns(),"inspection":terminal}
    if len(result.stdout.encode())>16<<10: raise ValueError("finite actual cgroup frame")
    parse_cgroup_raw(result.stdout)
    return {"container":name,"state":"live","startNanos":start,"endNanos":time.monotonic_ns(),"raw":result.stdout}


def parse_cgroup_raw(text):
    lines=text.splitlines()
    if len(text.encode())>16<<10 or not 6<=len(lines)<=256 or lines[0]!="memory.current" or lines[2]!="memory.peak" or lines[4]!="memory.stat":
        raise ValueError("actual complete cgroup-v2 raw grammar")
    def unsigned(value):
        if not value.isascii() or not value.isdecimal() or str(int(value))!=value or not 0<=int(value)<1<<64: raise ValueError("native unsigned cgroup counter")
        return int(value)
    current,peak=unsigned(lines[1]),unsigned(lines[3]);stats={}
    for line in lines[5:]:
        fields=line.split()
        if len(fields)!=2 or fields[0] in stats: raise ValueError("duplicate/malformed native cgroup statistic")
        stats[fields[0]]=unsigned(fields[1])
    if not {"anon","file","kernel","kernel_stack","pagetables","sock","slab"}<=set(stats): raise ValueError("incomplete kernel-accounted cgroup statistics")
    # current/peak/stat are separate genuine kernel file reads, not an atomic
    # memory snapshot. Keep all raw values, never synthesize a reconciliation.
    return current,peak,stats


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"cost RSS refused: {error}") from error
