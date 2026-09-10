#!/usr/bin/env python3
"""Render the R1 sweep directory produced by run.sh as markdown tables."""
import glob
import json
import os
import re
import sys

out = sys.argv[1]
rows = []
for f in sorted(glob.glob(os.path.join(out, "*.json"))):
    if f.endswith(".counting.json"):
        continue
    d = json.load(open(f))
    cfg = os.path.basename(f)[:-5]
    st = os.path.join(out, cfg + ".strace")
    msync = fsync = pwrite = 0
    if os.path.exists(st):
        for line in open(st):
            m = re.match(r"\s*[\d,.]+\s+[\d,.]+\s+\d+\s+(\d+)\s+(?:\d+\s+)?(\w+)\s*$", line)
            if not m:
                continue
            n, name = int(m.group(1)), m.group(2)
            if name == "msync":
                msync = n
            elif name in ("fsync", "fdatasync"):
                fsync += n
            elif name in ("pwrite64", "write"):
                pwrite += n
    cnt = os.path.join(out, cfg + ".counting.json")
    lost = len(d["digest"]["Problems"] or [])
    rows.append((d["mode"], d["producers"], d["batch_rows"], d["rows_per_sec"], d["commit_p50_ms"], d["commit_p99_ms"],
                 d["peak_rss_bytes"] / 2**20, d["dir_bytes"] / 2**20, d["unique_retries"], lost, msync, fsync, pwrite,
                 d["rows"], d["digest"]["Hash"][:12], [round(p["rows_per_sec"]) for p in d["phases"]]))

print("| mode | producers | batch | rows/s | commit p50 ms | p99 ms | peak RSS MiB | dir MiB | unique retries | lost RMW facts | msync | fsync | write+pwrite | msync/row | digest | phase rows/s |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in rows:
    print(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]:.1f} | {r[4]:.1f} | {r[5]:.1f} | {r[6]:.0f} | {r[7]:.0f} | {r[8]} | {r[9]} | {r[10]} | {r[11]} | {r[12]} | {r[10]/r[13]:.2f} | {r[14]} | {r[15]} |")
