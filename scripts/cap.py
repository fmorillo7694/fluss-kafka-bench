#!/usr/bin/env python3
"""Capture CPU/mem/throughput/checkpoint for a Flink join job.
Usage: cap.py <job-id> <label>   (run inside repo; uses docker + Flink REST)
Appends a row to bench/results/join_resources.csv and prints a summary.
"""
import sys, subprocess, json, urllib.request, os, datetime

JID, LABEL = sys.argv[1], sys.argv[2]
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(HERE, "bench", "results", "join_resources.csv")
DC = ["docker", "compose", "-f", os.path.join(HERE, "docker", "docker-compose.yml")]

def sh(args):
    return subprocess.run(args, capture_output=True, text=True, timeout=60).stdout

def rest(path):
    try:
        return json.load(urllib.request.urlopen(f"http://localhost:8082{path}", timeout=15))
    except Exception:
        return {}

# TM reporter: avg CPU load + heap across TMs
rep = sh(DC + ["exec", "-T", "taskmanager", "bash", "-lc", "curl -s localhost:9249/ 2>/dev/null"])
cpu = [float(l.split()[-1]) for l in rep.splitlines() if l.startswith("flink_taskmanager_Status_JVM_CPU_Load{")]
heap = [float(l.split()[-1]) for l in rep.splitlines() if l.startswith("flink_taskmanager_Status_JVM_Memory_Heap_Used{")]
tm_cpu = sum(cpu)/len(cpu) if cpu else 0.0
tm_heap = sum(heap)/len(heap) if heap else 0.0

# docker stats for container cpu%/mem
stats = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}"])
def ctr(name):
    for l in stats.splitlines():
        if name in l:
            p = l.split()
            c = p[1].rstrip("%")
            m = p[2].replace("MiB", "").replace("GiB", "")
            mult = 1024 if "GiB" in p[2] else 1
            try: return float(c), float(m)*mult
            except: return 0.0, 0.0
    return 0.0, 0.0
tm_c, tm_m = ctr("taskmanager"); tab_c, tab_m = ctr("tablet-server")

# job: checkpoint size + throughput (busiest vertex numRecordsOutPerSecond)
cps = rest(f"/jobs/{JID}/checkpoints").get("summary", {}).get("state_size", {}).get("avg", 0)
j = rest(f"/jobs/{JID}")
tput = 0.0
for v in j.get("vertices", []):
    m = v.get("metrics", {})
    # write-records-rate may be absent; fall back to 0
    try: tput = max(tput, float(m.get("write-records-rate", 0) or 0))
    except: pass
state = j.get("state", "?")

row = [datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), LABEL,
       f"{tm_cpu:.4f}", f"{int(tm_heap)}", f"{tm_c}", f"{int(tm_m)}",
       f"{tab_c}", f"{int(tab_m)}", f"{int(cps)}", state]
hdr = "ts,label,tm_cpu_load,tm_heap_bytes,tm_ctr_cpu_pct,tm_ctr_mem_mib,tablet_ctr_cpu_pct,tablet_ctr_mem_mib,checkpoint_bytes,state"
if not os.path.exists(OUT):
    open(OUT, "w").write(hdr + "\n")
open(OUT, "a").write(",".join(row) + "\n")
print(f"[{LABEL}] TM_cpu_load={tm_cpu:.3f} TM_heap={int(tm_heap)//1048576}MB | "
      f"tm_ctr={tm_c}%/{int(tm_m)}MB tablet_ctr={tab_c}%/{int(tab_m)}MB | cp={int(cps)}B state={state}")
