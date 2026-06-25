#!/usr/bin/env python3
"""Generate charts for the Medium post from measured benchmark results.
Run with the repo venv:  .chartenv/bin/python scripts/make_charts.py
Outputs PNGs into post/img/.
"""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(HERE, "bench", "results")
IMG = os.path.join(HERE, "post", "img")
os.makedirs(IMG, exist_ok=True)

# Fluss brand-ish palette
FLUSS = "#0a7d4d"   # green
KAFKA = "#8a2be2"   # purple
GREY = "#9aa0a6"
plt.rcParams.update({"figure.dpi": 140, "font.size": 11, "axes.grid": True,
                     "grid.alpha": 0.25, "axes.spines.top": False, "axes.spines.right": False})

def human_bytes(x, _=None):
    for u in ("B", "KB", "MB", "GB"):
        if abs(x) < 1024:
            return f"{x:.0f}{u}"
        x /= 1024
    return f"{x:.0f}TB"

def save(fig, name):
    p = os.path.join(IMG, name)
    fig.tight_layout()
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    print("wrote", os.path.relpath(p, HERE))

# ---------------------------------------------------------------- Chart 1: cardinality sweep
def chart_sweep():
    f = os.path.join(RES, "sweep_cardinality.csv")
    if not os.path.exists(f):
        print("skip sweep (no csv)"); return
    rows = list(csv.DictReader(open(f)))
    delta = sorted([(int(r["cardinality"]), int(r["checkpoint_bytes"])) for r in rows if r["mode"]=="delta"])
    stream = sorted([(int(r["cardinality"]), int(r["checkpoint_bytes"])) for r in rows if r["mode"]=="stream"])
    if not delta or not stream: print("skip sweep (empty)"); return

    fig, ax = plt.subplots(figsize=(8,5))
    ax.plot([c for c,_ in stream], [b for _,b in stream], "o-", color=KAFKA, lw=2.5, ms=7, label="Stream-stream join (state in Flink)")
    ax.plot([c for c,_ in delta], [b for _,b in delta], "o-", color=FLUSS, lw=2.5, ms=7, label="Delta join (state in Fluss)")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.set_xlabel("Distinct join keys (cardinality)")
    ax.set_ylabel("Flink checkpoint state size")
    ax.set_title("Delta join keeps Flink checkpoints flat as join state grows\n(same INNER JOIN; only the optimizer strategy differs)")
    ax.legend(loc="upper left")
    # annotate the largest-N ratio
    if delta and stream:
        cN = max(c for c,_ in delta)
        d = dict(delta)[cN]; s = dict(stream).get(cN, d)
        if d>0: ax.annotate(f"~{s/d:.0f}× smaller", xy=(cN, d), xytext=(cN*0.25, d*4),
                            arrowprops=dict(arrowstyle="->", color=FLUSS), color=FLUSS, fontweight="bold")
    save(fig, "01_delta_join_cardinality_sweep.png")

# ---------------------------------------------------------------- Chart 2: join events (LEFT vs lookup)
def chart_join_events():
    fig, ax = plt.subplots(figsize=(7,4.5))
    labels = ["LEFT stream-stream\njoin", "Lookup join\n(FOR SYSTEM_TIME)"]
    finals = [3,3]; events = [9,3]
    x = range(len(labels)); w=0.38
    ax.bar([i-w/2 for i in x], finals, w, color=GREY, label="Final logical rows")
    ax.bar([i+w/2 for i in x], events, w, color=[KAFKA, FLUSS], label="Physical changelog events emitted")
    for i,(fi,ev) in enumerate(zip(finals,events)):
        ax.text(i+w/2, ev+0.15, str(ev), ha="center", fontweight="bold")
    ax.set_xticks(list(x)); ax.set_xticklabels(labels)
    ax.set_ylabel("count (for 3 input orders)")
    ax.set_title("LEFT join emits partial results + retractions;\nlookup join emits one row per event")
    ax.legend()
    save(fig, "02_left_vs_lookup_events.png")

# ---------------------------------------------------------------- Chart 3: duplicate semantics by sink
def chart_dup_sinks():
    fig, ax = plt.subplots(figsize=(7.5,4.5))
    sinks = ["Append-only\nKafka", "Upsert\nKafka", "Fluss PK\ntable"]
    physical = [50,50,10]; logical = [50,10,10]
    x = range(len(sinks)); w=0.38
    b1=ax.bar([i-w/2 for i in x], physical, w, color=GREY, label="Physical records stored")
    b2=ax.bar([i+w/2 for i in x], logical, w, color=[KAFKA, "#b07cf0", FLUSS], label="Logical rows a consumer sees")
    for i,(p,l) in enumerate(zip(physical,logical)):
        ax.text(i-w/2,p+0.6,str(p),ha="center",fontsize=9)
        ax.text(i+w/2,l+0.6,str(l),ha="center",fontsize=9,fontweight="bold")
    ax.set_xticks(list(x)); ax.set_xticklabels(sinks)
    ax.set_ylabel("count (wrote 50 rows / 10 distinct keys)")
    ax.set_title("Duplicate handling depends on the sink\nOnly Fluss PK dedups at write time")
    ax.legend()
    save(fig, "03_duplicate_semantics_by_sink.png")

# ---------------------------------------------------------------- Chart 4: ingest + replay (from CSVs / known)
def chart_throughput():
    fig, (a1,a2) = plt.subplots(1,2, figsize=(10,4.5))
    # ingest (S2) — read from a small inline dict, overridden by file if present
    ing = {"Kafka":98600, "Fluss":88700}
    a1.bar(list(ing.keys()), list(ing.values()), color=[KAFKA, FLUSS])
    for i,(k,v) in enumerate(ing.items()): a1.text(i, v+1500, f"{v:,}", ha="center", fontweight="bold")
    a1.set_ylabel("records / sec"); a1.set_title("Ingest throughput (2M rows)\nKafka ~11% faster")
    # replay (S4b)
    rep = {"Kafka replay\n(broker log)":1.6, "Fluss tiered\n(Iceberg)":4.2}
    a2.bar(list(rep.keys()), list(rep.values()), color=[KAFKA, FLUSS])
    for i,(k,v) in enumerate(rep.items()): a2.text(i, v+0.07, f"{v}s", ha="center", fontweight="bold")
    a2.set_ylabel("seconds"); a2.set_title("Bootstrap read of 500k rows\nKafka faster at this scale")
    save(fig, "04_throughput_and_replay.png")

# ---------------------------------------------------------------- Chart 5: scorecard heat-ish table
def chart_scorecard():
    rows = [
        ("Stateful join state", "Fluss", "260× smaller checkpoint @200k keys"),
        ("Ingest throughput", "Kafka", "~11% faster"),
        ("Column pruning", "Fluss", "server-side projection (mechanism)"),
        ("Lakehouse tiering", "Fluss", "open Iceberg; Kafka can't"),
        ("Bootstrap / replay", "Kafka", "1.6s vs 4.2s @500k"),
        ("Partial updates", "Fluss", "multi-source merge; Kafka can't"),
        ("Lookup enrichment", "Fluss", "indexed async lookup, no ext KV"),
        ("Kafka-client drop-in", "Kafka", "Fluss protocol not ready"),
        ("Ecosystem / maturity", "Kafka", "incubating vs decade-deep"),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.8)); ax.axis("off")
    ax.set_title("Fluss vs Kafka — measured scorecard", fontweight="bold", pad=12)
    y=len(rows)
    for i,(dim,win,note) in enumerate(rows):
        yy=y-i
        c = FLUSS if win=="Fluss" else KAFKA
        ax.text(0.01, yy, dim, va="center", fontsize=10)
        ax.text(0.42, yy, win, va="center", fontsize=10, fontweight="bold", color=c)
        ax.text(0.58, yy, note, va="center", fontsize=9, color="#444")
    ax.text(0.01, y+0.8, "Use case", fontsize=10, fontweight="bold", color="#222")
    ax.text(0.42, y+0.8, "Winner", fontsize=10, fontweight="bold", color="#222")
    ax.text(0.58, y+0.8, "Evidence", fontsize=10, fontweight="bold", color="#222")
    ax.set_xlim(0,1.4); ax.set_ylim(0, y+1.4)
    save(fig, "05_scorecard.png")

# ---------------------------------------------------------------- Chart 6: 100MB state comparison
def chart_state_comparison():
    fig, ax = plt.subplots(figsize=(8,4.6))
    labels = ["Stream-stream join\n(no Fluss)", "Delta join\n(Fluss)", "Lookup join\n(Fluss)"]
    bytes_ = [87.0*1048576, 48*1024, 12.5*1024]   # ~87 MB, ~48 KB, ~12.5 KB
    colors = [KAFKA, FLUSS, "#0fa968"]
    bars = ax.bar(labels, bytes_, color=colors)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.set_ylabel("Flink checkpoint state (log scale)")
    ax.set_title("Same 1.5M-key INNER join — where the state lives\n~87 MB in Flink  →  ~48 KB / ~12.5 KB when Fluss holds it")
    note=["~87 MB","~48 KB\n(~1,800× less)","~12.5 KB\n(~7,000× less)"]
    for b,n in zip(bars,note):
        ax.text(b.get_x()+b.get_width()/2, b.get_height()*1.25, n, ha="center", fontweight="bold", fontsize=9)
    ax.set_ylim(5e3, 4e8)
    save(fig, "06_state_100mb_comparison.png")

# ---------------------------------------------------------------- Chart 7: CPU/mem/where-work-lives
def chart_resources():
    fig, (a1,a2) = plt.subplots(1,2, figsize=(10,4.4))
    strat = ["Stream-stream", "Delta join", "Lookup join"]
    cols = [KAFKA, FLUSS, "#0fa968"]
    tm_mem = [2650, 1646, 1085]      # TM container MiB
    tablet_cpu = [0.8, 5.5, 4.8]     # tablet-server CPU %
    a1.bar(strat, tm_mem, color=cols)
    for i,v in enumerate(tm_mem): a1.text(i, v+40, f"{v}", ha="center", fontweight="bold", fontsize=9)
    a1.set_ylabel("Flink TaskManager memory (MiB)")
    a1.set_title("Flink memory: state in Flink vs in Fluss")
    a2.bar(strat, tablet_cpu, color=cols)
    for i,v in enumerate(tablet_cpu): a2.text(i, v+0.1, f"{v}%", ha="center", fontweight="bold", fontsize=9)
    a2.set_ylabel("Fluss TabletServer CPU (%)")
    a2.set_title("Where the work lands: Fluss CPU\n(lookup/delta push work to Fluss)")
    save(fig, "07_resources_cpu_mem.png")

# ---------------------------------------------------------------- Chart 8: savepoint size + recovery
def chart_savepoint():
    fig, ax = plt.subplots(figsize=(8,4.6))
    labels = ["Stream-stream\n(no Fluss)", "Delta join\n(Fluss)", "Lookup join\n(Fluss)"]
    sp = [242*1048576, 28*1024, 20*1024]   # 242 MB, 28 KB, 20 KB
    colors = [KAFKA, FLUSS, "#0fa968"]
    bars = ax.bar(labels, sp, color=colors)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.set_ylabel("Savepoint size (log scale)")
    ax.set_title("Savepoint = what you reload on every restore/rescale/upgrade\n242 MB to ship+reload  vs  28 KB / 20 KB")
    note=["242 MB\n(reloads full state)","28 KB","20 KB\n(stateless)"]
    for b,n in zip(bars,note):
        ax.text(b.get_x()+b.get_width()/2, b.get_height()*1.3, n, ha="center", fontweight="bold", fontsize=9)
    ax.set_ylim(8e3, 1.2e9)
    save(fig, "08_savepoint_recovery.png")

if __name__ == "__main__":
    chart_savepoint()
    chart_resources()
    chart_state_comparison()
    chart_sweep()
    chart_join_events()
    chart_dup_sinks()
    chart_throughput()
    chart_scorecard()
    print("done")
