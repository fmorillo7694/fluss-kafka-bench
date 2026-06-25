#!/usr/bin/env python3
"""Render the post's tables as PNG images (Medium can't do HTML tables).
Run: .chartenv/bin/python scripts/make_tables.py  ->  post/img/tNN_*.png
Matches the chart palette (Fluss green / Kafka purple).
"""
import os, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMG = os.path.join(HERE, "post", "img")
os.makedirs(IMG, exist_ok=True)
FLUSS="#0a7d4d"; KAFKA="#8a2be2"; HDR="#10231b"; HDRTX="#ffffff"
plt.rcParams.update({"font.size": 12})

def render(name, headers, rows, title, colcolors=None, widths=None, fontsize=12):
    nC=len(headers); nR=len(rows)
    fig_w=min(13, 2.0+1.9*nC); fig_h=1.0+0.5*(nR+1)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h)); ax.axis("off")
    if title: ax.set_title(title, fontweight="bold", fontsize=fontsize+2, pad=14)
    tbl = ax.table(cellText=rows, colLabels=headers, cellLoc="left", loc="center",
                   colWidths=widths)
    tbl.auto_set_font_size(False); tbl.set_fontsize(fontsize); tbl.scale(1, 1.6)
    for (r,c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#d8dde1"); cell.PAD=0.04
        if r==0:
            cell.set_facecolor(HDR); cell.set_text_props(color=HDRTX, fontweight="bold")
        else:
            cell.set_facecolor("#ffffff" if r%2 else "#f6f8fa")
            if colcolors and c in colcolors:
                cell.set_text_props(color=colcolors[c], fontweight="bold")
    p=os.path.join(IMG,name); fig.tight_layout()
    fig.savefig(p, dpi=160, bbox_inches="tight"); plt.close(fig)
    print("wrote", os.path.relpath(p, HERE))

# t01 — cardinality sweep
render("t01_cardinality_sweep.png",
    ["Distinct keys","Delta join","Stream-stream join","Ratio"],
    [["1,000","31 KB","97 KB","3×"],
     ["10,000","39 KB","595 KB","15×"],
     ["50,000","42 KB","2.97 MB","71×"],
     ["200,000","53 KB","13.8 MB","261×"]],
    "Delta-join vs stream-stream checkpoint state, by cardinality",
    colcolors={1:FLUSS,2:KAFKA,3:FLUSS})

# t02 — 100MB 3-way
render("t02_state_100mb.png",
    ["Strategy (same 1.5M-key INNER join)","Operator","Flink checkpoint state"],
    [["Stream-stream join (no Fluss)","stateful Join","~87 MB"],
     ["Delta join (Fluss)","DeltaJoin","~48 KB  (~1,800× less)"],
     ["Lookup join (Fluss)","LookupJoin","~12.5 KB  (~7,000× less)"]],
    "Same workload, three ways — where the join state lives",
    colcolors={2:FLUSS}, widths=[0.46,0.20,0.34])

# t03 — scenario scorecard (the 5-row measured table)
render("t03_scenarios.png",
    ["Scenario","Measurement","Result"],
    [["Stateful join state (200k keys)","checkpoint size: delta vs stream","54 KB vs 14.2 MB (~260×) — Fluss"],
     ["Ingest throughput (2M rows)","records/sec via Flink","Kafka 98.6k vs Fluss 88.7k (~11%)"],
     ["Column pruning (30-col)","projection pushdown","proven in plan; magnitude not isolable"],
     ["Lakehouse tiering (500k)","tier to Iceberg + query","open Iceberg Parquet; Kafka can't"],
     ["Bootstrap / replay (500k)","read all from offset 0","Kafka 1.6 s vs Fluss-tiered 4.2 s"]],
    "Measured scenarios", widths=[0.30,0.28,0.42], fontsize=11)

# t04 — LEFT vs lookup
render("t04_left_vs_lookup.png",
    ["","LEFT stream-stream join","Lookup join (FOR SYSTEM_TIME AS OF)"],
    [["Events for 3 final rows","9","3"],
     ["Late dimension update","retracts + re-emits","ignored (point-in-time)"],
     ["Flink state","both sides materialized","none"],
     ["Sink","must be upsert/retraction-aware","append-only is fine"]],
    "LEFT stream-stream join vs lookup join",
    colcolors={1:KAFKA,2:FLUSS}, widths=[0.26,0.34,0.40], fontsize=11)

# t05 — duplicate/sink rule
render("t05_duplicate_sink_rule.png",
    ["Join","Output\nchangelog","Append-only\nKafka","Upsert sink\n(Fluss PK / upsert-kafka)"],
    [["INNER, insert-only sources","[I]","OK — 1 record/key","OK — 1 row/key"],
     ["INNER over CDC/updating","[I,UA,D]","rejected","folds by key"],
     ["LEFT / outer join","[I,UA,D]","rejected","folds, ~2–3× changelog events"]],
    "Duplicates depend on changelog mode + sink (this is Flink semantics)",
    colcolors={2:KAFKA}, widths=[0.26,0.15,0.22,0.37], fontsize=10.5)

# t06 — savepoint + recovery
render("t06_savepoint_recovery.png",
    ["Strategy","Savepoint size","Recovery (submit → RUNNING, restored)"],
    [["Stream-stream (no Fluss)","242 MB","6.7 s"],
     ["Delta join (Fluss)","28 KB","7.0 s"],
     ["Lookup join (Fluss)","20 KB","6.7 s"]],
    "Savepoint size + recovery time",
    colcolors={1:FLUSS}, widths=[0.36,0.28,0.36])

if __name__ == "__main__":
    print("done — 6 table images in post/img/")
