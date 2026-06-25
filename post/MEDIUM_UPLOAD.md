# Publishing to Medium

Two paths. The **import** path is easiest — one URL brings in everything (charts AND
tables) as images.

## Option A — Import from GitHub Pages (recommended)

Medium's importer needs a *rendered* public page (not raw HTML / a GitHub blob view).
GitHub Pages serves exactly that, and `docs/index.html` is built for it: all 6 tables
are already swapped for table images, and every image points at an absolute Pages URL,
so Medium fetches them during import.

1. **Enable GitHub Pages** on the repo: Settings → Pages → Source = "Deploy from a
   branch", Branch = `main`, Folder = `/docs`. Wait ~1 min.
   Page goes live at: **https://fmorillo7694.github.io/fluss-kafka-bench/**
2. Confirm it renders (charts + table-images all load).
3. In Medium: **Profile → Stories → Import a story** (or `https://medium.com/p/import`),
   paste the Pages URL, click Import.
4. Medium converts it to a draft with all 14 images inline. Skim for spacing, set the
   title/subtitle, publish.

> If you fork/rename the repo, rebuild `docs/` with the new Pages URL:
> ```bash
> ./scripts/build-pages.sh https://<user>.github.io/<repo>
> ```

## Option B — Manual paste

Paste the prose from [`fluss-vs-kafka.md`](fluss-vs-kafka.md) into Medium's editor and
drop in each image (`+` → image) at the marked spot. Image order:

| # | Image | Section |
|---|-------|---------|
| 1 | `05_scorecard.png` | TL;DR scorecard |
| 2 | `01_delta_join_cardinality_sweep.png` | §2 checkpoint-vs-cardinality chart |
| 3 | `t01_cardinality_sweep.png` | §2 cardinality table |
| 4 | `06_state_100mb_comparison.png` | §2 100 MB-load chart |
| 5 | `t02_state_100mb.png` | §2 100 MB-load table |
| 6 | `04_throughput_and_replay.png` | §4a ingest + replay chart |
| 7 | `t03_scenarios.png` | §4a scenarios table |
| 8 | `02_left_vs_lookup_events.png` | §8 LEFT vs lookup chart |
| 9 | `t04_left_vs_lookup.png` | §8 LEFT vs lookup table |
| 10 | `03_duplicate_semantics_by_sink.png` | §8 duplicate-by-sink chart |
| 11 | `t05_duplicate_sink_rule.png` | §9 duplicate/sink rule table |
| 12 | `07_resources_cpu_mem.png` | §9 CPU/memory chart |
| 13 | `08_savepoint_recovery.png` | §9 savepoint chart |
| 14 | `t06_savepoint_recovery.png` | §9 savepoint + recovery table |

## Files
- `fluss-vs-kafka.md` — prose source (manual paste).
- `fluss-vs-kafka.html` — standalone styled page; native HTML tables (for a personal site).
- `../docs/index.html` — GitHub Pages build for Medium import (tables → images, absolute URLs).
- `img/01–08*.png` — chart images. `img/t01–t06*.png` — table images.

## Regenerate
```bash
python3 -m venv .chartenv && .chartenv/bin/pip install matplotlib pandas
.chartenv/bin/python scripts/make_charts.py    # 8 chart PNGs
.chartenv/bin/python scripts/make_tables.py    # 6 table PNGs
./scripts/build-pages.sh https://fmorillo7694.github.io/fluss-kafka-bench   # rebuild docs/
```
