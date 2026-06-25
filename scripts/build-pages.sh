#!/usr/bin/env bash
# Build docs/ (GitHub Pages) for Medium import: HTML tables -> table images, image
# srcs -> absolute Pages URLs so Medium fetches them on import.
#
# Usage: ./scripts/build-pages.sh https://<user>.github.io/<repo>
set -euo pipefail
cd "$(dirname "$0")/.."
PAGES="${1:?usage: build-pages.sh https://<user>.github.io/<repo>}"
PAGES="${PAGES%/}"

mkdir -p docs/img
cp post/img/*.png docs/img/
touch docs/.nojekyll

python3 - "$PAGES" <<'PY'
import sys, re
pages=sys.argv[1]
html=open('post/fluss-vs-kafka.html').read()
imgs=[
 ("t01_cardinality_sweep.png","Delta-join vs stream-stream checkpoint state, by cardinality"),
 ("t02_state_100mb.png","Same 1.5M-key join, three ways — where the state lives"),
 ("t03_scenarios.png","Measured scenarios"),
 ("t04_left_vs_lookup.png","LEFT stream-stream join vs lookup join"),
 ("t05_duplicate_sink_rule.png","Duplicates depend on changelog mode + sink (Flink semantics)"),
 ("t06_savepoint_recovery.png","Savepoint size + recovery time"),
]
parts=re.split(r'(<table>.*?</table>)', html, flags=re.S)
out=[]; i=0
for p in parts:
    if p.startswith('<table>'):
        n,c=imgs[i]; i+=1
        out.append(f'<figure>\n    <img src="{pages}/img/{n}" alt="{c}">\n    <figcaption>{c}</figcaption>\n  </figure>')
    else:
        out.append(p)
assert i==6, f"expected 6 tables, got {i}"
res=''.join(out).replace('src="img/', f'src="{pages}/img/')
open('docs/index.html','w').write(res)
print(f"docs/index.html built | tables->images: {i} | total img refs: {res.count(pages+'/img/')}")
PY
echo ">> Pages build ready in docs/  (enable: Settings->Pages->main /docs)"
