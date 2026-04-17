# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

The repo is split into two top-level directories by intent, plus top-level HTML deliverables.

```
openintel_data/
├── analysis/              # generated analysis code + results (commit this)
│   ├── scripts/           # Python analysis scripts + export_metrics.py, share config.py
│   ├── output/            # flat PNG charts from scripts 01–10 (gitignored)
│   ├── deep_analysis/     # per-step dirs from 11_deep_analysis.py (22 steps)
│   ├── network_analysis/  # per-step dirs from 12/12b (25 steps, 7 phases)
│   ├── docs/              # data_catalog.json emitted by 00_data_catalog.py
│   └── web/               # Astro 5 + ECharts 5 + Tailwind v4 frontend (31 pages)
├── downloads/             # downloaded source data (gitignored, ~12 GB)
│   ├── openintel/zone/{ch,ee,fr,gov,li,nu,se,sk,root}/*.parquet
│   ├── openintel/toplist/{tranco,umbrella,radar,majestic}/*.parquet
│   └── common-crawl/{cluster.idx, webgraph/*.gz}
├── dist/web/              # Astro build output (gitignored)
├── presentation.html      # legacy 34-page report (standalone, retained as archive)
└── tutorial.html          # legacy 47-page interactive tutorial (retained)
```

`README.md` may still reference the pre-reorg paths (`data/`, `scripts/`, top-level `output/`); trust this file, not the README.

## Common commands

Install dependencies (add `networkx igraph scipy powerlaw scikit-learn` for script 12):
```bash
pip install duckdb pandas pyarrow matplotlib seaborn boto3 \
            networkx igraph scipy powerlaw scikit-learn
```

Scripts are run individually, not through a harness. They must be invoked from the repo root (or with an absolute path) so the `__file__`-derived path resolution works:
```bash
python3 analysis/scripts/00_data_catalog.py        # catalog + schema dump → analysis/docs/data_catalog.json
python3 analysis/scripts/01_overview.py            # records/domains per TLD → analysis/output/*.png
python3 analysis/scripts/11_deep_analysis.py       # 22-step deep analysis → analysis/deep_analysis/step_NN_<name>/
python3 analysis/scripts/12_network_analysis.py    # steps 1–17 of complex-network analysis
python3 analysis/scripts/12b_network_continue.py   # steps 18–25, rebuilds edge tables from parquet (no pickled state)
```

No tests, no linters, no build step. Scripts either succeed end-to-end or fail loudly; verify by inspecting the printed tables and the generated `result.txt` / PNGs.

## Architecture

### Two path-resolution conventions

Scripts 00–10 share `analysis/scripts/config.py`, which exports `BASE_DIR` (=`analysis/`), `REPO_DIR` (=repo root), `DATA_DIR`, `ZONE_DIR`, `TOPLIST_DIR`, `OUTPUT_DIR`, plus helpers: `zone_glob(tld)`, `toplist_glob(name)`, `all_zone_sql()` (a pre-joined `'path','path',…` string ready to drop inside DuckDB `read_parquet([…])`). Scripts 08–10 additionally derive `REPO_DIR / "downloads" / "common-crawl"` — always route new common-crawl paths through `REPO_DIR`, never `BASE_DIR`.

Scripts 11, 12, 12b are standalone — they re-declare their own `BASE = __file__.parent.parent` (= `analysis/`) and `REPO = BASE.parent`, then compute `DATA`, `CC_DIR`, `OUT` inline. They do **not** import from `config.py`. When editing any of these four path-resolution blocks, keep both conventions consistent with the current two-tier layout.

### DuckDB-over-parquet query pattern

All heavy analysis runs against parquet files directly via DuckDB — no database loads, no intermediate materializations. Typical shape:
```python
conn = get_conn()   # duckdb.connect(); SET threads TO 4
conn.execute(f"""
    SELECT … FROM read_parquet('{zone_glob("gov")}')
    WHERE query_type='A' AND ip4_address IS NOT NULL
""").fetchdf()
```
Scripts 11 and 12 also set `SET memory_limit='4GB'`/`'6GB'` and build `CREATE OR REPLACE TABLE` edge tables (domain→NS, domain→IP→AS, CNAME chains) that are reused across later steps within the same connection. 12b's first action is to rebuild those tables from parquet because state doesn't persist across script invocations.

### Output conventions

- `save_fig("name")` (from `config.py`) always writes to `analysis/output/name.png` and calls `plt.close()`. matplotlib backend is forced to `Agg` — scripts are headless.
- Scripts 11 and 12 bypass `save_fig` and write per-step artifacts into `analysis/{deep,network}_analysis/step_NN_<slug>/{result.txt,chart.png}` via their own `step_dir`/`save`/`savefig` helpers. Final report is `summary_report.md` in the same parent dir.
- Shared constant `STATUS_CODE_MAP` maps DNS rcodes (0=NOERROR, 2=SERVFAIL, 3=NXDOMAIN, 65533=TIMEOUT, …) used in overview and anomaly scripts.

### Script numbering signals dependency

Scripts are numbered 00–12b in roughly narrative order — later scripts read data written or assumed by earlier ones. 00 emits `analysis/docs/data_catalog.json`; 07 cross-references TopList against ccTLD zones; 08/09/10 pull in Common Crawl WebGraph + CDX; 11 runs a 22-step narrative analysis; 12+12b run a 25-step complex-network analysis targeting IMC/SIGCOMM-tier venues (see `analysis/network_analysis/research_plan.md`). `12_network_analysis.py` is ~86 KB and was deliberately split: when extending step 18+, edit `12b_network_continue.py`, not `12`.

### Data scale to keep in mind

~232 M DNS records / ~33 M unique domains / 99 columns across 8 ccTLDs + root + 4 toplists, plus 134 M Common Crawl WebGraph domains. Don't `SELECT *` without a `LIMIT`. A full scan across all zones with `read_parquet([{all_zone_sql()}])` is routine but takes tens of seconds — acceptable for one-shot analysis, not for tight loops.

## Data sources

Raw parquet is downloaded from OpenINTEL's unsigned S3-compatible endpoint (`https://object.openintel.nl`, bucket `openintel-public`, prefix `fdns/basis={zonefile|toplist}/source=<src>/year=YYYY/month=MM/day=DD/`). See `00_data_catalog.py` for the canonical boto3 snippet and the CC-BY-NC-SA-4.0 license notice. Common Crawl data under `downloads/common-crawl/` comes from `https://data.commoncrawl.org/` (CC-MAIN-2026-12 in the committed scripts).

## Frontend site — `analysis/web/`

The web presentation layer is an **Astro 5 SSG** with **ECharts 5** for interactive charts, **Tailwind CSS v4** (Oxide engine, `@theme` tokens in `src/styles/global.css`), and Astro's **View Transitions** for silky page swaps. It ships **1 home page + 30 sub-pages** covering all 81 analysis charts with click-to-reveal annotation drawers and keyboard navigation (`←/→`, `g`, `d`, `Esc`).

### Running

```bash
cd analysis/web
pnpm install                # ~20s first time
pnpm dev                    # http://localhost:4321, auto-runs sync-charts pre-hook
pnpm build                  # → ../../dist/web/   (relative-pathed, no CDN at runtime)
pnpm preview                # serve the build
pnpm astro check            # tsc-equivalent; must be 0 errors
```

Data pipeline (**run after analysis scripts update**):
```bash
python3 analysis/scripts/export_metrics.py                 # charts + annotations
python3 analysis/scripts/export_metrics.py --annotations   # fast, no DuckDB
python3 analysis/scripts/export_metrics.py --charts        # only the 10 Tier-A aggregates (~2 min)
```

### Architecture

- **Navigation map** is declared in `analysis/web/src/lib/navigation.ts` (`TIERS` = 7 tiers, 30 `SubPage`s). Every sub-page page file calls `<SectionLayout path="...">` which reads this registry to render breadcrumb, page order, and prev/next cards.
- **Charts** are either **interactive ECharts** (18 hero charts, data sourced from `src/data/charts/<id>.json`, built into an option via a function in `src/lib/chart-builders.ts`, mounted by `<EChart>` → the `ResizeObserver`-enabled client runtime in `src/lib/echarts-runtime.ts`) or **PNG images** (63 charts, served from `public/charts/` and wrapped in `<PngChart>`). Every chart, interactive or PNG, is wrapped in `<ChartCard>` which gives it the bold title + "图解" button.
- **Annotations** are bundled at build time from `src/data/annotations_bundle.json` (generated by `export_metrics.py`). `<AnnotationDrawer>` is rendered once per page by `SectionLayout` and looks up by chart id. Drawer opens via button click, `d` key, or `?chart=<id>&details=1` deep link.
- **Chart-PNG sync**: `analysis/web/scripts/sync-charts.mjs` (a Node script, not Astro) runs automatically in the `predev` / `prebuild` hook and copies PNGs from `analysis/output/`, `analysis/deep_analysis/step_*/`, and `analysis/network_analysis/step_*/` into `public/charts/{output,deep,network}/`. `public/charts/` is gitignored — it's a pure projection.
- **ECharts bundle** is imported modularly in `src/lib/echarts-runtime.ts` (only charts + components actually used). The client chunk is ~260 KB gzipped, loaded only on pages that embed `<EChart>`.
- **Theme tokens** in `src/styles/global.css` (e.g. `--color-accent: #0071e3`) are auto-exposed as Tailwind v4 utilities (`bg-accent`, `text-text-0`, etc.). `src/lib/echarts-theme.ts` mirrors the same palette inside ECharts via `echarts.registerTheme("openintel", THEME)`.

### Adding / editing a sub-page

1. Add the entry to `TIERS` in `src/lib/navigation.ts` (sets order, slug, chart counts — drives TOC + breadcrumb + prev/next).
2. Create `src/pages/<tier>/<slug>.astro` that wraps `<SectionLayout path="...">` and emits `<ChartCard>` elements (≤ 3 per page).
3. For a new interactive chart: add its aggregation to `export_metrics.py` (`TIER_A_CHARTS` list pattern), run `python3 analysis/scripts/export_metrics.py --charts`, then write a builder in `src/lib/chart-builders.ts` and import `src/data/charts/<id>.json` in the page.
4. For annotations: hand-edit `TIER_A_ANNOTATIONS` or `OUTPUT_PNG_ANNOTATIONS` in `export_metrics.py`, then `--annotations`. Step-dir `result.txt` files are parsed automatically; the matching id is `deep_<NN>` or `net_<NN>`.
