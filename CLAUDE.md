# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

The repo is split into two top-level directories by intent, plus top-level HTML deliverables.

```
openintel_data/
├── analysis/              # generated analysis code + results (commit this)
│   ├── scripts/           # all Python analysis scripts, share config.py
│   ├── output/            # flat PNG charts from scripts 01–10 (gitignored)
│   ├── deep_analysis/     # per-step dirs from 11_deep_analysis.py (22 steps)
│   ├── network_analysis/  # per-step dirs from 12/12b (25 steps, 7 phases)
│   └── docs/              # data_catalog.json emitted by 00_data_catalog.py
├── downloads/             # downloaded source data (gitignored, ~12 GB)
│   ├── openintel/zone/{ch,ee,fr,gov,li,nu,se,sk,root}/*.parquet
│   ├── openintel/toplist/{tranco,umbrella,radar,majestic}/*.parquet
│   └── common-crawl/{cluster.idx, webgraph/*.gz}
├── presentation.html      # 34-page report (standalone, no build step)
└── tutorial.html          # 47-page interactive tutorial
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
