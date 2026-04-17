# Phase 1 — Horizontal Coverage Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-populate the empty `downloads/` tree, broaden the OpenINTEL ccTLD set (8 → up to 15), ingest Common Crawl CC-MAIN-2026-12 host-level WebGraph, integrate the already-downloaded RIR rDNS pytricia parquet as a new analysis dimension, and rerun scripts 00–12b + frontend build.

**Architecture:** All new code lives under `analysis/scripts/`; existing scripts 01–12b get minimal surgical edits (new step insertions, not rewrites). Long-running scripts (11, 12, 12b) get an opt-in checkpoint layer. RIR integration is purely additive (no regression risk to existing steps). Frontend gains two new sub-pages.

**Tech Stack:** DuckDB 1.x, boto3 (unsigned for OpenINTEL), requests (for CC HTTPS), pandas, matplotlib, networkx/igraph, Astro 5 + ECharts 5 + Tailwind v4.

**Snapshot date:** 2026-04-10 (matches committed `analysis/docs/data_catalog.json`).

---

## File Structure

### New files

| Path | Responsibility | LOC |
|---|---|---|
| `analysis/scripts/download_data.py` | CLI downloader with `openintel`/`common-crawl`/`verify` subcommands, MANIFEST.json-based resume | ~250 |
| `analysis/scripts/rir_rdns.py` | Shared module: load RIR pytricia parquet → DuckDB table, expose SQL fragment for `IP → (rname, source)` prefix join | ~120 |
| `analysis/scripts/_checkpoint.py` | `already_done(step_dir) / mark_done(step_dir)` helpers + edge-cache DuckDB attach | ~60 |
| `analysis/scripts/13_rir_enrichment.py` | New analysis: RIR delegation distribution, rDNS coverage, hoster naming pattern clustering | ~280 |
| `analysis/web/src/pages/tier-3-extra/rir-enrichment.astro` | Frontend page for RIR dimensions | ~90 |
| `analysis/web/src/pages/tier-4-extra/host-graph.astro` | Frontend page for host-level WebGraph | ~90 |

### Modified files

| Path | Edit |
|---|---|
| `analysis/scripts/config.py` | Add `CC_DIR`, `HOST_GRAPH_DIR`, `RIR_DIR`, `rir_glob()` |
| `analysis/scripts/00_data_catalog.py` | Add RIR rDNS and host-graph entries to catalog |
| `analysis/scripts/08_webgraph_analysis.py` | Add `--level=host\|domain` flag; host mode reads new graph files |
| `analysis/scripts/09_pagerank_vs_toplist.py` | Replace local PageRank compute with `host-ranks.txt.gz` lookup; keep domain PR as legacy |
| `analysis/scripts/11_deep_analysis.py` | Insert step_05b (rDNS pattern clustering); wrap all 22 steps with checkpoint; bump `SET memory_limit='12GB'` |
| `analysis/scripts/12_network_analysis.py` | Wrap steps with checkpoint; attach persisted edge-cache DuckDB |
| `analysis/scripts/12b_network_continue.py` | Wrap steps with checkpoint; reuse edge-cache from 12 |
| `analysis/scripts/export_metrics.py` | Add 2 Tier-A charts (RIR delegation pie, host-ranks vs toplist scatter) + ~10 annotations |
| `analysis/web/src/lib/navigation.ts` | Add 2 `SubPage` entries under tier 3 and tier 4 |
| `analysis/web/src/lib/chart-builders.ts` | Add 2 builders: `buildRirDelegation`, `buildHostRanksToplist` |

### Downloads (gitignored)

```
downloads/openintel/zone/{ch,dk,ee,fr,gov,ie,li,nl,no,nu,ru,se,sk,us,at,root}/*.parquet
downloads/openintel/toplist/{tranco,umbrella,radar,majestic}/*.parquet
downloads/common-crawl/cluster.idx
downloads/common-crawl/webgraph/domain/*.gz
downloads/common-crawl/webgraph/host/*.gz
downloads/MANIFEST.json
```

---

## Task 1: Update `config.py` for new paths

**Files:**
- Modify: `analysis/scripts/config.py`

- [ ] **Step 1: Add CC and RIR constants**

Open `analysis/scripts/config.py`. After the existing `OUTPUT_DIR.mkdir(...)` line, add:

```python
# ── Common Crawl ─────────────────────────────────────
CC_DIR = REPO_DIR / "downloads" / "common-crawl"
WG_DIR = CC_DIR / "webgraph"
WG_DOMAIN_DIR = WG_DIR / "domain"
WG_HOST_DIR = WG_DIR / "host"

# ── RIR rDNS (already under data/rir-data, not downloads/) ─────────────
RIR_DIR = REPO_DIR / "data" / "rir-data" / "rirs-rdns-formatted" / "type=enriched"

def rir_glob(year="2026", month="03", day="29") -> str:
    """Default to 2026-03-29 (closest snapshot to 2026-04-10 DNS data)."""
    return str(RIR_DIR / f"year={year}" / f"month={month}" / f"day={day}" / "hour=00" / "*.parquet")
```

- [ ] **Step 2: Make ZONE_TLDS tolerant of absent directory**

Replace:
```python
ZONE_TLDS = sorted(
    [d.name for d in ZONE_DIR.iterdir()
     if d.is_dir() and d.name != "root" and any(d.glob("*.parquet"))]
)
```
with:
```python
ZONE_TLDS = sorted(
    [d.name for d in ZONE_DIR.iterdir()
     if d.is_dir() and d.name != "root" and any(d.glob("*.parquet"))]
) if ZONE_DIR.exists() else []
```
Do the same wrap for `TOPLISTS`. This lets `config.py` be imported before any data is downloaded (needed by `download_data.py` itself).

- [ ] **Step 3: Verify import still works**

```bash
python3 -c "from analysis.scripts import config; print('ZONES=', config.ZONE_TLDS, 'CC=', config.CC_DIR)"
```
Expected: `ZONES= [] CC= /Volumes/data/openintel-dns-analysis/downloads/common-crawl`

- [ ] **Step 4: Commit**

```bash
git add analysis/scripts/config.py
git commit -m "config: add CC_DIR/RIR_DIR and tolerate empty downloads"
```

---

## Task 2: Build `download_data.py`

**Files:**
- Create: `analysis/scripts/download_data.py`

- [ ] **Step 1: Skeleton with argparse**

Create `analysis/scripts/download_data.py`. Top of file:

```python
#!/usr/bin/env python3
"""Download OpenINTEL parquet + Common Crawl webgraph data.

Usage:
  python3 analysis/scripts/download_data.py openintel --date 2026-04-10
  python3 analysis/scripts/download_data.py common-crawl --crawl CC-MAIN-2026-12 --host-graph
  python3 analysis/scripts/download_data.py verify
"""
import argparse, json, sys, hashlib, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import requests

REPO = Path(__file__).resolve().parent.parent.parent
DOWNLOADS = REPO / "downloads"
MANIFEST = DOWNLOADS / "MANIFEST.json"

OPENINTEL_ENDPOINT = "https://object.openintel.nl"
OPENINTEL_BUCKET = "openintel-public"
CC_BASE = "https://data.commoncrawl.org"

ZONE_CANDIDATES = ["ch","dk","ee","fr","gov","ie","li","nl","no","nu","ru","se","sk","us","at","root"]
TOPLISTS = ["tranco","umbrella","radar","majestic"]
```

- [ ] **Step 2: MANIFEST load/save helpers**

```python
def load_manifest() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {"openintel": {}, "common_crawl": {}, "failed": []}

def save_manifest(m: dict):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(m, indent=2, sort_keys=True))
```

- [ ] **Step 3: OpenINTEL S3 client + list/download functions**

```python
def oi_client():
    return boto3.client(
        "s3",
        endpoint_url=OPENINTEL_ENDPOINT,
        config=Config(signature_version=UNSIGNED, retries={"max_attempts": 3}),
    )

def oi_list(s3, basis: str, source: str, date: str) -> list[tuple[str, int]]:
    """Return [(key, size), ...]. basis = 'zonefile' | 'toplist'. date = 'YYYY-MM-DD'."""
    y, m, d = date.split("-")
    prefix = f"fdns/basis={basis}/source={source}/year={y}/month={m}/day={d}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=OPENINTEL_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append((obj["Key"], obj["Size"]))
    return keys

def oi_download_one(s3, key: str, size: int, dest: Path) -> tuple[str, bool, str]:
    """Download one key. Returns (key, ok, msg). Skips if dest exists with correct size."""
    if dest.exists() and dest.stat().st_size == size:
        return (key, True, "skip-exists")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    for attempt in range(3):
        try:
            s3.download_file(OPENINTEL_BUCKET, key, str(tmp))
            tmp.rename(dest)
            return (key, True, f"downloaded {size/1e6:.1f} MB")
        except Exception as e:
            if attempt == 2:
                return (key, False, f"failed: {e}")
            time.sleep(2 ** attempt * 2)
```

- [ ] **Step 4: `openintel` subcommand**

```python
def cmd_openintel(args):
    s3 = oi_client()
    manifest = load_manifest()
    all_jobs = []   # list[(category, source, key, size, dest)]
    # Zones
    for tld in ZONE_CANDIDATES:
        keys = oi_list(s3, "zonefile", tld, args.date)
        if not keys:
            print(f"  [skip] tld={tld} reason=no objects on {args.date}")
            continue
        for key, size in keys:
            fname = key.rsplit("/", 1)[1]
            dest = DOWNLOADS / "openintel" / "zone" / tld / fname
            all_jobs.append(("zone", tld, key, size, dest))
        print(f"  [list] tld={tld} files={len(keys)} total={sum(s for _,s in keys)/1e6:.0f} MB")
    # Toplists
    for tl in TOPLISTS:
        keys = oi_list(s3, "toplist", tl, args.date)
        for key, size in keys:
            fname = key.rsplit("/", 1)[1]
            dest = DOWNLOADS / "openintel" / "toplist" / tl / fname
            all_jobs.append(("toplist", tl, key, size, dest))
        print(f"  [list] toplist={tl} files={len(keys)} total={sum(s for _,s in keys)/1e6:.0f} MB")

    total_bytes = sum(j[3] for j in all_jobs)
    print(f"\nTotal: {len(all_jobs)} files, {total_bytes/1e9:.2f} GB\n")

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(oi_download_one, s3, k, sz, dest): (cat, src, k) for cat, src, k, sz, dest in all_jobs}
        for fut in as_completed(futures):
            cat, src, k = futures[fut]
            key, ok, msg = fut.result()
            status = "OK" if ok else "FAIL"
            print(f"  [{status}] {cat}/{src}/{k.rsplit('/',1)[1]}  {msg}")
            if ok:
                manifest["openintel"][key] = {"size": next(j[3] for j in all_jobs if j[2] == key), "cat": cat, "src": src}
            else:
                manifest["failed"].append({"key": key, "msg": msg})
    save_manifest(manifest)
```

- [ ] **Step 5: `common-crawl` subcommand**

```python
CC_FILES_DOMAIN = [
    "cc-main-2026-12-domain-vertices.paths.gz",
    "cc-main-2026-12-domain-edges.paths.gz",
    "cc-main-2026-12-domain-ranks.txt.gz",
]
CC_FILES_HOST = [
    "cc-main-2026-12-host-vertices.paths.gz",
    "cc-main-2026-12-host-edges.paths.gz",
    "cc-main-2026-12-host-ranks.txt.gz",
]

def cc_url(crawl: str, filename: str) -> str:
    # e.g. https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-feb-mar-apr/domain/cc-main-2026-12-domain-vertices.paths.gz
    # Actual path layout varies by crawl; this fn centralizes the guess.
    period = "cc-main-2026-feb-mar-apr"  # for CC-MAIN-2026-12; adjust for other crawls
    level = "domain" if "domain" in filename else "host"
    return f"{CC_BASE}/projects/hyperlinkgraph/{period}/{level}/{filename}"

def http_download(url: str, dest: Path, expected_size: int | None = None) -> tuple[bool, str]:
    if dest.exists() and (expected_size is None or dest.stat().st_size == expected_size):
        return (True, "skip-exists")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            tmp.rename(dest)
            return (True, f"downloaded {dest.stat().st_size/1e6:.1f} MB")
        except Exception as e:
            if attempt == 2:
                return (False, f"failed: {e}")
            time.sleep(2 ** attempt * 2)

def cmd_common_crawl(args):
    manifest = load_manifest()
    # cluster.idx
    cluster_url = f"{CC_BASE}/cc-index/collections/{args.crawl}/indexes/cluster.idx"
    cluster_dest = DOWNLOADS / "common-crawl" / "cluster.idx"
    ok, msg = http_download(cluster_url, cluster_dest)
    print(f"  [{'OK' if ok else 'FAIL'}] cluster.idx  {msg}")
    # Graph files
    files = list(CC_FILES_DOMAIN)
    if args.host_graph:
        files += CC_FILES_HOST
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        for fn in files:
            url = cc_url(args.crawl, fn)
            level = "domain" if "domain" in fn else "host"
            dest = DOWNLOADS / "common-crawl" / "webgraph" / level / fn
            futures[ex.submit(http_download, url, dest)] = fn
        for fut in as_completed(futures):
            fn = futures[fut]
            ok, msg = fut.result()
            print(f"  [{'OK' if ok else 'FAIL'}] {fn}  {msg}")
            if ok:
                manifest["common_crawl"][fn] = {"crawl": args.crawl}
            else:
                manifest["failed"].append({"key": fn, "msg": msg})
    save_manifest(manifest)
```

- [ ] **Step 6: `verify` subcommand + argparse wiring**

```python
def cmd_verify(args):
    manifest = load_manifest()
    missing = []
    for key, meta in manifest.get("openintel", {}).items():
        fname = key.rsplit("/", 1)[1]
        if meta["cat"] == "zone":
            dest = DOWNLOADS / "openintel" / "zone" / meta["src"] / fname
        else:
            dest = DOWNLOADS / "openintel" / "toplist" / meta["src"] / fname
        if not dest.exists() or dest.stat().st_size != meta["size"]:
            missing.append(key)
    for key in manifest.get("common_crawl", {}):
        # scan both domain/ and host/
        found = any((DOWNLOADS / "common-crawl" / "webgraph" / lvl / key).exists() for lvl in ("domain","host"))
        if not found:
            missing.append(key)
    if missing:
        print(f"MISSING {len(missing)} files")
        for k in missing[:10]:
            print(f"  {k}")
        sys.exit(1)
    print(f"OK: {len(manifest['openintel'])} openintel + {len(manifest['common_crawl'])} cc files present")
    if manifest["failed"]:
        print(f"WARN: {len(manifest['failed'])} failed entries in manifest")

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s1 = sub.add_parser("openintel"); s1.add_argument("--date", required=True); s1.set_defaults(func=cmd_openintel)
    s2 = sub.add_parser("common-crawl"); s2.add_argument("--crawl", required=True); s2.add_argument("--host-graph", action="store_true"); s2.set_defaults(func=cmd_common_crawl)
    s3 = sub.add_parser("verify"); s3.set_defaults(func=cmd_verify)
    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Smoke test — list-only (no actual download) of one TLD**

Run in a Python shell (from repo root):
```python
import sys; sys.path.insert(0, "analysis/scripts")
from download_data import oi_client, oi_list
s3 = oi_client()
print(oi_list(s3, "zonefile", "ch", "2026-04-10")[:2])
```
Expected: 2 tuples of `(key, size)` with size > 0. If empty, check date format or S3 endpoint.

- [ ] **Step 8: Commit**

```bash
git add analysis/scripts/download_data.py
git commit -m "feat: add download_data.py for OpenINTEL + Common Crawl"
```

---

## Task 3: Execute downloads

**Files:** (none — only `downloads/` changes, gitignored)

- [ ] **Step 1: Dry-run — list total size for each TLD**

Quick script to estimate total volume (paste into Python REPL):
```python
import sys; sys.path.insert(0, "analysis/scripts")
from download_data import oi_client, oi_list, ZONE_CANDIDATES, TOPLISTS
s3 = oi_client()
for tld in ZONE_CANDIDATES:
    keys = oi_list(s3, "zonefile", tld, "2026-04-10")
    print(f"{tld:6s} {len(keys):3d} files  {sum(s for _,s in keys)/1e6:8.0f} MB")
for tl in TOPLISTS:
    keys = oi_list(s3, "toplist", tl, "2026-04-10")
    print(f"{tl:10s} {len(keys):3d} files  {sum(s for _,s in keys)/1e6:8.0f} MB")
```
Record totals. If any TLD shows 0 files, confirm it simply won't be available (expected for some of dk/ru/no/us).

- [ ] **Step 2: Run OpenINTEL download**

```bash
python3 analysis/scripts/download_data.py openintel --date 2026-04-10 2>&1 | tee /tmp/openintel_download.log
```
Expected: ~10–30 min depending on bandwidth. Log shows `[OK]` for each file. At end, `downloads/openintel/zone/` has 9 original TLD dirs + some subset of new ones.

- [ ] **Step 3: Run Common Crawl download**

```bash
python3 analysis/scripts/download_data.py common-crawl --crawl CC-MAIN-2026-12 --host-graph 2>&1 | tee /tmp/cc_download.log
```
Expected: ~1–3 hours depending on bandwidth. At end, `downloads/common-crawl/webgraph/{domain,host}/` each contain 3 `.gz` files.

If a specific CC URL 404s, the `cc_url()` path guess in Step 5 of Task 2 may need adjusting — check `https://data.commoncrawl.org/projects/hyperlinkgraph/` in a browser for the correct period slug.

- [ ] **Step 4: Verify**

```bash
python3 analysis/scripts/download_data.py verify
du -sh downloads/ downloads/openintel/zone downloads/openintel/toplist downloads/common-crawl
```
Expected: `OK: N openintel + M cc files present`. Total size ~20–40 GB.

- [ ] **Step 5: Commit MANIFEST only (not the data)**

```bash
# MANIFEST.json is not gitignored (only downloads/*.{parquet,gz,idx} are); check .gitignore first
cat .gitignore | grep downloads
# If MANIFEST.json would be gitignored, add an exception:
# echo "!downloads/MANIFEST.json" >> .gitignore
git add downloads/MANIFEST.json .gitignore 2>/dev/null || true
git commit -m "data: record MANIFEST.json for 2026-04-10 snapshot + CC-MAIN-2026-12" || echo "nothing to commit"
```

---

## Task 4: Build `rir_rdns.py` shared module

**Files:**
- Create: `analysis/scripts/rir_rdns.py`

- [ ] **Step 1: Write the module**

```python
"""RIR rDNS prefix tree — shared loader for scripts 11, 12, 13.

Source: data/rir-data/rirs-rdns-formatted/type=enriched/...
Schema: prefix (VARCHAR), rname, rdata, source (APNIC/ARIN/RIPE/LACNIC/AFRINIC), ...
"""
from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parent.parent.parent
DEFAULT_SNAPSHOT = REPO / "data" / "rir-data" / "rirs-rdns-formatted" / "type=enriched" / "year=2026" / "month=03" / "day=29" / "hour=00"

def load_rir_prefix(conn: duckdb.DuckDBPyConnection, snapshot_dir: Path | None = None) -> int:
    """Create TABLE rir_prefix(prefix, rname, rir_source) in conn. Returns row count."""
    snap = snapshot_dir or DEFAULT_SNAPSHOT
    parquets = list(snap.glob("*.parquet"))
    if not parquets:
        raise FileNotFoundError(f"No RIR parquet under {snap}")
    # NS records are the common case for delegation; PTR when available adds rDNS name
    # We keep the raw rtype so callers can filter.
    paths = ", ".join(f"'{p}'" for p in parquets)
    conn.execute(f"""
        CREATE OR REPLACE TABLE rir_prefix AS
        SELECT
            prefix,
            start_address,
            end_address,
            rname,
            rdata,
            rtype,
            source AS rir_source
        FROM read_parquet([{paths}])
        WHERE af = 4   -- v4 only for Phase 1
    """)
    n = conn.execute("SELECT count(*) FROM rir_prefix").fetchone()[0]
    # Index by start_address as int for fast prefix lookup
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rir_start ON rir_prefix(start_address)")
    return n

def lookup_ip_to_rdns_sql(ip_col: str) -> str:
    """Return a SQL fragment that joins rir_prefix to an outer query's IP column.

    Usage:
        SELECT d.query_name, r.rname, r.rir_source
        FROM read_parquet([...]) d
        LEFT JOIN rir_prefix r
        ON {lookup_ip_to_rdns_sql('d.ip4_address')}
    """
    return f"""
    (
        host({ip_col}::INET) BETWEEN host(r.start_address::INET) AND host(r.end_address::INET)
    )
    """

def coverage_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Quick summary of the loaded RIR table."""
    r = conn.execute("SELECT count(*), count(DISTINCT rname), count(DISTINCT rir_source) FROM rir_prefix").fetchone()
    by_rir = dict(conn.execute("SELECT rir_source, count(*) FROM rir_prefix GROUP BY rir_source").fetchall())
    return {"rows": r[0], "unique_rnames": r[1], "rir_sources": r[2], "by_rir": by_rir}
```

- [ ] **Step 2: Smoke test**

```bash
python3 -c "
import sys; sys.path.insert(0, 'analysis/scripts')
import duckdb
from rir_rdns import load_rir_prefix, coverage_stats
conn = duckdb.connect()
n = load_rir_prefix(conn)
print(f'Loaded {n:,} prefix rows')
print(coverage_stats(conn))
"
```
Expected:
```
Loaded 3,703,402 prefix rows
{'rows': 3703402, 'unique_rnames': ~1.5M, 'rir_sources': 5, 'by_rir': {'APNIC': ..., 'ARIN': ..., 'RIPE': ..., 'LACNIC': ..., 'AFRINIC': ...}}
```

- [ ] **Step 3: Commit**

```bash
git add analysis/scripts/rir_rdns.py
git commit -m "feat: add rir_rdns.py shared module for prefix lookup"
```

---

## Task 5: Build `_checkpoint.py`

**Files:**
- Create: `analysis/scripts/_checkpoint.py`

- [ ] **Step 1: Write helpers**

```python
"""Step-level checkpoint for long-running analysis scripts (11, 12, 12b).

Usage:
    from _checkpoint import done, mark
    if done(step_dir): continue
    ... run step ...
    mark(step_dir)

Override with env var FORCE=1 to recompute all steps, or rm step_XX/.ok for selective rerun.
"""
import os
from pathlib import Path
from datetime import datetime

FORCE = os.environ.get("FORCE") == "1"

def done(step_dir: Path) -> bool:
    """True if step_dir has result.txt + chart.png + .ok sentinel AND not forced."""
    if FORCE:
        return False
    return (
        (step_dir / "result.txt").exists()
        and (step_dir / "chart.png").exists()
        and (step_dir / ".ok").exists()
    )

def mark(step_dir: Path):
    (step_dir / ".ok").write_text(datetime.now().isoformat() + "\n")
```

- [ ] **Step 2: Smoke test**

```bash
python3 -c "
import sys; sys.path.insert(0, 'analysis/scripts')
from pathlib import Path
from _checkpoint import done, mark
import tempfile
with tempfile.TemporaryDirectory() as td:
    d = Path(td) / 'step_99_smoke'
    d.mkdir()
    print('empty:', done(d))
    (d/'result.txt').write_text('x'); (d/'chart.png').write_bytes(b'x')
    print('no .ok:', done(d))
    mark(d)
    print('marked:', done(d))
"
```
Expected: `empty: False`, `no .ok: False`, `marked: True`.

- [ ] **Step 3: Commit**

```bash
git add analysis/scripts/_checkpoint.py
git commit -m "feat: add checkpoint helpers for long-running scripts"
```

---

## Task 6: Rerun `00_data_catalog.py`

**Files:**
- Modify: `analysis/scripts/00_data_catalog.py` (add RIR + host-graph entries)

- [ ] **Step 1: Append RIR + host-graph scan**

In `analysis/scripts/00_data_catalog.py`, locate the final catalog-save block (should be near bottom, writes `analysis/docs/data_catalog.json`). Before that save, insert:

```python
# RIR rDNS
from config import REPO_DIR
rir_dir = REPO_DIR / "data" / "rir-data" / "rirs-rdns-formatted"
rir_files = list(rir_dir.rglob("*.parquet"))
if rir_files:
    rir_rows = conn.execute(f"SELECT count(*) FROM read_parquet({[str(p) for p in rir_files]})").fetchone()[0]
    catalog.append({
        "name": "rir-rdns",
        "category": "enrichment",
        "files": len(rir_files),
        "size_mb": round(sum(p.stat().st_size for p in rir_files) / 1e6, 1),
        "records": rir_rows,
        "domains": None,
        "query_types": None,
        "ts_range": "2025-03-01 .. 2026-03-29 (4 snapshots)",
    })
    print(f"{'rir-rdns':10s} {len(rir_files):>5d} {catalog[-1]['size_mb']:>10.1f} {rir_rows:>14,}")

# Common Crawl host-graph
cc_host = REPO_DIR / "downloads" / "common-crawl" / "webgraph" / "host"
if cc_host.exists():
    host_files = list(cc_host.glob("*.gz"))
    total_mb = sum(f.stat().st_size for f in host_files) / 1e6
    catalog.append({
        "name": "cc-host-graph",
        "category": "webgraph",
        "files": len(host_files),
        "size_mb": round(total_mb, 1),
        "records": None,  # can't cheaply count gz rows
        "domains": None,
        "query_types": None,
        "ts_range": "CC-MAIN-2026-12",
    })
    print(f"{'cc-host':10s} {len(host_files):>5d} {total_mb:>10.1f}")
```

- [ ] **Step 2: Run**

```bash
python3 analysis/scripts/00_data_catalog.py 2>&1 | tee /tmp/catalog.log
```
Expected: prints updated TLD table with new entries, total_records > 231.8M, new entries for `rir-rdns` and `cc-host-graph`.

- [ ] **Step 3: Inspect**

```bash
python3 -c "
import json
c = json.load(open('analysis/docs/data_catalog.json'))
print('date:', c['date'], 'total_records:', c['total_records'], 'total_mb:', c['total_size_mb'])
print('datasets:', [(d['name'], d.get('size_mb')) for d in c['datasets']])
"
```
Expected: listing includes the new TLDs + rir-rdns + cc-host-graph.

- [ ] **Step 4: Commit**

```bash
git add analysis/scripts/00_data_catalog.py analysis/docs/data_catalog.json
git commit -m "catalog: include RIR rDNS + CC host-graph + expanded TLD set"
```

---

## Task 7: Rerun flat scripts 01–07

**Files:** (no code changes — only rerun and commit updated PNGs)

- [ ] **Step 1: Run scripts in sequence**

```bash
for n in 01_overview 02_dns_records 03_geo_network 04_security 05_domain_infra 06_anomaly 07_toplist_analysis; do
    echo "=== $n ==="
    python3 analysis/scripts/${n}.py 2>&1 | tail -20
done
```
Expected: each script finishes within 2–5 min. Warnings about new TLDs are fine; errors are not.

- [ ] **Step 2: Sanity-check one chart**

```bash
ls -la analysis/output/dns_ipv4_vs_ipv6.png
```
Expected: recent mtime (just regenerated).

- [ ] **Step 3: Commit updated charts**

```bash
git add analysis/output/
git commit -m "rerun 01–07: expanded TLD set (2026-04-10 snapshot)"
```

---

## Task 8: Extend `08_webgraph_analysis.py` for host-graph

**Files:**
- Modify: `analysis/scripts/08_webgraph_analysis.py`

- [ ] **Step 1: Add `--level` flag and branching read**

Near top of `08_webgraph_analysis.py`, after imports, add:

```python
import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--level", choices=["domain", "host"], default="domain")
args = ap.parse_args()
LEVEL = args.level
```

Find the line that reads the webgraph file (grep `vertices` or `edges`). Replace hard-coded `domain/...` paths with:

```python
WG_SUBDIR = CC_DIR / "webgraph" / LEVEL
vertices_file = next(WG_SUBDIR.glob(f"cc-main-2026-12-{LEVEL}-vertices.paths.gz"))
edges_file    = next(WG_SUBDIR.glob(f"cc-main-2026-12-{LEVEL}-edges.paths.gz"))
```

All output artifact names should be suffixed with `_{LEVEL}` so the two runs coexist:
```python
OUTPUT_SUFFIX = f"_{LEVEL}" if LEVEL != "domain" else ""
save_fig(f"webgraph_degree_distribution{OUTPUT_SUFFIX}")
```
(Apply to every `save_fig(...)` call in this script — grep it.)

- [ ] **Step 2: Run both levels**

```bash
python3 analysis/scripts/08_webgraph_analysis.py --level domain
python3 analysis/scripts/08_webgraph_analysis.py --level host
```
Expected: `analysis/output/webgraph_*.png` for domain, `analysis/output/webgraph_*_host.png` for host. Host version takes 5–15× longer.

- [ ] **Step 3: Commit**

```bash
git add analysis/scripts/08_webgraph_analysis.py analysis/output/webgraph_*
git commit -m "08: support --level=host for CC host-graph analysis"
```

---

## Task 9: Extend `09_pagerank_vs_toplist.py` to use `host-ranks`

**Files:**
- Modify: `analysis/scripts/09_pagerank_vs_toplist.py`

- [ ] **Step 1: Replace local PR compute with official ranks file**

In `09_pagerank_vs_toplist.py`, find the block that computes PageRank (look for `networkx.pagerank` or `nx.pagerank`). Wrap it with:

```python
# Prefer CC's pre-computed ranks if available (much more accurate, since
# it's computed over the full graph with proper convergence).
USE_OFFICIAL = (CC_DIR / "webgraph" / "host" / "cc-main-2026-12-host-ranks.txt.gz").exists()

if USE_OFFICIAL:
    import gzip
    ranks_path = CC_DIR / "webgraph" / "host" / "cc-main-2026-12-host-ranks.txt.gz"
    # Format (per CC docs): #harmonicc_pos  harmonicc_val  pr_pos  pr_val  host_rev  host
    # host_rev is e.g. "com.example.www" — reverse dotted; we reverse back.
    print(f"  using official host-ranks: {ranks_path}")
    ranks = {}
    with gzip.open(ranks_path, "rt") as f:
        next(f)  # header
        for line in f:
            parts = line.rstrip().split("\t")
            if len(parts) < 6:
                continue
            pr = float(parts[3])
            host_rev = parts[4]
            host = ".".join(reversed(host_rev.split(".")))
            ranks[host] = pr
    print(f"  loaded {len(ranks):,} host ranks")
else:
    # legacy path: compute locally on domain graph (deprecated for host level)
    # ... existing nx.pagerank code unchanged ...
```

After this block, existing code that joins `ranks` against toplists should keep working (`ranks` is still a `dict[str, float]`).

- [ ] **Step 2: Run**

```bash
python3 analysis/scripts/09_pagerank_vs_toplist.py 2>&1 | tail -20
```
Expected: `using official host-ranks` message; completes in <5 min (vs ~30 min for local PR compute).

- [ ] **Step 3: Commit**

```bash
git add analysis/scripts/09_pagerank_vs_toplist.py analysis/output/cc_*.png
git commit -m "09: use CC official host-ranks (faster + more accurate)"
```

---

## Task 10: Rerun `10_cc_index_analysis.py`

**Files:** (no changes)

- [ ] **Step 1: Run**

```bash
python3 analysis/scripts/10_cc_index_analysis.py 2>&1 | tail -15
```
Expected: same output as before; cluster.idx is the only input and it's been re-downloaded.

- [ ] **Step 2: Commit**

```bash
git add analysis/output/cc_index_*.png
git commit -m "10: rerun on fresh cluster.idx"
```

---

## Task 11: Add step_05b to `11_deep_analysis.py` + checkpoint wrap

**Files:**
- Modify: `analysis/scripts/11_deep_analysis.py`

- [ ] **Step 1: Import checkpoint + RIR modules**

Near top of the file after existing imports:

```python
from _checkpoint import done, mark
from rir_rdns import load_rir_prefix, coverage_stats
```

Bump `memory_limit`:
```python
conn.execute("SET memory_limit='12GB'")
```

- [ ] **Step 2: Wrap each step with checkpoint**

For each of the 22 steps, wrap the body. Example for step 5 — change:
```python
d = step_dir(5, "as_concentration")

as_data = conn.execute(...)...
```
to:
```python
d = step_dir(5, "as_concentration")
if done(d):
    print(f"  [skip] step_05 (use FORCE=1 to rerun)")
    findings[5] = (d / "result.txt").read_text().splitlines()[-1]  # last line likely has the finding
else:
    as_data = conn.execute(...)...
    # ... existing body unchanged ...
    mark(d)
```

For `findings[N] = ...` assignments that occur inside the else-branch, also extract the final `findings[N]` line from the saved `result.txt` when skipping — simplest: change each step to append the finding as the LAST line of `result.txt` so the skip-branch's `splitlines()[-1]` works. Edit `save_result(d, text)` call sites to always append `findings_line` at end:

```python
# Before save_result, ensure findings[n] is set, then:
save_result(d, "\n".join(lines) + f"\n\n# FINDING\n{findings[n]}")
```
Apply this pattern across all 22 steps. Where the step has no finding, store an empty string.

- [ ] **Step 3: Insert new step_05b — rDNS pattern clustering**

After existing step 5 block (line ~296 in current file), insert:

```python
# ======================================================================
#  STEP 05b: rDNS 命名模式聚类 (NEW — RIR rDNS integration)
# ======================================================================
print("\n" + "="*70)
print("STEP 05b: rDNS 命名模式聚类")
print("="*70)
# step_dir() uses '{n:02d}' format; 5b needs manual path:
d = OUT / "step_05b_rdns_patterns"; d.mkdir(exist_ok=True)

if done(d):
    print("  [skip] step_05b")
else:
    load_rir_prefix(conn)
    # For each A record IP, prefix-join to RIR rname; extract apex of rname (e.g. "ec2.amazonaws.com")
    df = conn.execute(f"""
        WITH sample AS (
            SELECT ip4_address, query_name
            FROM read_parquet([{all_zone_sql()}])
            WHERE query_type='A' AND ip4_address IS NOT NULL AND status_code=0
            USING SAMPLE 2000000
        ),
        joined AS (
            SELECT s.query_name, s.ip4_address,
                   r.rname, r.rir_source
            FROM sample s
            LEFT JOIN rir_prefix r
              ON host(s.ip4_address::INET) BETWEEN host(r.start_address::INET) AND host(r.end_address::INET)
        )
        SELECT
            CASE
                WHEN rname IS NULL THEN '__no_rdns__'
                ELSE regexp_extract(rname, '([^.]+\\.[^.]+)\\.?$', 1)
            END AS rdns_apex,
            count(*) AS n
        FROM joined
        GROUP BY rdns_apex ORDER BY n DESC LIMIT 25
    """).fetchdf()
    lines = ["# Step 05b — rDNS naming pattern clustering (Top 25)\n",
             "Sample: 2M A-record IPs, prefix-joined to RIR rDNS.\n"]
    for _, r in df.iterrows():
        lines.append(f"  {r['rdns_apex']:40s} {r['n']:>10,}")
    total = int(df["n"].sum())
    no_rdns = int(df.loc[df["rdns_apex"] == "__no_rdns__", "n"].sum()) if "__no_rdns__" in df["rdns_apex"].values else 0
    rdns_cov = (1 - no_rdns / total) * 100 if total > 0 else 0
    findings[55] = f"rDNS 覆盖率 {rdns_cov:.1f}%；Top apex: {df.iloc[0]['rdns_apex']}"
    save_result(d, "\n".join(lines) + f"\n\n# FINDING\n{findings[55]}")
    fig, ax = plt.subplots(figsize=(12, 8))
    plot_df = df.head(20).iloc[::-1]
    ax.barh(plot_df["rdns_apex"], plot_df["n"] / 1000, color=COLORS[2])
    ax.set_xlabel("Thousands of IPs")
    ax.set_title("Step 05b: Top 20 rDNS naming patterns")
    plt.tight_layout(); save_chart(d)
    mark(d)
```

- [ ] **Step 4: Run**

```bash
FORCE=1 python3 analysis/scripts/11_deep_analysis.py 2>&1 | tee /tmp/deep.log | tail -40
```
Expected: ~60–90 min; 23 step directories created (22 existing + 05b); `summary_report.md` regenerated.

Then test checkpoint:
```bash
python3 analysis/scripts/11_deep_analysis.py 2>&1 | tail -30
```
Expected: all 23 steps report `[skip]`; completes in <30 seconds.

- [ ] **Step 5: Commit**

```bash
git add analysis/scripts/11_deep_analysis.py analysis/deep_analysis/
git commit -m "11: add step_05b rDNS patterns + checkpoint wrap"
```

---

## Task 12: Checkpoint wrap for `12_network_analysis.py` + `12b_network_continue.py`

**Files:**
- Modify: `analysis/scripts/12_network_analysis.py`
- Modify: `analysis/scripts/12b_network_continue.py`

- [ ] **Step 1: Add edge-cache persistence in 12**

At top of `12_network_analysis.py`, after DuckDB connection setup, replace the `CREATE OR REPLACE TABLE edge_*` blocks with an edge-cache gate:

```python
from _checkpoint import done, mark, FORCE
EDGE_CACHE = OUT / "_edge_cache.duckdb"

if EDGE_CACHE.exists() and not FORCE:
    print(f"  [edge-cache] attaching {EDGE_CACHE}")
    conn.execute(f"ATTACH '{EDGE_CACHE}' AS cache (READ_ONLY)")
    conn.execute("CREATE OR REPLACE VIEW edge_domain_ns AS SELECT * FROM cache.edge_domain_ns")
    conn.execute("CREATE OR REPLACE VIEW edge_domain_ip_as AS SELECT * FROM cache.edge_domain_ip_as")
    conn.execute("CREATE OR REPLACE VIEW edge_cname AS SELECT * FROM cache.edge_cname")
else:
    print(f"  [edge-cache] rebuilding into {EDGE_CACHE}")
    if EDGE_CACHE.exists(): EDGE_CACHE.unlink()
    conn.execute(f"ATTACH '{EDGE_CACHE}' AS cache")
    # ... existing CREATE OR REPLACE TABLE edge_domain_ns ... statements,
    #     but with each CREATE targeting cache.edge_* ...
    # Example for one:
    conn.execute(f"""
        CREATE OR REPLACE TABLE cache.edge_domain_ns AS
        SELECT query_name AS domain, ns_address AS ns
        FROM read_parquet([{all_zone_sql()}])
        WHERE query_type = 'NS' AND ns_address IS NOT NULL
    """)
    # ... and similarly for edge_domain_ip_as, edge_cname ...
    conn.execute("CREATE OR REPLACE VIEW edge_domain_ns AS SELECT * FROM cache.edge_domain_ns")
    # (same for other two views)
```

(Keep the exact SQL bodies from the existing file; only the target of `CREATE OR REPLACE TABLE` changes from local to `cache.*`.)

- [ ] **Step 2: Wrap all 17 steps in 12 with checkpoint**

For each step, transform:
```python
d = step_dir(N, "step_name")
# ... body: conn.execute, plt.* calls, save_result, save_chart ...
```
into:
```python
d = step_dir(N, "step_name")
if done(d):
    print(f"  [skip] step_{N:02d} (use FORCE=1 to rerun)")
else:
    # ... original body unchanged ...
    mark(d)
```
Apply to every `step_dir(...)` site (17 of them in `12_network_analysis.py`).

- [ ] **Step 3: Wrap steps 18–25 in 12b**

In `12b_network_continue.py`, attach the same `_edge_cache.duckdb` so it doesn't rebuild from parquet:

```python
from _checkpoint import done, mark, FORCE
EDGE_CACHE = OUT / "_edge_cache.duckdb"
if not EDGE_CACHE.exists():
    raise RuntimeError(f"Missing {EDGE_CACHE}. Run 12 first.")
conn.execute(f"ATTACH '{EDGE_CACHE}' AS cache (READ_ONLY)")
# create same views as in 12
```

Remove the "rebuild edge tables from parquet" block at top of 12b (it's no longer needed).

Wrap steps 18–25 with the same checkpoint pattern.

- [ ] **Step 4: Run**

```bash
FORCE=1 python3 analysis/scripts/12_network_analysis.py 2>&1 | tee /tmp/net.log | tail -40
python3 analysis/scripts/12b_network_continue.py 2>&1 | tee /tmp/net2.log | tail -40
```
Expected: 12 takes 60–120 min (first run builds edge-cache); 12b takes 30–60 min (reads cache).

Test checkpoint:
```bash
python3 analysis/scripts/12_network_analysis.py 2>&1 | tail -30
```
Expected: all 17 steps `[skip]`, <30s.

- [ ] **Step 5: Commit**

```bash
git add analysis/scripts/12_network_analysis.py analysis/scripts/12b_network_continue.py analysis/network_analysis/
git commit -m "12/12b: checkpoint + persistent edge-cache"
```

---

## Task 13: Build `13_rir_enrichment.py`

**Files:**
- Create: `analysis/scripts/13_rir_enrichment.py`

- [ ] **Step 1: Skeleton**

```python
#!/usr/bin/env python3
"""13 — RIR rDNS-enriched analysis.

Produces analysis/rir_enrichment/step_XX/{result.txt, chart.png}:
  01  RIR delegation distribution (sample IPs → RIR source pie)
  02  rDNS coverage per ccTLD (has-PTR rate by TLD)
  03  Hoster rDNS naming clusters (cross-TLD)
  04  RIR × ccTLD dependency matrix
  05  RIR Shannon entropy per TLD
"""
import sys, os, pathlib
sys.path.insert(0, os.path.dirname(__file__))

import duckdb, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from config import REPO_DIR, BASE_DIR, ZONE_TLDS, zone_glob, all_zone_sql, get_conn
from rir_rdns import load_rir_prefix, coverage_stats
from _checkpoint import done, mark

OUT = BASE_DIR / "rir_enrichment"
OUT.mkdir(exist_ok=True)
COLORS = ["#4e79a7","#f28e2b","#e15759","#76b7b2","#59a14f","#edc948","#b07aa1"]

conn = get_conn()
conn.execute("SET memory_limit='8GB'")

def step_dir(n, name):
    d = OUT / f"step_{n:02d}_{name}"; d.mkdir(exist_ok=True); return d
def save(d, text): (d/"result.txt").write_text(text); print(text[:400])
def savefig(d):   p = d/"chart.png"; plt.savefig(p, bbox_inches="tight", dpi=150); plt.close(); print(f"  -> {p}")

print("Loading RIR prefix table …")
n = load_rir_prefix(conn)
print(f"  {n:,} prefix rows; {coverage_stats(conn)}")
```

- [ ] **Step 2: Step 01 — RIR delegation distribution**

Append to file:
```python
# ==================================================================
# STEP 01: RIR delegation distribution
# ==================================================================
d = step_dir(1, "rir_delegation")
if not done(d):
    df = conn.execute(f"""
        WITH sample AS (
            SELECT ip4_address FROM read_parquet([{all_zone_sql()}])
            WHERE query_type='A' AND ip4_address IS NOT NULL AND status_code=0
            USING SAMPLE 1000000
        )
        SELECT r.rir_source, count(*) AS n
        FROM sample s
        LEFT JOIN rir_prefix r ON host(s.ip4_address::INET) BETWEEN host(r.start_address::INET) AND host(r.end_address::INET)
        GROUP BY r.rir_source ORDER BY n DESC
    """).fetchdf()
    total = int(df["n"].sum())
    lines = ["# Step 01 — RIR delegation distribution (1M sample)"]
    for _, r in df.iterrows():
        src = r["rir_source"] or "(no match)"
        lines.append(f"  {src:12s} {r['n']:>10,} ({r['n']/total*100:5.1f}%)")
    save(d, "\n".join(lines))

    fig, ax = plt.subplots(figsize=(9,9))
    labels = [x or "no-match" for x in df["rir_source"]]
    ax.pie(df["n"], labels=labels, autopct="%.1f%%", colors=COLORS)
    ax.set_title("RIR delegation of IPs observed in OpenINTEL 2026-04-10")
    savefig(d); mark(d)
```

- [ ] **Step 3: Steps 02–05**

Similar pattern for:

**Step 02** — rDNS coverage per ccTLD: for each TLD, compute `has_rdns / total_ips` ratio. Bar chart.

**Step 03** — Hoster naming clusters: regex-extract apex from rname, top-20 cross-TLD. Stacked bar by TLD.

**Step 04** — RIR × ccTLD dependency matrix: heatmap, rows=TLD, cols=RIR, values=% of that TLD's IPs delegated by that RIR.

**Step 05** — Shannon entropy of RIR distribution per TLD: bar chart; high entropy = diverse hosting, low = concentrated.

Write these out following the same template (sample, query, fetchdf, save, plot, mark). Each is 20–40 lines.

- [ ] **Step 4: Run**

```bash
python3 analysis/scripts/13_rir_enrichment.py 2>&1 | tail -30
```
Expected: 5 step dirs under `analysis/rir_enrichment/`; total runtime ~10–20 min.

- [ ] **Step 5: Commit**

```bash
git add analysis/scripts/13_rir_enrichment.py analysis/rir_enrichment/
git commit -m "feat: add 13_rir_enrichment.py (5-step RIR-dimension analysis)"
```

---

## Task 14: Frontend — navigation + chart-builders

**Files:**
- Modify: `analysis/web/src/lib/navigation.ts`
- Modify: `analysis/web/src/lib/chart-builders.ts`
- Modify: `analysis/scripts/export_metrics.py`

- [ ] **Step 1: Add 2 sub-pages to navigation**

Read current `navigation.ts` to see the `TIERS` structure and locate tier-3 / tier-4 arrays. Append to tier-3's `pages` array:

```ts
{
  slug: "rir-enrichment",
  title: "RIR rDNS 富化",
  subtitle: "Phase-1 扩展：委派分布 + rDNS 命名模式",
  chartCount: 5,
  path: "tier-3-extra/rir-enrichment",
},
```
And to tier-4's:
```ts
{
  slug: "host-graph",
  title: "Host-level WebGraph",
  subtitle: "Phase-1 扩展：3B host 粒度",
  chartCount: 2,
  path: "tier-4-extra/host-graph",
},
```
Adjust the tier `totalCharts` fields accordingly.

- [ ] **Step 2: Add 2 chart builders**

In `analysis/web/src/lib/chart-builders.ts`, append:

```ts
export function buildRirDelegation(data: { rir: string; n: number }[]) {
  return {
    title: { text: "RIR delegation of observed IPs", left: "center" },
    tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
    series: [{
      type: "pie",
      radius: ["35%", "70%"],
      data: data.map(d => ({ name: d.rir || "no-match", value: d.n })),
      label: { formatter: "{b}\n{d}%" },
    }],
  };
}

export function buildHostRanksToplist(data: { host: string; pr: number; toplist_rank: number | null }[]) {
  return {
    title: { text: "CC host-rank vs Toplist rank", left: "center" },
    xAxis: { type: "log", name: "CC host PageRank" },
    yAxis: { type: "log", name: "Toplist rank (lower = better)", inverse: true },
    tooltip: { trigger: "item", formatter: (p: any) => `${p.data[2]}<br/>PR=${p.data[0].toExponential(2)}<br/>toplist=${p.data[1]}` },
    series: [{
      type: "scatter",
      data: data.filter(d => d.toplist_rank).map(d => [d.pr, d.toplist_rank, d.host]),
      symbolSize: 4,
      itemStyle: { opacity: 0.4 },
    }],
  };
}
```

- [ ] **Step 3: Add metric export hooks in `export_metrics.py`**

Find the `TIER_A_CHARTS` list. Add two entries that write `src/data/charts/rir_delegation.json` and `charts/host_ranks_toplist.json`, sourcing data from `analysis/rir_enrichment/step_01/result.txt` (parse lines) and a new small query that joins `host-ranks.txt.gz` top-1000 with toplist membership.

Also add ~10 annotation entries to `OUTPUT_PNG_ANNOTATIONS` / `TIER_A_ANNOTATIONS` covering the new step outputs (rir_enrichment/step_01..05 and the host-graph PNGs from Task 8).

- [ ] **Step 4: Create the two Astro page files**

```bash
mkdir -p analysis/web/src/pages/tier-3-extra analysis/web/src/pages/tier-4-extra
```

Copy an existing simple page (e.g. `analysis/web/src/pages/tier-1/overview.astro`) as a template; edit `path` in `SectionLayout`, title, and `<ChartCard>` list to reference:
- tier-3-extra/rir-enrichment.astro: 1 interactive (RIR delegation pie) + 4 PNG charts from `rir_enrichment/step_02..05`
- tier-4-extra/host-graph.astro: 1 interactive (host-ranks vs toplist) + PNGs from `output/webgraph_*_host.png`

- [ ] **Step 5: Run export + build**

```bash
python3 analysis/scripts/export_metrics.py --annotations
python3 analysis/scripts/export_metrics.py --charts
cd analysis/web
pnpm install
pnpm astro check
pnpm build
```
Expected: `pnpm astro check` reports 0 errors; `pnpm build` outputs 32 HTML pages to `dist/web/`.

- [ ] **Step 6: Sanity-preview**

```bash
cd analysis/web && pnpm preview &
sleep 3
curl -s http://localhost:4321/tier-3-extra/rir-enrichment/ | grep -c "RIR delegation"
```
Expected: `1` (the h1). Kill the preview server afterwards.

- [ ] **Step 7: Commit**

```bash
git add analysis/web/ analysis/scripts/export_metrics.py
git commit -m "frontend: add tier-3-extra/rir-enrichment + tier-4-extra/host-graph pages"
```

---

## Task 15: Final verification + README update

**Files:**
- Modify: `README.md` (if it still references pre-reorg paths)

- [ ] **Step 1: Run full verify suite**

```bash
python3 analysis/scripts/download_data.py verify
python3 analysis/scripts/00_data_catalog.py | tail -5
ls analysis/rir_enrichment/ analysis/deep_analysis/ analysis/network_analysis/ | head -30
(cd analysis/web && pnpm astro check)
```

Expected: all green.

- [ ] **Step 2: Update README if needed**

Grep README for `data/openintel` or `scripts/` (top-level). Replace with `downloads/openintel` and `analysis/scripts/`. Also add a short "Phase 1 expansion" note mentioning:
- Expanded ccTLD coverage (list actual TLDs that downloaded successfully)
- RIR rDNS integration (new dimension)
- CC host-graph at 3B node granularity
- Checkpoint system for reruns

- [ ] **Step 3: Record precision-improvement metrics in commit**

Compute delta vs previous values (from `git log -1 -- analysis/deep_analysis/step_05_as_concentration/result.txt`). Put into commit message.

```bash
git add README.md
git commit -m "docs: Phase-1 horizontal expansion complete

Coverage deltas vs 2026-04-10 baseline:
  - ccTLDs analyzed: 9 -> N (added: <list>)
  - Total records: 232M -> ~XXXM
  - CC webgraph nodes: 134M -> ~3B (host-level)
  - rDNS coverage for A-record IPs: XX%
  - RIR distribution entropy (avg over TLDs): X.XX"
```

---

## Execution Notes

- **Checkpoint recovery:** if any of 11/12/12b crashes mid-run, just rerun the same command — completed steps skip via `.ok` sentinels. To force a specific step, `rm analysis/deep_analysis/step_NN*/.ok` then rerun.
- **Bandwidth fallback:** if CC downloads are too slow (>4 h estimated), skip `--host-graph` in Task 3 Step 3 and leave Tasks 8/9 partially done (domain-level only). Leave host-related subpages out of navigation until data arrives.
- **Disk monitoring:** `df -h /Volumes/data` between Tasks 3 and 12 — DuckDB edge-cache + spill files can temporarily need another 10–30 GB on top of raw downloads.
- **No TDD:** project has no test framework. Verification is via running the script and inspecting the printed tables / generated PNGs / committed `result.txt` files.
