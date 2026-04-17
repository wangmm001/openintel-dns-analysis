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


def load_manifest() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {"openintel": {}, "common_crawl": {}, "failed": []}

def save_manifest(m: dict):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(m, indent=2, sort_keys=True))


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


def cmd_openintel(args):
    s3 = oi_client()
    manifest = load_manifest()
    all_jobs = []
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


# Latest published webgraph as of 2026-04 (covers CC-MAIN-2025-51, -2026-05, -2026-08).
# CC-MAIN-2026-12's own webgraph is not published yet (expected ~June 2026).
WG_SLUG = "cc-main-2025-26-dec-jan-feb"

CC_FILES_DOMAIN = [
    f"{WG_SLUG}-domain-vertices.paths.gz",
    f"{WG_SLUG}-domain-edges.paths.gz",
    f"{WG_SLUG}-domain-ranks.txt.gz",
]
CC_FILES_HOST = [
    f"{WG_SLUG}-host-vertices.paths.gz",
    f"{WG_SLUG}-host-edges.paths.gz",
    f"{WG_SLUG}-host-ranks.txt.gz",
]

def cc_url(crawl: str, filename: str) -> str:
    level = "domain" if "domain" in filename else "host"
    return f"{CC_BASE}/projects/hyperlinkgraph/{WG_SLUG}/{level}/{filename}"

def cc_follow_manifest(manifest_path: Path, dest_dir: Path) -> list[tuple[str, bool, str]]:
    """Read a .paths.gz manifest, download each listed part file into dest_dir/parts/.

    Manifest format: one S3-relative path per line. Prepend CC_BASE to form URL.
    """
    import gzip
    parts_dir = dest_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    results = []
    with gzip.open(manifest_path, "rt") as f:
        lines = [l.strip() for l in f if l.strip()]
    print(f"  [manifest] {manifest_path.name}: {len(lines)} part files")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        for rel in lines:
            url = f"{CC_BASE}/{rel}"
            dest = parts_dir / rel.rsplit("/", 1)[-1]
            futures[ex.submit(http_download, url, dest)] = rel
        for fut in as_completed(futures):
            rel = futures[fut]
            ok, msg = fut.result()
            results.append((rel, ok, msg))
            print(f"    [{'OK' if ok else 'FAIL'}] {rel.rsplit('/',1)[-1]}  {msg}")
    return results

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
    cluster_url = f"{CC_BASE}/cc-index/collections/{args.crawl}/indexes/cluster.idx"
    cluster_dest = DOWNLOADS / "common-crawl" / "cluster.idx"
    ok, msg = http_download(cluster_url, cluster_dest)
    print(f"  [{'OK' if ok else 'FAIL'}] cluster.idx  {msg}")
    files = list(CC_FILES_DOMAIN)
    if args.host_graph:
        files += CC_FILES_HOST
    manifests_to_follow = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        for fn in files:
            url = cc_url(args.crawl, fn)
            level = "domain" if "domain" in fn else "host"
            dest = DOWNLOADS / "common-crawl" / "webgraph" / level / fn
            futures[ex.submit(http_download, url, dest)] = (fn, level, dest)
        for fut in as_completed(futures):
            fn, level, dest = futures[fut]
            ok, msg = fut.result()
            print(f"  [{'OK' if ok else 'FAIL'}] {fn}  {msg}")
            if ok:
                manifest["common_crawl"][fn] = {"crawl": args.crawl, "slug": WG_SLUG}
                if fn.endswith(".paths.gz") and args.follow_manifests:
                    manifests_to_follow.append((dest, dest.parent))
            else:
                manifest["failed"].append({"key": fn, "msg": msg})
    save_manifest(manifest)

    if manifests_to_follow:
        print(f"\nFollowing {len(manifests_to_follow)} manifests for part files …")
        for manifest_path, dest_dir in manifests_to_follow:
            results = cc_follow_manifest(manifest_path, dest_dir)
            for rel, ok, msg in results:
                if ok:
                    manifest["common_crawl"][rel] = {"crawl": args.crawl, "slug": WG_SLUG, "type": "part"}
                else:
                    manifest["failed"].append({"key": rel, "msg": msg})
        save_manifest(manifest)


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
    s2 = sub.add_parser("common-crawl"); s2.add_argument("--crawl", required=True); s2.add_argument("--host-graph", action="store_true"); s2.add_argument("--follow-manifests", action="store_true", help="Download part files referenced by each .paths.gz manifest (100+ GB for host graph)"); s2.set_defaults(func=cmd_common_crawl)
    s3 = sub.add_parser("verify"); s3.set_defaults(func=cmd_verify)
    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
