#!/usr/bin/env python3
"""00 - 数据目录与格式详解

生成完整的数据资产清单：
  - 各数据集的文件数量、大小、记录数、域名数
  - 99 列字段的完整字典（名称、类型、非空率、示例值）
  - 数据质量评估
  - 数据获取方法文档
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from pathlib import Path
import duckdb
from config import (
    get_conn, ZONE_DIR, TOPLIST_DIR, OUTPUT_DIR,
    ZONE_TLDS, TOPLISTS, zone_glob, toplist_glob, BASE_DIR
)

conn = get_conn()

# ═══════════════════════════════════════════════════════
# 1. 数据资产清单
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("OpenINTEL 开源 DNS 数据 — 完整资产清单")
print("=" * 70)

catalog = []

def scan_dataset(name, category, glob_path):
    """扫描一个数据集的元信息"""
    parquet_files = list(Path(glob_path).parent.glob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in parquet_files)
    r = conn.execute(f"""
        SELECT count(*) AS records,
               count(DISTINCT query_name) AS domains,
               count(DISTINCT query_type) AS qtypes,
               min(timestamp) AS ts_min,
               max(timestamp) AS ts_max
        FROM read_parquet('{glob_path}')
    """).fetchone()
    return {
        "name": name,
        "category": category,
        "files": len(parquet_files),
        "size_mb": round(total_bytes / 1e6, 1),
        "records": r[0],
        "domains": r[1],
        "query_types": r[2],
        "ts_range": f"{r[3]} - {r[4]}" if r[3] else "N/A",
    }

# Zone-based
print("\n[Zone-Based ccTLD 区域文件]")
print(f"{'Name':8s} {'Files':>5s} {'Size(MB)':>10s} {'Records':>14s} {'Domains':>12s}")
print("-" * 55)
for tld in ZONE_TLDS:
    info = scan_dataset(tld, "zone", zone_glob(tld))
    catalog.append(info)
    print(f"{info['name']:8s} {info['files']:>5d} {info['size_mb']:>10.1f} {info['records']:>14,} {info['domains']:>12,}")

# Root
info = scan_dataset("root", "zone", zone_glob("root"))
catalog.append(info)
print(f"{'root':8s} {info['files']:>5d} {info['size_mb']:>10.1f} {info['records']:>14,} {info['domains']:>12,}")

# TopList
print(f"\n[TopList 热门域名排行]")
print(f"{'Name':10s} {'Files':>5s} {'Size(MB)':>10s} {'Records':>14s} {'Domains':>12s}")
print("-" * 57)
for tl in TOPLISTS:
    info = scan_dataset(tl, "toplist", toplist_glob(tl))
    catalog.append(info)
    print(f"{info['name']:10s} {info['files']:>5d} {info['size_mb']:>10.1f} {info['records']:>14,} {info['domains']:>12,}")

# Summary
total_records = sum(c["records"] for c in catalog)
total_domains = sum(c["domains"] for c in catalog)
total_mb = sum(c["size_mb"] for c in catalog)
print(f"\n总计: {len(catalog)} 数据集, {total_mb/1e3:.1f} GB, {total_records:,} 条记录, {total_domains:,} 个域名")

# ═══════════════════════════════════════════════════════
# 2. 99 列字段字典
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("99 列字段完整字典")
print("=" * 70)

# Use a small dataset for column inspection
ref_path = zone_glob("gov")
schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{ref_path}')").fetchdf()

# Get non-null rates and sample values from a mix of datasets
sample_sql = f"""
    SELECT * FROM read_parquet('{ref_path}')
    USING SAMPLE 5000
"""
df_sample = conn.execute(sample_sql).fetchdf()

field_dict = []
FIELD_GROUPS = {
    "query_type": "查询元数据", "query_name": "查询元数据",
    "response_type": "响应元数据", "response_name": "响应元数据",
    "response_ttl": "响应元数据", "timestamp": "响应元数据",
    "rtt": "响应元数据", "worker_id": "响应元数据",
    "status_code": "响应元数据", "ad_flag": "响应元数据",
    "extended_error": "响应元数据", "section": "响应元数据",
    "ip4_address": "A 记录", "ip6_address": "AAAA 记录",
    "country": "GeoIP", "as": "GeoIP", "as_full": "GeoIP", "ip_prefix": "GeoIP",
    "cname_name": "CNAME", "dname_name": "DNAME",
    "mx_address": "MX 记录", "mx_preference": "MX 记录",
    "ns_address": "NS 记录",
    "txt_text": "TXT 记录",
    "soa_mname": "SOA 记录", "soa_rname": "SOA 记录",
    "soa_serial": "SOA 记录", "soa_refresh": "SOA 记录",
    "soa_retry": "SOA 记录", "soa_expire": "SOA 记录", "soa_minimum": "SOA 记录",
    "caa_flags": "CAA 记录", "caa_tag": "CAA 记录", "caa_value": "CAA 记录",
    "ptr_name": "PTR 记录",
}

print(f"\n{'#':>3s} {'字段名':30s} {'类型':10s} {'非空率':>8s} {'分组':15s} {'示例值'}")
print("-" * 110)

for i, row in schema.iterrows():
    col = row["column_name"]
    dtype = row["column_type"]
    non_null = df_sample[col].notna().sum()
    pct = f"{non_null/len(df_sample)*100:.1f}%"
    group = FIELD_GROUPS.get(col, "")

    # Assign group by prefix
    if not group:
        for prefix, g in [
            ("ds_", "DS 记录"), ("dnskey_", "DNSKEY 记录"),
            ("rrsig_", "RRSIG 签名"), ("nsec3_", "NSEC3"),
            ("nsec3param_", "NSEC3PARAM"), ("nsec_", "NSEC"),
            ("cds_", "CDS 记录"), ("cdnskey_", "CDNSKEY 记录"),
            ("spf_", "SPF 记录"), ("tlsa_", "TLSA 记录"),
        ]:
            if col.startswith(prefix):
                group = g
                break

    # Sample value
    sample_vals = df_sample[col].dropna()
    example = str(sample_vals.iloc[0])[:50] if len(sample_vals) > 0 else "-"

    field_dict.append({
        "index": i + 1, "name": col, "type": dtype,
        "non_null_pct": pct, "group": group, "example": example,
    })
    print(f"{i+1:>3d} {col:30s} {dtype:10s} {pct:>8s} {group:15s} {example}")

# ═══════════════════════════════════════════════════════
# 3. 数据质量评估
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("数据质量评估")
print("=" * 70)

# Check across all zone data
all_globs = ", ".join(f"'{zone_glob(t)}'" for t in ZONE_TLDS)
quality = conn.execute(f"""
    SELECT
        count(*) AS total_records,
        count(DISTINCT query_name) AS unique_domains,
        count(CASE WHEN status_code = 0 THEN 1 END) * 100.0 / count(*) AS noerror_pct,
        count(CASE WHEN response_ttl IS NOT NULL THEN 1 END) * 100.0 / count(*) AS ttl_pct,
        count(CASE WHEN rtt IS NOT NULL AND rtt > 0 THEN 1 END) * 100.0 / count(*) AS rtt_pct,
        count(CASE WHEN country IS NOT NULL AND country NOT IN ('--','-') THEN 1 END) * 100.0 / count(*) AS geo_pct,
        count(DISTINCT query_type) AS query_types
    FROM read_parquet([{all_globs}])
""").fetchone()

print(f"""
  Zone 数据质量:
    NOERROR 率:    {quality[2]:.1f}%
    TTL 完整率:    {quality[3]:.1f}%
    RTT 完整率:    {quality[4]:.1f}%
    GeoIP 命中率:  {quality[5]:.1f}%
    查询类型覆盖:  {quality[6]} 种
""")

# ═══════════════════════════════════════════════════════
# 4. 数据获取指南
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("数据获取方法")
print("=" * 70)

guide = """
  S3 兼容接口（免认证）:
    Endpoint:  https://object.openintel.nl
    Bucket:    openintel-public
    路径结构:  fdns/basis={zonefile|toplist}/source={source}/year=YYYY/month=MM/day=DD/

  可用 Zone 区域 (公开):
    ch, ee, fr, gov, li, nu, se, sk, root, fed.us

  可用 TopList (公开):
    tranco, umbrella, radar, majestic
    (alexa, crux 已停更)

  下载示例 (Python boto3):
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    s3 = boto3.client('s3',
        endpoint_url='https://object.openintel.nl',
        config=Config(signature_version=UNSIGNED),
        region_name='us-east-1')

    # 列出可用文件
    resp = s3.list_objects_v2(
        Bucket='openintel-public',
        Prefix='fdns/basis=toplist/source=tranco/year=2026/month=04/day=10/')

  下载示例 (wget):
    wget -O tranco.parquet \\
      "https://object.openintel.nl/openintel-public/\\
       fdns/basis=toplist/source=tranco/year=2026/month=04/day=10/\\
       part-00000-xxx.gz.parquet"

  查询示例 (DuckDB):
    SELECT query_name, ip4_address, country
    FROM read_parquet('data/openintel/zone/gov/*.parquet')
    WHERE query_type = 'A' AND ip4_address IS NOT NULL
    LIMIT 10;

  许可证: CC BY-NC-SA 4.0 (非商业, 署名, 相同方式共享)
"""
print(guide)

# ═══════════════════════════════════════════════════════
# 5. 保存数据目录 JSON
# ═══════════════════════════════════════════════════════
catalog_output = {
    "project": "OpenINTEL DNS Open Data Analysis",
    "date": "2026-04-10",
    "s3_endpoint": "https://object.openintel.nl",
    "s3_bucket": "openintel-public",
    "license": "CC BY-NC-SA 4.0",
    "total_records": total_records,
    "total_domains": total_domains,
    "total_size_mb": total_mb,
    "datasets": catalog,
    "schema": {
        "total_columns": len(field_dict),
        "columns": field_dict,
    },
}

catalog_path = BASE_DIR / "docs" / "data_catalog.json"
with open(catalog_path, "w") as f:
    json.dump(catalog_output, f, ensure_ascii=False, indent=2, default=str)
print(f"  -> 数据目录已保存: {catalog_path}")

print("\n[00_data_catalog] 完成!")
conn.close()
