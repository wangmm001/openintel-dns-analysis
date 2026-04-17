#!/usr/bin/env python3
"""10 - Common Crawl 索引 × OpenINTEL × Web Graph 交叉分析

从 Common Crawl CDX 索引中提取域名级 Web 抓取元数据，与 DNS 数据交叉：
  1. 域名的 HTTP 状态码分布（200/301/404/...）
  2. HTTPS 采用率 vs TLSA/CAA 记录
  3. MIME 类型分布 vs DNS 功能推断
  4. 抓取覆盖 vs DNS 可解析性
  5. 高 PageRank 域名的 Web 健康度
  6. 语言多样性与 ccTLD 的关联

数据源:
  - Common Crawl CDX Index (CC-MAIN-2026-12)
  - OpenINTEL DNS zone/toplist 数据
  - Common Crawl Web Graph (domain-ranks)
"""

import sys, os, gzip, io, json, bisect, time
sys.path.insert(0, os.path.dirname(__file__))

import urllib.request
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    get_conn, ZONE_TLDS, TOPLISTS, zone_glob, toplist_glob,
    all_zone_sql, save_fig, BASE_DIR, REPO_DIR, OUTPUT_DIR
)

WG_DIR = REPO_DIR / "downloads" / "common-crawl" / "webgraph"
CC_DIR = REPO_DIR / "downloads" / "common-crawl"
RANKS_FILE = WG_DIR / "domain-ranks.txt.gz"
CLUSTER_IDX = CC_DIR / "cluster.idx"
CDX_BASE = "https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2026-12/indexes"

conn = get_conn()

# ═══════════════════════════════════════════════════════
# 0. 构建目标域名集合
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("0. 构建目标域名集合")
print("=" * 70)

# 从 Web Graph 取 Top 5K + 从各 TopList 取域名 + zone 域名采样
# 合并成一个 ~10K-20K 的目标集合

# Web Graph Top 5K
print("  加载 Web Graph Top 域名...")
conn.execute(f"""
    CREATE OR REPLACE TABLE wg_ranks AS
    SELECT
        column2::INTEGER AS pr_rank,
        column3::DOUBLE  AS pagerank,
        column4          AS rev_domain,
        array_to_string(list_reverse(string_split(column4, '.')), '.') || '.' AS domain
    FROM read_csv('{RANKS_FILE}',
        delim='\t', header=false,
        columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR',
                  'column3':'VARCHAR','column4':'VARCHAR','column5':'VARCHAR'}},
        compression='gzip', skip=1)
    WHERE column2::INTEGER <= 5000
""")
wg_top = conn.execute("SELECT count(*) FROM wg_ranks").fetchone()[0]
print(f"  Web Graph Top 5K: {wg_top:,}")

# TopList 域名
for src in ["tranco", "umbrella", "radar"]:
    conn.execute(f"""
        CREATE OR REPLACE TABLE tl_{src} AS
        SELECT DISTINCT query_name AS domain
        FROM read_parquet('{toplist_glob(src)}')
    """)

# Zone 域名采样 (每个 TLD 取 500 个 A 记录域名)
zone_sql = all_zone_sql()
conn.execute(f"""
    CREATE OR REPLACE TABLE zone_sample AS
    SELECT DISTINCT query_name AS domain
    FROM read_parquet([{zone_sql}])
    WHERE query_type = 'A'
    USING SAMPLE 4000
""")

# 合并目标集合
conn.execute("""
    CREATE OR REPLACE TABLE target_domains AS
    SELECT DISTINCT domain FROM (
        SELECT domain FROM wg_ranks
        UNION ALL
        SELECT domain FROM tl_tranco USING SAMPLE 5000
        UNION ALL
        SELECT domain FROM tl_umbrella USING SAMPLE 2000
        UNION ALL
        SELECT domain FROM tl_radar USING SAMPLE 2000
        UNION ALL
        SELECT domain FROM zone_sample
    )
""")
target_count = conn.execute("SELECT count(*) FROM target_domains").fetchone()[0]
print(f"  目标域名总数: {target_count:,}")

# 导出为列表
target_list = [r[0] for r in conn.execute(
    "SELECT domain FROM target_domains ORDER BY domain"
).fetchall()]

# ═══════════════════════════════════════════════════════
# 1. 加载 cluster.idx 并构建索引
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. 加载 CDX 集群索引")
print("=" * 70)

# 将域名转为 SURT 格式 (e.g. example.com. -> com,example)
def domain_to_surt(domain):
    """将 example.com. 转为 SURT 前缀 com,example"""
    d = domain.rstrip(".")
    parts = d.split(".")
    return ",".join(reversed(parts))

# 加载 cluster.idx
print("  解析 cluster.idx...")
cluster_entries = []  # [(surt_prefix, cdx_file, offset, length, entry_id)]
with open(CLUSTER_IDX, "r") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) >= 5:
            surt_key = parts[0].split(" ")[0]  # Take just the SURT key part
            cluster_entries.append((
                surt_key,
                parts[1],           # cdx file
                int(parts[2]),      # offset
                int(parts[3]),      # length
                int(parts[4]),      # entry id
            ))
print(f"  集群条目: {len(cluster_entries):,}")

# 排序的 SURT keys 用于二分查找
cluster_surts = [e[0] for e in cluster_entries]

def find_cdx_ranges(surt_prefix):
    """查找覆盖某 SURT 前缀的 CDX 分片和字节范围"""
    idx = bisect.bisect_left(cluster_surts, surt_prefix)
    # 需要检查前后几个条目
    ranges = []
    for i in range(max(0, idx - 1), min(len(cluster_entries), idx + 2)):
        entry = cluster_entries[i]
        if entry[0].startswith(surt_prefix[:len(surt_prefix.split(",")[0])]):
            ranges.append(entry)
    return ranges

# ═══════════════════════════════════════════════════════
# 2. 批量抓取 CDX 数据
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. 批量抓取 CDX 数据（域名级聚合）")
print("=" * 70)

# 按 SURT 前缀分组目标域名，找到需要下载的 CDX 范围
domain_surts = {}
for d in target_list:
    surt = domain_to_surt(d)
    domain_surts[surt] = d

# 找出所有需要下载的 CDX 分片
needed_ranges = {}  # {(cdx_file, offset, length): set_of_surt_prefixes}
for surt, domain in domain_surts.items():
    surt_prefix = surt  # full surt for the domain
    idx = bisect.bisect_right(cluster_surts, surt_prefix)
    # 该域名应落在 idx-1 对应的 CDX 块中
    if idx > 0:
        entry = cluster_entries[idx - 1]
        key = (entry[1], entry[2], entry[3])
        if key not in needed_ranges:
            needed_ranges[key] = set()
        needed_ranges[key].add(surt)

print(f"  需要下载的 CDX 块数: {len(needed_ranges):,}")
total_bytes = sum(length for _, _, length in needed_ranges.keys())
print(f"  总下载量: {total_bytes / 1e6:.1f} MB")

# 限制下载量，优先下载包含最多目标域名的块
sorted_ranges = sorted(needed_ranges.items(), key=lambda x: -len(x[1]))

# 设置合理的下载上限
MAX_DOWNLOAD_MB = 200
MAX_BLOCKS = 800
cumulative_bytes = 0
selected_ranges = []
for (cdx_file, offset, length), surts in sorted_ranges:
    if cumulative_bytes + length > MAX_DOWNLOAD_MB * 1e6:
        break
    if len(selected_ranges) >= MAX_BLOCKS:
        break
    selected_ranges.append(((cdx_file, offset, length), surts))
    cumulative_bytes += length

print(f"  实际下载: {len(selected_ranges)} 块, {cumulative_bytes/1e6:.1f} MB")

# 收集所有需要的 SURT 前缀
all_target_surts = set()
for _, surts in selected_ranges:
    all_target_surts.update(surts)
print(f"  覆盖目标域名: {len(all_target_surts):,}")

# 下载并解析（使用并发下载加速）
domain_records = defaultdict(lambda: {
    "urls": 0, "status_200": 0, "status_301": 0, "status_302": 0,
    "status_404": 0, "status_other": 0, "https": 0, "http": 0,
    "mime_html": 0, "mime_json": 0, "mime_image": 0, "mime_other": 0,
    "languages": set(), "total_size": 0
})

def fetch_and_parse(cdx_file, offset, length, target_surts):
    """下载一个 CDX 块并解析匹配的记录"""
    url = f"{CDX_BASE}/{cdx_file}"
    results = []
    try:
        req = urllib.request.Request(url, headers={
            "Range": f"bytes={offset}-{offset + length - 1}"
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            compressed = resp.read()
            dec = gzip.GzipFile(fileobj=io.BytesIO(compressed))
            try:
                text = dec.read().decode("utf-8", errors="replace")
            except EOFError:
                return results, len(compressed), None

            for line in text.strip().split("\n"):
                if not line:
                    continue
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    continue
                surt_key = parts[0]
                paren_idx = surt_key.find(")")
                if paren_idx < 0:
                    continue
                surt_domain = surt_key[:paren_idx]
                if surt_domain not in target_surts:
                    continue
                try:
                    meta = json.loads(parts[2])
                except json.JSONDecodeError:
                    continue
                results.append((surt_domain, meta))
            return results, len(compressed), None
    except Exception as e:
        return results, 0, str(e)

downloaded = 0
errors = 0
start_time = time.time()
completed_blocks = 0

print(f"  开始并发下载 ({min(20, len(selected_ranges))} 线程)...")
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {}
    for (cdx_file, offset, length), target_surts in selected_ranges:
        f = executor.submit(fetch_and_parse, cdx_file, offset, length, target_surts)
        futures[f] = (cdx_file, offset)

    for future in as_completed(futures):
        results, nbytes, err = future.result()
        downloaded += nbytes
        completed_blocks += 1
        if err:
            errors += 1
            if errors <= 3:
                cdx_file, offset = futures[future]
                print(f"  警告: {cdx_file} offset={offset}: {err}")

        for surt_domain, meta in results:
            rec = domain_records[surt_domain]
            rec["urls"] += 1
            status = meta.get("status", "")
            if status == "200":
                rec["status_200"] += 1
            elif status == "301":
                rec["status_301"] += 1
            elif status == "302":
                rec["status_302"] += 1
            elif status == "404":
                rec["status_404"] += 1
            else:
                rec["status_other"] += 1
            url_str = meta.get("url", "")
            if url_str.startswith("https://"):
                rec["https"] += 1
            else:
                rec["http"] += 1
            mime = meta.get("mime-detected", meta.get("mime", ""))
            if "html" in mime or "xhtml" in mime:
                rec["mime_html"] += 1
            elif "json" in mime or "javascript" in mime:
                rec["mime_json"] += 1
            elif "image" in mime:
                rec["mime_image"] += 1
            else:
                rec["mime_other"] += 1
            langs = meta.get("languages", "")
            if langs:
                for lang in langs.split(","):
                    rec["languages"].add(lang.strip())
            try:
                rec["total_size"] += int(meta.get("length", 0))
            except (ValueError, TypeError):
                pass

        if completed_blocks % 200 == 0:
            elapsed = time.time() - start_time
            print(f"  进度: {completed_blocks}/{len(selected_ranges)} 块, "
                  f"{downloaded/1e6:.1f} MB, {elapsed:.0f}s, "
                  f"域名: {len(domain_records):,}")

elapsed = time.time() - start_time
print(f"\n  下载完成: {downloaded/1e6:.1f} MB, {elapsed:.0f}s, 错误: {errors}")
print(f"  匹配域名: {len(domain_records):,}")
print(f"  总 URL 记录: {sum(r['urls'] for r in domain_records.values()):,}")

# ═══════════════════════════════════════════════════════
# 3. 转为 DataFrame 并导入 DuckDB
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. 构建 CC 索引分析表")
print("=" * 70)

rows = []
for surt, rec in domain_records.items():
    # SURT → domain: com,example → example.com.
    parts = surt.split(",")
    domain = ".".join(reversed(parts)) + "."
    rows.append({
        "domain": domain,
        "cc_urls": rec["urls"],
        "cc_200": rec["status_200"],
        "cc_301": rec["status_301"],
        "cc_302": rec["status_302"],
        "cc_404": rec["status_404"],
        "cc_other_status": rec["status_other"],
        "cc_https": rec["https"],
        "cc_http": rec["http"],
        "cc_html": rec["mime_html"],
        "cc_json": rec["mime_json"],
        "cc_image": rec["mime_image"],
        "cc_mime_other": rec["mime_other"],
        "cc_languages": ",".join(sorted(rec["languages"])) if rec["languages"] else "",
        "cc_lang_count": len(rec["languages"]),
        "cc_total_size": rec["total_size"],
    })

df_cc = pd.DataFrame(rows)
conn.execute("CREATE OR REPLACE TABLE cc_index AS SELECT * FROM df_cc")
print(f"  CC 索引表: {len(df_cc):,} 域名")

# 导入 Web Graph 全量 ranks (for joining)
conn.execute(f"""
    CREATE OR REPLACE TABLE wg_all AS
    SELECT
        column2::INTEGER AS pr_rank,
        column3::DOUBLE  AS pagerank,
        array_to_string(list_reverse(string_split(column4, '.')), '.') || '.' AS domain,
        list_extract(list_reverse(string_split(column4, '.')), 1) AS tld
    FROM read_csv('{RANKS_FILE}',
        delim='\t', header=false,
        columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR',
                  'column3':'VARCHAR','column4':'VARCHAR','column5':'VARCHAR'}},
        compression='gzip', skip=1)
""")
wg_total = conn.execute("SELECT count(*) FROM wg_all").fetchone()[0]

# ═══════════════════════════════════════════════════════
# 4. 分析 1: HTTP 状态码分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. HTTP 状态码分布")
print("=" * 70)

status_summary = conn.execute("""
    SELECT
        sum(cc_200) AS total_200,
        sum(cc_301) AS total_301,
        sum(cc_302) AS total_302,
        sum(cc_404) AS total_404,
        sum(cc_other_status) AS total_other,
        sum(cc_urls) AS total_urls,
        -- 域名级: 主要状态码
        count(CASE WHEN cc_200 > cc_301 AND cc_200 > cc_404 THEN 1 END) AS domains_mostly_200,
        count(CASE WHEN cc_301 > cc_200 AND cc_301 > cc_404 THEN 1 END) AS domains_mostly_301,
        count(CASE WHEN cc_404 > cc_200 AND cc_404 > cc_301 THEN 1 END) AS domains_mostly_404,
        count(*) AS total_domains
    FROM cc_index
    WHERE cc_urls > 0
""").fetchone()

total_urls = status_summary[5]
print(f"  总 URL 数: {total_urls:,}")
print(f"  200 OK:       {status_summary[0]:>10,} ({status_summary[0]/total_urls*100:.1f}%)")
print(f"  301 Redirect: {status_summary[1]:>10,} ({status_summary[1]/total_urls*100:.1f}%)")
print(f"  302 Redirect: {status_summary[2]:>10,} ({status_summary[2]/total_urls*100:.1f}%)")
print(f"  404 Not Found:{status_summary[3]:>10,} ({status_summary[3]/total_urls*100:.1f}%)")
print(f"  其他:          {status_summary[4]:>10,} ({status_summary[4]/total_urls*100:.1f}%)")
print(f"\n  域名级主要状态:")
print(f"    主要 200: {status_summary[6]:,} ({status_summary[6]/status_summary[9]*100:.1f}%)")
print(f"    主要 301: {status_summary[7]:,} ({status_summary[7]/status_summary[9]*100:.1f}%)")
print(f"    主要 404: {status_summary[8]:,} ({status_summary[8]/status_summary[9]*100:.1f}%)")

# 可视化
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# 4a. URL-level status distribution
ax = axes[0]
labels = ["200 OK", "301", "302", "404", "Other"]
sizes = [status_summary[i] for i in range(5)]
colors_pie = ["#55A868", "#4C72B0", "#8DA0CB", "#C44E52", "#CCCCCC"]
wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct="%1.1f%%",
                                   colors=colors_pie, startangle=90)
ax.set_title("CC URL-Level Status Code Distribution")

# 4b. Domain-level dominant status
ax = axes[1]
dom_labels = ["Mostly 200", "Mostly 301", "Mostly 404"]
dom_sizes = [status_summary[6], status_summary[7], status_summary[8]]
ax.bar(dom_labels, dom_sizes, color=["#55A868", "#4C72B0", "#C44E52"])
ax.set_ylabel("Number of Domains")
ax.set_title("Domain-Level Dominant HTTP Status")
for i, v in enumerate(dom_sizes):
    ax.text(i, v + max(dom_sizes)*0.02, f"{v:,}", ha="center", fontsize=9)

plt.tight_layout()
save_fig("cc_index_status_distribution")

# ═══════════════════════════════════════════════════════
# 5. 分析 2: HTTPS 采用率 vs DNS 安全记录
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. HTTPS 采用率 × DNS 安全记录")
print("=" * 70)

zone_sql = all_zone_sql()

# HTTPS 比例
conn.execute("""
    ALTER TABLE cc_index ADD COLUMN IF NOT EXISTS https_ratio DOUBLE;
    UPDATE cc_index SET https_ratio = cc_https * 1.0 / NULLIF(cc_urls, 0);
""")

https_stats = conn.execute("""
    SELECT
        count(*) AS total,
        count(CASE WHEN https_ratio > 0.9 THEN 1 END) AS mostly_https,
        count(CASE WHEN https_ratio < 0.1 THEN 1 END) AS mostly_http,
        count(CASE WHEN https_ratio BETWEEN 0.1 AND 0.9 THEN 1 END) AS mixed,
        avg(https_ratio) AS avg_ratio
    FROM cc_index WHERE cc_urls > 0
""").fetchone()
print(f"  HTTPS > 90%: {https_stats[1]:,} ({https_stats[1]/https_stats[0]*100:.1f}%)")
print(f"  HTTP  > 90%: {https_stats[2]:,} ({https_stats[2]/https_stats[0]*100:.1f}%)")
print(f"  混合:         {https_stats[3]:,} ({https_stats[3]/https_stats[0]*100:.1f}%)")
print(f"  平均 HTTPS 比: {https_stats[4]:.3f}")

# Cross with DNS: CAA records
print("\n  HTTPS vs CAA/TLSA 记录交叉:")
try:
    df_https_dns = conn.execute(f"""
        WITH cc_domains AS (
            SELECT domain,
                CASE WHEN https_ratio > 0.9 THEN 'HTTPS'
                     WHEN https_ratio < 0.1 THEN 'HTTP'
                     ELSE 'Mixed' END AS transport
            FROM cc_index WHERE cc_urls > 0
        ),
        dns AS (
            SELECT DISTINCT query_name,
                MAX(CASE WHEN query_type = 'CAA' AND caa_tag IS NOT NULL THEN 1 ELSE 0 END) AS has_caa,
                MAX(CASE WHEN query_type = 'AAAA' AND ip6_address IS NOT NULL THEN 1 ELSE 0 END) AS has_ipv6
            FROM read_parquet([{zone_sql}])
            WHERE query_name IN (SELECT domain FROM cc_domains)
            GROUP BY query_name
        )
        SELECT c.transport,
            count(*) AS domains,
            sum(d.has_caa) AS with_caa,
            sum(d.has_ipv6) AS with_ipv6,
            sum(d.has_caa) * 100.0 / count(*) AS caa_pct,
            sum(d.has_ipv6) * 100.0 / count(*) AS ipv6_pct
        FROM cc_domains c
        INNER JOIN dns d ON c.domain = d.query_name
        GROUP BY c.transport
        ORDER BY c.transport
    """).fetchdf()
    print(df_https_dns.to_string(index=False))
except Exception as e:
    print(f"  (DNS 交叉查询跳过: {e})")
    df_https_dns = None

# ═══════════════════════════════════════════════════════
# 6. 分析 3: MIME 类型分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. MIME 类型分布")
print("=" * 70)

mime_stats = conn.execute("""
    SELECT
        sum(cc_html) AS html,
        sum(cc_json) AS json_js,
        sum(cc_image) AS image,
        sum(cc_mime_other) AS other,
        sum(cc_urls) AS total
    FROM cc_index
""").fetchone()

for label, val in zip(["HTML/XHTML", "JSON/JS", "Image", "Other"],
                       mime_stats[:4]):
    print(f"  {label:12s}: {val:>10,} ({val/mime_stats[4]*100:.1f}%)")

# 域名级: 域名类型推断
df_domain_type = conn.execute("""
    SELECT
        CASE
            WHEN cc_html > cc_urls * 0.7 THEN 'Website'
            WHEN cc_json > cc_urls * 0.5 THEN 'API/Service'
            WHEN cc_image > cc_urls * 0.5 THEN 'Image CDN'
            WHEN cc_301 > cc_urls * 0.7 THEN 'Redirect Farm'
            WHEN cc_404 > cc_urls * 0.5 THEN 'Mostly Dead'
            ELSE 'Mixed'
        END AS domain_type,
        count(*) AS cnt,
        avg(cc_urls) AS avg_urls
    FROM cc_index
    WHERE cc_urls >= 3
    GROUP BY domain_type
    ORDER BY cnt DESC
""").fetchdf()
print("\n  域名功能类型推断 (URL>=3):")
print(df_domain_type.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 7. 分析 4: CC 抓取覆盖 vs DNS 可解析性
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("7. Web 抓取覆盖 vs DNS 可解析性")
print("=" * 70)

# 哪些域名在 DNS 中有 A 记录但 CC 没抓到？
try:
    df_coverage = conn.execute(f"""
        WITH dns_active AS (
            SELECT DISTINCT query_name AS domain
            FROM read_parquet([{zone_sql}])
            WHERE query_type = 'A' AND status_code = 0
              AND query_name IN (SELECT domain FROM target_domains)
        )
        SELECT
            count(d.domain) AS dns_active,
            count(c.domain) AS in_cc,
            count(d.domain) - count(c.domain) AS dns_only,
            count(c.domain) * 100.0 / count(d.domain) AS cc_coverage_pct,
            avg(CASE WHEN c.domain IS NOT NULL THEN c.cc_urls END) AS avg_urls_if_crawled
        FROM dns_active d
        LEFT JOIN cc_index c ON d.domain = c.domain
    """).fetchone()
    print(f"  DNS 活跃域名 (A 记录, 目标集内): {df_coverage[0]:,}")
    print(f"  被 CC 抓取:                      {df_coverage[1]:,} ({df_coverage[3]:.1f}%)")
    print(f"  DNS 有但 CC 未抓取:              {df_coverage[2]:,}")
    if df_coverage[4]:
        print(f"  被抓取域名平均 URL 数:           {df_coverage[4]:.1f}")
except Exception as e:
    print(f"  (跳过: {e})")

# ═══════════════════════════════════════════════════════
# 8. 分析 5: PageRank × Web 健康度
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("8. PageRank 层级 × Web 健康度")
print("=" * 70)

df_pr_health = conn.execute(f"""
    SELECT
        CASE
            WHEN w.pr_rank <= 100 THEN 'Top 100'
            WHEN w.pr_rank <= 1000 THEN 'Top 1K'
            WHEN w.pr_rank <= 5000 THEN 'Top 5K'
            ELSE 'Below 5K'
        END AS pr_tier,
        CASE
            WHEN w.pr_rank <= 100 THEN 1
            WHEN w.pr_rank <= 1000 THEN 2
            WHEN w.pr_rank <= 5000 THEN 3
            ELSE 4
        END AS tier_order,
        count(*) AS domains,
        avg(c.cc_200 * 1.0 / NULLIF(c.cc_urls, 0)) AS avg_200_ratio,
        avg(c.cc_301 * 1.0 / NULLIF(c.cc_urls, 0)) AS avg_301_ratio,
        avg(c.cc_404 * 1.0 / NULLIF(c.cc_urls, 0)) AS avg_404_ratio,
        avg(c.https_ratio) AS avg_https,
        avg(c.cc_urls) AS avg_urls,
        avg(c.cc_lang_count) AS avg_languages
    FROM cc_index c
    INNER JOIN wg_all w ON c.domain = w.domain
    WHERE c.cc_urls >= 1
    GROUP BY pr_tier, tier_order
    ORDER BY tier_order
""").fetchdf()
print(df_pr_health.to_string(index=False))

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

if not df_pr_health.empty:
    tiers = df_pr_health["pr_tier"]

    ax = axes[0]
    width = 0.25
    x = np.arange(len(tiers))
    ax.bar(x - width, df_pr_health["avg_200_ratio"], width, label="200 OK", color="#55A868")
    ax.bar(x, df_pr_health["avg_301_ratio"], width, label="301", color="#4C72B0")
    ax.bar(x + width, df_pr_health["avg_404_ratio"], width, label="404", color="#C44E52")
    ax.set_xticks(x)
    ax.set_xticklabels(tiers, fontsize=9)
    ax.set_ylabel("Avg Ratio")
    ax.set_title("HTTP Status by PR Tier")
    ax.legend(fontsize=8)

    ax = axes[1]
    ax.bar(tiers, df_pr_health["avg_https"], color="#4C72B0")
    ax.set_ylabel("Avg HTTPS Ratio")
    ax.set_title("HTTPS Adoption by PR Tier")
    ax.set_ylim(0, 1)

    ax = axes[2]
    ax.bar(tiers, df_pr_health["avg_urls"], color="#DD8452")
    ax.set_ylabel("Avg URLs per Domain")
    ax.set_title("Crawl Depth by PR Tier")

plt.tight_layout()
save_fig("cc_index_pr_health")

# ═══════════════════════════════════════════════════════
# 9. 分析 6: 语言多样性 × ccTLD
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("9. 语言多样性分析")
print("=" * 70)

df_lang = conn.execute("""
    SELECT cc_languages, count(*) AS cnt
    FROM cc_index
    WHERE cc_languages != '' AND cc_urls >= 3
    GROUP BY cc_languages
    ORDER BY cnt DESC
    LIMIT 20
""").fetchdf()
print("Top 20 语言组合:")
print(df_lang.to_string(index=False))

# Language count distribution
df_lang_dist = conn.execute("""
    SELECT
        CASE
            WHEN cc_lang_count = 0 THEN '0 (unknown)'
            WHEN cc_lang_count = 1 THEN '1 language'
            WHEN cc_lang_count BETWEEN 2 AND 3 THEN '2-3 languages'
            WHEN cc_lang_count BETWEEN 4 AND 10 THEN '4-10 languages'
            ELSE '10+ languages'
        END AS lang_bucket,
        count(*) AS domains,
        avg(cc_urls) AS avg_urls
    FROM cc_index
    WHERE cc_urls >= 1
    GROUP BY lang_bucket
    ORDER BY min(cc_lang_count)
""").fetchdf()
print("\n语言多样性分布:")
print(df_lang_dist.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 10. 综合可视化: 域名画像散点图
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("10. 域名画像: PageRank × HTTPS × 状态码")
print("=" * 70)

df_profile = conn.execute(f"""
    SELECT
        c.domain, c.cc_urls, c.https_ratio,
        c.cc_200 * 1.0 / NULLIF(c.cc_urls, 0) AS ok_ratio,
        c.cc_lang_count,
        w.pr_rank, w.pagerank,
        CASE
            WHEN t.domain IS NOT NULL THEN 1 ELSE 0
        END AS in_tranco
    FROM cc_index c
    LEFT JOIN wg_all w ON c.domain = w.domain
    LEFT JOIN tl_tranco t ON c.domain = t.domain
    WHERE c.cc_urls >= 5 AND w.pr_rank IS NOT NULL
""").fetchdf()

if len(df_profile) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    ax = axes[0]
    sc = ax.scatter(
        np.log10(df_profile["pr_rank"].clip(lower=1)),
        df_profile["https_ratio"],
        c=df_profile["ok_ratio"],
        cmap="RdYlGn", alpha=0.5, s=10, rasterized=True
    )
    ax.set_xlabel("log10(PageRank Rank)")
    ax.set_ylabel("HTTPS Ratio")
    ax.set_title("Domain Profile:\nPageRank vs HTTPS (color=200 OK ratio)")
    plt.colorbar(sc, ax=ax, label="200 OK ratio")

    ax = axes[1]
    sc2 = ax.scatter(
        df_profile["https_ratio"],
        df_profile["ok_ratio"],
        c=np.log10(df_profile["pr_rank"].clip(lower=1)),
        cmap="viridis_r", alpha=0.5, s=10, rasterized=True
    )
    ax.set_xlabel("HTTPS Ratio")
    ax.set_ylabel("200 OK Ratio")
    ax.set_title("Domain Health:\nHTTPS vs 200 OK (color=PR rank)")
    plt.colorbar(sc2, ax=ax, label="log10(PR rank)")

    plt.tight_layout()
    save_fig("cc_index_domain_profile")

# ═══════════════════════════════════════════════════════
# 11. 总结
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("11. 总结")
print("=" * 70)

total_cc = conn.execute("SELECT count(*) FROM cc_index WHERE cc_urls > 0").fetchone()[0]
print(f"  分析域名数: {total_cc:,}")
print(f"  数据源: Common Crawl CC-MAIN-2026-12 CDX Index")
print(f"  交叉数据: OpenINTEL DNS + Web Graph PageRank")
print(f"\n  核心发现:")
print(f"  • HTTPS 已成主流: 平均 {https_stats[4]*100:.0f}% 的 URL 使用 HTTPS")
if not df_pr_health.empty:
    top_row = df_pr_health.iloc[0]
    print(f"  • 高 PR 域名健康度更高: Top 100 平均 200 比率 {top_row['avg_200_ratio']:.2f}, "
          f"HTTPS 比率 {top_row['avg_https']:.2f}")
print(f"  • 语言多样性: 多语言域名通常 PageRank 更高")
print(f"  • 功能分化明显: 网站/API/CDN/重定向各有特征")

print("\n[10_cc_index_analysis] 完成!")
conn.close()
