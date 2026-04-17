#!/usr/bin/env python3
"""08 - Common Crawl Web Graph × OpenINTEL 交叉分析

聚焦域名之间的链接关系，而非网页内容本身：
  - Web Graph 规模概览（1.34 亿域名，54 亿条链接）
  - 域名 PageRank / 谐波中心性分布
  - TLD 在 Web Graph 中的地位
  - OpenINTEL 域名在 Web Graph 中的覆盖率与排名
  - 高 PageRank 域名的 DNS 基础设施特征
  - 链接关系与 DNS 托管的关联
"""

import sys, os, gzip
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
from config import (
    get_conn, ZONE_TLDS, TOPLISTS, zone_glob, toplist_glob,
    all_zone_sql, save_fig, BASE_DIR, REPO_DIR, OUTPUT_DIR
)

WG_DIR = REPO_DIR / "downloads" / "common-crawl" / "webgraph"
VERTICES_FILE = WG_DIR / "domain-vertices.txt.gz"
RANKS_FILE = WG_DIR / "domain-ranks.txt.gz"

conn = get_conn()

# ═══════════════════════════════════════════════════════
# 0. 导入 Web Graph 数据到 DuckDB
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("0. 导入 Common Crawl Web Graph 数据")
print("=" * 70)

# Vertices: id \t reversed_domain \t num_hosts
print("  加载 domain vertices...")
conn.execute(f"""
    CREATE OR REPLACE TABLE wg_vertices AS
    SELECT
        column0::INTEGER AS node_id,
        column1 AS rev_domain,
        column2::INTEGER AS num_hosts,
        -- 反转域名恢复正常格式: com.example -> example.com
        list_reverse(string_split(column1, '.')) AS parts
    FROM read_csv('{VERTICES_FILE}',
        delim='\t', header=false, columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR'}},
        compression='gzip')
""")
vtx_count = conn.execute("SELECT count(*) FROM wg_vertices").fetchone()[0]
print(f"  顶点数: {vtx_count:,}")

# 提取 TLD 和正常域名
conn.execute("""
    ALTER TABLE wg_vertices ADD COLUMN tld VARCHAR;
    UPDATE wg_vertices SET tld = parts[1];
""")
conn.execute("""
    ALTER TABLE wg_vertices ADD COLUMN domain VARCHAR;
    UPDATE wg_vertices SET domain = array_to_string(parts, '.') || '.';
""")

# Ranks: harmonicc_pos \t harmonicc_val \t pr_pos \t pr_val \t host_rev \t n_hosts
print("  加载 domain ranks...")
conn.execute(f"""
    CREATE OR REPLACE TABLE wg_ranks AS
    SELECT
        row_number() OVER () - 1 AS rank_row,
        column0::INTEGER AS harmonic_rank,
        column1::DOUBLE AS harmonic,
        column2::INTEGER AS pr_rank,
        column3::DOUBLE AS pagerank,
        column4 AS rev_domain,
        column5::INTEGER AS num_hosts_r
    FROM read_csv('{RANKS_FILE}',
        delim='\t', header=false,
        columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR',
                  'column3':'VARCHAR','column4':'VARCHAR','column5':'VARCHAR'}},
        compression='gzip',
        skip=1)
""")
rank_count = conn.execute("SELECT count(*) FROM wg_ranks").fetchone()[0]
print(f"  排名数: {rank_count:,}")

# Join vertices + ranks via rev_domain
conn.execute("""
    CREATE OR REPLACE TABLE wg AS
    SELECT v.node_id, v.rev_domain, v.domain, v.tld, v.num_hosts,
           r.harmonic, r.harmonic_rank, r.pagerank, r.pr_rank
    FROM wg_vertices v
    JOIN wg_ranks r ON v.rev_domain = r.rev_domain
""")
wg_count = conn.execute("SELECT count(*) FROM wg").fetchone()[0]
print(f"  合并后: {wg_count:,} 域名")

# ═══════════════════════════════════════════════════════
# 1. Web Graph 规模概览
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. Common Crawl Web Graph 规模概览")
print("=" * 70)

stats = conn.execute("""
    SELECT
        count(*) AS total_domains,
        sum(num_hosts) AS total_hosts,
        avg(num_hosts) AS avg_hosts,
        max(num_hosts) AS max_hosts,
        count(DISTINCT tld) AS tld_count,
        avg(pagerank) AS avg_pr,
        max(pagerank) AS max_pr,
        percentile_disc(0.5) WITHIN GROUP (ORDER BY pagerank) AS median_pr,
        percentile_disc(0.99) WITHIN GROUP (ORDER BY pagerank) AS p99_pr
    FROM wg
""").fetchone()

print(f"  域名总数:     {stats[0]:>15,}")
print(f"  主机总数:     {stats[1]:>15,}")
print(f"  平均主机/域名: {stats[2]:>15.1f}")
print(f"  最大主机数:   {stats[3]:>15,}")
print(f"  TLD 种类:     {stats[4]:>15,}")
print(f"  PageRank: 平均={stats[5]:.6f}, 中位={stats[7]:.6f}, P99={stats[8]:.6f}, 最大={stats[6]:.4f}")

# ═══════════════════════════════════════════════════════
# 2. TLD 在 Web Graph 中的分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. TLD 在 Web Graph 中的分布 (Top 20)")
print("=" * 70)

df_tld = conn.execute("""
    SELECT tld, count(*) AS domain_count,
           sum(num_hosts) AS host_count,
           avg(pagerank) AS avg_pr,
           max(pagerank) AS max_pr
    FROM wg
    GROUP BY tld
    ORDER BY domain_count DESC
    LIMIT 20
""").fetchdf()
print(df_tld.to_string(index=False))

fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(df_tld["tld"][::-1], df_tld["domain_count"][::-1],
        color=sns.color_palette("viridis", len(df_tld)))
ax.set_xlabel("Number of Domains")
ax.set_title("Common Crawl Web Graph: Top 20 TLDs by Domain Count")
for i, v in enumerate(df_tld["domain_count"][::-1]):
    ax.text(v, i, f" {v/1e6:.1f}M", va="center", fontsize=7)
save_fig("webgraph_tld_distribution")

# ═══════════════════════════════════════════════════════
# 3. PageRank 分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. PageRank 分布")
print("=" * 70)

df_pr_hist = conn.execute("""
    SELECT pagerank FROM wg
    WHERE pagerank > 0
    USING SAMPLE 500000
""").fetchdf()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
ax = axes[0]
ax.hist(np.log10(df_pr_hist["pagerank"]), bins=100, color="#4C72B0", edgecolor="white")
ax.set_xlabel("log10(PageRank)")
ax.set_ylabel("Count")
ax.set_title("PageRank Distribution (sampled 500K)")

# Top 20 by PageRank
df_top_pr = conn.execute("""
    SELECT domain, tld, num_hosts, pagerank, harmonic
    FROM wg
    ORDER BY pagerank DESC
    LIMIT 20
""").fetchdf()
print("Top 20 域名 by PageRank:")
print(df_top_pr.to_string(index=False))

ax = axes[1]
ax.barh(df_top_pr["domain"][::-1].str.rstrip('.'), df_top_pr["pagerank"][::-1],
        color=sns.color_palette("Reds_r", len(df_top_pr)))
ax.set_xlabel("PageRank")
ax.set_title("Top 20 Domains by PageRank")
plt.tight_layout()
save_fig("webgraph_pagerank_distribution")

# ═══════════════════════════════════════════════════════
# 4. OpenINTEL 域名在 Web Graph 中的覆盖
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. OpenINTEL 域名在 Web Graph 中的覆盖")
print("=" * 70)

# Load OpenINTEL unique domains into temp table
zone_sql = all_zone_sql()
conn.execute(f"""
    CREATE OR REPLACE TABLE oi_domains AS
    SELECT DISTINCT query_name AS domain
    FROM read_parquet([{zone_sql}])
    WHERE query_type = 'A'
""")
oi_total = conn.execute("SELECT count(*) FROM oi_domains").fetchone()[0]

# Join
conn.execute("""
    CREATE OR REPLACE TABLE oi_wg AS
    SELECT o.domain, w.tld, w.num_hosts, w.pagerank, w.harmonic
    FROM oi_domains o
    INNER JOIN wg w ON o.domain = w.domain
""")
matched = conn.execute("SELECT count(*) FROM oi_wg").fetchone()[0]

print(f"  OpenINTEL 域名总数 (A 记录): {oi_total:,}")
print(f"  在 Web Graph 中找到: {matched:,} ({matched/oi_total*100:.1f}%)")
print(f"  未在 Web Graph 中: {oi_total - matched:,}")

# Per-TLD coverage
print("\n  各 TLD 覆盖率:")
for tld in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT
            (SELECT count(DISTINCT query_name) FROM read_parquet('{zone_glob(tld)}') WHERE query_type='A') AS oi_cnt,
            (SELECT count(*) FROM oi_wg WHERE tld = '{tld}') AS wg_cnt
    """).fetchone()
    if r[0] > 0:
        print(f"    .{tld:4s}: {r[1]:>10,} / {r[0]:>10,} ({r[1]/r[0]*100:.1f}%)")

# ═══════════════════════════════════════════════════════
# 5. 高 PageRank 域名的 DNS 特征
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. 高 PageRank 域名的 DNS 基础设施特征")
print("=" * 70)

# Top 10K by PageRank that are in OpenINTEL
conn.execute("""
    CREATE OR REPLACE TABLE top_pr_oi AS
    SELECT * FROM oi_wg ORDER BY pagerank DESC LIMIT 10000
""")

# NS provider distribution for top PageRank domains
df_ns_top = conn.execute(f"""
    WITH top_domains AS (SELECT domain FROM top_pr_oi),
    ns_data AS (
        SELECT
            d.query_name,
            list_extract(string_split(d.ns_address, '.'), greatest(len(string_split(d.ns_address, '.')) - 2, 1))
            || '.' ||
            list_extract(string_split(d.ns_address, '.'), greatest(len(string_split(d.ns_address, '.')) - 1, 1))
            AS ns_provider
        FROM read_parquet([{zone_sql}]) d
        INNER JOIN top_domains t ON d.query_name = t.domain
        WHERE d.query_type = 'NS' AND d.ns_address IS NOT NULL
    )
    SELECT ns_provider, count(DISTINCT query_name) AS domain_count
    FROM ns_data
    WHERE ns_provider IS NOT NULL AND ns_provider != '.'
    GROUP BY ns_provider
    ORDER BY domain_count DESC
    LIMIT 10
""").fetchdf()
print("Top PageRank 域名的 NS 提供商 (Top 10):")
print(df_ns_top.to_string(index=False))

# DNSSEC rate for high vs low PageRank
pr_security = conn.execute(f"""
    WITH ranked AS (
        SELECT o.domain, o.pagerank,
            CASE WHEN o.pagerank >= (SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY pagerank) FROM oi_wg)
                 THEN 'Top 10%'
                 WHEN o.pagerank >= (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY pagerank) FROM oi_wg)
                 THEN 'Middle'
                 ELSE 'Bottom 50%' END AS pr_tier
        FROM oi_wg o
    )
    SELECT pr_tier,
        count(*) AS total,
        count(DISTINCT CASE WHEN d.query_type='DS' AND d.ds_key_tag IS NOT NULL
              THEN d.query_name END) AS has_ds,
        count(DISTINCT CASE WHEN d.query_type='AAAA' AND d.ip6_address IS NOT NULL
              THEN d.query_name END) AS has_ipv6
    FROM ranked r
    JOIN read_parquet([{zone_sql}]) d ON r.domain = d.query_name
    GROUP BY pr_tier
    ORDER BY pr_tier
""").fetchdf()
print("\nPageRank 分层 vs DNSSEC & IPv6:")
for _, row in pr_security.iterrows():
    ds_pct = row["has_ds"] / row["total"] * 100
    v6_pct = row["has_ipv6"] / row["total"] * 100
    print(f"  {row['pr_tier']:12s}: DNSSEC {ds_pct:.1f}%, IPv6 {v6_pct:.1f}% ({row['total']:,} domains)")

# ═══════════════════════════════════════════════════════
# 6. Web Graph 域名规模 vs DNS 主机数
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. Web Graph 主机数分布")
print("=" * 70)

df_hosts = conn.execute("""
    SELECT
        CASE
            WHEN num_hosts = 1 THEN '1 host'
            WHEN num_hosts BETWEEN 2 AND 5 THEN '2-5 hosts'
            WHEN num_hosts BETWEEN 6 AND 20 THEN '6-20 hosts'
            WHEN num_hosts BETWEEN 21 AND 100 THEN '21-100 hosts'
            WHEN num_hosts > 100 THEN '100+ hosts'
        END AS host_bucket,
        count(*) AS domain_count,
        avg(pagerank) AS avg_pr
    FROM wg
    GROUP BY host_bucket
    ORDER BY min(num_hosts)
""").fetchdf()
print(df_hosts.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 7. TLD 平均 PageRank 对比（我们的 ccTLD vs 全球）
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("7. ccTLD 在全球 Web Graph 中的地位")
print("=" * 70)

our_tlds = ZONE_TLDS
df_cctld_rank = conn.execute(f"""
    SELECT tld,
           count(*) AS domains_in_webgraph,
           avg(pagerank) AS avg_pagerank,
           sum(CASE WHEN pagerank > (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY pagerank) FROM wg)
               THEN 1 ELSE 0 END) AS top1pct_count
    FROM wg
    WHERE tld IN ({','.join(f"'{t}'" for t in our_tlds)})
    GROUP BY tld
    ORDER BY avg_pagerank DESC
""").fetchdf()
print(df_cctld_rank.to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
ax = axes[0]
ax.barh(df_cctld_rank["tld"][::-1], df_cctld_rank["avg_pagerank"][::-1],
        color=sns.color_palette("Set2", len(df_cctld_rank)))
ax.set_xlabel("Average PageRank")
ax.set_title("Our ccTLDs: Average PageRank in Web Graph")

ax = axes[1]
ax.barh(df_cctld_rank["tld"][::-1], df_cctld_rank["domains_in_webgraph"][::-1],
        color=sns.color_palette("Blues_r", len(df_cctld_rank)))
ax.set_xlabel("Domains in Web Graph")
ax.set_title("Our ccTLDs: Presence in Web Graph")
for i, v in enumerate(df_cctld_rank["domains_in_webgraph"][::-1]):
    ax.text(v, i, f" {v/1e6:.2f}M", va="center", fontsize=8)
plt.tight_layout()
save_fig("webgraph_cctld_ranking")

print("\n[08_webgraph_analysis] 完成!")
conn.close()
