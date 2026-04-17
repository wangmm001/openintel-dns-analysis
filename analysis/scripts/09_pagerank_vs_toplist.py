#!/usr/bin/env python3
"""09 - PageRank vs TopList 排名对比分析

Common Crawl Web Graph 的 PageRank 反映链接结构影响力，
TopList (Tranco/Umbrella/Radar) 反映实际流量/DNS 查询热度。
两者排名不一致的域名具有特殊研究价值：
  - 高 PageRank 低流量 → link farm / 历史遗留 / 被嵌入资源
  - 高流量低 PageRank → 新兴平台 / 移动端优先 / 封闭生态

分析内容:
  1. 数据匹配概览
  2. PageRank 排名 vs TopList 排名散点图
  3. 排名差异最大的域名
  4. 按 TLD 的排名偏差模式
  5. 高 PR 低流量域名的 DNS 特征
  6. 高流量低 PR 域名的 DNS 特征
  7. 排名一致性的行业分布
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from config import (
    get_conn, TOPLISTS, toplist_glob, all_zone_sql, zone_glob,
    ZONE_TLDS, save_fig, BASE_DIR, REPO_DIR, OUTPUT_DIR
)

WG_DIR = REPO_DIR / "downloads" / "common-crawl" / "webgraph"
RANKS_FILE = WG_DIR / "domain-ranks.txt.gz"
VERTICES_FILE = WG_DIR / "domain-vertices.txt.gz"

conn = get_conn()

# 使用的 TopList 源
TOPLIST_SOURCES = [t for t in TOPLISTS if t != "majestic"]  # tranco, umbrella, radar

# ═══════════════════════════════════════════════════════
# 0. 导入数据
# ═══════════════════════════════════════════════════════
print("=" * 70)
print("0. 导入 Web Graph + TopList 数据")
print("=" * 70)

# Web Graph ranks → 直接从 ranks 文件建表，含 PageRank 排名
print("  加载 Web Graph ranks...")
conn.execute(f"""
    CREATE OR REPLACE TABLE wg_ranks AS
    SELECT
        column0::INTEGER AS harmonic_rank,
        column1::DOUBLE  AS harmonic,
        column2::INTEGER AS pr_rank,
        column3::DOUBLE  AS pagerank,
        column4          AS rev_domain,
        column5::INTEGER AS num_hosts,
        -- 反转域名恢复正常格式
        array_to_string(list_reverse(string_split(column4, '.')), '.') || '.' AS domain
    FROM read_csv('{RANKS_FILE}',
        delim='\t', header=false,
        columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR',
                  'column3':'VARCHAR','column4':'VARCHAR','column5':'VARCHAR'}},
        compression='gzip', skip=1)
""")
wg_total = conn.execute("SELECT count(*) FROM wg_ranks").fetchone()[0]
print(f"  Web Graph 域名数: {wg_total:,}")

# TopList 域名（去重取唯一域名集合）
for src in TOPLIST_SOURCES:
    print(f"  加载 TopList: {src}...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE tl_{src} AS
        SELECT DISTINCT query_name AS domain
        FROM read_parquet('{toplist_glob(src)}')
    """)
    cnt = conn.execute(f"SELECT count(*) FROM tl_{src}").fetchone()[0]
    print(f"    域名数: {cnt:,}")

# ═══════════════════════════════════════════════════════
# 1. 匹配概览：各 TopList 在 Web Graph 中的覆盖
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. TopList 在 Web Graph 中的覆盖率")
print("=" * 70)

coverage_rows = []
for src in TOPLIST_SOURCES:
    r = conn.execute(f"""
        SELECT
            (SELECT count(*) FROM tl_{src}) AS tl_total,
            count(*) AS matched,
            avg(w.pr_rank) AS avg_pr_rank,
            min(w.pr_rank) AS best_pr_rank,
            avg(w.pagerank) AS avg_pagerank
        FROM tl_{src} t
        INNER JOIN wg_ranks w ON t.domain = w.domain
    """).fetchone()
    tl_total, matched, avg_pr_rank, best_pr_rank, avg_pr = r
    pct = matched / tl_total * 100
    print(f"  {src:10s}: {matched:>10,} / {tl_total:>10,} ({pct:.1f}%) "
          f"| avg PR rank: {avg_pr_rank:,.0f} | avg PageRank: {avg_pr:.6f}")
    coverage_rows.append({
        "source": src, "total": tl_total, "matched": matched,
        "coverage_pct": pct, "avg_pr_rank": avg_pr_rank
    })

# ═══════════════════════════════════════════════════════
# 2. 以 Tranco 为主：构建排名对比表
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. Tranco vs PageRank 排名对比")
print("=" * 70)

# Tranco 没有显式 rank 列，用 row_number 按域名出现顺序模拟
# 实际 Tranco 列表是按流量排序的，OpenINTEL 数据中域名去重后
# 用字母序 + 出现频次近似排名；这里用 PR rank 的百分位做对比
conn.execute(f"""
    CREATE OR REPLACE TABLE tranco_vs_pr AS
    WITH tranco_domains AS (
        SELECT DISTINCT query_name AS domain
        FROM read_parquet('{toplist_glob("tranco")}')
    )
    SELECT
        t.domain,
        w.pr_rank,
        w.pagerank,
        w.harmonic_rank,
        w.harmonic,
        w.num_hosts,
        -- 提取 TLD
        list_extract(string_split(trim(t.domain, '.'), '.'),
                     len(string_split(trim(t.domain, '.'), '.'))) AS tld,
        -- PR 百分位 (0=最好, 1=最差)
        w.pr_rank * 1.0 / {wg_total} AS pr_percentile,
        -- 是否在各 TopList 中
        (SELECT count(*) > 0 FROM tl_umbrella u WHERE u.domain = t.domain) AS in_umbrella,
        (SELECT count(*) > 0 FROM tl_radar r WHERE r.domain = t.domain) AS in_radar
    FROM tranco_domains t
    INNER JOIN wg_ranks w ON t.domain = w.domain
    ORDER BY w.pr_rank
""")

matched_count = conn.execute("SELECT count(*) FROM tranco_vs_pr").fetchone()[0]
print(f"  Tranco ∩ Web Graph: {matched_count:,} 域名")

stats = conn.execute("""
    SELECT
        avg(pr_rank) AS avg_rank,
        percentile_disc(0.5) WITHIN GROUP (ORDER BY pr_rank) AS median_rank,
        percentile_disc(0.1) WITHIN GROUP (ORDER BY pr_rank) AS p10_rank,
        percentile_disc(0.9) WITHIN GROUP (ORDER BY pr_rank) AS p90_rank,
        avg(pagerank) AS avg_pr,
        max(pagerank) AS max_pr
    FROM tranco_vs_pr
""").fetchone()
print(f"  PR rank: 平均={stats[0]:,.0f}, 中位={stats[1]:,}, P10={stats[2]:,}, P90={stats[3]:,}")
print(f"  PageRank: 平均={stats[4]:.6f}, 最大={stats[5]:.6f}")

# ═══════════════════════════════════════════════════════
# 3. 排名散点图 + 密度图
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. 排名分布可视化")
print("=" * 70)

# 为 Tranco 域名分配一个伪排名（按 PR rank 排序后的序号）
df_scatter = conn.execute("""
    SELECT
        row_number() OVER (ORDER BY pr_rank) AS tranco_order,
        pr_rank,
        pagerank,
        tld,
        domain
    FROM tranco_vs_pr
""").fetchdf()

fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# 3a. PR rank 分布直方图
ax = axes[0]
ax.hist(np.log10(df_scatter["pr_rank"].clip(lower=1)),
        bins=80, color="#4C72B0", edgecolor="white", alpha=0.8)
ax.set_xlabel("log10(PageRank Rank)")
ax.set_ylabel("Count")
ax.set_title("Tranco Domains:\nPageRank Rank Distribution")
ax.axvline(np.log10(wg_total * 0.01), color="red", ls="--", alpha=0.6, label="Top 1%")
ax.axvline(np.log10(wg_total * 0.10), color="orange", ls="--", alpha=0.6, label="Top 10%")
ax.legend(fontsize=8)

# 3b. PageRank 值分布
ax = axes[1]
pr_vals = df_scatter["pagerank"]
ax.hist(np.log10(pr_vals[pr_vals > 0]), bins=80, color="#DD8452", edgecolor="white", alpha=0.8)
ax.set_xlabel("log10(PageRank)")
ax.set_ylabel("Count")
ax.set_title("Tranco Domains:\nPageRank Value Distribution")

# 3c. CDF：Tranco 域名在 Web Graph 中的排名百分位
ax = axes[2]
sorted_pctile = np.sort(df_scatter["pr_rank"].values) / wg_total
ax.plot(sorted_pctile, np.linspace(0, 1, len(sorted_pctile)),
        color="#55A868", linewidth=2)
ax.set_xlabel("PageRank Percentile (0=best)")
ax.set_ylabel("CDF")
ax.set_title("Tranco Domains:\nCumulative PR Percentile")
ax.axvline(0.01, color="red", ls="--", alpha=0.5, label="Top 1%")
ax.axvline(0.10, color="orange", ls="--", alpha=0.5, label="Top 10%")
ax.legend(fontsize=8)
ax.set_xlim(0, 1)

plt.tight_layout()
save_fig("pagerank_vs_tranco_distribution")

# ═══════════════════════════════════════════════════════
# 4. 排名差异最大的域名
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. 排名差异分析：高 PR 低流量 vs 高流量低 PR")
print("=" * 70)

# 4a. 高 PageRank 但不在任何 TopList 中的域名
print("\n--- 高 PageRank 但不在 Tranco 中 (Web Graph Top 1000) ---")
df_high_pr_no_tranco = conn.execute(f"""
    WITH tranco_set AS (
        SELECT DISTINCT query_name AS domain FROM read_parquet('{toplist_glob("tranco")}')
    )
    SELECT w.domain, w.pr_rank, w.pagerank, w.harmonic_rank, w.num_hosts
    FROM wg_ranks w
    LEFT JOIN tranco_set t ON w.domain = t.domain
    WHERE t.domain IS NULL
      AND w.pr_rank <= 1000
    ORDER BY w.pr_rank
    LIMIT 30
""").fetchdf()
print(df_high_pr_no_tranco.to_string(index=False))

# 4b. 在 Tranco 中但 PageRank 极低的域名
print("\n--- Tranco 域名中 PageRank 排名最差的 30 个 ---")
df_tranco_low_pr = conn.execute("""
    SELECT domain, pr_rank, pagerank, tld, num_hosts,
           in_umbrella, in_radar
    FROM tranco_vs_pr
    ORDER BY pr_rank DESC
    LIMIT 30
""").fetchdf()
print(df_tranco_low_pr.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 5. 按 TLD 的排名偏差模式
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. TLD 维度的排名偏差")
print("=" * 70)

df_tld_bias = conn.execute(f"""
    SELECT
        tld,
        count(*) AS domains,
        avg(pr_percentile) AS avg_pr_pctile,
        percentile_disc(0.5) WITHIN GROUP (ORDER BY pr_percentile) AS median_pr_pctile,
        avg(pagerank) AS avg_pagerank,
        -- 在 Top 1% Web Graph 中的占比
        sum(CASE WHEN pr_percentile < 0.01 THEN 1 ELSE 0 END) * 100.0 / count(*) AS pct_in_top1,
        sum(CASE WHEN pr_percentile < 0.10 THEN 1 ELSE 0 END) * 100.0 / count(*) AS pct_in_top10
    FROM tranco_vs_pr
    GROUP BY tld
    HAVING count(*) >= 100
    ORDER BY avg_pr_pctile
""").fetchdf()
print("各 TLD Tranco 域名的 PageRank 百分位（越小=排名越好）:")
print(df_tld_bias.head(30).to_string(index=False))

# 可视化 Top 20 TLD
df_plot = df_tld_bias.head(20).copy()
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

ax = axes[0]
colors = plt.cm.RdYlGn_r(df_plot["avg_pr_pctile"] / df_plot["avg_pr_pctile"].max())
bars = ax.barh(df_plot["tld"][::-1], df_plot["avg_pr_pctile"][::-1], color=colors[::-1])
ax.set_xlabel("Average PR Percentile (lower = higher PageRank)")
ax.set_title("TLD Bias: Tranco Domains\' Average PageRank Percentile")

ax = axes[1]
ax.barh(df_plot["tld"][::-1], df_plot["pct_in_top1"][::-1],
        color="#C44E52", alpha=0.7, label="Top 1%")
ax.barh(df_plot["tld"][::-1], df_plot["pct_in_top10"][::-1],
        color="#4C72B0", alpha=0.4, label="Top 10%")
ax.set_xlabel("% of Tranco domains in Web Graph Top tier")
ax.set_title("TLD Bias: % Tranco Domains in PR Top Tiers")
ax.legend()

plt.tight_layout()
save_fig("pagerank_vs_tranco_tld_bias")

# ═══════════════════════════════════════════════════════
# 6. 三榜共识 vs 分歧
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. 三榜共识分析（Tranco + Umbrella + Radar vs PageRank）")
print("=" * 70)

df_consensus = conn.execute("""
    SELECT
        CASE
            WHEN in_umbrella AND in_radar THEN '3 lists (all)'
            WHEN in_umbrella OR in_radar  THEN '2 lists'
            ELSE '1 list (Tranco only)'
        END AS list_presence,
        count(*) AS domains,
        avg(pr_percentile) AS avg_pr_pctile,
        avg(pagerank) AS avg_pagerank,
        sum(CASE WHEN pr_percentile < 0.01 THEN 1 ELSE 0 END) * 100.0 / count(*) AS pct_top1,
        sum(CASE WHEN pr_percentile < 0.10 THEN 1 ELSE 0 END) * 100.0 / count(*) AS pct_top10
    FROM tranco_vs_pr
    GROUP BY list_presence
    ORDER BY avg_pr_pctile
""").fetchdf()
print(df_consensus.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
x = range(len(df_consensus))
width = 0.35
ax.bar([i - width/2 for i in x], df_consensus["pct_top1"], width,
       label="In PR Top 1%", color="#C44E52")
ax.bar([i + width/2 for i in x], df_consensus["pct_top10"], width,
       label="In PR Top 10%", color="#4C72B0")
ax.set_xticks(x)
ax.set_xticklabels(df_consensus["list_presence"], fontsize=9)
ax.set_ylabel("% of domains")
ax.set_title("TopList Consensus vs PageRank Tier")
ax.legend()
for i, row in df_consensus.iterrows():
    ax.text(i - width/2, row["pct_top1"] + 0.5, f'{row["pct_top1"]:.1f}%',
            ha="center", fontsize=8)
    ax.text(i + width/2, row["pct_top10"] + 0.5, f'{row["pct_top10"]:.1f}%',
            ha="center", fontsize=8)
plt.tight_layout()
save_fig("pagerank_vs_tranco_consensus")

# ═══════════════════════════════════════════════════════
# 7. 高 PR 低流量域名的 DNS 特征分析
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("7. 高 PageRank 低流量域名的 DNS 基础设施特征")
print("=" * 70)

zone_sql = all_zone_sql()

# 定义分组: Top 1K PR + in Tranco vs Top 1K PR + NOT in Tranco
conn.execute(f"""
    CREATE OR REPLACE TABLE pr_top1k AS
    SELECT domain, pr_rank, pagerank, num_hosts
    FROM wg_ranks
    WHERE pr_rank <= 1000
""")

conn.execute(f"""
    CREATE OR REPLACE TABLE pr_top1k_labeled AS
    SELECT
        p.*,
        CASE WHEN t.domain IS NOT NULL THEN 'In Tranco' ELSE 'Not in Tranco' END AS tranco_status
    FROM pr_top1k p
    LEFT JOIN tl_tranco t ON p.domain = t.domain
""")

label_stats = conn.execute("""
    SELECT tranco_status, count(*) AS cnt
    FROM pr_top1k_labeled
    GROUP BY tranco_status
""").fetchdf()
print("Top 1000 PR 域名的 Tranco 覆盖:")
print(label_stats.to_string(index=False))

# TLD 分布差异
print("\nTop 1K PR: TLD 分布（按 Tranco 状态）")
df_tld_diff = conn.execute("""
    SELECT
        list_extract(string_split(trim(domain, '.'), '.'),
                     len(string_split(trim(domain, '.'), '.'))) AS tld,
        tranco_status,
        count(*) AS cnt
    FROM pr_top1k_labeled
    GROUP BY tld, tranco_status
    HAVING count(*) >= 5
    ORDER BY cnt DESC
    LIMIT 30
""").fetchdf()
print(df_tld_diff.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 8. 排名对比散点图（Tranco 中的域名）
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("8. 排名散点图: Harmonic Rank vs PageRank Rank")
print("=" * 70)

df_rank_scatter = conn.execute("""
    SELECT pr_rank, harmonic_rank, pagerank, tld, domain,
           in_umbrella, in_radar
    FROM tranco_vs_pr
    USING SAMPLE 50000
""").fetchdf()

fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# 8a. PR rank vs Harmonic rank (colored by consensus)
ax = axes[0]
df_rank_scatter["consensus"] = (
    df_rank_scatter["in_umbrella"].astype(int) +
    df_rank_scatter["in_radar"].astype(int) + 1
)
scatter = ax.scatter(
    np.log10(df_rank_scatter["pr_rank"].clip(lower=1)),
    np.log10(df_rank_scatter["harmonic_rank"].clip(lower=1)),
    c=df_rank_scatter["consensus"],
    cmap="RdYlGn_r", alpha=0.3, s=3, rasterized=True
)
ax.plot([0, 9], [0, 9], "k--", alpha=0.3, label="y=x")
ax.set_xlabel("log10(PageRank Rank)")
ax.set_ylabel("log10(Harmonic Centrality Rank)")
ax.set_title("PR Rank vs Harmonic Rank\n(Tranco domains, colored by list count)")
cbar = plt.colorbar(scatter, ax=ax)
cbar.set_label("# TopLists")

# 8b. PageRank 值 vs num_hosts
ax = axes[1]
ax.scatter(
    np.log10(df_rank_scatter["pr_rank"].clip(lower=1)),
    np.log10(df_rank_scatter["pagerank"].clip(lower=1e-12)),
    c=df_rank_scatter["consensus"],
    cmap="RdYlGn_r", alpha=0.3, s=3, rasterized=True
)
ax.set_xlabel("log10(PageRank Rank)")
ax.set_ylabel("log10(PageRank Value)")
ax.set_title("PR Rank vs PR Value\n(Tranco domains)")

plt.tight_layout()
save_fig("pagerank_vs_tranco_scatter")

# ═══════════════════════════════════════════════════════
# 9. 跨榜排名一致性热力图
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("9. PageRank 分层 × TopList 覆盖热力图")
print("=" * 70)

df_heatmap = conn.execute(f"""
    WITH pr_tiers AS (
        SELECT domain,
            CASE
                WHEN pr_rank <= 1000 THEN 'Top 1K'
                WHEN pr_rank <= 10000 THEN 'Top 10K'
                WHEN pr_rank <= 100000 THEN 'Top 100K'
                WHEN pr_rank <= 1000000 THEN 'Top 1M'
                ELSE 'Below 1M'
            END AS pr_tier,
            CASE
                WHEN pr_rank <= 1000 THEN 1
                WHEN pr_rank <= 10000 THEN 2
                WHEN pr_rank <= 100000 THEN 3
                WHEN pr_rank <= 1000000 THEN 4
                ELSE 5
            END AS tier_order
        FROM wg_ranks
    )
    SELECT
        p.pr_tier, p.tier_order,
        count(*) AS wg_total,
        sum(CASE WHEN t.domain IS NOT NULL THEN 1 ELSE 0 END) AS in_tranco,
        sum(CASE WHEN u.domain IS NOT NULL THEN 1 ELSE 0 END) AS in_umbrella,
        sum(CASE WHEN r.domain IS NOT NULL THEN 1 ELSE 0 END) AS in_radar,
        sum(CASE WHEN t.domain IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / count(*) AS tranco_pct,
        sum(CASE WHEN u.domain IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / count(*) AS umbrella_pct,
        sum(CASE WHEN r.domain IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / count(*) AS radar_pct
    FROM pr_tiers p
    LEFT JOIN tl_tranco t ON p.domain = t.domain
    LEFT JOIN tl_umbrella u ON p.domain = u.domain
    LEFT JOIN tl_radar r ON p.domain = r.domain
    GROUP BY p.pr_tier, p.tier_order
    ORDER BY p.tier_order
""").fetchdf()
print(df_heatmap[["pr_tier", "wg_total", "in_tranco", "tranco_pct",
                   "in_umbrella", "umbrella_pct", "in_radar", "radar_pct"]].to_string(index=False))

# 热力图
heatmap_data = df_heatmap.set_index("pr_tier")[["tranco_pct", "umbrella_pct", "radar_pct"]]
heatmap_data.columns = ["Tranco", "Umbrella", "Radar"]

fig, ax = plt.subplots(figsize=(8, 5))
sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="YlOrRd",
            ax=ax, cbar_kws={"label": "% covered by TopList"})
ax.set_title("TopList Coverage by PageRank Tier")
ax.set_ylabel("PageRank Tier")
plt.tight_layout()
save_fig("pagerank_vs_tranco_heatmap")

# ═══════════════════════════════════════════════════════
# 10. 总结统计
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("10. 总结")
print("=" * 70)

summary = conn.execute(f"""
    SELECT
        (SELECT count(*) FROM wg_ranks) AS wg_total,
        (SELECT count(*) FROM tl_tranco) AS tranco_total,
        (SELECT count(*) FROM tranco_vs_pr) AS matched,
        (SELECT count(*) FROM tranco_vs_pr WHERE pr_percentile < 0.01) AS tranco_in_top1pct,
        (SELECT count(*) FROM tranco_vs_pr WHERE pr_percentile < 0.10) AS tranco_in_top10pct,
        (SELECT count(*) FROM wg_ranks WHERE pr_rank <= 1000
            AND domain NOT IN (SELECT domain FROM tl_tranco)) AS top1k_not_in_tranco
""").fetchone()

print(f"  Web Graph 总域名:         {summary[0]:>12,}")
print(f"  Tranco 总域名:            {summary[1]:>12,}")
print(f"  交集域名:                 {summary[2]:>12,} ({summary[2]/summary[1]*100:.1f}%)")
print(f"  Tranco 在 PR Top 1%:      {summary[3]:>12,} ({summary[3]/summary[2]*100:.1f}%)")
print(f"  Tranco 在 PR Top 10%:     {summary[4]:>12,} ({summary[4]/summary[2]*100:.1f}%)")
print(f"  PR Top 1K 不在 Tranco:    {summary[5]:>12,}")

print("\n解读:")
print("  • 高 PR + 不在 Tranco → 可能是嵌入式资源域名（CDN/广告/追踪）、")
print("    历史权威站点、或 link farm")
print("  • 在 Tranco + PR 极低 → 新兴移动应用、封闭平台、")
print("    或主要通过直接访问/App 而非 Web 链接获取流量")
print("  • 三榜共识 + 高 PR → 真正的互联网核心基础设施域名")

print("\n[09_pagerank_vs_toplist] 完成!")
conn.close()
