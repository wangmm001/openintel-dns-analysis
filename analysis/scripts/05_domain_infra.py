#!/usr/bin/env python3
"""05 - 域名基础设施分析

输出：
  - CNAME 使用率（CDN 使用指标）
  - SOA 参数分析（refresh, retry, expire 分布）
  - NS 记录数量分布（DNS 冗余度）
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from config import get_conn, parquet_glob, TLDS, save_fig

conn = get_conn()
globs = ", ".join(f"'{parquet_glob(t)}'" for t in TLDS)

# ═══════════════════════════════════════════════════════
# 1. CNAME 使用率
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. CNAME 使用率（按 TLD）")
print("=" * 60)

cname_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_domains,
            count(DISTINCT CASE WHEN response_type = 'CNAME' AND cname_name IS NOT NULL
                  THEN query_name END) AS has_cname
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    cname_stats.append({
        "tld": tld,
        "total": r[0],
        "cname_count": r[1],
        "cname_pct": round(r[1] / r[0] * 100, 2),
    })

df_cname = pd.DataFrame(cname_stats)
print(df_cname.to_string(index=False))

# CNAME 目标域 Top 20（指向哪些 CDN/平台）
print("\nCNAME 目标域 Top 20:")
df_cname_target = conn.execute(f"""
    WITH cname_data AS (
        SELECT
            list_extract(string_split(cname_name, '.'), greatest(len(string_split(cname_name, '.')) - 2, 1))
            || '.' ||
            list_extract(string_split(cname_name, '.'), greatest(len(string_split(cname_name, '.')) - 1, 1))
            AS target_domain
        FROM read_parquet([{globs}])
        WHERE response_type = 'CNAME' AND cname_name IS NOT NULL
    )
    SELECT target_domain, count(*) AS cnt
    FROM cname_data
    WHERE target_domain IS NOT NULL AND target_domain != '.'
    GROUP BY target_domain
    ORDER BY cnt DESC
    LIMIT 20
""").fetchdf()
print(df_cname_target.to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

ax = axes[0]
ax.bar(df_cname["tld"], df_cname["cname_pct"], color=sns.color_palette("Set2", len(df_cname)))
ax.set_ylabel("% of Domains")
ax.set_title("CNAME Usage Rate by TLD")
for i, v in enumerate(df_cname["cname_pct"]):
    ax.text(i, v + 0.2, f"{v:.1f}%", ha="center", fontsize=8)

ax = axes[1]
ax.barh(df_cname_target["target_domain"][::-1], df_cname_target["cnt"][::-1],
        color=sns.color_palette("Oranges_r", len(df_cname_target)))
ax.set_xlabel("Record Count")
ax.set_title("Top 20 CNAME Target Domains")
plt.tight_layout()
save_fig("infra_cname")

# ═══════════════════════════════════════════════════════
# 2. SOA 参数分析
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. SOA 参数分析")
print("=" * 60)

df_soa = conn.execute(f"""
    SELECT
        quantile_cont(soa_refresh, [0.25, 0.5, 0.75]) AS refresh_q,
        quantile_cont(soa_retry, [0.25, 0.5, 0.75]) AS retry_q,
        quantile_cont(soa_expire, [0.25, 0.5, 0.75]) AS expire_q,
        quantile_cont(soa_minimum, [0.25, 0.5, 0.75]) AS minimum_q,
        avg(soa_refresh) AS refresh_avg,
        avg(soa_retry) AS retry_avg,
        avg(soa_expire) AS expire_avg,
        avg(soa_minimum) AS minimum_avg
    FROM read_parquet([{globs}])
    WHERE query_type = 'SOA' AND soa_refresh IS NOT NULL
""").fetchdf()

for param in ["refresh", "retry", "expire", "minimum"]:
    q = df_soa[f"{param}_q"].iloc[0]
    avg = df_soa[f"{param}_avg"].iloc[0]
    print(f"  SOA {param:>8s}: Q25={q[0]:>8.0f}  Median={q[1]:>8.0f}  Q75={q[2]:>8.0f}  Mean={avg:>10.0f}")

# SOA primary nameserver Top 10
print("\nSOA Primary Nameserver (mname) Top 10:")
df_soa_mname = conn.execute(f"""
    SELECT
        list_extract(string_split(soa_mname, '.'), greatest(len(string_split(soa_mname, '.')) - 2, 1))
        || '.' ||
        list_extract(string_split(soa_mname, '.'), greatest(len(string_split(soa_mname, '.')) - 1, 1))
        AS mname_domain,
        count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE query_type = 'SOA' AND soa_mname IS NOT NULL
    GROUP BY mname_domain
    ORDER BY domain_count DESC
    LIMIT 10
""").fetchdf()
print(df_soa_mname.to_string(index=False))

# SOA 参数箱线图（采样）
df_soa_sample = conn.execute(f"""
    SELECT soa_refresh, soa_retry, soa_expire, soa_minimum
    FROM read_parquet([{globs}])
    WHERE query_type = 'SOA' AND soa_refresh IS NOT NULL
      AND soa_refresh > 0 AND soa_refresh <= 604800
      AND soa_retry > 0 AND soa_retry <= 604800
      AND soa_expire > 0 AND soa_expire <= 4838400
      AND soa_minimum > 0 AND soa_minimum <= 604800
    USING SAMPLE 100000
""").fetchdf()

fig, ax = plt.subplots(figsize=(10, 5))
data = [df_soa_sample[c].dropna() / 3600 for c in ["soa_refresh", "soa_retry", "soa_expire", "soa_minimum"]]
bp = ax.boxplot(data, labels=["Refresh", "Retry", "Expire", "Minimum"],
                patch_artist=True, showfliers=False)
colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
for patch, color in zip(bp["boxes"], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
ax.set_ylabel("Hours")
ax.set_title("SOA Timer Parameters Distribution (sampled, outliers hidden)")
save_fig("infra_soa_params")

# ═══════════════════════════════════════════════════════
# 3. NS 记录数量分布（DNS 冗余度）
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. NS 记录数量分布（DNS 冗余度）")
print("=" * 60)

df_ns_count = conn.execute(f"""
    WITH ns_per_domain AS (
        SELECT query_name, count(DISTINCT ns_address) AS ns_count
        FROM read_parquet([{globs}])
        WHERE query_type = 'NS' AND ns_address IS NOT NULL
        GROUP BY query_name
    )
    SELECT ns_count, count(*) AS domain_count
    FROM ns_per_domain
    GROUP BY ns_count
    ORDER BY ns_count
""").fetchdf()
print(df_ns_count.to_string(index=False))

total_ns_domains = df_ns_count["domain_count"].sum()
print(f"\n有 NS 记录的域名总数: {total_ns_domains:,}")
for _, row in df_ns_count.head(10).iterrows():
    print(f"  {int(row['ns_count'])} 个 NS: {row['domain_count']:>10,} ({row['domain_count']/total_ns_domains*100:.1f}%)")

fig, ax = plt.subplots(figsize=(10, 5))
plot_data = df_ns_count[df_ns_count["ns_count"] <= 10]
ax.bar(plot_data["ns_count"], plot_data["domain_count"], color="#4C72B0")
ax.set_xlabel("Number of NS Records per Domain")
ax.set_ylabel("Number of Domains")
ax.set_title("DNS Redundancy: NS Record Count per Domain")
ax.set_xticks(range(1, 11))
for _, row in plot_data.iterrows():
    if row["domain_count"] > total_ns_domains * 0.01:
        ax.text(row["ns_count"], row["domain_count"],
                f"{row['domain_count']/total_ns_domains*100:.1f}%",
                ha="center", va="bottom", fontsize=8)
save_fig("infra_ns_redundancy")

print("\n[05_domain_infra] 完成!")
conn.close()
