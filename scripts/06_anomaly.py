#!/usr/bin/env python3
"""06 - 异常检测

输出：
  - 异常低 TTL 值域名
  - SERVFAIL / 异常 status_code 域名
  - RTT 异常值分析
  - 无 NS 记录或 SOA 异常的域名
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from config import get_conn, parquet_glob, TLDS, save_fig, STATUS_CODE_MAP

conn = get_conn()
globs = ", ".join(f"'{parquet_glob(t)}'" for t in TLDS)

# ═══════════════════════════════════════════════════════
# 1. 异常低 TTL 域名（TTL=0 或 TTL < 60）
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. 异常低 TTL 域名 (TTL < 60s)")
print("=" * 60)

df_low_ttl = conn.execute(f"""
    SELECT
        CASE
            WHEN response_ttl = 0 THEN 'TTL=0'
            WHEN response_ttl > 0 AND response_ttl < 60 THEN 'TTL 1-59'
            WHEN response_ttl >= 60 AND response_ttl < 300 THEN 'TTL 60-299'
            WHEN response_ttl >= 300 THEN 'TTL >= 300'
        END AS ttl_bucket,
        count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE response_ttl IS NOT NULL AND response_type IN ('A', 'AAAA')
    GROUP BY ttl_bucket
    ORDER BY ttl_bucket
""").fetchdf()
print(df_low_ttl.to_string(index=False))

# TTL=0 的域名样例
print("\nTTL=0 域名 Top 20:")
df_ttl0 = conn.execute(f"""
    SELECT query_name, count(*) AS record_count
    FROM read_parquet([{globs}])
    WHERE response_ttl = 0 AND response_type IN ('A', 'AAAA')
    GROUP BY query_name
    ORDER BY record_count DESC
    LIMIT 20
""").fetchdf()
print(df_ttl0.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
colors = ["#C44E52", "#DD8452", "#CCB974", "#55A868"]
ax.bar(df_low_ttl["ttl_bucket"], df_low_ttl["domain_count"], color=colors[:len(df_low_ttl)])
ax.set_ylabel("Number of Domains")
ax.set_title("A/AAAA Record TTL Distribution Buckets")
for i, row in df_low_ttl.iterrows():
    ax.text(i, row["domain_count"], f"{row['domain_count']:,}", ha="center", va="bottom", fontsize=8)
save_fig("anomaly_ttl_buckets")

# ═══════════════════════════════════════════════════════
# 2. SERVFAIL / 异常 status_code 域名
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. SERVFAIL 与异常响应域名")
print("=" * 60)

# 按 TLD 统计 SERVFAIL 率
servfail_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total,
            count(DISTINCT CASE WHEN status_code = 2 THEN query_name END) AS servfail,
            count(DISTINCT CASE WHEN status_code = 65533 THEN query_name END) AS timeout
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    servfail_stats.append({
        "tld": tld,
        "total": r[0],
        "servfail": r[1],
        "servfail_pct": round(r[1] / r[0] * 100, 2),
        "timeout": r[2],
        "timeout_pct": round(r[2] / r[0] * 100, 2),
    })

df_sf = pd.DataFrame(servfail_stats)
print(df_sf.to_string(index=False))

# 频繁 SERVFAIL 的域名
print("\n频繁 SERVFAIL 域名 Top 20:")
df_sf_domains = conn.execute(f"""
    SELECT query_name, count(*) AS servfail_count,
           count(DISTINCT query_type) AS query_types_affected
    FROM read_parquet([{globs}])
    WHERE status_code = 2
    GROUP BY query_name
    ORDER BY servfail_count DESC
    LIMIT 20
""").fetchdf()
print(df_sf_domains.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(df_sf))
w = 0.35
ax.bar(x - w/2, df_sf["servfail_pct"], w, label="SERVFAIL", color="#C44E52")
ax.bar(x + w/2, df_sf["timeout_pct"], w, label="TIMEOUT", color="#8172B2")
ax.set_xticks(x)
ax.set_xticklabels(df_sf["tld"])
ax.set_ylabel("% of Domains")
ax.set_title("SERVFAIL & TIMEOUT Rate by TLD")
ax.legend()
save_fig("anomaly_servfail_timeout")

# ═══════════════════════════════════════════════════════
# 3. RTT 异常值分析
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. RTT 异常值分析")
print("=" * 60)

df_rtt_stats = conn.execute(f"""
    SELECT
        min(rtt) AS rtt_min,
        quantile_cont(rtt, 0.25) AS rtt_q25,
        quantile_cont(rtt, 0.5) AS rtt_median,
        quantile_cont(rtt, 0.75) AS rtt_q75,
        quantile_cont(rtt, 0.95) AS rtt_p95,
        quantile_cont(rtt, 0.99) AS rtt_p99,
        max(rtt) AS rtt_max,
        avg(rtt) AS rtt_mean
    FROM read_parquet([{globs}])
    WHERE rtt IS NOT NULL AND rtt > 0
""").fetchdf()
print("RTT 统计 (秒):")
print(df_rtt_stats.T.rename(columns={0: "value"}).to_string())

# RTT 分布直方图
df_rtt_sample = conn.execute(f"""
    SELECT rtt
    FROM read_parquet([{globs}])
    WHERE rtt IS NOT NULL AND rtt > 0 AND rtt < 2.0
    USING SAMPLE 500000
""").fetchdf()

fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(df_rtt_sample["rtt"] * 1000, bins=100, color="#4C72B0", edgecolor="white", log=True)
ax.set_xlabel("RTT (ms)")
ax.set_ylabel("Count (log scale)")
ax.set_title("DNS Query RTT Distribution (sampled 500K, < 2s)")
ax.axvline(x=100, color="orange", linestyle="--", alpha=0.7, label="100ms")
ax.axvline(x=500, color="red", linestyle="--", alpha=0.7, label="500ms")
ax.legend()
save_fig("anomaly_rtt_distribution")

# 高延迟域名 Top 20
print("\n高平均 RTT 域名 Top 20 (>= 10 条记录):")
df_high_rtt = conn.execute(f"""
    SELECT query_name, count(*) AS record_count,
           round(avg(rtt), 4) AS avg_rtt,
           round(max(rtt), 4) AS max_rtt
    FROM read_parquet([{globs}])
    WHERE rtt IS NOT NULL AND rtt > 0
    GROUP BY query_name
    HAVING count(*) >= 10
    ORDER BY avg_rtt DESC
    LIMIT 20
""").fetchdf()
print(df_high_rtt.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 4. NXDOMAIN 域名分析
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("4. NXDOMAIN 域名分析（按 TLD）")
print("=" * 60)

nx_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total,
            count(DISTINCT CASE WHEN status_code = 3 THEN query_name END) AS nxdomain
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    nx_stats.append({
        "tld": tld,
        "total": r[0],
        "nxdomain": r[1],
        "nxdomain_pct": round(r[1] / r[0] * 100, 2),
    })

df_nx = pd.DataFrame(nx_stats)
print(df_nx.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(df_nx["tld"], df_nx["nxdomain_pct"], color=sns.color_palette("Reds", len(df_nx)))
ax.set_ylabel("% of Domains")
ax.set_title("NXDOMAIN Rate by TLD")
for i, v in enumerate(df_nx["nxdomain_pct"]):
    ax.text(i, v + 0.1, f"{v:.1f}%", ha="center", fontsize=8)
save_fig("anomaly_nxdomain_rate")

print("\n[06_anomaly] 完成!")
conn.close()
