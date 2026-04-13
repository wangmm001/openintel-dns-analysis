#!/usr/bin/env python3
"""02 - DNS 记录类型深度分析

输出：
  - IPv4 vs IPv6 采用率（按 TLD）
  - MX 邮件服务商 Top 20
  - NS DNS 托管商 Top 20
  - TXT 记录: SPF / DKIM / DMARC 采用率
  - TTL 值分布
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
# 1. IPv4 vs IPv6 采用率（按 TLD）
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. IPv4 vs IPv6 采用率（按 TLD）")
print("=" * 60)

ipv_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_domains,
            count(DISTINCT CASE WHEN query_type='A' AND ip4_address IS NOT NULL
                  THEN query_name END) AS has_ipv4,
            count(DISTINCT CASE WHEN query_type='AAAA' AND ip6_address IS NOT NULL
                  THEN query_name END) AS has_ipv6
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    total = r[0]
    ipv_stats.append({
        "tld": tld,
        "total_domains": total,
        "ipv4_pct": round(r[1] / total * 100, 2),
        "ipv6_pct": round(r[2] / total * 100, 2),
    })

df_ip = pd.DataFrame(ipv_stats)
print(df_ip.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(df_ip))
w = 0.35
ax.bar(x - w/2, df_ip["ipv4_pct"], w, label="IPv4 (A)", color="#4C72B0")
ax.bar(x + w/2, df_ip["ipv6_pct"], w, label="IPv6 (AAAA)", color="#DD8452")
ax.set_xticks(x)
ax.set_xticklabels(df_ip["tld"])
ax.set_ylabel("% of Domains")
ax.set_title("IPv4 vs IPv6 Adoption by TLD")
ax.legend()
for i, (v4, v6) in enumerate(zip(df_ip["ipv4_pct"], df_ip["ipv6_pct"])):
    ax.text(i - w/2, v4 + 0.5, f"{v4:.0f}%", ha="center", fontsize=7)
    ax.text(i + w/2, v6 + 0.5, f"{v6:.0f}%", ha="center", fontsize=7)
save_fig("dns_ipv4_vs_ipv6")

# ═══════════════════════════════════════════════════════
# 2. MX 邮件服务商 Top 20
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. MX 邮件服务商 Top 20（按域名提取）")
print("=" * 60)

df_mx = conn.execute(f"""
    WITH mx_data AS (
        SELECT
            query_name,
            -- 提取 MX 地址的注册域（倒数第3段.倒数第2段.倒数第1段）
            -- mx_address 以 '.' 结尾，split 后最后一个是空串
            list_extract(string_split(mx_address, '.'), greatest(len(string_split(mx_address, '.')) - 2, 1))
            || '.' ||
            list_extract(string_split(mx_address, '.'), greatest(len(string_split(mx_address, '.')) - 1, 1))
            AS mx_provider
        FROM read_parquet([{globs}])
        WHERE query_type = 'MX' AND mx_address IS NOT NULL
    )
    SELECT mx_provider, count(DISTINCT query_name) AS domain_count
    FROM mx_data
    WHERE mx_provider IS NOT NULL AND mx_provider != '.'
    GROUP BY mx_provider
    ORDER BY domain_count DESC
    LIMIT 20
""").fetchdf()
print(df_mx.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(df_mx["mx_provider"][::-1], df_mx["domain_count"][::-1],
        color=sns.color_palette("Blues_r", len(df_mx)))
ax.set_xlabel("Number of Domains")
ax.set_title("Top 20 Mail Providers (by MX record)")
save_fig("dns_mx_providers")

# ═══════════════════════════════════════════════════════
# 3. NS DNS 托管商 Top 20
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. NS DNS 托管商 Top 20")
print("=" * 60)

df_ns = conn.execute(f"""
    WITH ns_data AS (
        SELECT
            query_name,
            list_extract(string_split(ns_address, '.'), greatest(len(string_split(ns_address, '.')) - 2, 1))
            || '.' ||
            list_extract(string_split(ns_address, '.'), greatest(len(string_split(ns_address, '.')) - 1, 1))
            AS ns_provider
        FROM read_parquet([{globs}])
        WHERE query_type = 'NS' AND ns_address IS NOT NULL
    )
    SELECT ns_provider, count(DISTINCT query_name) AS domain_count
    FROM ns_data
    WHERE ns_provider IS NOT NULL AND ns_provider != '.'
    GROUP BY ns_provider
    ORDER BY domain_count DESC
    LIMIT 20
""").fetchdf()
print(df_ns.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(df_ns["ns_provider"][::-1], df_ns["domain_count"][::-1],
        color=sns.color_palette("Greens_r", len(df_ns)))
ax.set_xlabel("Number of Domains")
ax.set_title("Top 20 DNS Hosting Providers (by NS record)")
save_fig("dns_ns_providers")

# ═══════════════════════════════════════════════════════
# 4. TXT 记录: SPF / DKIM / DMARC 采用率
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("4. SPF / DKIM / DMARC 采用率")
print("=" * 60)

df_txt = conn.execute(f"""
    SELECT
        count(DISTINCT query_name) AS total_domains,
        count(DISTINCT CASE WHEN lower(txt_text) LIKE '%v=spf1%' THEN query_name END) AS spf_domains,
        count(DISTINCT CASE WHEN lower(txt_text) LIKE '%v=dkim1%' THEN query_name END) AS dkim_domains,
        count(DISTINCT CASE WHEN lower(txt_text) LIKE '%v=dmarc1%' THEN query_name END) AS dmarc_domains
    FROM read_parquet([{globs}])
    WHERE query_type = 'TXT' AND txt_text IS NOT NULL
""").fetchdf()

total = df_txt["total_domains"].iloc[0]
results = {
    "SPF": df_txt["spf_domains"].iloc[0],
    "DKIM": df_txt["dkim_domains"].iloc[0],
    "DMARC": df_txt["dmarc_domains"].iloc[0],
}
print(f"有 TXT 记录的域名总数: {total:,}")
for k, v in results.items():
    print(f"  {k}: {v:,} ({v/total*100:.2f}%)")

fig, ax = plt.subplots(figsize=(7, 4))
names = list(results.keys())
vals = [v / total * 100 for v in results.values()]
bars = ax.bar(names, vals, color=["#4C72B0", "#DD8452", "#55A868"])
ax.set_ylabel("% of Domains with TXT records")
ax.set_title("Email Security TXT Record Adoption")
for bar, v in zip(bars, vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
            f"{v:.1f}%", ha="center")
save_fig("dns_txt_email_security")

# ═══════════════════════════════════════════════════════
# 5. TTL 值分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("5. TTL 值分布")
print("=" * 60)

df_ttl = conn.execute(f"""
    SELECT
        response_ttl AS ttl,
        count(*) AS cnt
    FROM read_parquet([{globs}])
    WHERE response_ttl IS NOT NULL AND response_ttl >= 0
    GROUP BY response_ttl
    ORDER BY cnt DESC
    LIMIT 20
""").fetchdf()
print("Top 20 最常见 TTL 值:")
print(df_ttl.to_string(index=False))

# TTL 分布直方图（对数刻度）
df_ttl_hist = conn.execute(f"""
    SELECT response_ttl AS ttl
    FROM read_parquet([{globs}])
    WHERE response_ttl IS NOT NULL AND response_ttl > 0 AND response_ttl <= 604800
    USING SAMPLE 500000
""").fetchdf()

fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(df_ttl_hist["ttl"], bins=100, color="#4C72B0", edgecolor="white", log=True)
ax.set_xlabel("TTL (seconds)")
ax.set_ylabel("Count (log scale)")
ax.set_title("TTL Value Distribution (sampled 500K records)")
ax.axvline(x=300, color="red", linestyle="--", alpha=0.7, label="5 min")
ax.axvline(x=3600, color="orange", linestyle="--", alpha=0.7, label="1 hour")
ax.axvline(x=86400, color="green", linestyle="--", alpha=0.7, label="1 day")
ax.legend()
save_fig("dns_ttl_distribution")

print("\n[02_dns_records] 完成!")
conn.close()
