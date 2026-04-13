#!/usr/bin/env python3
"""07 - TopList 数据综合分析

将 Tranco / Umbrella / Radar 热门域名与 ccTLD 区域数据交叉分析：
  - 各 TopList 规模概览
  - TLD 分布对比（TopList vs 区域）
  - 热门域名的 DNSSEC 部署率 vs 全量
  - 热门域名的 IPv6 采用率 vs 全量
  - 热门域名的邮件安全 (SPF/DMARC)
  - TopList 重叠度分析
  - Root Zone 分析
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from config import get_conn, parquet_glob, TLDS, save_fig

conn = get_conn()

# Data paths
TOPLIST_FILES = {
    "tranco":  "tranco/tranco.gz.parquet",
    "umbrella": "umbrella/umbrella.gz.parquet",
    "radar":   "radar/radar.gz.parquet",
}
ROOT_FILE = "root/root.gz.parquet"
zone_globs = ", ".join(f"'{parquet_glob(t)}'" for t in TLDS)

# ═══════════════════════════════════════════════════════
# 1. 数据集概览
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. 数据集概览")
print("=" * 60)

overview = []
for name, path in TOPLIST_FILES.items():
    r = conn.execute(f"""
        SELECT count(*) AS total,
               count(DISTINCT query_name) AS domains
        FROM read_parquet('{path}')
    """).fetchone()
    overview.append({"source": name, "records": r[0], "domains": r[1]})

# Zone-based total
r = conn.execute(f"""
    SELECT count(*) AS total, count(DISTINCT query_name) AS domains
    FROM read_parquet([{zone_globs}])
""").fetchone()
overview.append({"source": "ccTLD zones (8)", "records": r[0], "domains": r[1]})

# Root
r = conn.execute(f"""
    SELECT count(*) AS total, count(DISTINCT query_name) AS domains
    FROM read_parquet('{ROOT_FILE}')
""").fetchone()
overview.append({"source": "root zone", "records": r[0], "domains": r[1]})

df_ov = pd.DataFrame(overview)
print(df_ov.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 2. TopList TLD 分布 — 热门网站集中在哪些 TLD?
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. TopList TLD 分布 (Top 15)")
print("=" * 60)

tld_frames = []
for name, path in TOPLIST_FILES.items():
    df = conn.execute(f"""
        SELECT
            reverse(split_part(reverse(query_name), '.', 2)) AS tld,
            count(DISTINCT query_name) AS cnt
        FROM read_parquet('{path}')
        GROUP BY tld
        ORDER BY cnt DESC
        LIMIT 15
    """).fetchdf()
    df["source"] = name
    tld_frames.append(df)
    print(f"\n  [{name}] Top 15 TLDs:")
    for _, row in df.iterrows():
        print(f"    .{row['tld']:10s} {row['cnt']:>10,}")

df_tld_all = pd.concat(tld_frames)

# Grouped bar chart
fig, ax = plt.subplots(figsize=(14, 6))
pivot = df_tld_all.pivot_table(index="tld", columns="source", values="cnt", fill_value=0)
# Keep only top TLDs by total
pivot["total"] = pivot.sum(axis=1)
pivot = pivot.nlargest(12, "total").drop(columns="total")
pivot.plot(kind="bar", ax=ax, width=0.75, color=["#4C72B0", "#DD8452", "#55A868"])
ax.set_xlabel("TLD")
ax.set_ylabel("Number of Domains")
ax.set_title("Top 12 TLDs across TopLists")
ax.legend(title="Source")
plt.xticks(rotation=0)
save_fig("toplist_tld_distribution")

# ═══════════════════════════════════════════════════════
# 3. 热门域名 vs 全量: DNSSEC 部署率
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. 热门域名 vs 全量: DNSSEC 部署率")
print("=" * 60)

dnssec_cmp = []
for name, path in TOPLIST_FILES.items():
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total,
            count(DISTINCT CASE WHEN query_type='DS' AND ds_key_tag IS NOT NULL
                  THEN query_name END) AS has_ds
        FROM read_parquet('{path}')
    """).fetchone()
    dnssec_cmp.append({"source": name, "total": r[0], "ds_pct": round(r[1]/r[0]*100, 2)})

# Zone average
r = conn.execute(f"""
    SELECT
        count(DISTINCT query_name) AS total,
        count(DISTINCT CASE WHEN query_type='DS' AND ds_key_tag IS NOT NULL
              THEN query_name END) AS has_ds
    FROM read_parquet([{zone_globs}])
""").fetchone()
dnssec_cmp.append({"source": "ccTLD zones", "total": r[0], "ds_pct": round(r[1]/r[0]*100, 2)})

df_dnssec = pd.DataFrame(dnssec_cmp)
print(df_dnssec.to_string(index=False))

# ═══════════════════════════════════════════════════════
# 4. 热门域名 vs 全量: IPv6 采用率
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("4. 热门域名 vs 全量: IPv6 采用率")
print("=" * 60)

ipv6_cmp = []
for name, path in TOPLIST_FILES.items():
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total,
            count(DISTINCT CASE WHEN query_type='A' AND ip4_address IS NOT NULL
                  THEN query_name END) AS has_v4,
            count(DISTINCT CASE WHEN query_type='AAAA' AND ip6_address IS NOT NULL
                  THEN query_name END) AS has_v6
        FROM read_parquet('{path}')
    """).fetchone()
    ipv6_cmp.append({
        "source": name,
        "ipv4_pct": round(r[1]/r[0]*100, 1),
        "ipv6_pct": round(r[2]/r[0]*100, 1),
    })

r = conn.execute(f"""
    SELECT
        count(DISTINCT query_name) AS total,
        count(DISTINCT CASE WHEN query_type='A' AND ip4_address IS NOT NULL
              THEN query_name END) AS has_v4,
        count(DISTINCT CASE WHEN query_type='AAAA' AND ip6_address IS NOT NULL
              THEN query_name END) AS has_v6
    FROM read_parquet([{zone_globs}])
""").fetchone()
ipv6_cmp.append({
    "source": "ccTLD zones",
    "ipv4_pct": round(r[1]/r[0]*100, 1),
    "ipv6_pct": round(r[2]/r[0]*100, 1),
})

df_ipv6 = pd.DataFrame(ipv6_cmp)
print(df_ipv6.to_string(index=False))

# Combined comparison chart
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

ax = axes[0]
x = np.arange(len(df_dnssec))
ax.bar(x, df_dnssec["ds_pct"], color=["#4C72B0","#DD8452","#55A868","#C44E52"])
ax.set_xticks(x)
ax.set_xticklabels(df_dnssec["source"])
ax.set_ylabel("% of Domains with DS record")
ax.set_title("DNSSEC (DS) Deployment: TopLists vs ccTLD Zones")
for i, v in enumerate(df_dnssec["ds_pct"]):
    ax.text(i, v + 0.3, f"{v:.1f}%", ha="center", fontsize=10)

ax = axes[1]
x = np.arange(len(df_ipv6))
w = 0.35
ax.bar(x - w/2, df_ipv6["ipv4_pct"], w, label="IPv4", color="#4C72B0")
ax.bar(x + w/2, df_ipv6["ipv6_pct"], w, label="IPv6", color="#DD8452")
ax.set_xticks(x)
ax.set_xticklabels(df_ipv6["source"])
ax.set_ylabel("% of Domains")
ax.set_title("IPv4/IPv6 Adoption: TopLists vs ccTLD Zones")
ax.legend()
for i, (v4, v6) in enumerate(zip(df_ipv6["ipv4_pct"], df_ipv6["ipv6_pct"])):
    ax.text(i - w/2, v4 + 0.5, f"{v4:.0f}%", ha="center", fontsize=8)
    ax.text(i + w/2, v6 + 0.5, f"{v6:.0f}%", ha="center", fontsize=8)
plt.tight_layout()
save_fig("toplist_security_ipv6_comparison")

# ═══════════════════════════════════════════════════════
# 5. 热门域名邮件安全 (SPF / DMARC)
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("5. 热门域名 vs 全量: SPF / DMARC")
print("=" * 60)

email_cmp = []
for name, path in list(TOPLIST_FILES.items()) + [("ccTLD zones", f"[{zone_globs}]")]:
    src = path if name != "ccTLD zones" else path
    q = f"'{src}'" if name != "ccTLD zones" else src
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_txt,
            count(DISTINCT CASE WHEN lower(txt_text) LIKE '%v=spf1%' THEN query_name END) AS spf,
            count(DISTINCT CASE WHEN lower(txt_text) LIKE '%v=dmarc1%' THEN query_name END) AS dmarc
        FROM read_parquet({q})
        WHERE query_type = 'TXT' AND txt_text IS NOT NULL
    """).fetchone()
    total = r[0]
    email_cmp.append({
        "source": name,
        "txt_domains": total,
        "spf_pct": round(r[1]/total*100, 1) if total > 0 else 0,
        "dmarc_pct": round(r[2]/total*100, 1) if total > 0 else 0,
    })

df_email = pd.DataFrame(email_cmp)
print(df_email.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(df_email))
w = 0.35
ax.bar(x - w/2, df_email["spf_pct"], w, label="SPF", color="#4C72B0")
ax.bar(x + w/2, df_email["dmarc_pct"], w, label="DMARC", color="#DD8452")
ax.set_xticks(x)
ax.set_xticklabels(df_email["source"])
ax.set_ylabel("% of Domains with TXT records")
ax.set_title("Email Security: SPF & DMARC Adoption (TopLists vs ccTLD Zones)")
ax.legend()
for i, (s, d) in enumerate(zip(df_email["spf_pct"], df_email["dmarc_pct"])):
    ax.text(i - w/2, s + 0.5, f"{s:.0f}%", ha="center", fontsize=9)
    ax.text(i + w/2, d + 0.5, f"{d:.0f}%", ha="center", fontsize=9)
save_fig("toplist_email_security")

# ═══════════════════════════════════════════════════════
# 6. TopList 重叠度
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("6. TopList 域名重叠度")
print("=" * 60)

overlap = conn.execute(f"""
    WITH
        t AS (SELECT DISTINCT query_name FROM read_parquet('tranco/tranco.gz.parquet')),
        u AS (SELECT DISTINCT query_name FROM read_parquet('umbrella/umbrella.gz.parquet')),
        r AS (SELECT DISTINCT query_name FROM read_parquet('radar/radar.gz.parquet'))
    SELECT
        (SELECT count(*) FROM t) AS tranco_total,
        (SELECT count(*) FROM u) AS umbrella_total,
        (SELECT count(*) FROM r) AS radar_total,
        (SELECT count(*) FROM t INNER JOIN u USING(query_name)) AS tranco_umbrella,
        (SELECT count(*) FROM t INNER JOIN r USING(query_name)) AS tranco_radar,
        (SELECT count(*) FROM u INNER JOIN r USING(query_name)) AS umbrella_radar,
        (SELECT count(*) FROM t INNER JOIN u USING(query_name) INNER JOIN r USING(query_name)) AS all_three
""").fetchone()

print(f"  Tranco:   {overlap[0]:>10,}")
print(f"  Umbrella: {overlap[1]:>10,}")
print(f"  Radar:    {overlap[2]:>10,}")
print(f"  Tranco ∩ Umbrella: {overlap[3]:>10,} ({overlap[3]/min(overlap[0],overlap[1])*100:.1f}%)")
print(f"  Tranco ∩ Radar:    {overlap[4]:>10,} ({overlap[4]/min(overlap[0],overlap[2])*100:.1f}%)")
print(f"  Umbrella ∩ Radar:  {overlap[5]:>10,} ({overlap[5]/min(overlap[1],overlap[2])*100:.1f}%)")
print(f"  All three:         {overlap[6]:>10,} ({overlap[6]/min(overlap[0],overlap[1],overlap[2])*100:.1f}%)")

# ═══════════════════════════════════════════════════════
# 7. TopList 域名与 ccTLD 区域的交集
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("7. TopList 域名与 ccTLD 区域的交集")
print("=" * 60)

for name, path in TOPLIST_FILES.items():
    r = conn.execute(f"""
        WITH
            toplist AS (SELECT DISTINCT query_name FROM read_parquet('{path}')),
            zones AS (SELECT DISTINCT query_name FROM read_parquet([{zone_globs}]))
        SELECT
            (SELECT count(*) FROM toplist) AS tl_total,
            (SELECT count(*) FROM toplist INNER JOIN zones USING(query_name)) AS in_both
    """).fetchone()
    print(f"  {name}: {r[1]:,} / {r[0]:,} ({r[1]/r[0]*100:.1f}%) in ccTLD zones")

# ═══════════════════════════════════════════════════════
# 8. Root Zone 分析
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("8. Root Zone 分析")
print("=" * 60)

df_root = conn.execute(f"""
    SELECT query_type, count(*) AS cnt
    FROM read_parquet('{ROOT_FILE}')
    GROUP BY query_type
    ORDER BY cnt DESC
""").fetchdf()
print("Root Zone 查询类型分布:")
print(df_root.to_string(index=False))

print("\nRoot Zone 域名示例 (Top 20):")
df_root_names = conn.execute(f"""
    SELECT DISTINCT query_name
    FROM read_parquet('{ROOT_FILE}')
    ORDER BY query_name
    LIMIT 20
""").fetchdf()
for _, row in df_root_names.iterrows():
    print(f"  {row['query_name']}")

# Root zone DNSSEC
r = conn.execute(f"""
    SELECT
        count(DISTINCT query_name) AS total,
        count(DISTINCT CASE WHEN query_type='DS' AND ds_key_tag IS NOT NULL
              THEN query_name END) AS has_ds,
        count(DISTINCT CASE WHEN response_type='RRSIG'
              THEN query_name END) AS has_rrsig
    FROM read_parquet('{ROOT_FILE}')
""").fetchone()
print(f"\nRoot Zone DNSSEC:")
print(f"  总 TLD 数: {r[0]}")
print(f"  有 DS 记录: {r[1]} ({r[1]/r[0]*100:.1f}%)")
print(f"  有 RRSIG: {r[2]} ({r[2]/r[0]*100:.1f}%)")

# Root zone NS per TLD
df_root_ns = conn.execute(f"""
    SELECT query_name, count(DISTINCT ns_address) AS ns_count
    FROM read_parquet('{ROOT_FILE}')
    WHERE query_type = 'NS' AND ns_address IS NOT NULL
    GROUP BY query_name
    ORDER BY ns_count DESC
    LIMIT 10
""").fetchdf()
print(f"\nRoot Zone Top 10 TLD (by NS count):")
print(df_root_ns.to_string(index=False))

print("\n[07_toplist_analysis] 完成!")
conn.close()
