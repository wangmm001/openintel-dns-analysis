#!/usr/bin/env python3
"""03 - 地理与网络（AS）分析

输出：
  - IP 地理分布 Top 20 国家
  - AS 自治系统 Top 20
  - 各 TLD 域名的地理集中度（Top 5 国家占比）
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import get_conn, parquet_glob, TLDS, save_fig

conn = get_conn()
globs = ", ".join(f"'{parquet_glob(t)}'" for t in TLDS)

# ═══════════════════════════════════════════════════════
# 1. IP 地理分布 Top 20 国家
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. IP 地理分布 Top 20 国家")
print("=" * 60)

df_geo = conn.execute(f"""
    SELECT country, count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE country IS NOT NULL
      AND query_type IN ('A', 'AAAA')
      AND (ip4_address IS NOT NULL OR ip6_address IS NOT NULL)
      AND country NOT IN ('--', '-')
    GROUP BY country
    ORDER BY domain_count DESC
    LIMIT 20
""").fetchdf()
print(df_geo.to_string(index=False))

fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(df_geo["country"][::-1], df_geo["domain_count"][::-1],
        color=sns.color_palette("RdYlBu", len(df_geo)))
ax.set_xlabel("Number of Domains")
ax.set_title("Top 20 Countries by Domain IP Geolocation")
for i, v in enumerate(df_geo["domain_count"][::-1]):
    ax.text(v, i, f" {v:,}", va="center", fontsize=7)
save_fig("geo_top_countries")

# ═══════════════════════════════════════════════════════
# 2. AS 自治系统 Top 20
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. AS 自治系统 Top 20")
print("=" * 60)

df_as = conn.execute(f"""
    SELECT "as" AS asn, as_full, count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE "as" IS NOT NULL
      AND query_type IN ('A', 'AAAA')
      AND (ip4_address IS NOT NULL OR ip6_address IS NOT NULL)
    GROUP BY "as", as_full
    ORDER BY domain_count DESC
    LIMIT 20
""").fetchdf()
print(df_as.to_string(index=False))

fig, ax = plt.subplots(figsize=(12, 7))
labels = [f"AS{asn}" for asn in df_as["asn"]]
ax.barh(labels[::-1], df_as["domain_count"][::-1],
        color=sns.color_palette("viridis", len(df_as)))
ax.set_xlabel("Number of Domains")
ax.set_title("Top 20 Autonomous Systems by Domain Count")
for i, v in enumerate(df_as["domain_count"][::-1]):
    ax.text(v, i, f" {v:,}", va="center", fontsize=7)
save_fig("geo_top_as")

# ═══════════════════════════════════════════════════════
# 3. 各 TLD 域名的地理集中度
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. 各 TLD 域名的地理集中度 (Top 5 国家)")
print("=" * 60)

tld_geo = {}
for tld in TLDS:
    df = conn.execute(f"""
        SELECT country, count(DISTINCT query_name) AS cnt
        FROM read_parquet('{parquet_glob(tld)}')
        WHERE country IS NOT NULL
          AND query_type IN ('A', 'AAAA')
          AND (ip4_address IS NOT NULL OR ip6_address IS NOT NULL)
          AND country NOT IN ('--', '-')
        GROUP BY country
        ORDER BY cnt DESC
        LIMIT 5
    """).fetchdf()
    total = conn.execute(f"""
        SELECT count(DISTINCT query_name)
        FROM read_parquet('{parquet_glob(tld)}')
        WHERE country IS NOT NULL
          AND query_type IN ('A', 'AAAA')
          AND (ip4_address IS NOT NULL OR ip6_address IS NOT NULL)
          AND country NOT IN ('--', '-')
    """).fetchone()[0]
    tld_geo[tld] = (df, total)
    top5_pct = df["cnt"].sum() / total * 100 if total > 0 else 0
    print(f"\n  [{tld}] Top 5 占比: {top5_pct:.1f}%")
    for _, row in df.iterrows():
        print(f"    {row['country']}: {row['cnt']:>10,} ({row['cnt']/total*100:.1f}%)")

# 热力图: TLD x Country
all_countries = set()
for df, _ in tld_geo.values():
    all_countries.update(df["country"].tolist())
top_countries = sorted(all_countries)

heatmap_data = []
for tld in TLDS:
    df, total = tld_geo[tld]
    row = {}
    for _, r in df.iterrows():
        row[r["country"]] = round(r["cnt"] / total * 100, 1) if total > 0 else 0
    heatmap_data.append(row)

df_heat = pd.DataFrame(heatmap_data, index=TLDS).fillna(0)
# 只保留至少在一个 TLD 中出现的国家，按总和排序
col_order = df_heat.sum().sort_values(ascending=False).head(15).index
df_heat = df_heat[col_order]

fig, ax = plt.subplots(figsize=(12, 6))
sns.heatmap(df_heat, annot=True, fmt=".1f", cmap="YlOrRd", ax=ax,
            cbar_kws={"label": "% of Domains"})
ax.set_title("Geographic Concentration: % of Domains per Country per TLD")
ax.set_ylabel("TLD")
ax.set_xlabel("Country")
save_fig("geo_tld_heatmap")

print("\n[03_geo_network] 完成!")
conn.close()
