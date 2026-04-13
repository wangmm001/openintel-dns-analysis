#!/usr/bin/env python3
"""01 - 数据总览统计

输出：
  - 各 TLD 的域名数量与记录总数
  - query_type 全局分布
  - status_code 全局分布
  - 关键列非空率
  - 图表: overview_tld_records.png, overview_query_types.png, overview_status_codes.png
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import get_conn, parquet_glob, TLDS, STATUS_CODE_MAP, save_fig, OUTPUT_DIR

conn = get_conn()

# ═══════════════════════════════════════════════════════
# 1. 各 TLD 的域名数量与记录总数
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. 各 TLD 域名数量与记录总数")
print("=" * 60)

tld_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(*) AS total_records,
            count(DISTINCT query_name) AS unique_domains
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    tld_stats.append({"tld": tld, "total_records": r[0], "unique_domains": r[1]})

df_tld = pd.DataFrame(tld_stats)
df_tld["records_per_domain"] = (df_tld["total_records"] / df_tld["unique_domains"]).round(1)
print(df_tld.to_string(index=False))
print(f"\n总计: {df_tld['total_records'].sum():,} 条记录, {df_tld['unique_domains'].sum():,} 个域名")

# 图表 - TLD 域名数量
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
colors = sns.color_palette("Set2", len(TLDS))

ax = axes[0]
bars = ax.bar(df_tld["tld"], df_tld["unique_domains"], color=colors)
ax.set_title("Unique Domains per TLD")
ax.set_ylabel("Number of Domains")
ax.set_xlabel("TLD")
for bar, val in zip(bars, df_tld["unique_domains"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
            f"{val:,}", ha="center", va="bottom", fontsize=8)

ax = axes[1]
bars = ax.bar(df_tld["tld"], df_tld["total_records"], color=colors)
ax.set_title("Total Records per TLD")
ax.set_ylabel("Number of Records")
ax.set_xlabel("TLD")
for bar, val in zip(bars, df_tld["total_records"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
            f"{val/1e6:.1f}M", ha="center", va="bottom", fontsize=8)

plt.tight_layout()
save_fig("overview_tld_records")

# ═══════════════════════════════════════════════════════
# 2. query_type 全局分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. Query Type 全局分布")
print("=" * 60)

globs = ", ".join(f"'{parquet_glob(t)}'" for t in TLDS)
df_qt = conn.execute(f"""
    SELECT query_type, count(*) AS cnt
    FROM read_parquet([{globs}])
    GROUP BY query_type
    ORDER BY cnt DESC
""").fetchdf()
df_qt["pct"] = (df_qt["cnt"] / df_qt["cnt"].sum() * 100).round(2)
print(df_qt.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(df_qt["query_type"][::-1], df_qt["cnt"][::-1], color=sns.color_palette("viridis", len(df_qt)))
ax.set_xlabel("Record Count")
ax.set_title("Query Type Distribution (All TLDs)")
for i, (val, pct) in enumerate(zip(df_qt["cnt"][::-1], df_qt["pct"][::-1])):
    ax.text(val, i, f" {pct:.1f}%", va="center", fontsize=8)
save_fig("overview_query_types")

# ═══════════════════════════════════════════════════════
# 3. Status Code 全局分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. Status Code 全局分布")
print("=" * 60)

df_sc = conn.execute(f"""
    SELECT status_code, count(*) AS cnt
    FROM read_parquet([{globs}])
    GROUP BY status_code
    ORDER BY cnt DESC
""").fetchdf()
df_sc["status_name"] = df_sc["status_code"].map(STATUS_CODE_MAP).fillna("OTHER")
df_sc["pct"] = (df_sc["cnt"] / df_sc["cnt"].sum() * 100).round(2)
print(df_sc.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
wedges, texts, autotexts = ax.pie(
    df_sc["cnt"], labels=df_sc["status_name"],
    autopct="%1.1f%%", startangle=90,
    colors=sns.color_palette("pastel", len(df_sc))
)
ax.set_title("DNS Response Status Code Distribution")
save_fig("overview_status_codes")

# ═══════════════════════════════════════════════════════
# 4. 关键列非空率
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("4. 关键列非空率")
print("=" * 60)

key_cols = [
    "ip4_address", "ip6_address", "country", "as",
    "cname_name", "mx_address", "ns_address", "txt_text",
    "soa_mname", "caa_tag", "ds_key_tag", "dnskey_flags"
]
col_exprs = ", ".join(
    f"round(count(\"{c}\") * 100.0 / count(*), 2) AS \"{c}\""
    for c in key_cols
)
df_nn = conn.execute(f"""
    SELECT {col_exprs}
    FROM read_parquet([{globs}])
""").fetchdf()

print(df_nn.T.rename(columns={0: "non_null_pct"}).to_string())

fig, ax = plt.subplots(figsize=(10, 5))
vals = df_nn.iloc[0].values
ax.barh(key_cols[::-1], vals[::-1], color=sns.color_palette("coolwarm", len(key_cols)))
ax.set_xlabel("Non-Null %")
ax.set_title("Key Column Completeness")
ax.set_xlim(0, 100)
for i, v in enumerate(vals[::-1]):
    ax.text(v + 0.5, i, f"{v:.1f}%", va="center", fontsize=8)
save_fig("overview_column_completeness")

print("\n[01_overview] 完成!")
conn.close()
