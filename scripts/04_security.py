#!/usr/bin/env python3
"""04 - DNSSEC 与安全分析

输出：
  - DNSSEC 签名率（按 TLD）
  - DNSSEC 算法分布
  - CAA 记录采用率（按 TLD）
  - CDS/CDNSKEY 部署情况
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

DNSKEY_ALGO_MAP = {
    5: "RSA/SHA-1",
    7: "RSASHA1-NSEC3",
    8: "RSA/SHA-256",
    10: "RSA/SHA-512",
    13: "ECDSA-P256/SHA-256",
    14: "ECDSA-P384/SHA-384",
    15: "Ed25519",
    16: "Ed448",
}

# ═══════════════════════════════════════════════════════
# 1. DNSSEC 签名率（按 TLD）
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("1. DNSSEC 签名率（按 TLD）")
print("=" * 60)

dnssec_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_domains,
            count(DISTINCT CASE WHEN query_type = 'DS' AND ds_key_tag IS NOT NULL
                  THEN query_name END) AS has_ds,
            count(DISTINCT CASE WHEN query_type = 'DNSKEY' AND dnskey_flags IS NOT NULL
                  THEN query_name END) AS has_dnskey,
            count(DISTINCT CASE WHEN response_type = 'RRSIG'
                  THEN query_name END) AS has_rrsig
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    total = r[0]
    dnssec_stats.append({
        "tld": tld,
        "total": total,
        "ds_pct": round(r[1] / total * 100, 2),
        "dnskey_pct": round(r[2] / total * 100, 2),
        "rrsig_pct": round(r[3] / total * 100, 2),
    })

df_dnssec = pd.DataFrame(dnssec_stats)
print(df_dnssec.to_string(index=False))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(df_dnssec))
w = 0.25
ax.bar(x - w, df_dnssec["ds_pct"], w, label="DS", color="#4C72B0")
ax.bar(x, df_dnssec["dnskey_pct"], w, label="DNSKEY", color="#DD8452")
ax.bar(x + w, df_dnssec["rrsig_pct"], w, label="RRSIG", color="#55A868")
ax.set_xticks(x)
ax.set_xticklabels(df_dnssec["tld"])
ax.set_ylabel("% of Domains")
ax.set_title("DNSSEC Deployment by TLD")
ax.legend()
save_fig("security_dnssec_by_tld")

# ═══════════════════════════════════════════════════════
# 2. DNSSEC 算法分布
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("2. DNSSEC 算法分布 (DS)")
print("=" * 60)

df_algo = conn.execute(f"""
    SELECT
        ds_algorithm AS algo,
        count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE query_type = 'DS' AND ds_algorithm IS NOT NULL
    GROUP BY ds_algorithm
    ORDER BY domain_count DESC
""").fetchdf()
df_algo["algo_name"] = df_algo["algo"].astype(int).map(DNSKEY_ALGO_MAP).fillna("Other")
print(df_algo.to_string(index=False))

fig, ax = plt.subplots(figsize=(9, 5))
colors = sns.color_palette("Set2", len(df_algo))
wedges, texts, autotexts = ax.pie(
    df_algo["domain_count"],
    labels=df_algo["algo_name"],
    autopct="%1.1f%%",
    startangle=90,
    colors=colors,
)
ax.set_title("DNSSEC Algorithm Distribution (DS records)")
save_fig("security_dnssec_algorithms")

# ═══════════════════════════════════════════════════════
# 3. CAA 记录采用率（按 TLD）
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("3. CAA 记录采用率（按 TLD）")
print("=" * 60)

caa_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_domains,
            count(DISTINCT CASE WHEN query_type = 'CAA' AND caa_tag IS NOT NULL
                  THEN query_name END) AS has_caa
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    caa_stats.append({
        "tld": tld,
        "total": r[0],
        "caa_count": r[1],
        "caa_pct": round(r[1] / r[0] * 100, 2),
    })

df_caa = pd.DataFrame(caa_stats)
print(df_caa.to_string(index=False))

# CAA tag 值分布
print("\nCAA tag 值分布:")
df_caa_tag = conn.execute(f"""
    SELECT caa_tag, count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE query_type = 'CAA' AND caa_tag IS NOT NULL
    GROUP BY caa_tag
    ORDER BY domain_count DESC
    LIMIT 10
""").fetchdf()
print(df_caa_tag.to_string(index=False))

# CAA value Top CA
print("\nCAA issue value Top 10 CA:")
df_caa_ca = conn.execute(f"""
    SELECT caa_value, count(DISTINCT query_name) AS domain_count
    FROM read_parquet([{globs}])
    WHERE query_type = 'CAA' AND caa_tag = 'issue' AND caa_value IS NOT NULL
    GROUP BY caa_value
    ORDER BY domain_count DESC
    LIMIT 10
""").fetchdf()
print(df_caa_ca.to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

ax = axes[0]
ax.bar(df_caa["tld"], df_caa["caa_pct"], color=sns.color_palette("Set2", len(df_caa)))
ax.set_ylabel("% of Domains")
ax.set_title("CAA Record Adoption by TLD")
for i, v in enumerate(df_caa["caa_pct"]):
    ax.text(i, v + 0.1, f"{v:.1f}%", ha="center", fontsize=8)

ax = axes[1]
if len(df_caa_ca) > 0:
    ax.barh(df_caa_ca["caa_value"][::-1], df_caa_ca["domain_count"][::-1],
            color=sns.color_palette("Blues_r", len(df_caa_ca)))
ax.set_xlabel("Number of Domains")
ax.set_title("Top Certificate Authorities in CAA Records")
plt.tight_layout()
save_fig("security_caa")

# ═══════════════════════════════════════════════════════
# 4. CDS / CDNSKEY 部署情况
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("4. CDS / CDNSKEY 部署情况")
print("=" * 60)

cds_stats = []
for tld in TLDS:
    r = conn.execute(f"""
        SELECT
            count(DISTINCT query_name) AS total_domains,
            count(DISTINCT CASE WHEN query_type = 'CDS' AND cds_key_tag IS NOT NULL
                  THEN query_name END) AS has_cds,
            count(DISTINCT CASE WHEN query_type = 'CDNSKEY' AND cdnskey_flags IS NOT NULL
                  THEN query_name END) AS has_cdnskey
        FROM read_parquet('{parquet_glob(tld)}')
    """).fetchone()
    total = r[0]
    cds_stats.append({
        "tld": tld,
        "cds_pct": round(r[1] / total * 100, 2),
        "cdnskey_pct": round(r[2] / total * 100, 2),
    })

df_cds = pd.DataFrame(cds_stats)
print(df_cds.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(df_cds))
w = 0.35
ax.bar(x - w/2, df_cds["cds_pct"], w, label="CDS", color="#4C72B0")
ax.bar(x + w/2, df_cds["cdnskey_pct"], w, label="CDNSKEY", color="#DD8452")
ax.set_xticks(x)
ax.set_xticklabels(df_cds["tld"])
ax.set_ylabel("% of Domains")
ax.set_title("CDS / CDNSKEY Deployment by TLD")
ax.legend()
save_fig("security_cds_cdnskey")

print("\n[04_security] 完成!")
conn.close()
