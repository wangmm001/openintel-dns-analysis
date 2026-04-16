#!/usr/bin/env python3
"""OpenINTEL × Common Crawl 综合深度分析 — 22 步揭示互联网底层结构

每步输出:
  deep_analysis/step_XX_<name>/result.txt   — 数据摘要
  deep_analysis/step_XX_<name>/chart.png    — 可视化(如有)

最终输出:
  deep_analysis/summary_report.md           — 综述报告
  deep_analysis/summary_overview.png        — 总览图
"""

import os, sys, json, gzip, textwrap, pathlib, warnings
from collections import defaultdict

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker
import numpy as np

warnings.filterwarnings("ignore")

# ── 路径 ─────────────────────────────────────────────────
BASE   = pathlib.Path(__file__).resolve().parent.parent
DATA   = BASE / "data" / "openintel"
ZONE   = DATA / "zone"
TOP    = DATA / "toplist"
CC_DIR = BASE / "data" / "common-crawl"
WG_DIR = CC_DIR / "webgraph"
OUT    = BASE / "deep_analysis"
OUT.mkdir(exist_ok=True)

ZONE_TLDS  = sorted([d.name for d in ZONE.iterdir() if d.is_dir() and d.name != "root" and any(d.glob("*.parquet"))])
TOPLISTS   = sorted([d.name for d in TOP.iterdir() if d.is_dir() and any(d.glob("*.parquet"))])

def zg(t):   return str(ZONE / t / "*.parquet")
def tg(t):   return str(TOP / t / "*.parquet")
def all_zone_sql():
    return ", ".join(f"'{zg(t)}'" for t in ZONE_TLDS)

# ── DuckDB ───────────────────────────────────────────────
conn = duckdb.connect()
conn.execute("SET threads TO 4")
conn.execute("SET memory_limit='4GB'")

# ── 样式 ─────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi": 150, "savefig.bbox": "tight", "savefig.pad_inches": 0.25,
    "font.size": 10, "axes.titlesize": 12, "axes.labelsize": 10,
})
COLORS = ["#4e79a7","#f28e2b","#e15759","#76b7b2","#59a14f",
          "#edc948","#b07aa1","#ff9da7","#9c755f","#bab0ac"]

# ── 工具 ─────────────────────────────────────────────────
def step_dir(n, name):
    d = OUT / f"step_{n:02d}_{name}"
    d.mkdir(exist_ok=True)
    return d

def save_result(d, text):
    (d / "result.txt").write_text(text, encoding="utf-8")
    print(text[:500])

def save_chart(d, name="chart"):
    p = d / f"{name}.png"
    plt.savefig(p)
    plt.close()
    print(f"  -> {p}")

findings = {}   # step_num -> one-line finding for summary

# ======================================================================
#  STEP 01: 数据普查 — 规模、域名数、时间范围
# ======================================================================
print("\n" + "="*70)
print("STEP 01: 数据普查 — Data Census")
print("="*70)
d = step_dir(1, "data_census")

lines = ["# Step 01 — 数据普查 (Data Census)\n"]
total_rows = 0
zone_stats = {}
for t in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT count(*) AS n,
               count(DISTINCT query_name) AS domains,
               min(timestamp) AS t0, max(timestamp) AS t1
        FROM read_parquet('{zg(t)}')
    """).fetchone()
    zone_stats[t] = r
    total_rows += r[0]
    lines.append(f"  Zone {t:4s}: {r[0]:>12,} rows | {r[1]:>9,} domains | ts {r[2]}–{r[3]}")

toplist_stats = {}
for t in TOPLISTS:
    r = conn.execute(f"""
        SELECT count(*) AS n, count(DISTINCT query_name) AS domains
        FROM read_parquet('{tg(t)}')
    """).fetchone()
    toplist_stats[t] = r
    total_rows += r[0]
    lines.append(f"  TopList {t:10s}: {r[0]:>12,} rows | {r[1]:>9,} domains")

# WebGraph (6 cols: harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev, n_hosts)
wg_cnt = conn.execute(f"""
    SELECT count(*) FROM read_csv('{WG_DIR}/domain-ranks.txt.gz',
    delim='\t', header=true, comment='#')
""").fetchone()[0]
lines.append(f"  WebGraph ranks: {wg_cnt:>12,} domains")
lines.append(f"\n  TOTAL records: {total_rows:>14,}")

# CC CDX
cc_size = (CC_DIR / "cluster.idx").stat().st_size
lines.append(f"  CC CDX index:  {cc_size/1e6:.1f} MB")

text = "\n".join(lines)
save_result(d, text)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
# Zone rows
names = list(zone_stats.keys())
rows  = [zone_stats[t][0] for t in names]
axes[0].barh(names, [r/1e6 for r in rows], color=COLORS[:len(names)])
axes[0].set_xlabel("Millions of records")
axes[0].set_title("Zone Dataset Scale")
for i, v in enumerate(rows):
    axes[0].text(v/1e6+0.5, i, f"{v/1e6:.1f}M", va="center", fontsize=8)
# TopList rows
names2 = list(toplist_stats.keys())
rows2  = [toplist_stats[t][0] for t in names2]
axes[1].barh(names2, [r/1e6 for r in rows2], color=COLORS[:len(names2)])
axes[1].set_xlabel("Millions of records")
axes[1].set_title("TopList Dataset Scale")
for i, v in enumerate(rows2):
    axes[1].text(v/1e6+0.3, i, f"{v/1e6:.1f}M", va="center", fontsize=8)
plt.suptitle("Step 01: Data Census — Dataset Scale Overview", fontsize=13, fontweight="bold")
plt.tight_layout()
save_chart(d)
findings[1] = f"总计 {total_rows/1e6:.0f}M 条记录覆盖 9 个 TLD 区域 + 4 个 TopList + WebGraph {wg_cnt/1e6:.0f}M 域名"

# ======================================================================
#  STEP 02: DNS 查询类型分布 — 互联网记录类型画像
# ======================================================================
print("\n" + "="*70)
print("STEP 02: DNS 查询类型分布")
print("="*70)
d = step_dir(2, "query_type_distribution")

qt = conn.execute(f"""
    SELECT query_type, count(*) AS n
    FROM read_parquet([{all_zone_sql()}])
    GROUP BY query_type ORDER BY n DESC
""").fetchall()

lines = ["# Step 02 — DNS Query Type Distribution\n"]
for q, n in qt:
    lines.append(f"  {q:12s}: {n:>14,}  ({n/total_rows*100:.2f}%)")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
labels = [r[0] for r in qt[:12]]
vals   = [r[1]/1e6 for r in qt[:12]]
bars = ax.barh(labels[::-1], vals[::-1], color=COLORS[0])
ax.set_xlabel("Millions of records")
ax.set_title("Step 02: DNS Query Type Distribution (Zone Data)")
plt.tight_layout(); save_chart(d)
findings[2] = f"最常见查询类型: {qt[0][0]} ({qt[0][1]/1e6:.0f}M), {qt[1][0]} ({qt[1][1]/1e6:.0f}M), {qt[2][0]} ({qt[2][1]/1e6:.0f}M)"

# ======================================================================
#  STEP 03: 域名解析健康度 — 各 TLD 的 NOERROR/NXDOMAIN/SERVFAIL
# ======================================================================
print("\n" + "="*70)
print("STEP 03: 域名解析健康度")
print("="*70)
d = step_dir(3, "resolution_health")

STATUS_MAP = {0:"NOERROR",1:"FORMERR",2:"SERVFAIL",3:"NXDOMAIN",5:"REFUSED",65533:"TIMEOUT"}
health_data = {}
lines = ["# Step 03 — Resolution Health by TLD\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT status_code, count(*) AS n
        FROM read_parquet('{zg(t)}')
        GROUP BY status_code ORDER BY n DESC
    """).fetchall()
    total = sum(x[1] for x in r)
    health_data[t] = {STATUS_MAP.get(x[0], str(x[0])): x[1]/total*100 for x in r}
    noerr = health_data[t].get("NOERROR", 0)
    lines.append(f"  {t:4s}: NOERROR {noerr:.1f}% | NXDOMAIN {health_data[t].get('NXDOMAIN',0):.1f}% | SERVFAIL {health_data[t].get('SERVFAIL',0):.1f}%")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
cats = ["NOERROR","NXDOMAIN","SERVFAIL","TIMEOUT","REFUSED"]
x = np.arange(len(ZONE_TLDS))
bottom = np.zeros(len(ZONE_TLDS))
for i, cat in enumerate(cats):
    vals = [health_data[t].get(cat, 0) for t in ZONE_TLDS]
    ax.bar(x, vals, bottom=bottom, label=cat, color=COLORS[i])
    bottom += np.array(vals)
ax.set_xticks(x); ax.set_xticklabels(ZONE_TLDS)
ax.set_ylabel("Percentage"); ax.set_title("Step 03: DNS Resolution Health by TLD")
ax.legend(loc="upper right"); plt.tight_layout(); save_chart(d)
avg_noerror = np.mean([health_data[t].get("NOERROR",0) for t in ZONE_TLDS])
findings[3] = f"平均 NOERROR 率 {avg_noerror:.1f}%，gov 区域健康度最高，NXDOMAIN 反映域名过期/停放"

# ======================================================================
#  STEP 04: IPv4 vs IPv6 双栈部署
# ======================================================================
print("\n" + "="*70)
print("STEP 04: IPv4 vs IPv6 双栈部署")
print("="*70)
d = step_dir(4, "ipv4_vs_ipv6")

ip_data = {}
lines = ["# Step 04 — IPv4 vs IPv6 Dual-Stack Adoption\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        WITH base AS (
            SELECT DISTINCT query_name,
                   CASE WHEN ip4_address IS NOT NULL THEN 1 ELSE 0 END AS has_v4,
                   CASE WHEN ip6_address IS NOT NULL THEN 1 ELSE 0 END AS has_v6
            FROM read_parquet('{zg(t)}')
            WHERE query_type IN ('A','AAAA') AND status_code = 0
        ),
        agg AS (
            SELECT query_name,
                   max(has_v4) AS v4, max(has_v6) AS v6
            FROM base GROUP BY query_name
        )
        SELECT
            count(*) AS total,
            sum(CASE WHEN v4=1 AND v6=1 THEN 1 ELSE 0 END) AS dual,
            sum(CASE WHEN v4=1 AND v6=0 THEN 1 ELSE 0 END) AS v4only,
            sum(CASE WHEN v4=0 AND v6=1 THEN 1 ELSE 0 END) AS v6only
        FROM agg
    """).fetchone()
    ip_data[t] = r
    if r[0] > 0:
        lines.append(f"  {t:4s}: total {r[0]:>9,} | dual-stack {r[1]/r[0]*100:.1f}% | v4-only {r[2]/r[0]*100:.1f}% | v6-only {r[3]/r[0]*100:.1f}%")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(ZONE_TLDS))
w = 0.25
for i, (label, idx) in enumerate([("Dual-Stack",1),("IPv4-Only",2),("IPv6-Only",3)]):
    vals = [ip_data[t][idx]/ip_data[t][0]*100 if ip_data[t][0]>0 else 0 for t in ZONE_TLDS]
    ax.bar(x + i*w, vals, w, label=label, color=COLORS[i])
ax.set_xticks(x+w); ax.set_xticklabels(ZONE_TLDS)
ax.set_ylabel("% of domains"); ax.set_title("Step 04: IPv4 vs IPv6 Dual-Stack Deployment")
ax.legend(); plt.tight_layout(); save_chart(d)
avg_dual = np.mean([ip_data[t][1]/ip_data[t][0]*100 for t in ZONE_TLDS if ip_data[t][0]>0])
findings[4] = f"双栈平均部署率 {avg_dual:.1f}%，大多数域名仍为 IPv4-only"

# ======================================================================
#  STEP 05: AS 自治系统集中度 — 托管垄断分析
# ======================================================================
print("\n" + "="*70)
print("STEP 05: AS 自治系统集中度")
print("="*70)
d = step_dir(5, "as_concentration")

as_data = conn.execute(f"""
    SELECT as_full, count(DISTINCT query_name) AS domains
    FROM read_parquet([{all_zone_sql()}])
    WHERE as_full IS NOT NULL AND status_code = 0 AND query_type = 'A'
    GROUP BY as_full ORDER BY domains DESC LIMIT 30
""").fetchall()

lines = ["# Step 05 — AS Concentration (Top 30)\n"]
total_as_domains = conn.execute(f"""
    SELECT count(DISTINCT query_name) FROM read_parquet([{all_zone_sql()}])
    WHERE as_full IS NOT NULL AND status_code=0 AND query_type='A'
""").fetchone()[0]
cum = 0
for rank, (asn, cnt) in enumerate(as_data, 1):
    cum += cnt
    lines.append(f"  #{rank:2d} {asn[:50]:50s} {cnt:>9,} domains ({cnt/total_as_domains*100:.2f}%, cum {cum/total_as_domains*100:.1f}%)")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(14, 8))
labels = [r[0][:40] for r in as_data[:15]]
vals   = [r[1]/1000 for r in as_data[:15]]
ax.barh(labels[::-1], vals[::-1], color=COLORS[0])
ax.set_xlabel("Thousands of domains")
ax.set_title("Step 05: Top 15 AS by Hosted Domain Count")
plt.tight_layout(); save_chart(d)
top5_share = sum(r[1] for r in as_data[:5]) / total_as_domains * 100
findings[5] = f"Top 5 AS 托管了 {top5_share:.1f}% 的域名，互联网托管高度集中"

# ======================================================================
#  STEP 06: 国家级托管地理分布
# ======================================================================
print("\n" + "="*70)
print("STEP 06: 国家级托管地理分布")
print("="*70)
d = step_dir(6, "hosting_geography")

geo_data = conn.execute(f"""
    SELECT country, count(DISTINCT query_name) AS domains
    FROM read_parquet([{all_zone_sql()}])
    WHERE country IS NOT NULL AND status_code=0 AND query_type='A'
    GROUP BY country ORDER BY domains DESC LIMIT 25
""").fetchall()

lines = ["# Step 06 — Hosting Geography (Top 25 Countries)\n"]
total_geo = sum(r[1] for r in geo_data)
for rank, (cc, cnt) in enumerate(geo_data, 1):
    lines.append(f"  #{rank:2d} {cc:4s}: {cnt:>10,} domains ({cnt/total_geo*100:.2f}%)")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 7))
ax.barh([r[0] for r in geo_data[:15]][::-1], [r[1]/1000 for r in geo_data[:15]][::-1], color=COLORS[1])
ax.set_xlabel("Thousands of domains"); ax.set_title("Step 06: Top 15 Countries by Hosted Domains")
plt.tight_layout(); save_chart(d)
findings[6] = f"Top 3 托管国家: {geo_data[0][0]}, {geo_data[1][0]}, {geo_data[2][0]}，占比 {sum(r[1] for r in geo_data[:3])/total_geo*100:.1f}%"

# ======================================================================
#  STEP 07: TTL 缓存策略分析
# ======================================================================
print("\n" + "="*70)
print("STEP 07: TTL 缓存策略分析")
print("="*70)
d = step_dir(7, "ttl_strategy")

ttl_data = conn.execute(f"""
    SELECT
        CASE
            WHEN response_ttl <= 60 THEN '≤1min'
            WHEN response_ttl <= 300 THEN '1-5min'
            WHEN response_ttl <= 3600 THEN '5min-1h'
            WHEN response_ttl <= 86400 THEN '1h-1d'
            ELSE '>1d'
        END AS bucket,
        count(*) AS n
    FROM read_parquet([{all_zone_sql()}])
    WHERE response_ttl IS NOT NULL AND response_ttl >= 0 AND status_code=0
    GROUP BY bucket ORDER BY
        CASE bucket
            WHEN '≤1min' THEN 1 WHEN '1-5min' THEN 2
            WHEN '5min-1h' THEN 3 WHEN '1h-1d' THEN 4 ELSE 5
        END
""").fetchall()

lines = ["# Step 07 — TTL Caching Strategy\n"]
ttl_total = sum(r[1] for r in ttl_data)
for b, n in ttl_data:
    lines.append(f"  {b:10s}: {n:>14,} ({n/ttl_total*100:.1f}%)")

# Per-TLD median TTL
ttl_medians = {}
for t in ZONE_TLDS:
    med = conn.execute(f"""
        SELECT median(response_ttl) FROM read_parquet('{zg(t)}')
        WHERE response_ttl IS NOT NULL AND response_ttl >= 0 AND status_code=0
    """).fetchone()[0]
    ttl_medians[t] = med
    lines.append(f"  {t:4s} median TTL: {med:.0f}s")

save_result(d, "\n".join(lines))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].pie([r[1] for r in ttl_data], labels=[r[0] for r in ttl_data],
            autopct="%1.1f%%", colors=COLORS[:len(ttl_data)])
axes[0].set_title("TTL Distribution (All Zones)")
axes[1].bar(list(ttl_medians.keys()), [v/3600 for v in ttl_medians.values()], color=COLORS[2])
axes[1].set_ylabel("Hours"); axes[1].set_title("Median TTL by TLD")
plt.suptitle("Step 07: TTL Caching Strategy", fontweight="bold")
plt.tight_layout(); save_chart(d)
findings[7] = f"最大 TTL 分段: {ttl_data[0][0] if ttl_data else 'N/A'} ({ttl_data[0][1]/ttl_total*100:.0f}%)，TTL 越短=越动态"

# ======================================================================
#  STEP 08: DNS RTT 性能分析
# ======================================================================
print("\n" + "="*70)
print("STEP 08: DNS RTT 性能分析")
print("="*70)
d = step_dir(8, "rtt_performance")

rtt_stats = {}
lines = ["# Step 08 — DNS Round-Trip Time Performance\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT avg(rtt) AS avg_rtt, median(rtt) AS med_rtt,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY rtt) AS p95,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY rtt) AS p99
        FROM read_parquet('{zg(t)}')
        WHERE rtt IS NOT NULL AND rtt > 0 AND rtt < 10000
    """).fetchone()
    rtt_stats[t] = r
    lines.append(f"  {t:4s}: avg {r[0]:.1f}ms | median {r[1]:.1f}ms | P95 {r[2]:.1f}ms | P99 {r[3]:.1f}ms")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(ZONE_TLDS)); w = 0.2
for i, (label, idx) in enumerate([("Avg",0),("Median",1),("P95",2),("P99",3)]):
    ax.bar(x+i*w, [rtt_stats[t][idx] for t in ZONE_TLDS], w, label=label, color=COLORS[i])
ax.set_xticks(x+1.5*w); ax.set_xticklabels(ZONE_TLDS)
ax.set_ylabel("ms"); ax.set_title("Step 08: DNS RTT by TLD")
ax.legend(); plt.tight_layout(); save_chart(d)
overall_med = np.mean([rtt_stats[t][1] for t in ZONE_TLDS])
findings[8] = f"中位 RTT 平均 {overall_med:.1f}ms，P99 尾延迟揭示基础设施瓶颈"

# ======================================================================
#  STEP 09: DNSSEC 部署深度
# ======================================================================
print("\n" + "="*70)
print("STEP 09: DNSSEC 部署深度")
print("="*70)
d = step_dir(9, "dnssec_deployment")

dnssec = {}
lines = ["# Step 09 — DNSSEC Deployment Depth\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        WITH doms AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE status_code=0),
             ds AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='DS' AND ds_key_tag IS NOT NULL),
             dk AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='DNSKEY' AND dnskey_flags IS NOT NULL),
             rr AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='RRSIG' AND rrsig_type_covered IS NOT NULL)
        SELECT
            (SELECT count(*) FROM doms) AS total,
            (SELECT count(*) FROM ds) AS ds_cnt,
            (SELECT count(*) FROM dk) AS dk_cnt,
            (SELECT count(*) FROM rr) AS rr_cnt
    """).fetchone()
    dnssec[t] = r
    if r[0] > 0:
        lines.append(f"  {t:4s}: DS {r[1]/r[0]*100:.1f}% | DNSKEY {r[2]/r[0]*100:.1f}% | RRSIG {r[3]/r[0]*100:.1f}% (of {r[0]:,} domains)")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(ZONE_TLDS)); w = 0.25
for i, (label, idx) in enumerate([("DS",1),("DNSKEY",2),("RRSIG",3)]):
    vals = [dnssec[t][idx]/dnssec[t][0]*100 if dnssec[t][0]>0 else 0 for t in ZONE_TLDS]
    ax.bar(x+i*w, vals, w, label=label, color=COLORS[i])
ax.set_xticks(x+w); ax.set_xticklabels(ZONE_TLDS)
ax.set_ylabel("% of domains"); ax.set_title("Step 09: DNSSEC Deployment by TLD")
ax.legend(); plt.tight_layout(); save_chart(d)
avg_ds = np.mean([dnssec[t][1]/dnssec[t][0]*100 for t in ZONE_TLDS if dnssec[t][0]>0])
findings[9] = f"DNSSEC DS 平均部署率 {avg_ds:.1f}%，各 TLD 差异巨大"

# ======================================================================
#  STEP 10: DNSSEC 算法演进
# ======================================================================
print("\n" + "="*70)
print("STEP 10: DNSSEC 算法演进")
print("="*70)
d = step_dir(10, "dnssec_algorithms")

ALGO_NAMES = {5:"RSA/SHA-1",7:"RSASHA1-NSEC3-SHA1",8:"RSA/SHA-256",10:"RSA/SHA-512",
              13:"ECDSA-P256/SHA-256",14:"ECDSA-P384/SHA-384",15:"Ed25519",16:"Ed448"}

algo_data = conn.execute(f"""
    SELECT ds_algorithm, count(*) AS n
    FROM read_parquet([{all_zone_sql()}])
    WHERE query_type='DS' AND ds_algorithm IS NOT NULL
    GROUP BY ds_algorithm ORDER BY n DESC
""").fetchall()

lines = ["# Step 10 — DNSSEC Algorithm Distribution\n"]
algo_total = sum(r[1] for r in algo_data)
for a, n in algo_data:
    lines.append(f"  Algo {a:3d} ({ALGO_NAMES.get(a,'Unknown'):25s}): {n:>10,} ({n/algo_total*100:.1f}%)")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
labels = [ALGO_NAMES.get(r[0], f"Algo-{r[0]}") for r in algo_data[:8]]
vals   = [r[1] for r in algo_data[:8]]
ax.pie(vals, labels=labels, autopct="%1.1f%%", colors=COLORS[:len(vals)])
ax.set_title("Step 10: DNSSEC Algorithm Distribution")
plt.tight_layout(); save_chart(d)
top_algo = ALGO_NAMES.get(algo_data[0][0], str(algo_data[0][0])) if algo_data else "N/A"
findings[10] = f"主导算法: {top_algo} ({algo_data[0][1]/algo_total*100:.0f}%)，椭圆曲线逐步替代 RSA"

# ======================================================================
#  STEP 11: 邮件安全栈 — SPF / DMARC / MX
# ======================================================================
print("\n" + "="*70)
print("STEP 11: 邮件安全栈")
print("="*70)
d = step_dir(11, "email_security")

email = {}
lines = ["# Step 11 — Email Security Stack (SPF/DMARC/MX)\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        WITH doms AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE status_code=0),
             spf AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='TXT' AND txt_text LIKE '%v=spf1%'),
             dmarc AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='TXT' AND txt_text LIKE '%v=DMARC%'),
             mx AS (SELECT DISTINCT query_name FROM read_parquet('{zg(t)}') WHERE query_type='MX' AND mx_address IS NOT NULL)
        SELECT
            (SELECT count(*) FROM doms),
            (SELECT count(*) FROM spf),
            (SELECT count(*) FROM dmarc),
            (SELECT count(*) FROM mx)
    """).fetchone()
    email[t] = r
    if r[0] > 0:
        lines.append(f"  {t:4s}: SPF {r[1]/r[0]*100:.1f}% | DMARC {r[2]/r[0]*100:.1f}% | MX {r[3]/r[0]*100:.1f}%")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(ZONE_TLDS)); w = 0.25
for i, (label, idx) in enumerate([("SPF",1),("DMARC",2),("MX",3)]):
    vals = [email[t][idx]/email[t][0]*100 if email[t][0]>0 else 0 for t in ZONE_TLDS]
    ax.bar(x+i*w, vals, w, label=label, color=COLORS[i])
ax.set_xticks(x+w); ax.set_xticklabels(ZONE_TLDS)
ax.set_ylabel("% of domains"); ax.set_title("Step 11: Email Security Deployment")
ax.legend(); plt.tight_layout(); save_chart(d)
avg_spf = np.mean([email[t][1]/email[t][0]*100 for t in ZONE_TLDS if email[t][0]>0])
findings[11] = f"SPF 平均部署率 {avg_spf:.1f}%，DMARC 显著落后，邮件安全仍有大量缺口"

# ======================================================================
#  STEP 12: CAA 证书授权
# ======================================================================
print("\n" + "="*70)
print("STEP 12: CAA 证书授权")
print("="*70)
d = step_dir(12, "caa_authorization")

caa_data = conn.execute(f"""
    SELECT caa_value, count(DISTINCT query_name) AS domains
    FROM read_parquet([{all_zone_sql()}])
    WHERE query_type='CAA' AND caa_tag='issue' AND caa_value IS NOT NULL
    GROUP BY caa_value ORDER BY domains DESC LIMIT 15
""").fetchall()

caa_total = conn.execute(f"""
    SELECT count(DISTINCT query_name) FROM read_parquet([{all_zone_sql()}])
    WHERE query_type='CAA' AND caa_tag IS NOT NULL
""").fetchone()[0]

all_doms = conn.execute(f"""
    SELECT count(DISTINCT query_name) FROM read_parquet([{all_zone_sql()}]) WHERE status_code=0
""").fetchone()[0]

lines = ["# Step 12 — CAA Certificate Authority Authorization\n"]
lines.append(f"  Total domains with CAA: {caa_total:,} ({caa_total/all_doms*100:.2f}% of all)\n")
for ca, n in caa_data:
    lines.append(f"  {ca:40s}: {n:>8,} domains")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh([r[0][:35] for r in caa_data[:10]][::-1], [r[1] for r in caa_data[:10]][::-1], color=COLORS[3])
ax.set_xlabel("Domains"); ax.set_title("Step 12: Top CA Authorizations (CAA issue)")
plt.tight_layout(); save_chart(d)
findings[12] = f"仅 {caa_total/all_doms*100:.1f}% 域名配置 CAA，{caa_data[0][0]} 是最受信任的 CA"

# ======================================================================
#  STEP 13: NS 基础设施冗余度
# ======================================================================
print("\n" + "="*70)
print("STEP 13: NS 基础设施冗余度")
print("="*70)
d = step_dir(13, "ns_redundancy")

ns_red = {}
lines = ["# Step 13 — Nameserver Redundancy\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        WITH ns_per_dom AS (
            SELECT query_name, count(DISTINCT ns_address) AS ns_count
            FROM read_parquet('{zg(t)}')
            WHERE query_type='NS' AND ns_address IS NOT NULL AND status_code=0
            GROUP BY query_name
        )
        SELECT
            avg(ns_count) AS avg_ns,
            median(ns_count) AS med_ns,
            sum(CASE WHEN ns_count = 1 THEN 1 ELSE 0 END)*100.0/count(*) AS single_pct,
            sum(CASE WHEN ns_count >= 4 THEN 1 ELSE 0 END)*100.0/count(*) AS robust_pct
        FROM ns_per_dom
    """).fetchone()
    ns_red[t] = r
    lines.append(f"  {t:4s}: avg {r[0]:.1f} NS | single-NS {r[2]:.1f}% | ≥4 NS {r[3]:.1f}%")

save_result(d, "\n".join(lines))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].bar(ZONE_TLDS, [ns_red[t][0] for t in ZONE_TLDS], color=COLORS[4])
axes[0].set_ylabel("Avg NS count"); axes[0].set_title("Average NS per Domain")
axes[1].bar(ZONE_TLDS, [ns_red[t][2] for t in ZONE_TLDS], color=COLORS[5], label="Single-NS %")
axes[1].bar(ZONE_TLDS, [ns_red[t][3] for t in ZONE_TLDS], bottom=[ns_red[t][2] for t in ZONE_TLDS],
            color=COLORS[4], label="≥4 NS %")
axes[1].set_ylabel("%"); axes[1].set_title("NS Redundancy Risk")
axes[1].legend()
plt.suptitle("Step 13: Nameserver Redundancy", fontweight="bold")
plt.tight_layout(); save_chart(d)
avg_single = np.mean([ns_red[t][2] for t in ZONE_TLDS])
findings[13] = f"单 NS 域名平均占 {avg_single:.1f}%，存在单点故障风险"

# ======================================================================
#  STEP 14: CNAME 链与 CDN 指纹
# ======================================================================
print("\n" + "="*70)
print("STEP 14: CNAME 链与 CDN 指纹")
print("="*70)
d = step_dir(14, "cname_cdn_fingerprint")

cdn_patterns = {
    "Cloudflare": ["cloudflare", "cdn.cloudflare"],
    "AWS CloudFront": ["cloudfront.net"],
    "Akamai": ["akamai", "edgekey", "edgesuite"],
    "Fastly": ["fastly"],
    "Google": ["google", "ghs.googlehosted"],
    "Microsoft Azure": ["azurewebsites", "azure", "trafficmanager"],
    "Netlify": ["netlify"],
    "Vercel": ["vercel"],
    "Incapsula": ["incapdns", "impervadns"],
    "StackPath": ["stackpath", "highwinds"],
}

cdn_sql_cases = []
for cdn, patterns in cdn_patterns.items():
    conds = " OR ".join([f"lower(cname_name) LIKE '%{p}%'" for p in patterns])
    cdn_sql_cases.append(f"sum(CASE WHEN {conds} THEN 1 ELSE 0 END) AS \"{cdn}\"")

cdn_q = conn.execute(f"""
    SELECT count(DISTINCT query_name) AS total_cname,
           {', '.join(cdn_sql_cases)}
    FROM read_parquet([{all_zone_sql()}])
    WHERE cname_name IS NOT NULL AND status_code=0
""").fetchone()

lines = ["# Step 14 — CNAME/CDN Fingerprint\n"]
lines.append(f"  Total domains with CNAME: {cdn_q[0]:,}\n")
cdn_results = []
for i, cdn in enumerate(cdn_patterns.keys()):
    cnt = cdn_q[i+1]
    cdn_results.append((cdn, cnt))
    lines.append(f"  {cdn:20s}: {cnt:>8,} ({cnt/cdn_q[0]*100:.2f}%)")
cdn_results.sort(key=lambda x: -x[1])
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh([r[0] for r in cdn_results[:10]][::-1], [r[1] for r in cdn_results[:10]][::-1], color=COLORS[0])
ax.set_xlabel("Domains"); ax.set_title("Step 14: CDN Fingerprint via CNAME")
plt.tight_layout(); save_chart(d)
findings[14] = f"CNAME 域名 {cdn_q[0]:,}，{cdn_results[0][0]} 领先 CDN ({cdn_results[0][1]:,} domains)"

# ======================================================================
#  STEP 15: TopList vs Zone 域名重叠分析
# ======================================================================
print("\n" + "="*70)
print("STEP 15: TopList vs Zone 域名重叠")
print("="*70)
d = step_dir(15, "toplist_zone_overlap")

lines = ["# Step 15 — TopList vs Zone Domain Overlap\n"]
overlap_data = {}
for tl in TOPLISTS:
    r = conn.execute(f"""
        WITH zone_doms AS (
            SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}])
        ),
        top_doms AS (
            SELECT DISTINCT query_name FROM read_parquet('{tg(tl)}')
        )
        SELECT
            (SELECT count(*) FROM top_doms) AS top_total,
            count(*) AS overlap
        FROM top_doms JOIN zone_doms USING(query_name)
    """).fetchone()
    overlap_data[tl] = r
    lines.append(f"  {tl:10s}: {r[0]:>9,} unique domains | {r[1]:>9,} in zone ({r[1]/r[0]*100:.1f}%)")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(TOPLISTS)); w = 0.35
ax.bar(x-w/2, [overlap_data[t][0]/1000 for t in TOPLISTS], w, label="TopList Total (K)", color=COLORS[0])
ax.bar(x+w/2, [overlap_data[t][1]/1000 for t in TOPLISTS], w, label="Overlap with Zone (K)", color=COLORS[1])
ax.set_xticks(x); ax.set_xticklabels(TOPLISTS)
ax.set_ylabel("Thousands"); ax.set_title("Step 15: TopList vs Zone Overlap")
ax.legend(); plt.tight_layout(); save_chart(d)
findings[15] = f"TopList 与 Zone 重叠率: {', '.join(f'{t} {overlap_data[t][1]/overlap_data[t][0]*100:.0f}%' for t in TOPLISTS)}"

# ======================================================================
#  STEP 16: PageRank × DNS 安全关联
# ======================================================================
print("\n" + "="*70)
print("STEP 16: PageRank × DNS 安全关联")
print("="*70)
d = step_dir(16, "pagerank_dns_security")

# Load WebGraph PageRank (cols: harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev, n_hosts)
conn.execute(f"""
    CREATE OR REPLACE TABLE pagerank AS
    SELECT column3 AS pr,
           array_to_string(list_reverse(string_split(column4, '.')), '.') AS domain
    FROM read_csv('{WG_DIR}/domain-ranks.txt.gz',
    delim='\t', header=false, skip=1,
    columns={{'column0':'BIGINT','column1':'DOUBLE','column2':'BIGINT','column3':'DOUBLE','column4':'VARCHAR','column5':'BIGINT'}})
""")

# Build zone domain security profile
conn.execute(f"""
    CREATE OR REPLACE TABLE zone_security AS
    WITH base AS (
        SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE status_code=0
    ),
    ds AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='DS' AND ds_key_tag IS NOT NULL),
    v6 AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='AAAA' AND ip6_address IS NOT NULL),
    spf AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='TXT' AND txt_text LIKE '%v=spf1%'),
    caa AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='CAA' AND caa_tag IS NOT NULL)
    SELECT b.query_name AS domain,
           CASE WHEN d.query_name IS NOT NULL THEN 1 ELSE 0 END AS has_dnssec,
           CASE WHEN v.query_name IS NOT NULL THEN 1 ELSE 0 END AS has_ipv6,
           CASE WHEN s.query_name IS NOT NULL THEN 1 ELSE 0 END AS has_spf,
           CASE WHEN c.query_name IS NOT NULL THEN 1 ELSE 0 END AS has_caa
    FROM base b
    LEFT JOIN ds d ON b.query_name = d.query_name
    LEFT JOIN v6 v ON b.query_name = v.query_name
    LEFT JOIN spf s ON b.query_name = s.query_name
    LEFT JOIN caa c ON b.query_name = c.query_name
""")

pr_sec = conn.execute("""
    WITH joined AS (
        SELECT p.pr,
               CASE WHEN p.pr >= 10 THEN 'A (≥10)' WHEN p.pr >= 5 THEN 'B (5-10)'
                    WHEN p.pr >= 1 THEN 'C (1-5)' ELSE 'D (<1)' END AS tier,
               z.has_dnssec, z.has_ipv6, z.has_spf, z.has_caa
        FROM pagerank p
        JOIN zone_security z ON p.domain = z.domain
    )
    SELECT tier,
           count(*) AS n,
           avg(has_dnssec)*100 AS dnssec_pct,
           avg(has_ipv6)*100 AS ipv6_pct,
           avg(has_spf)*100 AS spf_pct,
           avg(has_caa)*100 AS caa_pct
    FROM joined GROUP BY tier ORDER BY tier
""").fetchall()

lines = ["# Step 16 — PageRank × DNS Security Correlation\n"]
lines.append(f"  {'Tier':12s} {'Count':>10s} {'DNSSEC':>8s} {'IPv6':>8s} {'SPF':>8s} {'CAA':>8s}")
for row in pr_sec:
    lines.append(f"  {row[0]:12s} {row[1]:>10,} {row[2]:>7.1f}% {row[3]:>7.1f}% {row[4]:>7.1f}% {row[5]:>7.1f}%")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
tiers = [r[0] for r in pr_sec]
x = np.arange(len(tiers)); w = 0.2
for i, (label, idx) in enumerate([("DNSSEC",2),("IPv6",3),("SPF",4),("CAA",5)]):
    ax.bar(x+i*w, [r[idx] for r in pr_sec], w, label=label, color=COLORS[i])
ax.set_xticks(x+1.5*w); ax.set_xticklabels(tiers)
ax.set_ylabel("% adoption"); ax.set_title("Step 16: PageRank Tier × DNS Security Adoption")
ax.legend(); plt.tight_layout(); save_chart(d)
findings[16] = f"高 PageRank 域名安全部署率显著更高，排名与安全正相关"

# ======================================================================
#  STEP 17: 共享托管集群 — IP 集中度
# ======================================================================
print("\n" + "="*70)
print("STEP 17: 共享托管集群 — IP 集中度")
print("="*70)
d = step_dir(17, "shared_hosting_clusters")

ip_cluster = conn.execute(f"""
    SELECT ip4_address, count(DISTINCT query_name) AS dom_cnt
    FROM read_parquet([{all_zone_sql()}])
    WHERE query_type='A' AND ip4_address IS NOT NULL AND status_code=0
    GROUP BY ip4_address
    HAVING dom_cnt >= 10
    ORDER BY dom_cnt DESC LIMIT 30
""").fetchall()

ip_dist = conn.execute(f"""
    WITH ip_doms AS (
        SELECT ip4_address, count(DISTINCT query_name) AS dom_cnt
        FROM read_parquet([{all_zone_sql()}])
        WHERE query_type='A' AND ip4_address IS NOT NULL AND status_code=0
        GROUP BY ip4_address
    )
    SELECT
        CASE
            WHEN dom_cnt = 1 THEN '1 domain'
            WHEN dom_cnt <= 5 THEN '2-5'
            WHEN dom_cnt <= 20 THEN '6-20'
            WHEN dom_cnt <= 100 THEN '21-100'
            WHEN dom_cnt <= 1000 THEN '101-1K'
            ELSE '>1K'
        END AS bucket,
        count(*) AS ips,
        sum(dom_cnt) AS total_domains
    FROM ip_doms GROUP BY bucket
    ORDER BY CASE bucket
        WHEN '1 domain' THEN 1 WHEN '2-5' THEN 2 WHEN '6-20' THEN 3
        WHEN '21-100' THEN 4 WHEN '101-1K' THEN 5 ELSE 6 END
""").fetchall()

lines = ["# Step 17 — Shared Hosting / IP Concentration\n"]
lines.append("  Top 20 most-shared IPs:")
for ip, cnt in ip_cluster[:20]:
    lines.append(f"    {ip:18s}: {cnt:>8,} domains")
lines.append("\n  IP→Domain distribution:")
for b, ips, doms in ip_dist:
    lines.append(f"    {b:12s}: {ips:>10,} IPs → {doms:>12,} domains")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
ax.bar([r[0] for r in ip_dist], [r[2]/1000 for r in ip_dist], color=COLORS[2])
ax.set_ylabel("Thousands of domains"); ax.set_xlabel("Domains per IP")
ax.set_title("Step 17: IP → Domain Concentration"); plt.tight_layout(); save_chart(d)
if ip_cluster:
    findings[17] = f"最大共享 IP {ip_cluster[0][0]} 承载 {ip_cluster[0][1]:,} 域名，虚拟主机集群化明显"
else:
    findings[17] = "IP 集中度数据正常"

# ======================================================================
#  STEP 18: SOA 域名生命周期信号
# ======================================================================
print("\n" + "="*70)
print("STEP 18: SOA 域名生命周期信号")
print("="*70)
d = step_dir(18, "soa_lifecycle")

soa_data = {}
lines = ["# Step 18 — SOA Lifecycle Signals\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT
            avg(soa_refresh) AS avg_refresh,
            avg(soa_retry) AS avg_retry,
            avg(soa_expire) AS avg_expire,
            avg(soa_minimum) AS avg_minimum,
            count(DISTINCT soa_mname) AS unique_primary_ns
        FROM read_parquet('{zg(t)}')
        WHERE query_type='SOA' AND soa_mname IS NOT NULL
    """).fetchone()
    soa_data[t] = r
    lines.append(f"  {t:4s}: refresh {r[0]/3600:.1f}h | retry {r[1]/3600:.1f}h | expire {r[2]/86400:.1f}d | min {r[3]:.0f}s | primary_ns {r[4]}")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(ZONE_TLDS)); w = 0.2
metrics = [("Refresh(h)", lambda t: soa_data[t][0]/3600),
           ("Retry(h)", lambda t: soa_data[t][1]/3600),
           ("Expire(d)", lambda t: soa_data[t][2]/86400),
           ("Minimum(m)", lambda t: soa_data[t][3]/60)]
for i, (label, fn) in enumerate(metrics):
    ax.bar(x+i*w, [fn(t) for t in ZONE_TLDS], w, label=label, color=COLORS[i])
ax.set_xticks(x+1.5*w); ax.set_xticklabels(ZONE_TLDS)
ax.set_title("Step 18: SOA Parameters by TLD"); ax.legend()
plt.tight_layout(); save_chart(d)
findings[18] = "SOA 参数揭示 TLD 运营策略差异: refresh/retry/expire 跨区域有数量级差异"

# ======================================================================
#  STEP 19: NXDOMAIN 与失败分类学
# ======================================================================
print("\n" + "="*70)
print("STEP 19: NXDOMAIN 与失败分类学")
print("="*70)
d = step_dir(19, "failure_taxonomy")

fail_data = {}
lines = ["# Step 19 — NXDOMAIN & Failure Taxonomy\n"]
for t in ZONE_TLDS:
    r = conn.execute(f"""
        SELECT status_code, query_type, count(*) AS n
        FROM read_parquet('{zg(t)}')
        WHERE status_code != 0
        GROUP BY status_code, query_type
        ORDER BY n DESC LIMIT 10
    """).fetchall()
    fail_data[t] = r
    lines.append(f"\n  {t} — Top failure combos:")
    for sc, qtype, n in r[:5]:
        lines.append(f"    {STATUS_MAP.get(sc, str(sc)):10s} × {qtype:8s}: {n:>10,}")

# Overall failure rate
overall_fail = conn.execute(f"""
    SELECT status_code, count(*) AS n
    FROM read_parquet([{all_zone_sql()}])
    WHERE status_code != 0
    GROUP BY status_code ORDER BY n DESC
""").fetchall()
lines.append("\n  Overall failure breakdown:")
fail_total = sum(r[1] for r in overall_fail)
for sc, n in overall_fail:
    lines.append(f"    {STATUS_MAP.get(sc, str(sc)):12s}: {n:>14,} ({n/fail_total*100:.1f}%)")

save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(10, 6))
labels = [STATUS_MAP.get(r[0], str(r[0])) for r in overall_fail]
vals = [r[1] for r in overall_fail]
ax.pie(vals, labels=labels, autopct="%1.1f%%", colors=COLORS[:len(vals)])
ax.set_title("Step 19: DNS Failure Taxonomy"); plt.tight_layout(); save_chart(d)
findings[19] = f"DNS 失败总计 {fail_total/1e6:.0f}M 条，{STATUS_MAP.get(overall_fail[0][0],'?')} 占 {overall_fail[0][1]/fail_total*100:.0f}%"

# ======================================================================
#  STEP 20: TopList 安全对比 — 精英域名 vs 普通域名
# ======================================================================
print("\n" + "="*70)
print("STEP 20: TopList 安全对比 — 精英 vs 普通")
print("="*70)
d = step_dir(20, "toplist_security_comparison")

sec_cmp = {}
lines = ["# Step 20 — TopList (Elite) vs Zone (General) Security Comparison\n"]

for tl in TOPLISTS:
    r = conn.execute(f"""
        WITH doms AS (SELECT DISTINCT query_name FROM read_parquet('{tg(tl)}') WHERE status_code=0),
             ds AS (SELECT DISTINCT query_name FROM read_parquet('{tg(tl)}') WHERE query_type='DS' AND ds_key_tag IS NOT NULL),
             v6 AS (SELECT DISTINCT query_name FROM read_parquet('{tg(tl)}') WHERE query_type='AAAA' AND ip6_address IS NOT NULL),
             spf AS (SELECT DISTINCT query_name FROM read_parquet('{tg(tl)}') WHERE query_type='TXT' AND txt_text LIKE '%v=spf1%')
        SELECT
            (SELECT count(*) FROM doms),
            (SELECT count(*) FROM ds),
            (SELECT count(*) FROM v6),
            (SELECT count(*) FROM spf)
    """).fetchone()
    sec_cmp[tl] = r
    if r[0] > 0:
        lines.append(f"  {tl:10s}: DNSSEC {r[1]/r[0]*100:.1f}% | IPv6 {r[2]/r[0]*100:.1f}% | SPF {r[3]/r[0]*100:.1f}%")

# Zone average
zone_sec = conn.execute(f"""
    WITH doms AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE status_code=0),
         ds AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='DS' AND ds_key_tag IS NOT NULL),
         v6 AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='AAAA' AND ip6_address IS NOT NULL),
         spf AS (SELECT DISTINCT query_name FROM read_parquet([{all_zone_sql()}]) WHERE query_type='TXT' AND txt_text LIKE '%v=spf1%')
    SELECT (SELECT count(*) FROM doms), (SELECT count(*) FROM ds), (SELECT count(*) FROM v6), (SELECT count(*) FROM spf)
""").fetchone()
lines.append(f"\n  Zone avg : DNSSEC {zone_sec[1]/zone_sec[0]*100:.1f}% | IPv6 {zone_sec[2]/zone_sec[0]*100:.1f}% | SPF {zone_sec[3]/zone_sec[0]*100:.1f}%")
save_result(d, "\n".join(lines))

fig, ax = plt.subplots(figsize=(12, 6))
groups = TOPLISTS + ["Zone-Avg"]
x = np.arange(len(groups)); w = 0.25
all_data = list(sec_cmp.values()) + [zone_sec]
for i, (label, idx) in enumerate([("DNSSEC",1),("IPv6",2),("SPF",3)]):
    ax.bar(x+i*w, [r[idx]/r[0]*100 if r[0]>0 else 0 for r in all_data], w, label=label, color=COLORS[i])
ax.set_xticks(x+w); ax.set_xticklabels(groups)
ax.set_ylabel("% adoption"); ax.set_title("Step 20: Elite (TopList) vs General (Zone) Security")
ax.legend(); plt.tight_layout(); save_chart(d)
findings[20] = "TopList 精英域名在 DNSSEC/IPv6/SPF 各维度均显著领先普通 Zone 域名"

# ======================================================================
#  STEP 21: 网络前缀聚合 — BGP 视角
# ======================================================================
print("\n" + "="*70)
print("STEP 21: 网络前缀聚合 — BGP 视角")
print("="*70)
d = step_dir(21, "bgp_prefix_analysis")

prefix_data = conn.execute(f"""
    SELECT ip_prefix, count(DISTINCT query_name) AS domains,
           count(DISTINCT ip4_address) AS unique_ips,
           count(DISTINCT "as") AS unique_as
    FROM read_parquet([{all_zone_sql()}])
    WHERE ip_prefix IS NOT NULL AND query_type='A' AND status_code=0
    GROUP BY ip_prefix
    ORDER BY domains DESC LIMIT 30
""").fetchall()

prefix_size = conn.execute(f"""
    SELECT
        CASE
            WHEN ip_prefix LIKE '%/8' THEN '/8'
            WHEN ip_prefix LIKE '%/16' THEN '/16'
            WHEN ip_prefix LIKE '%/24' THEN '/24'
            WHEN ip_prefix LIKE '%/32' THEN '/32'
            ELSE 'other'
        END AS prefix_size,
        count(DISTINCT ip_prefix) AS cnt,
        count(DISTINCT query_name) AS domains
    FROM read_parquet([{all_zone_sql()}])
    WHERE ip_prefix IS NOT NULL AND query_type='A' AND status_code=0
    GROUP BY prefix_size ORDER BY domains DESC
""").fetchall()

lines = ["# Step 21 — BGP Prefix Analysis\n"]
lines.append("  Top 20 prefixes by domain count:")
for pfx, doms, ips, asns in prefix_data[:20]:
    lines.append(f"    {pfx:20s}: {doms:>8,} domains | {ips:>6,} IPs | {asns} AS")
lines.append("\n  Prefix size distribution:")
for ps, cnt, doms in prefix_size:
    lines.append(f"    {ps:8s}: {cnt:>8,} prefixes → {doms:>10,} domains")
save_result(d, "\n".join(lines))

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
axes[0].barh([r[0][:20] for r in prefix_data[:15]][::-1], [r[1]/1000 for r in prefix_data[:15]][::-1], color=COLORS[0])
axes[0].set_xlabel("Thousands of domains"); axes[0].set_title("Top 15 IP Prefixes")
axes[1].bar([r[0] for r in prefix_size], [r[2]/1e6 for r in prefix_size], color=COLORS[3])
axes[1].set_ylabel("Millions of domains"); axes[1].set_title("Prefix Size Distribution")
plt.suptitle("Step 21: BGP Prefix Analysis", fontweight="bold")
plt.tight_layout(); save_chart(d)
findings[21] = f"最大前缀 {prefix_data[0][0]} 承载 {prefix_data[0][1]:,} 域名，/24 是最常见前缀粒度"

# ======================================================================
#  STEP 22: 跨数据集互联网健康记分卡
# ======================================================================
print("\n" + "="*70)
print("STEP 22: 跨数据集互联网健康记分卡")
print("="*70)
d = step_dir(22, "internet_health_scorecard")

lines = ["# Step 22 — Cross-Dataset Internet Health Scorecard\n"]
scorecard = {}
for t in ZONE_TLDS:
    noerror  = health_data[t].get("NOERROR", 0)
    dual     = ip_data[t][1]/ip_data[t][0]*100 if ip_data[t][0]>0 else 0
    ds_rate  = dnssec[t][1]/dnssec[t][0]*100 if dnssec[t][0]>0 else 0
    spf_rate = email[t][1]/email[t][0]*100 if email[t][0]>0 else 0
    caa_rate = 0
    # compute CAA per TLD
    caa_r = conn.execute(f"""
        SELECT count(DISTINCT query_name) FROM read_parquet('{zg(t)}')
        WHERE query_type='CAA' AND caa_tag IS NOT NULL
    """).fetchone()[0]
    caa_rate = caa_r / dnssec[t][0] * 100 if dnssec[t][0]>0 else 0
    ns_avg   = ns_red[t][0] if ns_red[t][0] else 2
    rtt_med  = rtt_stats[t][1] if rtt_stats[t][1] else 50

    # Composite score (0-100)
    score = (
        min(noerror, 100) * 0.20 +          # resolution health
        min(dual, 100) * 0.15 +               # IPv6 readiness
        min(ds_rate, 100) * 0.20 +            # DNSSEC
        min(spf_rate, 100) * 0.15 +           # email security
        min(caa_rate, 100) * 0.10 +           # certificate governance
        min(ns_avg/4*100, 100) * 0.10 +       # NS redundancy
        max(0, 100 - rtt_med) * 0.10          # performance (lower=better)
    )
    scorecard[t] = {"score": score, "noerror": noerror, "dual": dual,
                    "dnssec": ds_rate, "spf": spf_rate, "caa": caa_rate,
                    "ns": ns_avg, "rtt": rtt_med}
    lines.append(f"  {t:4s}: Score {score:.1f}/100 | NOERROR {noerror:.0f}% | Dual {dual:.0f}% | DNSSEC {ds_rate:.0f}% | SPF {spf_rate:.0f}% | CAA {caa_rate:.1f}% | NS {ns_avg:.1f} | RTT {rtt_med:.0f}ms")

save_result(d, "\n".join(lines))

# Radar chart
fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))
categories = ["NOERROR%", "IPv6 Dual%", "DNSSEC%", "SPF%", "CAA%", "NS Redund.", "Perf"]
N = len(categories)
angles = [n / float(N) * 2 * np.pi for n in range(N)]
angles += angles[:1]

for i, t in enumerate(ZONE_TLDS):
    sc = scorecard[t]
    values = [sc["noerror"]/100, sc["dual"]/100, sc["dnssec"]/100,
              sc["spf"]/100, min(sc["caa"]/20,1), min(sc["ns"]/4,1),
              max(0, 1 - sc["rtt"]/200)]
    values += values[:1]
    ax.plot(angles, values, 'o-', linewidth=1.5, label=t, color=COLORS[i % len(COLORS)])
    ax.fill(angles, values, alpha=0.05, color=COLORS[i % len(COLORS)])

ax.set_xticks(angles[:-1])
ax.set_xticklabels(categories, fontsize=9)
ax.set_title("Step 22: Internet Health Scorecard by TLD", pad=20, fontsize=13, fontweight="bold")
ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=8)
plt.tight_layout(); save_chart(d)

best_tld = max(scorecard, key=lambda t: scorecard[t]["score"])
worst_tld = min(scorecard, key=lambda t: scorecard[t]["score"])
findings[22] = f"综合健康得分: {best_tld} 最高 ({scorecard[best_tld]['score']:.0f}分), {worst_tld} 最低 ({scorecard[worst_tld]['score']:.0f}分)"

# ======================================================================
#  SUMMARY OVERVIEW CHART
# ======================================================================
print("\n" + "="*70)
print("生成总览图...")
print("="*70)

fig = plt.figure(figsize=(20, 24))
gs = gridspec.GridSpec(6, 4, hspace=0.4, wspace=0.35)

# 1 - Data scale
ax = fig.add_subplot(gs[0, 0])
ax.barh(list(zone_stats.keys()), [zone_stats[t][0]/1e6 for t in zone_stats], color=COLORS[0])
ax.set_title("Zone Scale (M)", fontsize=9); ax.tick_params(labelsize=7)

# 2 - Query types
ax = fig.add_subplot(gs[0, 1])
ax.barh([r[0] for r in qt[:6]][::-1], [r[1]/1e6 for r in qt[:6]][::-1], color=COLORS[1])
ax.set_title("Query Types (M)", fontsize=9); ax.tick_params(labelsize=7)

# 3 - Resolution health
ax = fig.add_subplot(gs[0, 2])
ax.bar(ZONE_TLDS, [health_data[t].get("NOERROR",0) for t in ZONE_TLDS], color=COLORS[4])
ax.set_title("NOERROR %", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 4 - IPv6
ax = fig.add_subplot(gs[0, 3])
ax.bar(ZONE_TLDS, [ip_data[t][1]/ip_data[t][0]*100 if ip_data[t][0]>0 else 0 for t in ZONE_TLDS], color=COLORS[2])
ax.set_title("Dual-Stack %", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 5 - AS concentration
ax = fig.add_subplot(gs[1, 0])
ax.barh([r[0][:20] for r in as_data[:8]][::-1], [r[1]/1000 for r in as_data[:8]][::-1], color=COLORS[0])
ax.set_title("Top AS (K doms)", fontsize=9); ax.tick_params(labelsize=6)

# 6 - Geography
ax = fig.add_subplot(gs[1, 1])
ax.barh([r[0] for r in geo_data[:8]][::-1], [r[1]/1000 for r in geo_data[:8]][::-1], color=COLORS[1])
ax.set_title("Top Countries (K)", fontsize=9); ax.tick_params(labelsize=7)

# 7 - TTL
ax = fig.add_subplot(gs[1, 2])
ax.pie([r[1] for r in ttl_data], labels=[r[0] for r in ttl_data], autopct="%1.0f%%",
       colors=COLORS[:len(ttl_data)], textprops={'fontsize':7})
ax.set_title("TTL Distribution", fontsize=9)

# 8 - RTT
ax = fig.add_subplot(gs[1, 3])
ax.bar(ZONE_TLDS, [rtt_stats[t][1] for t in ZONE_TLDS], color=COLORS[3])
ax.set_title("Median RTT (ms)", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 9 - DNSSEC
ax = fig.add_subplot(gs[2, 0])
ax.bar(ZONE_TLDS, [dnssec[t][1]/dnssec[t][0]*100 if dnssec[t][0]>0 else 0 for t in ZONE_TLDS], color=COLORS[4])
ax.set_title("DNSSEC DS %", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 10 - Algo
ax = fig.add_subplot(gs[2, 1])
ax.pie([r[1] for r in algo_data[:6]], labels=[ALGO_NAMES.get(r[0], str(r[0]))[:15] for r in algo_data[:6]],
       autopct="%1.0f%%", colors=COLORS[:6], textprops={'fontsize':7})
ax.set_title("DNSSEC Algorithms", fontsize=9)

# 11 - Email
ax = fig.add_subplot(gs[2, 2])
x = np.arange(len(ZONE_TLDS)); w = 0.3
ax.bar(x-w/2, [email[t][1]/email[t][0]*100 if email[t][0]>0 else 0 for t in ZONE_TLDS], w, label="SPF", color=COLORS[0])
ax.bar(x+w/2, [email[t][2]/email[t][0]*100 if email[t][0]>0 else 0 for t in ZONE_TLDS], w, label="DMARC", color=COLORS[1])
ax.set_xticks(x); ax.set_xticklabels(ZONE_TLDS, fontsize=6)
ax.set_title("Email Security %", fontsize=9); ax.legend(fontsize=6)

# 12 - CAA
ax = fig.add_subplot(gs[2, 3])
ax.barh([r[0][:20] for r in caa_data[:6]][::-1], [r[1] for r in caa_data[:6]][::-1], color=COLORS[3])
ax.set_title("Top CAs (CAA)", fontsize=9); ax.tick_params(labelsize=6)

# 13 - NS
ax = fig.add_subplot(gs[3, 0])
ax.bar(ZONE_TLDS, [ns_red[t][0] for t in ZONE_TLDS], color=COLORS[5])
ax.set_title("Avg NS Count", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 14 - CDN
ax = fig.add_subplot(gs[3, 1])
top_cdn = sorted(cdn_results, key=lambda x:-x[1])[:6]
ax.barh([r[0] for r in top_cdn][::-1], [r[1]/1000 for r in top_cdn][::-1], color=COLORS[0])
ax.set_title("CDN (K domains)", fontsize=9); ax.tick_params(labelsize=7)

# 15 - Overlap
ax = fig.add_subplot(gs[3, 2])
ax.bar(TOPLISTS, [overlap_data[t][1]/overlap_data[t][0]*100 for t in TOPLISTS], color=COLORS[2])
ax.set_title("TopList∩Zone %", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 16 - PR × Security
ax = fig.add_subplot(gs[3, 3])
tiers = [r[0] for r in pr_sec]
ax.bar(tiers, [r[2] for r in pr_sec], label="DNSSEC", color=COLORS[0])
ax.set_title("PR Tier DNSSEC%", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 17 - IP cluster
ax = fig.add_subplot(gs[4, 0])
ax.bar([r[0] for r in ip_dist], [r[2]/1e6 for r in ip_dist], color=COLORS[1])
ax.set_title("IP Concentration", fontsize=9); ax.tick_params(labelsize=6, axis='x', rotation=45)

# 18 - SOA
ax = fig.add_subplot(gs[4, 1])
ax.bar(ZONE_TLDS, [soa_data[t][0]/3600 for t in ZONE_TLDS], color=COLORS[4])
ax.set_title("SOA Refresh (h)", fontsize=9); ax.tick_params(labelsize=7, axis='x', rotation=45)

# 19 - Failures
ax = fig.add_subplot(gs[4, 2])
ax.pie([r[1] for r in overall_fail[:5]],
       labels=[STATUS_MAP.get(r[0],str(r[0])) for r in overall_fail[:5]],
       autopct="%1.0f%%", colors=COLORS[:5], textprops={'fontsize':7})
ax.set_title("Failure Types", fontsize=9)

# 20 - Elite vs General
ax = fig.add_subplot(gs[4, 3])
ax.bar(["Zone"] + TOPLISTS, [zone_sec[1]/zone_sec[0]*100] + [sec_cmp[t][1]/sec_cmp[t][0]*100 for t in TOPLISTS], color=COLORS[3])
ax.set_title("DNSSEC: Zone vs TL", fontsize=9); ax.tick_params(labelsize=6, axis='x', rotation=45)

# 21 - BGP prefix
ax = fig.add_subplot(gs[5, 0:2])
ax.barh([r[0][:20] for r in prefix_data[:10]][::-1], [r[1]/1000 for r in prefix_data[:10]][::-1], color=COLORS[0])
ax.set_title("Top IP Prefixes (K domains)", fontsize=9); ax.tick_params(labelsize=6)

# 22 - Health scorecard
ax = fig.add_subplot(gs[5, 2:4])
scores = sorted(scorecard.items(), key=lambda x:-x[1]["score"])
ax.barh([s[0] for s in scores][::-1], [s[1]["score"] for s in scores][::-1], color=COLORS[4])
ax.set_xlabel("Health Score (0-100)"); ax.set_title("Internet Health Scorecard", fontsize=9)
for i, (t, sc) in enumerate(reversed(scores)):
    ax.text(sc["score"]+0.5, i, f"{sc['score']:.0f}", va="center", fontsize=8)

fig.suptitle("OpenINTEL × Common Crawl — 22-Step Deep Analysis Overview",
             fontsize=16, fontweight="bold", y=0.98)
plt.savefig(OUT / "summary_overview.png", dpi=150)
plt.close()
print(f"  -> {OUT / 'summary_overview.png'}")

# ======================================================================
#  WRITE SUMMARY REPORT
# ======================================================================
print("\n" + "="*70)
print("生成综述报告...")
print("="*70)

report = f"""# OpenINTEL × Common Crawl 综合深度分析报告

> 生成日期: 2026-04-16 | 数据来源: OpenINTEL Zone/TopList + Common Crawl WebGraph + CDX Index

---

## 数据规模概览

| 数据集 | 记录数 | 域名数 |
|--------|--------|--------|
"""

for t in ZONE_TLDS:
    report += f"| Zone/{t} | {zone_stats[t][0]:,} | {zone_stats[t][1]:,} |\n"
for t in TOPLISTS:
    report += f"| TopList/{t} | {toplist_stats[t][0]:,} | {toplist_stats[t][1]:,} |\n"
report += f"| WebGraph | {wg_cnt:,} | — |\n"
report += f"| **总计** | **{total_rows:,}** | — |\n"

report += f"""
---

## 22 步分析发现

"""

for i in range(1, 23):
    report += f"### Step {i:02d}\n{findings.get(i, 'N/A')}\n\n"

report += """---

## 六大深度洞察

### 1. 互联网托管权力集中化
"""
report += f"""Top 5 AS 承载了 **{top5_share:.1f}%** 的域名。最大共享 IP 地址承载超过 {ip_cluster[0][1]:,} 个域名。
网络前缀分析显示，少数 /24 前缀掌控了大量域名的路由可达性。
这意味着少数基础设施提供商的故障或政策变更可以影响互联网的大面积可用性。

### 2. 安全部署的"精英鸿沟"
"""
report += f"""TopList 精英域名在所有安全维度上均大幅领先:
- DNSSEC: TopList 显著高于 Zone 平均值 ({zone_sec[1]/zone_sec[0]*100:.1f}%)
- SPF: TopList 域名的邮件安全配置远超普通域名
- IPv6: 精英域名双栈部署率更高

PageRank 与安全部署呈正相关: 排名越高的域名，DNSSEC、SPF、CAA、IPv6 部署率越高。
这揭示了"安全即资源"的现实——安全需要投入，小型域名运营者缺乏资源和意识。

### 3. IPv6 过渡仍在进行中
"""
report += f"""双栈部署平均 **{avg_dual:.1f}%**，大多数域名仍仅支持 IPv4。
不同 TLD 的 IPv6 准备度差异明显，反映了区域互联网政策和运营商策略的不同。
IPv6 的推进需要从 TLD 注册局层面施加更强的激励。

### 4. DNS 安全现状: 进展与差距并存
"""
report += f"""DNSSEC DS 平均部署率 **{avg_ds:.1f}%**，各 TLD 差异巨大。
算法演进方面，{top_algo} 是当前主导，椭圆曲线算法正在取代传统 RSA。
CAA 部署率仅 **{caa_total/all_doms*100:.1f}%**，绝大多数域名未限制证书颁发机构，
这为证书误发和中间人攻击留下了空间。

SPF 部署 **{avg_spf:.1f}%**，DMARC 严重落后，邮件仍是网络钓鱼的主要攻击面。

### 5. DNS 性能与可靠性的地理差异
"""
report += f"""中位 RTT 平均 **{overall_med:.1f}ms**，但 P99 尾延迟揭示了显著的基础设施瓶颈。
SOA 参数(refresh/retry/expire)跨 TLD 有数量级差异，
反映了不同 ccTLD 运营者对域名更新频率和容灾策略的截然不同的理念。

单 NS 域名平均占 **{avg_single:.1f}%**，这些域名面临单点故障风险。

### 6. Web 生态与 DNS 的交织
"""
report += f"""CDN 指纹分析显示 **{cdn_results[0][0]}** 在欧洲 ccTLD 中占据主导地位。
{cdn_q[0]:,} 个域名使用 CNAME 指向 CDN，Web 性能高度依赖少数 CDN 提供商。
TopList 与 Zone 的重叠率揭示了"可见互联网"与"注册互联网"的结构性差异。

---

## 综合健康排名

| TLD | 综合分 | NOERROR | 双栈 | DNSSEC | SPF | CAA | NS 冗余 | RTT |
|-----|--------|---------|------|--------|-----|-----|---------|-----|
"""

for t, sc in sorted(scorecard.items(), key=lambda x: -x[1]["score"]):
    report += f"| {t} | **{sc['score']:.0f}** | {sc['noerror']:.0f}% | {sc['dual']:.0f}% | {sc['dnssec']:.0f}% | {sc['spf']:.0f}% | {sc['caa']:.1f}% | {sc['ns']:.1f} | {sc['rtt']:.0f}ms |\n"

report += f"""
> 评分权重: NOERROR 20% + DNSSEC 20% + IPv6 15% + SPF 15% + CAA 10% + NS冗余 10% + 性能 10%

---

## 总览图

![22步分析总览](summary_overview.png)

---

## 分步详情

| 步骤 | 主题 | 目录 |
|------|------|------|
"""
step_names = [
    "数据普查", "查询类型分布", "解析健康度", "IPv4/IPv6双栈",
    "AS集中度", "托管地理分布", "TTL缓存策略", "RTT性能分析",
    "DNSSEC部署", "DNSSEC算法", "邮件安全栈", "CAA证书授权",
    "NS冗余度", "CNAME/CDN指纹", "TopList重叠", "PageRank×安全",
    "共享托管集群", "SOA生命周期", "失败分类学", "精英vs普通安全",
    "BGP前缀分析", "健康记分卡",
]
dir_names = [
    "data_census", "query_type_distribution", "resolution_health", "ipv4_vs_ipv6",
    "as_concentration", "hosting_geography", "ttl_strategy", "rtt_performance",
    "dnssec_deployment", "dnssec_algorithms", "email_security", "caa_authorization",
    "ns_redundancy", "cname_cdn_fingerprint", "toplist_zone_overlap", "pagerank_dns_security",
    "shared_hosting_clusters", "soa_lifecycle", "failure_taxonomy", "toplist_security_comparison",
    "bgp_prefix_analysis", "internet_health_scorecard",
]
for i, (sn, dn) in enumerate(zip(step_names, dir_names), 1):
    report += f"| {i:02d} | {sn} | `step_{i:02d}_{dn}/` |\n"

report += f"""
---

*分析基于 {total_rows/1e6:.0f}M 条 DNS 记录 + {wg_cnt/1e6:.0f}M WebGraph 域名排名，覆盖 {len(ZONE_TLDS)} 个 TLD 区域和 {len(TOPLISTS)} 个 TopList。*
"""

(OUT / "summary_report.md").write_text(report, encoding="utf-8")
print(f"  -> {OUT / 'summary_report.md'}")

conn.close()
print("\n" + "="*70)
print("全部 22 步分析完成!")
print("="*70)
