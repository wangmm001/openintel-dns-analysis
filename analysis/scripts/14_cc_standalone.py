#!/usr/bin/env python3
"""14 — Common Crawl standalone analysis (Phase 1 expansion, no OpenINTEL dep).

Uses CC webgraph files in downloads/common-crawl/. Each step writes:
  analysis/cc_standalone/step_XX_<slug>/{result.txt, chart.png, .ok}

Steps:
  01  Domain-level PageRank distribution (log-log rank vs PR + histogram)
  02  Top-TLD concentration in domain top-N (stacked bar, 3 buckets)
  03  Domain name-length distribution (bar chart)
  04  Host-level PageRank distribution (overlay vs domain)
  05  cluster.idx domain frequency (top-20 bar chart)
"""
import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(__file__))

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from config import BASE_DIR, REPO_DIR, get_conn
from _checkpoint import done, mark

# ── Paths ──────────────────────────────────────────────────────────────────────
OUT = BASE_DIR / "cc_standalone"
OUT.mkdir(exist_ok=True)

CC = REPO_DIR / "downloads" / "common-crawl"
WG_SLUG = "cc-main-2025-26-dec-jan-feb"
DOMAIN_RANKS = CC / "webgraph" / "domain" / f"{WG_SLUG}-domain-ranks.txt.gz"
DOMAIN_VERTICES = CC / "webgraph" / "domain" / f"{WG_SLUG}-domain-vertices.txt.gz"
HOST_RANKS = CC / "webgraph" / "host" / f"{WG_SLUG}-host-ranks.txt.gz"
CLUSTER_IDX = CC / "cluster.idx"

COLORS = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
          "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
          "#86bcb6", "#499894", "#f1ce63", "#79706e", "#d37295"]

conn = get_conn()
conn.execute("SET memory_limit='8GB'")

# ── Helpers ────────────────────────────────────────────────────────────────────
def step_dir(n: int, name: str):
    d = OUT / f"step_{n:02d}_{name}"
    d.mkdir(exist_ok=True)
    return d

def save_result(d, text: str):
    (d / "result.txt").write_text(text)
    print(text[:600])

def save_chart(d):
    p = d / "chart.png"
    plt.savefig(p, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  -> {p}")

def read_ranks_sql(path, has_host_col=True):
    """Return SQL fragment for reading a CC ranks .txt.gz file.

    domain-ranks has 6 cols: harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev, host
      (note: 'host' is actually a vertex-id number, not hostname; real name is host_rev reversed)
    host-ranks has 5 cols: harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev
      (no 'host' col; harmonicc_val uses scientific notation like 3.7549092E7)
    """
    if has_host_col:
        cols_sql = (
            "{'harmonicc_pos': 'BIGINT', 'harmonicc_val': 'DOUBLE',"
            " 'pr_pos': 'BIGINT', 'pr_val': 'DOUBLE',"
            " 'host_rev': 'VARCHAR', 'host': 'VARCHAR'}"
        )
    else:
        cols_sql = (
            "{'harmonicc_pos': 'BIGINT', 'harmonicc_val': 'DOUBLE',"
            " 'pr_pos': 'BIGINT', 'pr_val': 'DOUBLE',"
            " 'host_rev': 'VARCHAR'}"
        )
    return (
        "SELECT harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev"
        + (" , host" if has_host_col else "")
        + f" FROM read_csv('{path}',"
        + f" delim='\\t', header=false, skip=1,"
        + f" columns={cols_sql},"
        + " compression='gzip', ignore_errors=true)"
    )

def host_rev_to_fwd(col: str) -> str:
    """SQL expression to convert 'com.example.www' → 'www.example.com'."""
    # Use DuckDB string_split + list_reverse + array_to_string
    return f"array_to_string(list_reverse(string_split({col}, '.')), '.')"

# ── Pre-flight ─────────────────────────────────────────────────────────────────
for f in [DOMAIN_RANKS, DOMAIN_VERTICES, HOST_RANKS, CLUSTER_IDX]:
    if not f.exists():
        raise SystemExit(f"Missing: {f}")

print(f"CC standalone analysis | slug={WG_SLUG}")
print(f"  domain-ranks  : {DOMAIN_RANKS.stat().st_size/1e9:.2f} GB")
print(f"  host-ranks    : {HOST_RANKS.stat().st_size/1e9:.2f} GB")
print(f"  domain-vertices: {DOMAIN_VERTICES.stat().st_size/1e9:.2f} GB")
print(f"  cluster.idx   : {CLUSTER_IDX.stat().st_size/1e6:.1f} MB")
print()

# ══════════════════════════════════════════════════════════════════════════════
# Step 01 — Domain-level PageRank distribution
# ══════════════════════════════════════════════════════════════════════════════
S01 = step_dir(1, "domain_pr_distribution")
if done(S01):
    print("Step 01 already done, skipping.")
else:
    t0 = time.time()
    print("Step 01: Domain-level PageRank distribution …")
    try:
        # Summary stats
        stats = conn.execute(f"""
            SELECT
                count(*)                                AS n,
                min(pr_val)                             AS pr_min,
                max(pr_val)                             AS pr_max,
                median(pr_val)                          AS pr_median,
                approx_quantile(pr_val, 0.99)           AS pr_p99,
                approx_quantile(pr_val, 0.999)          AS pr_p999
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
        """).fetchdf()

        n_total = int(stats["n"].iloc[0])
        pr_min = stats["pr_min"].iloc[0]
        pr_max = stats["pr_max"].iloc[0]
        pr_median = stats["pr_median"].iloc[0]
        pr_p99 = stats["pr_p99"].iloc[0]
        pr_p999 = stats["pr_p999"].iloc[0]

        # Top-10 by PR  (host_rev → fwd hostname via SQL)
        top10 = conn.execute(f"""
            SELECT {host_rev_to_fwd('host_rev')} AS hostname, pr_val, pr_pos
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            ORDER BY pr_pos ASC
            LIMIT 10
        """).fetchdf()

        # Bottom-10 by PR
        bot10 = conn.execute(f"""
            SELECT {host_rev_to_fwd('host_rev')} AS hostname, pr_val, pr_pos
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            ORDER BY pr_pos DESC
            LIMIT 10
        """).fetchdf()

        # Sample 10M rows for rank-vs-PR log-log plot (use every N-th row via pr_pos)
        # Sample ~0.5% evenly across the rank range
        sample_step = max(1, n_total // 500_000)
        sample_df = conn.execute(f"""
            SELECT pr_pos, pr_val, harmonicc_pos, harmonicc_val
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE pr_pos % {sample_step} = 0
            ORDER BY pr_pos
        """).fetchdf()

        # ── Chart: log-log rank vs PR scatter + histogram inset ──
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Left: log-log rank vs PR value
        ax = axes[0]
        ax.scatter(sample_df["pr_pos"], sample_df["pr_val"],
                   s=0.3, alpha=0.4, c=COLORS[0], linewidths=0)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("PageRank Rank (log scale)")
        ax.set_ylabel("PageRank Value (log scale)")
        ax.set_title(f"Domain PageRank: Rank vs Value\n(n={n_total:,}, sampled 1/{sample_step})")
        ax.grid(True, which="both", alpha=0.3)

        # Right: histogram of log10(pr_val) using all sample
        ax2 = axes[1]
        log_vals = np.log10(sample_df["pr_val"].dropna())
        ax2.hist(log_vals, bins=80, color=COLORS[0], edgecolor="none", alpha=0.8)
        ax2.axvline(np.log10(pr_median), color="red", linestyle="--", label=f"median={pr_median:.2e}")
        ax2.axvline(np.log10(pr_p99), color="orange", linestyle="--", label=f"p99={pr_p99:.2e}")
        ax2.set_xlabel("log10(pr_val)")
        ax2.set_ylabel("Count (sampled)")
        ax2.set_title("PageRank Value Distribution (log10)")
        ax2.legend(fontsize=9)
        ax2.grid(True, alpha=0.3)

        plt.suptitle("Common Crawl Domain-Level PageRank", fontsize=13, fontweight="bold")
        plt.tight_layout()
        save_chart(S01)

        # ── Percentile table ──
        pct_rows = []
        for q in [0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999]:
            v = np.quantile(sample_df["pr_val"].dropna(), q)
            pct_rows.append(f"  p{int(q*1000):>4d}‰ = {v:.4e}")

        txt = (
            f"Step 01 — Domain PageRank Distribution\n"
            f"{'='*50}\n"
            f"Total domains : {n_total:,}\n"
            f"PR min        : {pr_min:.4e}\n"
            f"PR max        : {pr_max:.4e}\n"
            f"PR median     : {pr_median:.4e}\n"
            f"PR p99        : {pr_p99:.4e}\n"
            f"PR p99.9      : {pr_p999:.4e}\n"
            f"\nPercentiles (sampled):\n" + "\n".join(pct_rows) +
            f"\n\nTop-10 domains by PageRank:\n"
            + top10.to_string(index=False) +
            f"\n\nBottom-10 domains by PageRank (lowest):\n"
            + bot10.to_string(index=False) +
            f"\n\nRuntime: {time.time()-t0:.1f}s\n"
        )
        save_result(S01, txt)
        mark(S01)
        print(f"  Step 01 done in {time.time()-t0:.1f}s")

    except Exception as e:
        print(f"  Step 01 FAILED: {e}")
        traceback.print_exc()
        save_result(S01, f"DONE_WITH_CONCERNS\nStep 01 failed: {e}\n{traceback.format_exc()}")

# ══════════════════════════════════════════════════════════════════════════════
# Step 02 — Top-TLD concentration in domain top-N
# ══════════════════════════════════════════════════════════════════════════════
S02 = step_dir(2, "top_tld_concentration")
if done(S02):
    print("Step 02 already done, skipping.")
else:
    t0 = time.time()
    print("Step 02: Top-TLD concentration in domain top-N …")
    try:
        # For each bucket, extract TLD = first label of host_rev (e.g. "com" from "com.example")
        # top-1k, top-10k, top-100k by PR rank (pr_pos)
        results = {}
        for bucket_name, limit in [("top_1k", 1000), ("top_10k", 10000), ("top_100k", 100000)]:
            df = conn.execute(f"""
                SELECT
                    LOWER(split_part(host_rev, '.', 1)) AS tld,
                    count(*) AS cnt
                FROM ({read_ranks_sql(DOMAIN_RANKS)})
                WHERE pr_pos <= {limit} AND host_rev IS NOT NULL
                GROUP BY tld
                ORDER BY cnt DESC
                LIMIT 20
            """).fetchdf()
            results[bucket_name] = df
            print(f"  {bucket_name}: {len(df)} TLDs")

        # Build unified TLD list (top-15 across all buckets)
        all_tlds_set = set()
        for df in results.values():
            all_tlds_set.update(df["tld"].tolist()[:15])
        # rank by top_1k presence first
        tld_rank = {t: i for i, t in enumerate(results["top_1k"]["tld"].tolist())}
        for t in results["top_10k"]["tld"].tolist():
            if t not in tld_rank:
                tld_rank[t] = len(tld_rank)
        for t in results["top_100k"]["tld"].tolist():
            if t not in tld_rank:
                tld_rank[t] = len(tld_rank)
        top_tlds = sorted(all_tlds_set, key=lambda t: tld_rank.get(t, 999))[:15]
        other_label = "other"

        # For stacked bar: get counts per TLD per bucket
        bucket_labels = ["top_1k", "top_10k", "top_100k"]
        bucket_limits = {"top_1k": 1000, "top_10k": 10000, "top_100k": 100000}

        # For each bucket, assign counts for our top-15 TLDs + "other"
        bar_data = {}  # tld -> [count_in_1k, count_in_10k, count_in_100k]
        for tld in top_tlds + [other_label]:
            bar_data[tld] = []
        for bk in bucket_labels:
            df = results[bk]
            tld_count = dict(zip(df["tld"], df["cnt"]))
            limit = bucket_limits[bk]
            other = limit
            for tld in top_tlds:
                v = tld_count.get(tld, 0)
                bar_data[tld].append(v)
                other -= v
            bar_data[other_label].append(max(0, other))

        # Normalize to percentage
        totals = [bucket_limits[bk] for bk in bucket_labels]
        bar_pct = {tld: [v / tot * 100 for v, tot in zip(vals, totals)]
                   for tld, vals in bar_data.items()}

        # Plot stacked horizontal bar
        fig, ax = plt.subplots(figsize=(12, 7))
        x = np.arange(len(bucket_labels))
        bottoms = np.zeros(len(bucket_labels))
        tld_colors = COLORS[:len(top_tlds)] + ["#aaaaaa"]
        for tld, color in zip(top_tlds + [other_label], tld_colors):
            vals = np.array(bar_pct[tld])
            ax.bar(x, vals, bottom=bottoms, label=tld, color=color, edgecolor="white", linewidth=0.5)
            # label inside bar if big enough
            for i, (v, b) in enumerate(zip(vals, bottoms)):
                if v > 2:
                    ax.text(i, b + v/2, f"{v:.1f}%", ha="center", va="center",
                            fontsize=8, color="white", fontweight="bold")
            bottoms += vals

        ax.set_xticks(x)
        ax.set_xticklabels(["Top 1,000", "Top 10,000", "Top 100,000"])
        ax.set_ylabel("Share of domains (%)")
        ax.set_title("TLD Concentration in Common Crawl PageRank Top-N Domains", fontweight="bold")
        ax.legend(loc="upper right", fontsize=8, ncol=2)
        ax.set_ylim(0, 105)
        ax.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        save_chart(S02)

        # Result text
        lines = [f"Step 02 — Top-TLD Concentration\n{'='*50}\n"]
        for bk in bucket_labels:
            df = results[bk]
            lines.append(f"\n{bk.replace('_',' ').upper()} (by PR rank):")
            lines.append(df.head(15).to_string(index=False))
        lines.append(f"\nRuntime: {time.time()-t0:.1f}s\n")
        save_result(S02, "\n".join(lines))
        mark(S02)
        print(f"  Step 02 done in {time.time()-t0:.1f}s")

    except Exception as e:
        print(f"  Step 02 FAILED: {e}")
        traceback.print_exc()
        save_result(S02, f"DONE_WITH_CONCERNS\nStep 02 failed: {e}\n{traceback.format_exc()}")

# ══════════════════════════════════════════════════════════════════════════════
# Step 03 — Domain name-length distribution
# ══════════════════════════════════════════════════════════════════════════════
S03 = step_dir(3, "domain_name_length")
if done(S03):
    print("Step 03 already done, skipping.")
else:
    t0 = time.time()
    print("Step 03: Domain name-length distribution …")
    try:
        # host_rev is "com.example", forward hostname is "example.com"
        # length is same for reversed or forward; use host_rev directly
        # Buckets: 1-5, 6-10, 11-15, 16-20, 21-30, 31+
        len_df = conn.execute(f"""
            SELECT
                len_bucket,
                count(*) AS cnt
            FROM (
                SELECT
                    CASE
                        WHEN length(host_rev) BETWEEN 1 AND 5   THEN '01-05'
                        WHEN length(host_rev) BETWEEN 6 AND 10  THEN '06-10'
                        WHEN length(host_rev) BETWEEN 11 AND 15 THEN '11-15'
                        WHEN length(host_rev) BETWEEN 16 AND 20 THEN '16-20'
                        WHEN length(host_rev) BETWEEN 21 AND 30 THEN '21-30'
                        ELSE '31+'
                    END AS len_bucket
                FROM ({read_ranks_sql(DOMAIN_RANKS)})
                WHERE host_rev IS NOT NULL
            )
            GROUP BY len_bucket
            ORDER BY len_bucket
        """).fetchdf()

        # Also get exact-length stats
        exact_stats = conn.execute(f"""
            SELECT
                length(host_rev) AS hlen,
                count(*) AS cnt
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE host_rev IS NOT NULL
            GROUP BY hlen
            ORDER BY hlen
        """).fetchdf()

        # Median / mean / max
        agg = conn.execute(f"""
            SELECT
                avg(length(host_rev))    AS avg_len,
                median(length(host_rev)) AS med_len,
                max(length(host_rev))    AS max_len,
                min(length(host_rev))    AS min_len
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE host_rev IS NOT NULL
        """).fetchdf()

        avg_len = agg["avg_len"].iloc[0]
        med_len = agg["med_len"].iloc[0]
        max_len = agg["max_len"].iloc[0]
        min_len = agg["min_len"].iloc[0]

        # Top-10 shortest and longest hostnames (convert host_rev → fwd)
        shortest = conn.execute(f"""
            SELECT {host_rev_to_fwd('host_rev')} AS hostname, length(host_rev) AS hlen
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE host_rev IS NOT NULL
            ORDER BY hlen ASC LIMIT 10
        """).fetchdf()
        longest = conn.execute(f"""
            SELECT {host_rev_to_fwd('host_rev')} AS hostname, length(host_rev) AS hlen
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE host_rev IS NOT NULL
            ORDER BY hlen DESC LIMIT 10
        """).fetchdf()

        # ── Chart ──
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Left: buckets bar chart
        ax = axes[0]
        labels = len_df["len_bucket"].tolist()
        counts = len_df["cnt"].tolist()
        bars = ax.bar(labels, counts, color=COLORS[:len(labels)], edgecolor="white")
        ax.set_xlabel("Hostname Length (characters)")
        ax.set_ylabel("Number of Domains")
        ax.set_title("Domain Hostname Length Distribution\n(bucketed)")
        for bar, cnt in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.01,
                    f"{cnt/1e6:.1f}M", ha="center", va="bottom", fontsize=9)
        ax.grid(True, axis="y", alpha=0.3)

        # Right: exact-length histogram (trim to ≤80 chars for readability)
        ax2 = axes[1]
        trim_df = exact_stats[exact_stats["hlen"] <= 100]
        ax2.bar(trim_df["hlen"], trim_df["cnt"], width=1, color=COLORS[0], alpha=0.8, edgecolor="none")
        ax2.axvline(med_len, color="red", linestyle="--", label=f"median={med_len:.0f}")
        ax2.axvline(avg_len, color="orange", linestyle="--", label=f"mean={avg_len:.1f}")
        ax2.set_xlabel("Hostname Length (characters)")
        ax2.set_ylabel("Count")
        ax2.set_title("Exact Hostname Length (≤80 chars)")
        ax2.legend(fontsize=9)
        ax2.grid(True, alpha=0.3)

        plt.suptitle("Common Crawl Domain Name-Length Distribution", fontsize=13, fontweight="bold")
        plt.tight_layout()
        save_chart(S03)

        txt = (
            f"Step 03 — Domain Name-Length Distribution\n"
            f"{'='*50}\n"
            f"min_len : {min_len}\n"
            f"max_len : {max_len}\n"
            f"avg_len : {avg_len:.2f}\n"
            f"med_len : {med_len:.0f}\n"
            f"\nBucket counts:\n" + len_df.to_string(index=False) +
            f"\n\nShortest hostnames:\n" + shortest.to_string(index=False) +
            f"\n\nLongest hostnames:\n" + longest.to_string(index=False) +
            f"\n\nRuntime: {time.time()-t0:.1f}s\n"
        )
        save_result(S03, txt)
        mark(S03)
        print(f"  Step 03 done in {time.time()-t0:.1f}s")

    except Exception as e:
        print(f"  Step 03 FAILED: {e}")
        traceback.print_exc()
        save_result(S03, f"DONE_WITH_CONCERNS\nStep 03 failed: {e}\n{traceback.format_exc()}")

# ══════════════════════════════════════════════════════════════════════════════
# Step 04 — Host-level PageRank distribution (overlay with domain)
# ══════════════════════════════════════════════════════════════════════════════
S04 = step_dir(4, "host_pr_distribution")
if done(S04):
    print("Step 04 already done, skipping.")
else:
    t0 = time.time()
    print("Step 04: Host-level PageRank distribution (5.6 GB, may take a while) …")
    try:
        # Use SAMPLE to keep memory reasonable for the 5.6 GB file
        # ~10M rows sample (host file has ~hundreds of millions)
        HOST_SAMPLE_FRAC = 5  # sample ~5% using modulo on pr_pos

        host_stats = conn.execute(f"""
            SELECT
                count(*)                                AS n,
                min(pr_val)                             AS pr_min,
                max(pr_val)                             AS pr_max,
                median(pr_val)                          AS pr_median,
                approx_quantile(pr_val, 0.99)           AS pr_p99,
                approx_quantile(pr_val, 0.999)          AS pr_p999
            FROM ({read_ranks_sql(HOST_RANKS, has_host_col=False)})
        """).fetchdf()

        n_host = int(host_stats["n"].iloc[0])
        host_pr_min = host_stats["pr_min"].iloc[0]
        host_pr_max = host_stats["pr_max"].iloc[0]
        host_pr_median = host_stats["pr_median"].iloc[0]
        host_pr_p99 = host_stats["pr_p99"].iloc[0]

        print(f"  host total: {n_host:,}")

        # Sample for plotting
        host_sample_step = max(1, n_host // 500_000)
        host_sample = conn.execute(f"""
            SELECT pr_pos, pr_val
            FROM ({read_ranks_sql(HOST_RANKS, has_host_col=False)})
            WHERE pr_pos % {host_sample_step} = 0
            ORDER BY pr_pos
        """).fetchdf()

        # Load domain sample for overlay (reuse step_01 data if available, else re-query)
        dom_step = max(1, 134_000_000 // 300_000)
        dom_sample = conn.execute(f"""
            SELECT pr_pos, pr_val
            FROM ({read_ranks_sql(DOMAIN_RANKS)})
            WHERE pr_pos % {dom_step} = 0
            ORDER BY pr_pos
        """).fetchdf()

        # Top-10 hosts by PR
        top10_host = conn.execute(f"""
            SELECT {host_rev_to_fwd('host_rev')} AS hostname, pr_val, pr_pos
            FROM ({read_ranks_sql(HOST_RANKS, has_host_col=False)})
            ORDER BY pr_pos ASC
            LIMIT 10
        """).fetchdf()

        # ── Chart: 2-panel ──
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Left: rank vs PR for both (log-log)
        ax = axes[0]
        ax.scatter(dom_sample["pr_pos"], dom_sample["pr_val"],
                   s=0.2, alpha=0.3, c=COLORS[0], linewidths=0, label=f"domain (n≈134M)")
        ax.scatter(host_sample["pr_pos"], host_sample["pr_val"],
                   s=0.2, alpha=0.3, c=COLORS[1], linewidths=0, label=f"host (n≈{n_host/1e6:.0f}M)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("PageRank Rank (log scale)")
        ax.set_ylabel("PageRank Value (log scale)")
        ax.set_title("Rank vs PR Value: Domain vs Host\n(sampled)")
        ax.legend(fontsize=9, markerscale=10)
        ax.grid(True, which="both", alpha=0.3)

        # Right: overlay histogram of log10(pr_val)
        ax2 = axes[1]
        dom_log = np.log10(dom_sample["pr_val"].dropna())
        host_log = np.log10(host_sample["pr_val"].dropna())
        bins = np.linspace(min(dom_log.min(), host_log.min()),
                           max(dom_log.max(), host_log.max()), 80)
        ax2.hist(dom_log, bins=bins, alpha=0.6, color=COLORS[0], label="domain",
                 density=True, edgecolor="none")
        ax2.hist(host_log, bins=bins, alpha=0.6, color=COLORS[1], label="host",
                 density=True, edgecolor="none")
        ax2.axvline(np.log10(host_pr_median), color="orange", linestyle="--",
                    label=f"host median={host_pr_median:.2e}")
        ax2.set_xlabel("log10(pr_val)")
        ax2.set_ylabel("Density (normalized)")
        ax2.set_title("PR Distribution Overlay: Domain vs Host")
        ax2.legend(fontsize=9)
        ax2.grid(True, alpha=0.3)

        plt.suptitle("Common Crawl Host-Level PageRank (vs Domain-Level)", fontsize=13, fontweight="bold")
        plt.tight_layout()
        save_chart(S04)

        # Domain vs host tail comparison
        dom_p99 = float(np.quantile(dom_sample["pr_val"].dropna(), 0.99))
        host_p99_sample = float(np.quantile(host_sample["pr_val"].dropna(), 0.99))

        txt = (
            f"Step 04 — Host-Level PageRank Distribution\n"
            f"{'='*50}\n"
            f"Total hosts   : {n_host:,}\n"
            f"PR min        : {host_pr_min:.4e}\n"
            f"PR max        : {host_pr_max:.4e}\n"
            f"PR median     : {host_pr_median:.4e}\n"
            f"PR p99        : {host_pr_p99:.4e}\n"
            f"\nDomain vs Host tail ratio (p99):\n"
            f"  domain p99  : {dom_p99:.4e}\n"
            f"  host p99    : {host_p99_sample:.4e}\n"
            f"  ratio       : {host_p99_sample/dom_p99:.2f}x\n"
            f"\nTop-10 hosts by PageRank:\n" + top10_host.to_string(index=False) +
            f"\n\nRuntime: {time.time()-t0:.1f}s\n"
        )
        save_result(S04, txt)
        mark(S04)
        print(f"  Step 04 done in {time.time()-t0:.1f}s")

    except Exception as e:
        print(f"  Step 04 FAILED: {e}")
        traceback.print_exc()
        save_result(S04, f"DONE_WITH_CONCERNS\nStep 04 failed: {e}\n{traceback.format_exc()}")

# ══════════════════════════════════════════════════════════════════════════════
# Step 05 — cluster.idx domain frequency
# ══════════════════════════════════════════════════════════════════════════════
S05 = step_dir(5, "cluster_idx_domain_freq")
if done(S05):
    print("Step 05 already done, skipping.")
else:
    t0 = time.time()
    print("Step 05: cluster.idx domain frequency …")
    try:
        # cluster.idx format: SURT_URL\ttimestamp\tcdx_file\toffset\tlength\tseq
        # SURT URL example: "com,example)/foo" → domain portion is before ")"
        # Reverse com,example → example.com

        # Read cluster.idx with DuckDB (plain text, tab-separated, no header)
        # Format: "SURT_URL TIMESTAMP\tcdx_file\toffset\tlength\tseq"  (5 tab-separated cols)
        # Col 0 = "com,example)/path YYYYMMDDHHMMSS"  → extract domain portion before ")"
        cluster_df = conn.execute(f"""
            SELECT
                surt_domain,
                count(*) AS entry_count
            FROM (
                SELECT
                    -- SURT domain: text before ')' in the first field (which also contains timestamp after space)
                    regexp_extract(col0, '^([^)]+)\\)', 1) AS surt_domain
                FROM read_csv('{CLUSTER_IDX}',
                    delim='\\t',
                    header=false,
                    columns={{
                        'col0': 'VARCHAR',
                        'col1': 'VARCHAR',
                        'col2': 'VARCHAR',
                        'col3': 'VARCHAR',
                        'col4': 'VARCHAR'
                    }},
                    ignore_errors=true)
                WHERE col0 IS NOT NULL AND col0 LIKE '%)%'
            )
            WHERE surt_domain IS NOT NULL AND surt_domain != ''
            GROUP BY surt_domain
            ORDER BY entry_count DESC
            LIMIT 50
        """).fetchdf()

        # Reverse SURT domain: "com,example" → "example.com"
        def reverse_surt(surt: str) -> str:
            if not surt:
                return surt
            # Remove port if present (e.g., "com,example:8080")
            parts = surt.split(":")
            domain_part = parts[0]
            labels = domain_part.split(",")
            labels.reverse()
            return ".".join(labels)

        cluster_df["domain"] = cluster_df["surt_domain"].apply(reverse_surt)

        # Total entries
        total_entries = conn.execute(f"""
            SELECT count(*) FROM read_csv('{CLUSTER_IDX}',
                delim='\\t',
                header=false,
                columns={{
                    'col0': 'VARCHAR',
                    'col1': 'VARCHAR',
                    'col2': 'VARCHAR',
                    'col3': 'VARCHAR',
                    'col4': 'VARCHAR'
                }},
                ignore_errors=true)
        """).fetchone()[0]

        top20 = cluster_df.head(20)

        # ── Chart ──
        fig, ax = plt.subplots(figsize=(12, 7))
        y = np.arange(len(top20))
        bars = ax.barh(y, top20["entry_count"], color=COLORS[0], edgecolor="white")
        ax.set_yticks(y)
        ax.set_yticklabels(top20["domain"].tolist(), fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Number of cluster.idx Entries")
        ax.set_title(
            f"Top-20 Domains by cluster.idx Coverage\n"
            f"(total entries: {total_entries:,})",
            fontweight="bold"
        )
        for bar, cnt in zip(bars, top20["entry_count"]):
            ax.text(bar.get_width() * 1.005, bar.get_y() + bar.get_height()/2,
                    f"{cnt:,}", va="center", fontsize=8)
        ax.grid(True, axis="x", alpha=0.3)
        plt.tight_layout()
        save_chart(S05)

        txt = (
            f"Step 05 — cluster.idx Domain Frequency\n"
            f"{'='*50}\n"
            f"Total cluster.idx entries : {total_entries:,}\n"
            f"\nTop-20 domains by entry count:\n"
            + top20[["domain", "entry_count"]].to_string(index=False) +
            f"\n\nRuntime: {time.time()-t0:.1f}s\n"
        )
        save_result(S05, txt)
        mark(S05)
        print(f"  Step 05 done in {time.time()-t0:.1f}s")

    except Exception as e:
        print(f"  Step 05 FAILED: {e}")
        traceback.print_exc()
        save_result(S05, f"DONE_WITH_CONCERNS\nStep 05 failed: {e}\n{traceback.format_exc()}")

# ── Summary ────────────────────────────────────────────────────────────────────
print("\nAll steps complete.")
steps_done = []
steps_concern = []
for n, name in [(1, "domain_pr_distribution"), (2, "top_tld_concentration"),
                (3, "domain_name_length"), (4, "host_pr_distribution"),
                (5, "cluster_idx_domain_freq")]:
    d = OUT / f"step_{n:02d}_{name}"
    ok = (d / ".ok").exists()
    concern = not ok and (d / "result.txt").exists()
    if ok:
        steps_done.append(n)
    elif concern:
        steps_concern.append(n)

print(f"  Completed OK : {steps_done}")
print(f"  With concerns: {steps_concern}")
print(f"  Output dir   : {OUT}")
