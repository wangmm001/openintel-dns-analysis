#!/usr/bin/env python3
"""13 — RIR rDNS standalone analysis (Phase 1, revised).

Each step writes:
  analysis/rir_enrichment/step_XX_<name>/{result.txt, chart.png, .ok}

Steps:
  01 — RIR delegation distribution (pie + table)
  02 — Prefix size distribution by RIR (grouped bar)
  03 — rDNS record-type distribution + coverage (heatmap)
  04 — Hoster-pattern clustering via rname regex (bar)
  05 — Country extraction via rname TLD suffix (bar)
"""
import sys
import os
import re

sys.path.insert(0, os.path.dirname(__file__))

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

from config import BASE_DIR, get_conn
from rir_rdns import load_rir_prefix, coverage_stats
from _checkpoint import done, mark

OUT = BASE_DIR / "rir_enrichment"
OUT.mkdir(exist_ok=True)

COLORS = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1"]

conn = get_conn()
conn.execute("SET memory_limit='6GB'")


def step_dir(n, name):
    d = OUT / f"step_{n:02d}_{name}"
    d.mkdir(exist_ok=True)
    return d


def save_result(d, text):
    (d / "result.txt").write_text(text)
    print(text[:800])


def save_chart(d):
    p = d / "chart.png"
    plt.savefig(p, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  -> {p}")


# ── Load RIR data ────────────────────────────────────────────────────────────
print("Loading RIR prefix table …")
n = load_rir_prefix(conn)
stats = coverage_stats(conn)
print(f"  {n:,} prefix rows; {stats}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 01: RIR delegation distribution
# ─────────────────────────────────────────────────────────────────────────────
d01 = step_dir(1, "rir_delegation")
if not done(d01):
    print("\n[Step 01] RIR delegation distribution …")

    df = conn.execute("""
        SELECT rir_source, count(*) AS prefix_count
        FROM rir_prefix
        GROUP BY rir_source
        ORDER BY prefix_count DESC
    """).fetchdf()

    total = df["prefix_count"].sum()
    df["pct"] = (df["prefix_count"] / total * 100).round(2)

    lines = [
        "Step 01 — RIR Delegation Distribution",
        "=" * 50,
        f"Total IPv4 prefix records: {total:,}",
        "",
        f"{'RIR':<12} {'Prefixes':>10} {'Share %':>8}",
        "-" * 32,
    ]
    for _, row in df.iterrows():
        lines.append(f"{row['rir_source']:<12} {row['prefix_count']:>10,} {row['pct']:>8.2f}%")
    lines.append("")
    lines.append(f"Largest RIR: {df.iloc[0]['rir_source']} ({df.iloc[0]['pct']:.1f}%)")
    text = "\n".join(lines)
    save_result(d01, text)

    # Pie chart
    fig, (ax_pie, ax_bar) = plt.subplots(1, 2, figsize=(14, 7))
    fig.suptitle("RIR Delegation Distribution of IPv4 rDNS Prefixes", fontsize=14, fontweight="bold")

    wedge_colors = COLORS[: len(df)]
    wedges, texts, autotexts = ax_pie.pie(
        df["prefix_count"],
        labels=df["rir_source"],
        autopct="%1.1f%%",
        colors=wedge_colors,
        startangle=140,
        pctdistance=0.8,
    )
    for at in autotexts:
        at.set_fontsize(9)
    ax_pie.set_title("Share by prefix count")

    bars = ax_bar.barh(df["rir_source"][::-1], df["prefix_count"][::-1], color=wedge_colors[::-1])
    ax_bar.set_xlabel("Number of IPv4 prefixes")
    ax_bar.set_title("Absolute prefix counts")
    for bar, val in zip(bars, df["prefix_count"][::-1]):
        ax_bar.text(
            bar.get_width() + total * 0.002, bar.get_y() + bar.get_height() / 2,
            f"{val:,}", va="center", fontsize=9,
        )
    ax_bar.set_xlim(0, df["prefix_count"].max() * 1.15)
    plt.tight_layout()
    save_chart(d01)
    mark(d01)
else:
    print("[skip] Step 01")


# ─────────────────────────────────────────────────────────────────────────────
# Step 02: Prefix size distribution by RIR
# ─────────────────────────────────────────────────────────────────────────────
d02 = step_dir(2, "prefix_size_distribution")
if not done(d02):
    print("\n[Step 02] Prefix size distribution by RIR …")

    # Extract CIDR prefix length directly from the prefix column (e.g. "1.2.3.0/24" -> 24)
    # Block size = 2^(32 - cidr_len)
    df_sizes = conn.execute("""
        SELECT
            rir_source,
            TRY_CAST(split_part(prefix, '/', 2) AS INTEGER) AS cidr_len
        FROM rir_prefix
        WHERE prefix LIKE '%/%'
    """).fetchdf()

    df_sizes["block_size"] = df_sizes["cidr_len"].apply(
        lambda c: 2 ** (32 - int(c)) if c is not None and 0 <= int(c) <= 32 else 1
    )

    # Bucket into CIDR size ranges
    def bucket(sz):
        if sz >= 16777216:   return "/8 (16M+)"
        elif sz >= 1048576:  return "/12 (1M+)"
        elif sz >= 65536:    return "/16 (65K+)"
        elif sz >= 4096:     return "/20 (4K+)"
        elif sz >= 256:      return "/24 (256+)"
        else:                return "< /24 (tiny)"

    df_sizes["bucket"] = df_sizes["block_size"].apply(bucket)

    pivot = df_sizes.groupby(["rir_source", "bucket"]).size().unstack(fill_value=0)

    bucket_order = ["/8 (16M+)", "/12 (1M+)", "/16 (65K+)", "/20 (4K+)", "/24 (256+)", "< /24 (tiny)"]
    # Keep only buckets that exist
    bucket_order = [b for b in bucket_order if b in pivot.columns]
    pivot = pivot[bucket_order]

    lines = [
        "Step 02 — Prefix Size Distribution by RIR",
        "=" * 60,
        "",
        "Prefix count per CIDR bucket per RIR:",
        "",
        pivot.to_string(),
        "",
        "Key observations:",
    ]
    for rir in pivot.index:
        dominant = pivot.loc[rir].idxmax()
        lines.append(f"  {rir}: dominant bucket = {dominant} ({pivot.loc[rir, dominant]:,} prefixes)")
    text = "\n".join(lines)
    save_result(d02, text)

    # Grouped bar chart
    x = np.arange(len(pivot.index))
    n_buckets = len(bucket_order)
    width = 0.8 / n_buckets
    bucket_colors = plt.cm.viridis(np.linspace(0.1, 0.9, n_buckets))

    fig, ax = plt.subplots(figsize=(14, 7))
    for i, bucket_name in enumerate(bucket_order):
        offset = (i - n_buckets / 2 + 0.5) * width
        vals = pivot[bucket_name].values
        bars = ax.bar(x + offset, vals, width=width * 0.9, label=bucket_name, color=bucket_colors[i])

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, fontsize=11)
    ax.set_xlabel("RIR", fontsize=12)
    ax.set_ylabel("Number of prefixes", fontsize=12)
    ax.set_title("IPv4 Prefix Size Distribution by RIR", fontsize=14, fontweight="bold")
    ax.legend(title="CIDR size bucket", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    plt.tight_layout()
    save_chart(d02)
    mark(d02)
else:
    print("[skip] Step 02")


# ─────────────────────────────────────────────────────────────────────────────
# Step 03: rDNS record-type distribution + coverage heatmap
# ─────────────────────────────────────────────────────────────────────────────
d03 = step_dir(3, "rdns_rtype_heatmap")
if not done(d03):
    print("\n[Step 03] rDNS record-type distribution + coverage …")

    df_rtype = conn.execute("""
        SELECT
            rir_source,
            COALESCE(rtype, '__NULL__') AS rtype,
            count(*) AS cnt
        FROM rir_prefix
        GROUP BY rir_source, rtype
        ORDER BY rir_source, cnt DESC
    """).fetchdf()

    pivot3 = df_rtype.pivot_table(
        index="rir_source", columns="rtype", values="cnt", fill_value=0
    )

    # Sort columns by total count descending
    pivot3 = pivot3[pivot3.sum().sort_values(ascending=False).index]

    lines = [
        "Step 03 — rDNS Record-Type Distribution + Coverage",
        "=" * 60,
        "",
        "Raw counts (rir_source × rtype):",
        "",
        pivot3.to_string(),
        "",
        "Coverage (% of prefixes per RIR that have each rtype):",
        "",
    ]
    row_totals = pivot3.sum(axis=1)
    pct_df = pivot3.div(row_totals, axis=0) * 100
    lines.append(pct_df.round(2).to_string())

    # Top rtype per RIR
    lines.append("\nDominant rtype per RIR:")
    for rir in pivot3.index:
        dom = pivot3.loc[rir].idxmax()
        lines.append(f"  {rir}: {dom} ({pivot3.loc[rir, dom]:,} records, {pct_df.loc[rir, dom]:.1f}%)")
    text = "\n".join(lines)
    save_result(d03, text)

    # Log-scaled heatmap
    log_pivot = np.log10(pivot3.values.astype(float) + 1)

    fig, ax = plt.subplots(figsize=(max(10, len(pivot3.columns) * 1.2), 6))
    im = ax.imshow(log_pivot, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(pivot3.columns)))
    ax.set_xticklabels(pivot3.columns, rotation=40, ha="right", fontsize=9)
    ax.set_yticks(range(len(pivot3.index)))
    ax.set_yticklabels(pivot3.index, fontsize=11)
    ax.set_title("rDNS Record-Type Distribution by RIR (log₁₀ count)", fontsize=13, fontweight="bold")
    cbar = plt.colorbar(im, ax=ax, fraction=0.03, pad=0.04)
    cbar.set_label("log₁₀(count + 1)", fontsize=10)

    # Annotate cells
    for i in range(len(pivot3.index)):
        for j in range(len(pivot3.columns)):
            val = pivot3.values[i, j]
            if val > 0:
                ax.text(j, i, f"{val:,}", ha="center", va="center",
                        fontsize=7, color="black" if log_pivot[i, j] < 3 else "white")

    plt.tight_layout()
    save_chart(d03)
    mark(d03)
else:
    print("[skip] Step 03")


# ─────────────────────────────────────────────────────────────────────────────
# Step 04: Hoster-pattern clustering via rname regex
# ─────────────────────────────────────────────────────────────────────────────
d04 = step_dir(4, "hoster_patterns")
if not done(d04):
    print("\n[Step 04] Hoster-pattern clustering via rname …")

    # Use rdata (NS server hostnames) not rname (which is always in-addr.arpa zones)
    # rdata examples: "dns15.dion.ne.jp.", "ns3.timeweb.org.", "a5-65.akam.net."
    df_rnames = conn.execute("""
        SELECT
            COALESCE(rdata, '__no_rdns__') AS rdata,
            count(*) AS cnt
        FROM rir_prefix
        WHERE rtype = 'NS'
        GROUP BY rdata
    """).fetchdf()

    def extract_apex(name: str) -> str:
        """
        Extract hoster apex domain from an NS rdata value.

        Examples:
          'dns15.dion.ne.jp.'         -> 'dion.ne.jp'
          'ns3.timeweb.org.'          -> 'timeweb.org'
          'ns2.wlfdle.rnc.net.cable.rogers.com.'  -> 'rogers.com'
          'a5-65.akam.net.'           -> 'akam.net'
          '__no_rdns__'               -> '__no_rdns__'

        Algorithm:
          1. Strip trailing dot, lowercase.
          2. If it's an in-addr.arpa name, return 'in-addr.arpa'.
          3. Split by '.'.
          4. Skip numeric-only labels.
          5. Take last 2 non-numeric labels (SLD + TLD) -> apex.
        """
        if not name or name == "__no_rdns__":
            return "__no_rdns__"
        name = name.rstrip(".").lower()
        if "in-addr.arpa" in name:
            return "in-addr.arpa"
        if "ip6.arpa" in name:
            return "ip6.arpa"
        labels = name.split(".")
        # Remove numeric-only labels
        non_numeric = [lbl for lbl in labels if not lbl.isdigit()]
        if len(non_numeric) == 0:
            return "__numeric__"
        if len(non_numeric) == 1:
            return non_numeric[0]
        # Take last 2 labels (SLD + TLD)
        return ".".join(non_numeric[-2:])

    df_rnames["apex"] = df_rnames["rdata"].apply(extract_apex)

    apex_counts = (
        df_rnames.groupby("apex")["cnt"].sum()
        .sort_values(ascending=False)
    )

    top20 = apex_counts.head(20)

    lines = [
        "Step 04 — Hoster-Pattern Clustering via NS rdata Apex",
        "=" * 60,
        "",
        f"Total distinct NS rdata values: {df_rnames['rdata'].nunique():,}",
        f"Total distinct apex domains: {len(apex_counts):,}",
        "",
        "Top 20 apex hosters (by prefix count):",
        "",
        f"{'Rank':<6} {'Apex domain':<35} {'Prefixes':>10} {'Share %':>8}",
        "-" * 62,
    ]
    grand_total = apex_counts.sum()
    for rank, (apex, cnt) in enumerate(top20.items(), 1):
        pct = cnt / grand_total * 100
        lines.append(f"{rank:<6} {apex:<35} {cnt:>10,} {pct:>8.2f}%")

    top5 = apex_counts.iloc[:5]
    top5_pct = top5.sum() / grand_total * 100
    lines.append(f"\nTop-5 hosters control {top5_pct:.1f}% of all prefixes")
    text = "\n".join(lines)
    save_result(d04, text)

    # Bar chart
    fig, ax = plt.subplots(figsize=(13, 8))
    y_pos = range(len(top20) - 1, -1, -1)
    bars = ax.barh(
        list(y_pos),
        top20.values,
        color=COLORS[0],
        edgecolor="white",
        linewidth=0.5,
    )
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(top20.index.tolist(), fontsize=10)
    ax.set_xlabel("Number of IPv4 prefixes", fontsize=12)
    ax.set_title("Top 20 rDNS Hoster Patterns by Apex Domain\n(extracted from rname field)", fontsize=13, fontweight="bold")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))

    for bar, val in zip(bars, top20.values):
        ax.text(
            bar.get_width() + grand_total * 0.001,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}", va="center", fontsize=8,
        )
    ax.set_xlim(0, top20.values.max() * 1.15)
    plt.tight_layout()
    save_chart(d04)
    mark(d04)
else:
    print("[skip] Step 04")


# ─────────────────────────────────────────────────────────────────────────────
# Step 05: Country extraction via rname TLD suffix
# ─────────────────────────────────────────────────────────────────────────────
d05 = step_dir(5, "country_tld_extraction")
if not done(d05):
    print("\n[Step 05] Country extraction via rname TLD suffix …")

    # Common ccTLD list (ISO 3166-1 alpha-2) for reference
    CCTLDS = {
        "ac","ad","ae","af","ag","ai","al","am","ao","aq","ar","as","at","au","aw",
        "ax","az","ba","bb","bd","be","bf","bg","bh","bi","bj","bm","bn","bo","bq",
        "br","bs","bt","bw","by","bz","ca","cc","cd","cf","cg","ch","ci","ck","cl",
        "cm","cn","co","cr","cu","cv","cw","cx","cy","cz","de","dj","dk","dm","do",
        "dz","ec","ee","eg","er","es","et","eu","fi","fj","fk","fm","fo","fr","ga",
        "gd","ge","gf","gg","gh","gi","gl","gm","gn","gp","gq","gr","gt","gu","gw",
        "gy","hk","hm","hn","hr","ht","hu","id","ie","il","im","in","io","iq","ir",
        "is","it","je","jm","jo","jp","ke","kg","kh","ki","km","kn","kp","kr","kw",
        "ky","kz","la","lb","lc","li","lk","lr","ls","lt","lu","lv","ly","ma","mc",
        "md","me","mg","mh","mk","ml","mm","mn","mo","mp","mq","mr","ms","mt","mu",
        "mv","mw","mx","my","mz","na","nc","ne","nf","ng","ni","nl","no","np","nr",
        "nu","nz","om","pa","pe","pf","pg","ph","pk","pl","pm","pn","pr","ps","pt",
        "pw","py","qa","re","ro","rs","ru","rw","sa","sb","sc","sd","se","sg","sh",
        "si","sk","sl","sm","sn","so","sr","ss","st","sv","sx","sy","sz","tc","td",
        "tf","tg","th","tj","tk","tl","tm","tn","to","tr","tt","tv","tw","tz","ua",
        "ug","uk","um","us","uy","uz","va","vc","ve","vg","vi","vn","vu","wf","ws",
        "ye","yt","za","zm","zw",
    }

    # Use rdata (NS nameserver hostnames) — rname is always in-addr.arpa, not useful for ccTLD
    # rdata examples: "dns15.dion.ne.jp.", "ns3.timeweb.org.", "ns2.rogers.com."
    df_rn = conn.execute("""
        SELECT
            rdata,
            count(*) AS cnt
        FROM rir_prefix
        WHERE rdata IS NOT NULL AND rtype = 'NS'
        GROUP BY rdata
    """).fetchdf()

    def extract_tld_country(name: str) -> str:
        """
        Extract the TLD from an NS rdata hostname and check if it's a ccTLD.
        Strips trailing dot, splits by '.', gets last non-empty label.
        Arpa names return '__arpa__', non-ccTLD get '__other__(<tld>)'.
        """
        if not name:
            return "__no_rdata__"
        name = name.rstrip(".").lower()
        labels = [l for l in name.split(".") if l]
        if not labels:
            return "__empty__"
        tld = labels[-1]
        if tld == "arpa":
            return "__arpa__"
        if tld in CCTLDS:
            return tld
        return f"__other__({tld})"

    df_rn["country_tld"] = df_rn["rdata"].apply(extract_tld_country)

    country_counts = (
        df_rn.groupby("country_tld")["cnt"].sum()
        .sort_values(ascending=False)
    )

    # Filter to proper ccTLDs only for chart (exclude __arpa__, __other__, etc.)
    cc_counts = country_counts[
        ~country_counts.index.str.startswith("__")
    ].head(20)

    special_counts = country_counts[
        country_counts.index.str.startswith("__")
    ]

    grand_total = country_counts.sum()

    lines = [
        "Step 05 — Country Code Extraction from NS rdata TLD Suffix",
        "=" * 60,
        "",
        f"Total NS rdata records analysed: {df_rn['cnt'].sum():,}",
        "",
        "Special categories:",
    ]
    for cat, cnt in special_counts.items():
        lines.append(f"  {cat:<25} {cnt:>10,}  ({cnt/grand_total*100:.1f}%)")

    lines.append("")
    lines.append("Top 20 ccTLD suffixes in rname field:")
    lines.append("")
    lines.append(f"{'Rank':<6} {'ccTLD':<12} {'Prefixes':>10} {'Share %':>8}")
    lines.append("-" * 40)
    for rank, (cc, cnt) in enumerate(cc_counts.items(), 1):
        pct = cnt / grand_total * 100
        lines.append(f"{rank:<6} {cc:<12} {cnt:>10,} {pct:>8.2f}%")

    if len(cc_counts) > 0:
        lines.append(f"\nTop ccTLD: {cc_counts.index[0]} ({cc_counts.iloc[0]:,} prefixes, {cc_counts.iloc[0]/grand_total*100:.1f}%)")
    text = "\n".join(lines)
    save_result(d05, text)

    # Bar chart — top 20 ccTLDs
    fig, ax = plt.subplots(figsize=(13, 8))
    if len(cc_counts) == 0:
        ax.text(0.5, 0.5, "No ccTLD suffixes found in NS rdata",
                ha="center", va="center", fontsize=14, transform=ax.transAxes)
    else:
        y_pos = range(len(cc_counts) - 1, -1, -1)
        n_bars = max(len(cc_counts), 1)
        bar_colors = plt.cm.tab20(np.linspace(0, 1, n_bars))
        bars = ax.barh(
            list(y_pos),
            cc_counts.values,
            color=bar_colors,
            edgecolor="white",
            linewidth=0.5,
        )
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels([cc.upper() for cc in cc_counts.index.tolist()], fontsize=10)
        ax.set_xlabel("Number of NS records", fontsize=12)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))

        max_val = cc_counts.values.max()
        for bar, val in zip(bars, cc_counts.values):
            ax.text(
                bar.get_width() + max_val * 0.008,
                bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8,
            )
        ax.set_xlim(0, max_val * 1.15)

    ax.set_title(
        "Top 20 Country Codes in NS rdata TLD Suffix\n"
        "(reveals which countries' nameserver infrastructure serves RIR prefixes)",
        fontsize=12, fontweight="bold",
    )
    plt.tight_layout()
    save_chart(d05)
    mark(d05)
else:
    print("[skip] Step 05")


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("Summary")
print("=" * 70)
step_outputs = list(OUT.glob("step_*/result.txt"))
print(f"Wrote {len(step_outputs)} step outputs to {OUT}")
for p in sorted(step_outputs):
    ok = (p.parent / ".ok").exists()
    chart = (p.parent / "chart.png").exists()
    print(f"  {'[ok]' if ok else '[!!]'} {'[chart]' if chart else '[no-chart]'} {p.parent.name}")
