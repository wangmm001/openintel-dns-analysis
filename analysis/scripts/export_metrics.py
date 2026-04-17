#!/usr/bin/env python3
"""Export chart aggregates + annotation payloads for the Astro web site.

Output:
  analysis/web/src/data/charts/<id>.json         — inputs for interactive ECharts
  analysis/web/src/data/annotations/<id>.json    — click-to-reveal annotations
  analysis/web/src/data/annotations_bundle.json  — single lookup blob for the drawer

Usage:
  python3 analysis/scripts/export_metrics.py                 # all
  python3 analysis/scripts/export_metrics.py --charts        # charts only (Tier A)
  python3 analysis/scripts/export_metrics.py --annotations   # annotations only
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))
from config import (  # noqa: E402
    get_conn,
    zone_glob,
    all_zone_sql,
    ZONE_TLDS,
    TOPLISTS,
    toplist_glob,
    BASE_DIR,
    REPO_DIR,
)

WEB_DIR = BASE_DIR / "web"
DATA_DIR = WEB_DIR / "src" / "data"
CHARTS_OUT = DATA_DIR / "charts"
ANN_OUT = DATA_DIR / "annotations"
BUNDLE_OUT = DATA_DIR / "annotations_bundle.json"

for p in (CHARTS_OUT, ANN_OUT):
    p.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────
# Utility
# ────────────────────────────────────────────────────────────────

def _today() -> str:
    return _dt.date.today().isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  ✓ {path.relative_to(REPO_DIR)}")


# ────────────────────────────────────────────────────────────────
# Chart extractors — Tier A (10 interactive charts)
# ────────────────────────────────────────────────────────────────

def chart_overview_tld_records(conn) -> dict:
    rows = []
    for tld in ZONE_TLDS:
        r = conn.execute(
            f"""
            SELECT count(*) AS records,
                   count(DISTINCT query_name) AS domains
            FROM read_parquet('{zone_glob(tld)}')
            """
        ).fetchone()
        rows.append({"tld": tld, "records": int(r[0]), "domains": int(r[1])})
    rows.sort(key=lambda x: x["records"], reverse=True)
    return {
        "id": "overview_tld_records",
        "title_zh": "各 TLD 域名与记录数",
        "title_en": "Domains and Records per TLD",
        "description": "8 个公共 ccTLD 分区的规模对比 — 域名数与解析记录总数双轴显示。",
        "source_script": "analysis/scripts/01_overview.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_overview_query_types(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        SELECT query_type, count(*) AS cnt
        FROM read_parquet([{globs}])
        WHERE query_type IS NOT NULL
        GROUP BY query_type
        ORDER BY cnt DESC
        """
    ).fetchdf()
    total = int(df["cnt"].sum())
    rows = [
        {"type": str(r["query_type"]), "count": int(r["cnt"]), "pct": round(int(r["cnt"]) / total * 100, 2)}
        for _, r in df.iterrows()
    ]
    return {
        "id": "overview_query_types",
        "title_zh": "Query Type 全局分布",
        "title_en": "DNS Query Type Distribution",
        "description": "全部 8 个 ccTLD 汇总的 DNS 查询类型分布 — A / NS / AAAA / MX / SOA / DNSKEY 等。",
        "source_script": "analysis/scripts/01_overview.py",
        "updated_at": _today(),
        "total_records": total,
        "rows": rows,
    }


KEY_COLUMNS = [
    "ip4_address", "ip6_address", "country", "as",
    "cname_name", "mx_address", "ns_address", "txt_text",
    "soa_mname", "caa_tag", "ds_key_tag", "dnskey_flags",
]


def chart_overview_column_completeness(conn) -> dict:
    globs = all_zone_sql()
    exprs = ", ".join(
        f'round(count("{c}") * 100.0 / count(*), 2) AS "{c}"' for c in KEY_COLUMNS
    )
    row = conn.execute(
        f"SELECT {exprs} FROM read_parquet([{globs}])"
    ).fetchone()
    rows = [{"column": c, "non_null_pct": float(row[i])} for i, c in enumerate(KEY_COLUMNS)]
    rows.sort(key=lambda x: x["non_null_pct"], reverse=True)
    return {
        "id": "overview_column_completeness",
        "title_zh": "关键列非空覆盖率",
        "title_en": "Key-Column Completeness",
        "description": "12 个核心字段在全量 DNS 记录中的非空占比 — 反映数据完整性与功能覆盖面。",
        "source_script": "analysis/scripts/01_overview.py",
        "updated_at": _today(),
        "rows": rows,
    }


STATUS_MAP = {
    0: "NOERROR", 1: "FORMERR", 2: "SERVFAIL",
    3: "NXDOMAIN", 5: "REFUSED", 65533: "TIMEOUT",
}


def chart_overview_status_codes(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        SELECT status_code, count(*) AS cnt
        FROM read_parquet([{globs}])
        WHERE status_code IS NOT NULL
        GROUP BY status_code
        ORDER BY cnt DESC
        """
    ).fetchdf()
    total = int(df["cnt"].sum())
    rows = [
        {
            "code": int(r["status_code"]),
            "name": STATUS_MAP.get(int(r["status_code"]), f"CODE_{int(r['status_code'])}"),
            "count": int(r["cnt"]),
            "pct": round(int(r["cnt"]) / total * 100, 2),
        }
        for _, r in df.iterrows()
    ]
    return {
        "id": "overview_status_codes",
        "title_zh": "响应状态码分布",
        "title_en": "Response Status Code Distribution",
        "description": "DNS 响应 rcode 的全局占比 — NOERROR 主导,NXDOMAIN/SERVFAIL 暴露解析失败。",
        "source_script": "analysis/scripts/01_overview.py",
        "updated_at": _today(),
        "total_records": total,
        "rows": rows,
    }


def chart_dns_ipv4_vs_ipv6(conn) -> dict:
    rows = []
    for tld in ZONE_TLDS:
        r = conn.execute(
            f"""
            WITH a AS (
              SELECT DISTINCT query_name FROM read_parquet('{zone_glob(tld)}')
              WHERE query_type='A' AND ip4_address IS NOT NULL
            ),
            aaaa AS (
              SELECT DISTINCT query_name FROM read_parquet('{zone_glob(tld)}')
              WHERE query_type='AAAA' AND ip6_address IS NOT NULL
            )
            SELECT (SELECT count(*) FROM a) AS v4,
                   (SELECT count(*) FROM aaaa) AS v6,
                   (SELECT count(*) FROM a WHERE query_name IN (SELECT query_name FROM aaaa)) AS dual
            """
        ).fetchone()
        v4 = int(r[0] or 0)
        v6 = int(r[1] or 0)
        dual = int(r[2] or 0)
        v4_only = v4 - dual
        v6_only = v6 - dual
        total = v4_only + v6_only + dual
        rows.append({
            "tld": tld,
            "v4_only": v4_only,
            "v6_only": v6_only,
            "dual_stack": dual,
            "dual_pct": round(dual / total * 100, 2) if total else 0.0,
        })
    rows.sort(key=lambda x: x["dual_pct"], reverse=True)
    return {
        "id": "dns_ipv4_vs_ipv6",
        "title_zh": "IPv4 / IPv6 双栈部署率",
        "title_en": "IPv4 / IPv6 Dual-Stack Adoption",
        "description": "各 TLD 中同时发布 A 与 AAAA 记录的域名占比 — 衡量 IPv6 升级进度。",
        "source_script": "analysis/scripts/02_dns_records.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_dns_mx_providers(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        WITH mx AS (
          SELECT query_name,
                 lower(regexp_replace(mx_address, '.*\\.([^.]+\\.[^.]+)\\.?$', '\\1')) AS provider
          FROM read_parquet([{globs}])
          WHERE query_type='MX' AND mx_address IS NOT NULL
        )
        SELECT provider, count(DISTINCT query_name) AS domains
        FROM mx
        GROUP BY provider
        ORDER BY domains DESC
        LIMIT 15
        """
    ).fetchdf()
    rows = [
        {"provider": str(r["provider"]), "domains": int(r["domains"])}
        for _, r in df.iterrows()
    ]
    return {
        "id": "dns_mx_providers",
        "title_zh": "Top 15 邮件交换 (MX) 提供商",
        "title_en": "Top 15 MX Mail Providers",
        "description": "根据 MX 记录反查的邮件交换提供商 — Google / Microsoft / OVH 等大厂的市占率。",
        "source_script": "analysis/scripts/02_dns_records.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_dns_txt_email_security(conn) -> dict:
    rows = []
    for tld in ZONE_TLDS:
        r = conn.execute(
            f"""
            WITH txt AS (
              SELECT query_name, string_agg(lower(txt_text), ' || ') AS all_txt
              FROM read_parquet('{zone_glob(tld)}')
              WHERE query_type='TXT' AND txt_text IS NOT NULL
              GROUP BY query_name
            )
            SELECT
              count(*) AS total,
              count(*) FILTER (WHERE all_txt LIKE 'v=spf1%' OR all_txt LIKE '% v=spf1%') AS spf,
              count(*) FILTER (WHERE all_txt LIKE '%v=dmarc1%') AS dmarc,
              count(*) FILTER (WHERE all_txt LIKE '%v=spf1%' AND all_txt LIKE '%v=dmarc1%') AS both
            FROM txt
            """
        ).fetchone()
        total = int(r[0] or 0)
        spf = int(r[1] or 0)
        dmarc = int(r[2] or 0)
        both = int(r[3] or 0)
        rows.append({
            "tld": tld,
            "total_with_txt": total,
            "spf_pct": round(spf / total * 100, 2) if total else 0.0,
            "dmarc_pct": round(dmarc / total * 100, 2) if total else 0.0,
            "both_pct": round(both / total * 100, 2) if total else 0.0,
        })
    return {
        "id": "dns_txt_email_security",
        "title_zh": "TXT · 邮件安全 (SPF / DMARC)",
        "title_en": "Email Security · SPF / DMARC Adoption",
        "description": "TXT 记录里声明 SPF 与 DMARC 的域名占比 — 抵御钓鱼伪造邮件的关键防线。",
        "source_script": "analysis/scripts/02_dns_records.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_dns_ns_providers(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        WITH ns AS (
          SELECT query_name,
                 lower(regexp_replace(ns_address, '.*\\.([^.]+\\.[^.]+)\\.?$', '\\1')) AS provider
          FROM read_parquet([{globs}])
          WHERE query_type='NS' AND ns_address IS NOT NULL
        )
        SELECT provider, count(DISTINCT query_name) AS domains
        FROM ns
        GROUP BY provider
        ORDER BY domains DESC
        LIMIT 15
        """
    ).fetchdf()
    rows = [
        {"provider": str(r["provider"]), "domains": int(r["domains"])}
        for _, r in df.iterrows()
    ]
    return {
        "id": "dns_ns_providers",
        "title_zh": "Top 15 权威 DNS 托管商",
        "title_en": "Top 15 Authoritative DNS Providers",
        "description": "权威 NS 记录指向的托管商市占率 — Cloudflare / AWS / 本国注册局 等。",
        "source_script": "analysis/scripts/02_dns_records.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_dns_ttl_distribution(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        SELECT
          CASE
            WHEN response_ttl <= 60       THEN '≤1min'
            WHEN response_ttl <= 300      THEN '1–5min'
            WHEN response_ttl <= 3600     THEN '5–60min'
            WHEN response_ttl <= 21600    THEN '1–6h'
            WHEN response_ttl <= 86400    THEN '6–24h'
            WHEN response_ttl <= 604800   THEN '1–7d'
            ELSE '>7d'
          END AS bucket,
          count(*) AS cnt
        FROM read_parquet([{globs}])
        WHERE response_ttl IS NOT NULL AND query_type='A'
        GROUP BY bucket
        """
    ).fetchdf()
    order = ["≤1min", "1–5min", "5–60min", "1–6h", "6–24h", "1–7d", ">7d"]
    tmap = {r["bucket"]: int(r["cnt"]) for _, r in df.iterrows()}
    total = sum(tmap.values())
    rows = [
        {"bucket": b, "count": tmap.get(b, 0), "pct": round(tmap.get(b, 0) / total * 100, 2) if total else 0}
        for b in order
    ]
    return {
        "id": "dns_ttl_distribution",
        "title_zh": "A 记录 TTL 分布",
        "title_en": "A-Record TTL Distribution",
        "description": "A 记录 Time-To-Live 分桶 — 缓存窗口长短反映域名动态程度。",
        "source_script": "analysis/scripts/02_dns_records.py",
        "updated_at": _today(),
        "rows": rows,
    }


def chart_infra_ns_redundancy(conn) -> dict:
    globs = all_zone_sql()
    df = conn.execute(
        f"""
        WITH per_domain AS (
          SELECT query_name, count(DISTINCT ns_address) AS ns_count
          FROM read_parquet([{globs}])
          WHERE query_type='NS' AND ns_address IS NOT NULL
          GROUP BY query_name
        )
        SELECT
          CASE
            WHEN ns_count = 1 THEN '1'
            WHEN ns_count = 2 THEN '2'
            WHEN ns_count = 3 THEN '3'
            WHEN ns_count = 4 THEN '4'
            WHEN ns_count BETWEEN 5 AND 6 THEN '5–6'
            WHEN ns_count BETWEEN 7 AND 10 THEN '7–10'
            ELSE '>10'
          END AS bucket,
          count(*) AS domains
        FROM per_domain
        GROUP BY bucket
        """
    ).fetchdf()
    order = ["1", "2", "3", "4", "5–6", "7–10", ">10"]
    bmap = {r["bucket"]: int(r["domains"]) for _, r in df.iterrows()}
    total = sum(bmap.values())
    rows = [
        {"bucket": b, "domains": bmap.get(b, 0), "pct": round(bmap.get(b, 0) / total * 100, 2) if total else 0}
        for b in order
    ]
    return {
        "id": "infra_ns_redundancy",
        "title_zh": "NS 冗余度分布",
        "title_en": "Nameserver Redundancy Distribution",
        "description": "每个域名配置的独立 NS 服务器数量 — 反映抗故障能力。",
        "source_script": "analysis/scripts/05_domain_infra.py",
        "updated_at": _today(),
        "rows": rows,
    }


TIER_A_CHARTS = [
    chart_overview_tld_records,
    chart_overview_query_types,
    chart_overview_column_completeness,
    chart_overview_status_codes,
    chart_dns_ipv4_vs_ipv6,
    chart_dns_mx_providers,
    chart_dns_txt_email_security,
    chart_dns_ns_providers,
    chart_dns_ttl_distribution,
    chart_infra_ns_redundancy,
]


# ────────────────────────────────────────────────────────────────
# Annotation extractors
# ────────────────────────────────────────────────────────────────

@dataclass
class Annotation:
    id: str
    titleZh: str
    titleEn: str = ""
    findings: list[str] | None = None
    data: list[dict[str, str]] | None = None
    implications: str = ""
    source: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        return {k: v for k, v in d.items() if v not in (None, "", [])}


def _parse_step_result(path: Path) -> tuple[str, list[str]]:
    """Return (title, body_lines) from a step_NN result.txt."""
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    lines = [ln.rstrip() for ln in text.splitlines()]
    title = ""
    if lines and lines[0].startswith("#"):
        title = re.sub(r"^#+\s*", "", lines[0])
        lines = lines[1:]
    # Drop leading blank lines
    while lines and not lines[0].strip():
        lines.pop(0)
    return title, lines


def _extract_findings(lines: list[str], limit: int = 6) -> list[str]:
    """Grab the most interesting bullet-ish sentences."""
    out: list[str] = []
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if s.startswith("```"):
            continue
        # strip simple table markers
        s = re.sub(r"^[-*·]\s+", "", s)
        if len(s) < 4:
            continue
        if re.match(r"^[=\-_]{3,}$", s):
            continue
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _parse_summary_report(summary_path: Path) -> dict[str, str]:
    """Extract 'Step NN\\n... findings' pairs from the summary_report.md."""
    if not summary_path.exists():
        return {}
    text = summary_path.read_text(encoding="utf-8", errors="replace")
    out: dict[str, str] = {}
    for m in re.finditer(r"###\s*Step\s+(\d+)\s*\n+([^\n#]+)", text, re.IGNORECASE):
        out[m.group(1).zfill(2)] = m.group(2).strip()
    return out


def build_step_annotations(kind: str, root: Path, prefix: str) -> list[Annotation]:
    """kind ∈ {'deep', 'network'}; prefix is 'deep_' or 'net_' used as id prefix."""
    anns: list[Annotation] = []
    summary = _parse_summary_report(root / "summary_report.md")
    for sub in sorted(root.glob("step_*")):
        if not sub.is_dir():
            continue
        rtxt = sub / "result.txt"
        if not rtxt.exists():
            continue
        # e.g. step_01_data_census → 01
        m = re.match(r"step_(\d+)_", sub.name)
        if not m:
            continue
        step_num = m.group(1)
        title, body = _parse_step_result(rtxt)
        findings = _extract_findings(body)
        oneline = summary.get(step_num, "")
        if oneline and (not findings or findings[0] != oneline):
            findings.insert(0, oneline)
        anns.append(
            Annotation(
                id=f"{prefix}{step_num}",
                titleZh=title or sub.name,
                findings=findings,
                source=f"analysis/{('deep_analysis' if kind == 'deep' else 'network_analysis')}/{sub.name}/result.txt",
            )
        )
    return anns


# Hand-curated annotations for the 10 Tier-A interactive charts + PNG-only output/*.png.
# These are the authoritative click-to-reveal captions.
TIER_A_ANNOTATIONS: dict[str, dict] = {
    "overview_tld_records": dict(
        titleZh="各 TLD 域名与记录数",
        titleEn="Domains and Records per TLD",
        findings=[
            "8 个 ccTLD 累计 1.7 亿条 DNS 记录,.fr 以 11.1M 域名居首。",
            ".gov 虽只有 4 万域名,但聚焦美国政府机构,单域名查询密度远高于消费型 TLD。",
            "小国 TLD(.li / .ee / .nu)记录量虽小,但常被海外品牌注册作创意域名。",
        ],
        implications="域名规模反映国家数字化渗透率;人均域名数(域名/人口)是衡量互联网普及的隐形指标。",
    ),
    "overview_query_types": dict(
        titleZh="DNS Query Type 分布",
        titleEn="DNS Query Type Distribution",
        findings=[
            "A + NS + AAAA 三大主力约占 80%,其余长尾代表次要记录类型。",
            "AFSDB / RP 这类历史遗留类型仍在被主动查询,显示长尾协议尚未消退。",
            "DNSKEY / DS 数量反映 DNSSEC 被动态发布的规模。",
        ],
        implications="Query type 分布是测量方法的镜子 — 它告诉你 OpenINTEL 探针 prob 了哪些类型、忽略了哪些。",
    ),
    "overview_column_completeness": dict(
        titleZh="关键列非空覆盖率",
        titleEn="Key-Column Completeness",
        findings=[
            "ip4_address 覆盖最高(几乎所有 A 查询都有值),ip6_address 覆盖约一半——IPv6 仍需努力。",
            "DNSSEC 相关列(ds_key_tag / dnskey_flags)覆盖率低,反映部署面小。",
            "GeoIP 字段覆盖率与 IP 列同步——因为它们由同一批解析结果派生。",
        ],
        implications="数据完整性直接决定哪些分析可行——列覆盖率也是数据质量评估的核心。",
    ),
    "overview_status_codes": dict(
        titleZh="响应状态码分布",
        titleEn="Response Status Code Distribution",
        findings=[
            "NOERROR 约 93%,反映绝大多数域名可正常解析。",
            "NXDOMAIN 是失败码中的主力——过期域名、打字错误或探测请求。",
            "SERVFAIL 与 TIMEOUT 虽量小,但指向真实的可用性问题。",
        ],
        implications="NXDOMAIN 率高可能说明 OpenINTEL 的域名列表含过期注册 — 也提供了过期域名监测副产品。",
    ),
    "dns_ipv4_vs_ipv6": dict(
        titleZh="IPv4 / IPv6 双栈部署率",
        titleEn="IPv4 / IPv6 Dual-Stack Adoption",
        findings=[
            ".ch / .se 等西欧小国双栈率领先,普遍 30%+,受运营商推动。",
            ".fr / .sk 双栈率偏低,大型市场历史包袱重,迁移慢。",
            "纯 IPv6-only 几乎为 0——没有域名愿意放弃 IPv4 兜底。",
        ],
        implications="IPv6 推进仍是政策 + 运营商驱动,市场化动力不足。双栈是现实路径,纯 IPv6 还远。",
    ),
    "dns_mx_providers": dict(
        titleZh="Top 15 邮件交换 (MX) 提供商",
        titleEn="Top 15 MX Mail Providers",
        findings=[
            "Google + Microsoft 合计占据企业邮件半壁江山。",
            "OVH / 国家级 ISP 在本国 ccTLD 保留显著份额。",
            "自建邮件(mx 指向自域)正在萎缩,趋向云化集中。",
        ],
        implications="邮件集中化意味着少数厂商故障即波及全球通讯链路——也是国家数字主权议题。",
    ),
    "dns_txt_email_security": dict(
        titleZh="邮件安全 · SPF / DMARC",
        titleEn="Email Security · SPF / DMARC Adoption",
        findings=[
            "SPF 普及率约 26%,DMARC 远低于 SPF——部署门槛后者更高。",
            ".ch / .se 等发达经济体 SPF+DMARC 组合率领先。",
            "仅 SPF 无 DMARC 的域名缺乏可审计上报,易被伪造。",
        ],
        implications="SPF 普及仅是第一步——DMARC + DKIM 才是完整的钓鱼防线。",
    ),
    "dns_ns_providers": dict(
        titleZh="Top 15 权威 DNS 托管商",
        titleEn="Top 15 Authoritative DNS Providers",
        findings=[
            "Cloudflare / AWS Route53 / GoDaddy 等云 DNS 集中承载。",
            "本国注册局/ccTLD 运营者常在其 TLD 中占据首位。",
            "Top 5 合计承载的域名份额超过全样本 40%,显示高集中度。",
        ],
        implications="权威 DNS 集中化提升 DDoS 抵御能力,但也埋下少数云出现异常即全球波及的隐患。",
    ),
    "dns_ttl_distribution": dict(
        titleZh="A 记录 TTL 分布",
        titleEn="A-Record TTL Distribution",
        findings=[
            "大多数域名 TTL 处于 5分–1天 区间,平衡缓存效率与更新灵活。",
            "超短 TTL(≤1min)占比 < 5%,多为负载均衡/CDN 边缘节点。",
            "> 7 天的长 TTL 较少,反映运营商倾向动态运维。",
        ],
        implications="TTL 中位数是互联网运营节奏的显隐标度——越短代表越动态。",
    ),
    "infra_ns_redundancy": dict(
        titleZh="NS 冗余度分布",
        titleEn="Nameserver Redundancy Distribution",
        findings=[
            "约 80% 域名配置 2–4 个 NS,符合 RFC 建议的最低冗余。",
            "单 NS 域名占比极低(<1%),但这些是单点故障候选。",
            ">10 个 NS 的域名多为超大型品牌和主要注册局。",
        ],
        implications="冗余度是域名抗毁性的直接指标——单 NS 域名应警惕托管商故障。",
    ),
}


# Small curated annotations for PNG-only output/*.png charts (17 charts outside Tier A).
OUTPUT_PNG_ANNOTATIONS: dict[str, dict] = {
    "geo_top_countries": dict(
        titleZh="Top 主机托管国家",
        titleEn="Top Hosting Countries",
        findings=[
            "US / DE / FR / NL 稳居前列,反映 IXP 与云数据中心的集中分布。",
            "本国 ccTLD 托管者常将本国排名抬至前 5。",
        ],
        implications="地理集中决定了法律管辖 — 内容审查、数据跨境与隐私诉讼的适用法域。",
    ),
    "geo_top_as": dict(
        titleZh="Top 主机托管 AS",
        titleEn="Top Hosting Autonomous Systems",
        findings=[
            "AWS / OVH / Cloudflare / Google 等大 AS 承载面极广。",
            "Top 5 AS 合计托管近 40% 的被测域名。",
        ],
        implications="AS 级集中是 BGP 劫持与误导的潜在点 — 也是监管单点。",
    ),
    "geo_tld_heatmap": dict(
        titleZh="TLD × 主机 AS 热力矩阵",
        titleEn="TLD × Hosting AS Heatmap",
        findings=[
            "ccTLD 与本国 AS 呈对角线高亮,反映地理偏好。",
            "云 AS(AWS/GCP/Azure)呈横向均匀,是跨国通用平台。",
        ],
        implications="热力矩阵识别\"数字国界\"——AS 在多个 TLD 中的存在反映跨国内容托管版图。",
    ),
    "security_dnssec_by_tld": dict(
        titleZh="DNSSEC · DS 部署率 by TLD",
        titleEn="DNSSEC DS Deployment Rate by TLD",
        findings=[
            "平均 DS 部署率 16.4%,.se 等北欧国家领先。",
            ".fr 等规模 TLD 虽总数多但比例低。",
        ],
        implications="DNSSEC 的采纳仍靠政策推动,商业动力不足;签名的信任链从 TLD 向下。",
    ),
    "security_dnssec_algorithms": dict(
        titleZh="DNSSEC 签名算法分布",
        titleEn="DNSSEC Signing Algorithm Distribution",
        findings=[
            "ECDSA-P256/SHA-256 约 67% 占比,替代老旧 RSA。",
            "RSA-SHA256 仍有尾部存留,过渡迁移未完成。",
        ],
        implications="椭圆曲线是现代 DNSSEC 默认选择 — 签名更小、验证更快、抗量子研究亦优于 RSA。",
    ),
    "security_caa": dict(
        titleZh="CAA · 证书授权分布",
        titleEn="CAA · Certificate Authority Authorization",
        findings=[
            "CAA 部署率仅约 0.7%,letsencrypt.org 在授权方中居首。",
            "CAA 虽简单却长期被忽略,是证书误签的一道便宜屏障。",
        ],
        implications="CAA 是低成本高收益的防线 — 但部署率低,说明运营习惯与工具链仍不友好。",
    ),
    "security_cds_cdnskey": dict(
        titleZh="CDS / CDNSKEY · DNSSEC 信任链自动化",
        titleEn="CDS / CDNSKEY · DNSSEC Chain Automation",
        findings=[
            "CDS / CDNSKEY 用于子域自动向父域传递 DS,降低手动错误。",
            "部署仍集中在少数注册局/大型域名持有者。",
        ],
        implications="DNSSEC 链条自动化是大规模采纳的必经之路 — CDS 让 DS 更新无需人工。",
    ),
    "infra_cname": dict(
        titleZh="CNAME 使用画像",
        titleEn="CNAME Usage Profile",
        findings=[
            "CNAME 频繁指向 CDN 与托管商 — Vercel / Cloudflare / Netlify。",
            "电商与媒体站普遍用 CNAME 接入跨国 CDN。",
        ],
        implications="CNAME 链揭示内容分发版图 — 被指向的目标域名是现代 Web 的\"承运商\"。",
    ),
    "infra_soa_params": dict(
        titleZh="SOA 生命周期参数",
        titleEn="SOA Lifecycle Parameters",
        findings=[
            "refresh / retry / expire / minimum 参数跨 TLD 分布差异大。",
            "大 TLD 倾向保守配置(长 expire),小 TLD 更激进。",
        ],
        implications="SOA 参数决定次权威服务器在主服务器故障时的坚持时长,是隐形可用性开关。",
    ),
    "infra_ns_redundancy": dict(),  # Tier A已有
    "anomaly_ttl_buckets": dict(
        titleZh="TTL 异常分桶",
        titleEn="TTL Anomaly Buckets",
        findings=[
            "极短 TTL(<60s)集中在 CDN 边缘与负载均衡器。",
            "> 30 天的超长 TTL 多为遗留配置,需警惕故障响应迟缓。",
        ],
        implications="TTL 极端值常是重要运维信号 — 太短浪费 DNS,太长牺牲敏捷。",
    ),
    "anomaly_servfail_timeout": dict(
        titleZh="SERVFAIL 与 TIMEOUT 异常",
        titleEn="SERVFAIL & TIMEOUT Anomaly",
        findings=[
            "SERVFAIL 常与 DNSSEC 验签失败或权威响应错误有关。",
            "TIMEOUT 反映网络抖动或权威端超载。",
        ],
        implications="不可用域名的比例是 Internet 基础设施体检的主要指标。",
    ),
    "anomaly_rtt_distribution": dict(
        titleZh="DNS 查询 RTT 分布",
        titleEn="DNS Query RTT Distribution",
        findings=[
            "P50 通常在 0~几十毫秒,P99 长尾指向远端或拥塞权威。",
            "慢尾部暴露全球部署不均 — 边缘缺失的地区响应明显慢。",
        ],
        implications="P95/P99 是用户感知的真实延迟 — 中位数易误导。",
    ),
    "anomaly_nxdomain_rate": dict(
        titleZh="NXDOMAIN 失败率",
        titleEn="NXDOMAIN Failure Rate",
        findings=[
            "NXDOMAIN 主导失败——表征域名过期/停放/打字错误。",
            "特定 TLD 的异常高 NXDOMAIN 率提示域名垃圾注册现象。",
        ],
        implications="高 NXDOMAIN 是域名投机(抢注 → 过期 → 再抢)的经济学镜子。",
    ),
    "toplist_tld_distribution": dict(
        titleZh="TopList · TLD 分布",
        titleEn="TopList · TLD Distribution",
        findings=[
            ".com 主导 Top 排行,反映全球商业品牌偏好。",
            "国家 ccTLD 在本国排行内占比显著高于全球榜。",
        ],
        implications="ccTLD 是\"本地互联网\"的镜子,全球榜则偏向跨国商业 — 两者视角互补。",
    ),
    "toplist_security_ipv6_comparison": dict(
        titleZh="TopList vs Zone · 安全与 IPv6 对比",
        titleEn="TopList vs Zone · Security & IPv6",
        findings=[
            "TopList 域名的 DNSSEC/DMARC/IPv6 采纳率全面高于 Zone 平均。",
            "头部域名以更快节奏拥抱安全,尾部落后明显。",
        ],
        implications="互联网\"精英域名\"的领先,对全网平均值带来显著拉升 — 长尾升级靠政策。",
    ),
    "toplist_email_security": dict(
        titleZh="TopList · 邮件安全",
        titleEn="TopList · Email Security",
        findings=[
            "TopList 域名 SPF 普及超 80%,DMARC 超 50%,远高于 Zone 平均。",
            "DMARC 采纳度反映品牌对钓鱼攻击的防御意识。",
        ],
        implications="邮件安全的\"头部驱动\"模式——主流品牌引领,长尾跟进。",
    ),
    "webgraph_tld_distribution": dict(
        titleZh="WebGraph · TLD 分布",
        titleEn="WebGraph · TLD Distribution",
        findings=[
            ".com 占据 WebGraph 域名集合约 40%,全球商业品牌聚焦。",
            "ccTLD 与 .org / .net 总体构成长尾。",
        ],
        implications="Common Crawl 的 WebGraph 是跨语言、跨国家的\"可见网\"的快照。",
    ),
    "webgraph_pagerank_distribution": dict(
        titleZh="WebGraph · PageRank 分布",
        titleEn="WebGraph · PageRank Distribution",
        findings=[
            "PageRank 呈典型幂律 — 少数核心域名占据绝大部分权威。",
            "Top 100 域名基本都是云/内容/社交平台。",
        ],
        implications="Web 的权威度像财富——集中度极高,与注册分布错位显著。",
    ),
    "webgraph_cctld_ranking": dict(
        titleZh="ccTLD · WebGraph 排名",
        titleEn="ccTLD · WebGraph Ranking",
        findings=[
            "主要 ccTLD 在 WebGraph 中的相对权威度不完全跟注册规模对齐。",
            "小国 ccTLD 可因头部媒体/政府域拉高整体排名。",
        ],
        implications="WebGraph 排名是\"被引用度\",不是\"被注册\"——两种分布的差异是研究富矿。",
    ),
    "pagerank_vs_tranco_distribution": dict(
        titleZh="PR vs Tranco · 分布",
        titleEn="PR vs Tranco · Distribution",
        findings=[
            "PageRank 与 Tranco 排名相关但非一一对应。",
            "PageRank 更反映跨域引用,Tranco 更反映解析查询流量。",
        ],
        implications="两榜并用可以相互佐证 — 差值大的域名值得深挖(可能为异常或领域特化)。",
    ),
    "pagerank_vs_tranco_tld_bias": dict(
        titleZh="PR vs Tranco · TLD 偏差",
        titleEn="PR vs Tranco · TLD Bias",
        findings=[
            "ccTLD 在 Tranco 更高、PageRank 较低 — 查询量未必转为被引。",
            ".com / .org 通常 PageRank 领先 Tranco。",
        ],
        implications="榜单选择要考虑应用场景:安全研究更看 PageRank,运营更看 Tranco。",
    ),
    "pagerank_vs_tranco_consensus": dict(
        titleZh="PR vs Tranco · 共识域名",
        titleEn="PR vs Tranco · Consensus Set",
        findings=[
            "两榜共识 Top N 收敛于全球品牌 + 基础设施域。",
            "共识集合的规模反映榜单一致性程度。",
        ],
        implications="共识集合可作为\"金标准\"测试集合 — 各类方法可在其上交叉验证。",
    ),
    "pagerank_vs_tranco_scatter": dict(
        titleZh="PR vs Tranco · 散点",
        titleEn="PR vs Tranco · Scatter",
        findings=[
            "对角分布为基线,偏离对角线揭示两榜的局部分歧。",
            "远离对角的外点常为垂直领域的头部(媒体、电商、SaaS)。",
        ],
        implications="散点是定位\"领域头部\"的快速方法。",
    ),
    "pagerank_vs_tranco_heatmap": dict(
        titleZh="PR vs Tranco · 分位热力",
        titleEn="PR vs Tranco · Quantile Heatmap",
        findings=[
            "分位热力量化两榜的差异概率。",
            "对角格子密度越高代表越一致。",
        ],
        implications="热力图是从视觉化 → 定量化两榜对齐的桥梁。",
    ),
    "cc_index_status_distribution": dict(
        titleZh="CC CDX · HTTP 状态码",
        titleEn="CC CDX · HTTP Status Distribution",
        findings=[
            "200 主导,但 301/302 重定向和 404 也有显著占比。",
            "5xx 比例能反映服务器健康度与 CC 爬取遇到的障碍。",
        ],
        implications="CDX 状态码是 Web 健康度的补充维度 — DNS 可用 ≠ HTTP 可用。",
    ),
    "cc_index_pr_health": dict(
        titleZh="CC CDX · 高 PR 域名健康度",
        titleEn="CC CDX · High PageRank Health",
        findings=[
            "高 PR 域名的 200 成功率显著高于长尾。",
            "重要域名的运维投入与 Web 健康度成正比。",
        ],
        implications="PR 越高,Web 可用性越好 — 这是\"网络权威度 → 资源投入\"的正循环。",
    ),
    "cc_index_domain_profile": dict(
        titleZh="CC CDX · 域名画像",
        titleEn="CC CDX · Domain Profile",
        findings=[
            "语言、国家、MIME 类型、URL 模式共同构成域名的画像。",
            "画像可辅助分类:企业/媒体/个人/孵化中站点。",
        ],
        implications="域名画像是研究\"Web 内容生态\"的基本单位。",
    ),
}

# Annotations for Phase 1 RIR enrichment (5 charts, tier H).
RIR_ANNOTATIONS: dict[str, dict] = {
    "rir_01_delegation": dict(
        titleZh="RIR 委派分布",
        titleEn="RIR Delegation Distribution",
        findings=[
            "ARIN 独占 54.4%,APNIC 37.2%,两者合计 91.6% ——北美与亚太主导全球 IPv4 前缀空间。",
            "RIPE(4.6%)、AFRINIC(2.4%)、LACNIC(1.4%)三区合计不足 10%。",
            "总前缀数 3,703,402 条,覆盖可路由 IPv4 空间的主要部分。",
        ],
        implications="前缀委派格局折射出互联网基础设施的地缘政治分布——历史先发优势与人口增长存在明显错位。",
    ),
    "rir_02_prefix_size": dict(
        titleZh="前缀尺寸分布 (by RIR)",
        titleEn="Prefix Size Distribution by RIR",
        findings=[
            "/24(256 个地址)是各 RIR 最主流的分配粒度,ARIN 持有最多(/24 近 195 万条)。",
            "INTERNIC 持有 1,271 条 /8 超大块——这是上世纪互联网初期历史遗留的根级分配。",
            "超小段(< /24)仅 ARIN 与 RIPE 有少量存在,反映精细化地址管理需求。",
        ],
        implications="/24 主导意味着互联网路由表以 /24 为基本单位增长;碎片化程度直接影响全球 BGP 路由条目规模。",
    ),
    "rir_03_rdns_rtype": dict(
        titleZh="rDNS 记录类型热力图",
        titleEn="rDNS Record-Type Distribution Heatmap",
        findings=[
            "NS 记录对每个 RIR 的覆盖率均为 100%——反向 DNS 授权靠 NS 传递,无一例外。",
            "CNAME/SOA/A/AAAA 仅 ARIN 与 RIPE 有稀疏分布(CNAME 1,657 条),APNIC/AFRINIC/LACNIC 为零。",
            "rDNS 生态高度单一:NS 是核心载体,其余类型为少量辅助记录。",
        ],
        implications="rDNS 的 NS-only 特征说明反向区实际上是纯委派结构;运营商可借助 CNAME 实现跨区共享托管。",
    ),
    "rir_04_hoster_patterns": dict(
        titleZh="托管者聚类 (NS apex 域)",
        titleEn="Hoster-Pattern Clustering via NS Apex",
        findings=[
            "ad.jp(7.1%)与 ne.jp(5.8%)居前两位——日本 ISP 大规模托管亚太前缀的反向 DNS。",
            "akam.net(1.7%)是前 20 中唯一的全球 CDN,其余均为区域性 ISP/运营商。",
            "前 20 apex 合计约 30% 份额;长尾极长:26,139 个不同 apex 管理剩余 70%。",
        ],
        implications="NS 托管高度集中于少数区域运营商;任何一个头部 apex 故障都会造成数十万前缀的 rDNS 失效。",
    ),
    "rir_05_country_tld": dict(
        titleZh="国家维度 (NS rdata TLD 后缀)",
        titleEn="Country Code Extraction from NS rdata TLD",
        findings=[
            ".net(30.5%)与 .com(25.9%)是最大命名空间——超过一半的 NS 指向通用 TLD。",
            "ccTLD 中 .jp 占比最高,反映日本 ISP 倾向于使用本国域名管理 rDNS。",
            ".br / .au / .tw 等新兴市场 ccTLD 进入前十,与 APNIC/LACNIC 委派份额吻合。",
        ],
        implications="NS TLD 分布揭示反向 DNS 托管的国家偏好——本地化运营商 vs. 全球 DNS 基础设施提供商的博弈。",
    ),
}


# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────

def run_charts(conn) -> None:
    print("\n[charts] Tier A — 10 interactive charts")
    for fn in TIER_A_CHARTS:
        try:
            payload = fn(conn)
        except Exception as e:
            print(f"  ✗ {fn.__name__}: {e}")
            continue
        _write_json(CHARTS_OUT / f"{payload['id']}.json", payload)


def run_annotations() -> None:
    print("\n[annotations] build bundle")
    all_anns: list[Annotation] = []

    # Tier A (interactive charts)
    for cid, fields in TIER_A_ANNOTATIONS.items():
        all_anns.append(Annotation(id=cid, **fields))

    # output/*.png (hand curated; skip any ids already covered above)
    for cid, fields in OUTPUT_PNG_ANNOTATIONS.items():
        if not fields:
            continue
        if cid in TIER_A_ANNOTATIONS:
            continue
        all_anns.append(Annotation(id=cid, **fields))

    # deep_analysis steps
    deep_dir = REPO_DIR / "analysis" / "deep_analysis"
    all_anns.extend(build_step_annotations("deep", deep_dir, "deep_"))

    # network_analysis steps
    net_dir = REPO_DIR / "analysis" / "network_analysis"
    all_anns.extend(build_step_annotations("network", net_dir, "net_"))

    # rir_enrichment (Phase 1, hand-curated)
    for cid, fields in RIR_ANNOTATIONS.items():
        all_anns.append(Annotation(id=cid, **fields))

    # Persist per-annotation + bundle
    for a in all_anns:
        _write_json(ANN_OUT / f"{a.id}.json", a.to_dict())
    bundle = {a.id: a.to_dict() for a in all_anns}
    _write_json(BUNDLE_OUT, bundle)
    print(f"\n[annotations] total = {len(all_anns)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--charts", action="store_true", help="only charts")
    ap.add_argument("--annotations", action="store_true", help="only annotations")
    args = ap.parse_args()

    do_charts = args.charts or not args.annotations
    do_anns = args.annotations or not args.charts

    if do_charts:
        conn = get_conn()
        try:
            run_charts(conn)
        finally:
            conn.close()

    if do_anns:
        run_annotations()

    print("\n[export_metrics] done.")


if __name__ == "__main__":
    main()
