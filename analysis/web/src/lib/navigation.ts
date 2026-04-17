export type TierKey = "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H";

export interface SubPage {
  slug: string;
  path: string;
  tier: TierKey;
  order: number;
  titleZh: string;
  titleEn: string;
  tagline: string;
  chartCount: number;
  interactive: number;
}

export interface Tier {
  key: TierKey;
  titleZh: string;
  titleEn: string;
  taglineZh: string;
  glow: "blue" | "purple" | "green" | "orange";
  pages: SubPage[];
}

export const TIERS: Tier[] = [
  {
    key: "A",
    titleZh: "数据总览",
    titleEn: "Data Overview",
    taglineZh: "232M 条记录的第一眼——域名规模、查询分布与响应健康",
    glow: "blue",
    pages: [
      { slug: "data-census", path: "/overview/data-census", tier: "A", order: 1,
        titleZh: "数据普查", titleEn: "Data Census",
        tagline: "8 大 ccTLD + 4 大榜单的骨架", chartCount: 3, interactive: 3 },
      { slug: "dns-portrait", path: "/overview/dns-portrait", tier: "A", order: 2,
        titleZh: "DNS 记录画像", titleEn: "DNS Records Portrait",
        tagline: "IPv4/IPv6、MX、TXT 三件套", chartCount: 3, interactive: 3 },
      { slug: "ns-ttl", path: "/overview/ns-ttl", tier: "A", order: 3,
        titleZh: "NS 与 TTL", titleEn: "Nameservers & TTL",
        tagline: "解析侧的冗余与缓存策略", chartCount: 3, interactive: 3 },
      { slug: "status-anomaly", path: "/overview/status-anomaly", tier: "A", order: 4,
        titleZh: "响应状态与异常", titleEn: "Status Codes & Anomalies",
        tagline: "13M 次失败的解剖", chartCount: 3, interactive: 1 },
    ],
  },
  {
    key: "B",
    titleZh: "地理与基础设施",
    titleEn: "Geo & Infrastructure",
    taglineZh: "域名托管在哪里,由谁承载,节奏如何",
    glow: "purple",
    pages: [
      { slug: "global-distribution", path: "/geo/global-distribution", tier: "B", order: 5,
        titleZh: "全球主机地理", titleEn: "Global Hosting Geography",
        tagline: "Top 国家 · Top AS · TLD 热力", chartCount: 3, interactive: 3 },
      { slug: "soa-cname", path: "/geo/soa-cname", tier: "B", order: 6,
        titleZh: "SOA 与 CNAME", titleEn: "SOA & CNAME",
        tagline: "基础设施的节奏与延迟画像", chartCount: 3, interactive: 0 },
      { slug: "toplist-mirror", path: "/geo/toplist-mirror", tier: "B", order: 7,
        titleZh: "排行榜镜像", titleEn: "TopList Mirrors",
        tagline: "Tranco/Umbrella/Radar/Majestic 对比", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "C",
    titleZh: "安全态势",
    titleEn: "Security Posture",
    taglineZh: "DNSSEC、CAA、邮件防护——信任根的真相",
    glow: "green",
    pages: [
      { slug: "dnssec", path: "/security/dnssec", tier: "C", order: 8,
        titleZh: "DNSSEC 部署实况", titleEn: "DNSSEC Deployment",
        tagline: "16.4% 信任根 · 算法 67% ECDSA", chartCount: 3, interactive: 3 },
      { slug: "caa-email", path: "/security/caa-email", tier: "C", order: 9,
        titleZh: "证书授权与邮件防护", titleEn: "CAA & Email Security",
        tagline: "0.7% 与 26.3% 的鸿沟", chartCount: 3, interactive: 0 },
      { slug: "ttl-anomaly", path: "/security/ttl-anomaly", tier: "C", order: 10,
        titleZh: "TTL 异常", titleEn: "TTL Anomalies",
        tagline: "动态与陈旧的边界", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "D",
    titleZh: "PageRank 与 WebGraph",
    titleEn: "PageRank & WebGraph",
    taglineZh: "可见互联网的权威度与注册互联网的错位",
    glow: "orange",
    pages: [
      { slug: "distribution", path: "/pagerank/distribution", tier: "D", order: 11,
        titleZh: "PageRank 分布", titleEn: "PageRank Distribution",
        tagline: "Common Crawl 134M 域名", chartCount: 3, interactive: 1 },
      { slug: "vs-toplist", path: "/pagerank/vs-toplist", tier: "D", order: 12,
        titleZh: "PR vs TopList", titleEn: "PR vs TopList",
        tagline: "可见网与注册网的错位", chartCount: 3, interactive: 0 },
      { slug: "scatter-heat", path: "/pagerank/scatter-heat", tier: "D", order: 13,
        titleZh: "相关性:散点与热力", titleEn: "Correlation: Scatter & Heatmap",
        tagline: "PageRank 与 Tranco 的双轴", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "E",
    titleZh: "Common Crawl CDX",
    titleEn: "Common Crawl CDX",
    taglineZh: "714K 样本域名的 Web 爬取快照",
    glow: "blue",
    pages: [
      { slug: "cdx-index", path: "/crawl/cdx-index", tier: "E", order: 14,
        titleZh: "CC 索引快照", titleEn: "CC Index Snapshot",
        tagline: "状态码 · PR 健康 · 域名画像", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "F",
    titleZh: "深度洞察",
    titleEn: "Deep Insights",
    taglineZh: "22 步综合分析——从普查到健康评分",
    glow: "purple",
    pages: [
      { slug: "01-census-query", path: "/deep/01-census-query", tier: "F", order: 15,
        titleZh: "第一章·普查", titleEn: "Ch.1 Census",
        tagline: "Step 01–03", chartCount: 3, interactive: 0 },
      { slug: "04-protocol-health", path: "/deep/04-protocol-health", tier: "F", order: 16,
        titleZh: "第二章·协议健康", titleEn: "Ch.2 Protocol Health",
        tagline: "Step 04–06", chartCount: 3, interactive: 0 },
      { slug: "07-performance", path: "/deep/07-performance", tier: "F", order: 17,
        titleZh: "第三章·性能", titleEn: "Ch.3 Performance",
        tagline: "Step 07–08", chartCount: 2, interactive: 0 },
      { slug: "09-dnssec-email", path: "/deep/09-dnssec-email", tier: "F", order: 18,
        titleZh: "第四章·安全部署", titleEn: "Ch.4 Security Deployment",
        tagline: "Step 09–11", chartCount: 3, interactive: 0 },
      { slug: "12-caa-ns-cname", path: "/deep/12-caa-ns-cname", tier: "F", order: 19,
        titleZh: "第五章·证书与冗余", titleEn: "Ch.5 CAA & Redundancy",
        tagline: "Step 12–14", chartCount: 3, interactive: 0 },
      { slug: "15-toplist-sharing", path: "/deep/15-toplist-sharing", tier: "F", order: 20,
        titleZh: "第六章·热榜与共享托管", titleEn: "Ch.6 TopList & Shared Hosting",
        tagline: "Step 15–17", chartCount: 3, interactive: 0 },
      { slug: "18-soa-failure", path: "/deep/18-soa-failure", tier: "F", order: 21,
        titleZh: "第七章·生命周期与失败学", titleEn: "Ch.7 Lifecycle & Failure Taxonomy",
        tagline: "Step 18–20", chartCount: 3, interactive: 0 },
      { slug: "21-bgp-scorecard", path: "/deep/21-bgp-scorecard", tier: "F", order: 22,
        titleZh: "第八章·BGP 与健康评分", titleEn: "Ch.8 BGP & Health Scorecard",
        tagline: "Step 21–22 + 总览", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "G",
    titleZh: "复杂网络",
    titleEn: "Complex Network",
    taglineZh: "25 步、7 阶段——面向 IMC/SIGCOMM 的论文级分析",
    glow: "green",
    pages: [
      { slug: "phase1-graph-construction-a", path: "/network/phase1-graph-construction-a", tier: "G", order: 23,
        titleZh: "阶段一·多层图构建 I", titleEn: "Phase 1 · Graph Construction I",
        tagline: "Step 01–03", chartCount: 3, interactive: 0 },
      { slug: "phase1-graph-construction-b", path: "/network/phase1-graph-construction-b", tier: "G", order: 24,
        titleZh: "阶段一·多层图构建 II", titleEn: "Phase 1 · Graph Construction II",
        tagline: "Step 04–05", chartCount: 2, interactive: 0 },
      { slug: "phase2-power-law", path: "/network/phase2-power-law", tier: "G", order: 25,
        titleZh: "阶段二·标度无关", titleEn: "Phase 2 · Scale-Free",
        tagline: "Step 06–08 · 幂律 α=1.75", chartCount: 3, interactive: 1 },
      { slug: "phase3-small-world", path: "/network/phase3-small-world", tier: "G", order: 26,
        titleZh: "阶段三·小世界", titleEn: "Phase 3 · Small-World",
        tagline: "Step 09–11 · σ≫1", chartCount: 3, interactive: 0 },
      { slug: "phase4-centrality-a", path: "/network/phase4-centrality-a", tier: "G", order: 27,
        titleZh: "阶段四·中心性 I", titleEn: "Phase 4 · Centrality I",
        tagline: "Step 12–14", chartCount: 3, interactive: 1 },
      { slug: "phase4-centrality-b", path: "/network/phase4-centrality-b", tier: "G", order: 28,
        titleZh: "阶段四·中心性 II + 介观", titleEn: "Phase 4 · Centrality II + Mesoscale",
        tagline: "Step 15–17", chartCount: 3, interactive: 1 },
      { slug: "phase5-bow-tie-motifs", path: "/network/phase5-bow-tie-motifs", tier: "G", order: 29,
        titleZh: "阶段五·bow-tie 与 motif", titleEn: "Phase 5 · Bow-Tie & Motifs",
        tagline: "Step 18–20", chartCount: 3, interactive: 0 },
      { slug: "phase6-7-resilience", path: "/network/phase6-7-resilience", tier: "G", order: 30,
        titleZh: "阶段六·七·韧性与综合", titleEn: "Phase 6-7 · Resilience & Synthesis",
        tagline: "Step 21–24 · 拓扑 → 安全预测", chartCount: 3, interactive: 0 },
    ],
  },
  {
    key: "H",
    titleZh: "Phase 1 · RIR 富化",
    titleEn: "Phase 1 · RIR Enrichment",
    taglineZh: "3.7M IPv4 prefix · 5 大 RIR · 反向 DNS 命名模式聚类",
    glow: "orange",
    pages: [
      { slug: "rir-enrichment", path: "/phase1/rir-enrichment", tier: "H", order: 31,
        titleZh: "RIR rDNS 全景", titleEn: "RIR rDNS Landscape",
        tagline: "委派分布 · 前缀尺寸 · rtype 分布 · 托管者聚类 · 国家维度", chartCount: 5, interactive: 0 },
    ],
  },
];

export const ALL_PAGES: SubPage[] = TIERS.flatMap((t) => t.pages).sort(
  (a, b) => a.order - b.order,
);

export function pageByPath(path: string): SubPage | undefined {
  return ALL_PAGES.find((p) => p.path === path);
}

export function neighbors(path: string): { prev?: SubPage; next?: SubPage; tier?: Tier } {
  const idx = ALL_PAGES.findIndex((p) => p.path === path);
  if (idx === -1) return {};
  const prev = idx > 0 ? ALL_PAGES[idx - 1] : undefined;
  const next = idx < ALL_PAGES.length - 1 ? ALL_PAGES[idx + 1] : undefined;
  const tier = TIERS.find((t) => t.key === ALL_PAGES[idx].tier);
  return { prev, next, tier };
}
