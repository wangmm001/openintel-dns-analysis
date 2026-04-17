# OpenINTEL DNS 开源数据分析教程

基于 [OpenINTEL](https://openintel.nl) 项目的 DNS 测量数据，构建完整的开源数据分析教程。

## 数据规模

| 维度 | 数值 |
|------|------|
| DNS 数据集 | 13 个（8 ccTLD 区域 + 4 TopList + Root） |
| DNS 记录数 | 2.32 亿 |
| DNS 域名数 | 3,347 万 |
| Parquet 总量 | 11.2 GB |
| 字段数 | 99 列 |
| **Phase 1 新增** | RIR rDNS 3.7M IPv4 前缀 + CC WebGraph (cc-main-2025-26-dec-jan-feb, 288M host / 12.4B edge) |

## 项目结构

```
├── downloads/openintel/          # 原始数据（gitignored，需自行下载）
│   ├── zone/                     # 区域文件: ch/ee/fr/gov/li/nu/se/sk/root
│   └── toplist/                  # 排行榜: tranco/umbrella/radar/majestic
├── downloads/common-crawl/       # Common Crawl CDX + WebGraph
├── analysis/
│   ├── scripts/
│   │   ├── config.py             # 全局配置 (BASE_DIR/REPO_DIR/zone_glob/...)
│   │   ├── 00_data_catalog.py    # 数据目录
│   │   ├── 01–07 基础分析脚本
│   │   ├── 08–10 WebGraph + CC 交叉
│   │   ├── 11_deep_analysis.py   # 22 步深度洞察
│   │   ├── 12_network_analysis.py + 12b_network_continue.py  # 25 步复杂网络
│   │   └── export_metrics.py     # 为前端抽取 JSON (charts + annotations)
│   ├── output/                   # 01–10 脚本的 PNG 输出
│   ├── deep_analysis/step_NN_*/  # 22 步，每步 chart.png + result.txt
│   ├── network_analysis/step_NN_*/  # 25 步，每步 chart.png + result.txt
│   ├── docs/data_catalog.json
│   └── web/                      # Astro 5 + ECharts 5 前端（1 首页 + 30 子页）
├── dist/web/                     # Astro 构建产物 (gitignored)
├── tutorial.html                 # 旧版教程（保留为归档）
└── presentation.html             # 旧版报告（保留为归档）
```

## 快速开始

### 1. 安装依赖

```bash
pip install duckdb pandas pyarrow matplotlib seaborn boto3
```

### 2. 下载数据

```python
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client('s3',
    endpoint_url='https://object.openintel.nl',
    config=Config(signature_version=UNSIGNED),
    region_name='us-east-1')

# 列出 Tranco TopList 数据
resp = s3.list_objects_v2(
    Bucket='openintel-public',
    Prefix='fdns/basis=toplist/source=tranco/year=2026/month=04/day=10/')
```

### 3. 运行分析

```bash
python analysis/scripts/00_data_catalog.py   # 数据目录
python analysis/scripts/01_overview.py       # 数据总览
python analysis/scripts/07_toplist_analysis.py  # TopList 交叉分析
```

### 4. 启动前端站点（新）

```bash
cd analysis/web
pnpm install                 # 一次性
pnpm dev                     # http://localhost:4321 — 1 首页 + 30 子页
pnpm build                   # 生成 dist/web/ 静态站
```

首次/数据更新后运行 `python3 analysis/scripts/export_metrics.py` 可重新抽取 10 张交互图的 JSON 聚合与 84 张图的结构化注解。

旧版 `tutorial.html`（47 页）与 `presentation.html`（34 页）仍可在浏览器打开,作为归档。

## 数据来源

- **S3 接口**: `https://object.openintel.nl` / Bucket: `openintel-public`
- **许可证**: CC BY-NC-SA 4.0
- **项目主页**: https://openintel.nl

## Phase 1 — 横向覆盖扩展（2026-04-17）

在已有的 DNS + CC 分析之上新增两个正交维度，不改动既有分析的数值结论：

### 1. RIR rDNS 富化（standalone 5 步分析）

用 `data/rir-data/rirs-rdns-formatted/` 里 3.7M 条 IPv4 prefix → rDNS NS/PTR 映射做纯 RIR 维度分析：

```bash
python analysis/scripts/13_rir_enrichment.py
```

输出到 `analysis/rir_enrichment/step_{01..05}_*/`，含 `chart.png` + `result.txt`。关键发现：

- **ARIN 54.4%**, APNIC 37.2%, RIPE 4.6%, AFRINIC 2.4%, LACNIC 1.4% — 北美 + 亚太合计 92% 的 IPv4 prefix
- /24 块在每个 RIR 都占 >95% — 细粒度分配是事实标准；INTERNIC 的历史大块（1271 条 /8 类）是异常
- NS 记录占 rDNS 的 99.9%+（其他 rtype 合计 <0.1%）
- **NS rdata 命名模式** 里 Japan ISP 基础设施（ad.jp/ne.jp）在全球 NS 中占比最高（12.9% 合计），Akamai 第 3（1.7%）
- ccTLD 维度下 `jp` 领先，反映 APNIC 的 Japan 权重

前端在 `/phase1/rir-enrichment/` 新增一页 Tier H。

### 2. Common Crawl 独立分析（standalone 5 步）

```bash
python analysis/scripts/14_cc_standalone.py
```

输出到 `analysis/cc_standalone/step_{01..05}_*/`。数据源仅为 CC webgraph + cluster.idx，不跨库 join，避开 OpenINTEL 依赖。关键发现：

- 134.2M 域名 + 288.6M host（host ≈ 2.15 × domain）
- PageRank 极度集中：median = 3.72e-9（最小值地板），p99 仅 2.95e-8；`google.com` + `googleapis.com` 分别 0.015 和 0.015 独占前 2
- `.com` 在域名 top-1k 占 57%、top-10k 占 47%、top-100k 占 44% — 长尾里 ccTLD 占比回升
- Hostname 长度中位数 15，11–15 字符这个桶占 36.6%（49.2M）
- host PR 比 domain PR 更均匀（p99 10×小），top-host 是 `twitter.com`，不同于 top-domain 的 `google.com`
- cluster.idx 80.7 万条里 YouTube 独占 142 条（CDX 索引页最广），其次 `forsale.godaddy.com`（125 条）

前端在 `/phase1/cc-standalone/` 新增 Tier H 第二页。

### 3. 下载基础设施

新建 `analysis/scripts/download_data.py`（OpenINTEL + Common Crawl 幂等下载器，`MANIFEST.json` 续传）。子命令：

```bash
python analysis/scripts/download_data.py openintel --date 2026-04-10
python analysis/scripts/download_data.py common-crawl --crawl CC-MAIN-2026-12 --host-graph
python analysis/scripts/download_data.py verify
```

CC webgraph 使用最新发布的 slug `cc-main-2025-26-dec-jan-feb`（CC-MAIN-2026-12 的图还未发布，预计 2026-06）。

### 4. Checkpoint 基础设施

新建 `analysis/scripts/_checkpoint.py`：通过 `.ok` sentinel + 环境变量 `FORCE=1` 支持 13/14 的断点续跑。每步目录下有 `result.txt` + `chart.png` + `.ok` 就视为已完成。

### 带宽限制与 scope 调整

本次执行中发现从本机到 `object.openintel.nl`（荷兰）带宽约 **8 KB/s**，全量 OpenINTEL 重下载不可行（≈ 58 天）。相比之下到 AWS us-east-1 的 Common Crawl 带宽约 200 KB/s—3.5 MB/s，CC 数据可顺畅拉取。

因此本期（Phase 1）聚焦于不依赖 OpenINTEL 重下载的三块：RIR 分析、CC 基础设施下载、前端呈现。`analysis/output/` 和 `analysis/deep_analysis/`、`analysis/network_analysis/` 的既有分析结果不变。

## 未来规划

本项目将扩展为多源开源数据分析平台：

```
open-data-lab/
├── data/
│   ├── openintel/        ← 当前项目
│   ├── czds/             ← ICANN gTLD 区域文件
│   ├── common-crawl/     ← Web 爬取数据
│   └── rapid7-sonar/     ← IP 扫描数据
```
