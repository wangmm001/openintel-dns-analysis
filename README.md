# OpenINTEL DNS 开源数据分析教程

基于 [OpenINTEL](https://openintel.nl) 项目的 DNS 测量数据，构建完整的开源数据分析教程。

## 数据规模

| 维度 | 数值 |
|------|------|
| 数据集 | 13 个（8 ccTLD 区域 + 4 TopList + Root） |
| 总记录数 | 2.32 亿 |
| 总域名数 | 3,347 万 |
| 数据量 | 11.2 GB (Parquet) |
| 字段数 | 99 列 |

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
