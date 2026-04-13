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
├── data/openintel/              # 原始数据（需自行下载）
│   ├── zone/                    # 区域文件: ch/ee/fr/gov/li/nu/se/sk/root
│   └── toplist/                 # 排行榜: tranco/umbrella/radar/majestic
├── scripts/                     # 分析脚本
│   ├── config.py                # 全局配置
│   ├── 00_data_catalog.py       # 数据目录与格式详解
│   ├── 01_overview.py           # 数据总览
│   ├── 02_dns_records.py        # DNS 记录深度分析
│   ├── 03_geo_network.py        # 地理与网络分析
│   ├── 04_security.py           # DNSSEC 与安全分析
│   ├── 05_domain_infra.py       # 域名基础设施
│   ├── 06_anomaly.py            # 异常检测
│   └── 07_toplist_analysis.py   # TopList 交叉分析
├── docs/                        # 文档
│   └── data_catalog.json        # 完整数据目录
├── output/                      # 图表输出
├── tutorial.html                # 开源数据分析教程（47 页）
└── presentation.html            # 数据分析报告（34 页）
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
python scripts/00_data_catalog.py   # 数据目录
python scripts/01_overview.py       # 数据总览
python scripts/07_toplist_analysis.py  # TopList 交叉分析
```

### 4. 查看教程

浏览器打开 `tutorial.html`（47 页交互式教程）或 `presentation.html`（34 页分析报告）。

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
