"""OpenINTEL DNS 开源数据分析教程 - 全局配置

项目结构:
  data/openintel/zone/{ch,ee,fr,gov,li,nu,se,sk,root}/*.parquet
  data/openintel/toplist/{tranco,umbrella,radar,majestic}/*.parquet
"""

import os
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# ── 路径 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "openintel"
ZONE_DIR = DATA_DIR / "zone"
TOPLIST_DIR = DATA_DIR / "toplist"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── TLD 区域列表 ─────────────────────────────────────
ZONE_TLDS = sorted(
    [d.name for d in ZONE_DIR.iterdir()
     if d.is_dir() and d.name != "root" and any(d.glob("*.parquet"))]
)

# ── TopList 列表 ─────────────────────────────────────
TOPLISTS = sorted(
    [d.name for d in TOPLIST_DIR.iterdir()
     if d.is_dir() and any(d.glob("*.parquet"))]
)

# ── DuckDB ────────────────────────────────────────────
def get_conn():
    """返回一个新的 DuckDB 连接"""
    conn = duckdb.connect()
    conn.execute("SET threads TO 4")
    return conn


def zone_glob(tld: str) -> str:
    """返回某个 TLD 区域目录下所有 parquet 文件的 glob 路径"""
    return str(ZONE_DIR / tld / "*.parquet")


def toplist_glob(name: str) -> str:
    """返回某个 TopList 的 parquet glob"""
    return str(TOPLIST_DIR / name / "*.parquet")


def all_zone_globs() -> list[str]:
    """返回所有 zone TLD 的 glob 列表"""
    return [zone_glob(t) for t in ZONE_TLDS]


def all_zone_sql() -> str:
    """返回 DuckDB SQL 中用于 read_parquet([...]) 的字符串"""
    return ", ".join(f"'{zone_glob(t)}'" for t in ZONE_TLDS)


# ── 兼容旧脚本 ───────────────────────────────────────
# 让 01-06 脚本不需要改路径也能跑
TLDS = ZONE_TLDS

def parquet_glob(tld: str) -> str:
    """兼容旧脚本的路径函数"""
    return zone_glob(tld)


# ── 可视化 ────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({
    "figure.figsize": (12, 6),
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.3,
})

STATUS_CODE_MAP = {
    0: "NOERROR",
    1: "FORMERR",
    2: "SERVFAIL",
    3: "NXDOMAIN",
    5: "REFUSED",
    65533: "TIMEOUT",
}


def save_fig(name: str):
    """保存当前图表到 output/ 并关闭"""
    path = OUTPUT_DIR / f"{name}.png"
    plt.savefig(path)
    plt.close()
    print(f"  -> 图表已保存: {path}")
