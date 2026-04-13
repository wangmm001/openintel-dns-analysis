"""OpenINTEL DNS 数据分析项目 - 全局配置"""

import os
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# ── 路径 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR  # parquet 文件直接在各 TLD 子目录下
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── TLD 列表 ─────────────────────────────────────────
TLDS = sorted(
    [d.name for d in DATA_DIR.iterdir()
     if d.is_dir() and d.name not in (
         "scripts", "output", ".git", "__pycache__",
         "tranco", "umbrella", "radar", "root", "majestic",
     )]
)

# ── DuckDB ────────────────────────────────────────────
def get_conn():
    """返回一个新的 DuckDB 连接"""
    conn = duckdb.connect()
    conn.execute("SET threads TO 4")
    return conn


def parquet_glob(tld: str) -> str:
    """返回某个 TLD 目录下所有 parquet 文件的 glob 路径"""
    return str(DATA_DIR / tld / "*.parquet")


def all_parquet_glob() -> str:
    """返回所有 TLD 的 parquet glob（用 DuckDB list 语法）"""
    return [parquet_glob(t) for t in TLDS]


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
