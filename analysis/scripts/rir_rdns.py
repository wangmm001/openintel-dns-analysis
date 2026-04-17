"""RIR rDNS prefix tree — shared loader for scripts 11, 12, 13.

Source: data/rir-data/rirs-rdns-formatted/type=enriched/...
Schema: prefix (VARCHAR), rname, rdata, source (APNIC/ARIN/RIPE/LACNIC/AFRINIC), ...
"""
from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parent.parent.parent
DEFAULT_SNAPSHOT = REPO / "data" / "rir-data" / "rirs-rdns-formatted" / "type=enriched" / "year=2026" / "month=03" / "day=29" / "hour=00"

def load_rir_prefix(conn: duckdb.DuckDBPyConnection, snapshot_dir: Path | None = None) -> int:
    """Create TABLE rir_prefix(prefix, rname, rir_source) in conn. Returns row count."""
    snap = snapshot_dir or DEFAULT_SNAPSHOT
    parquets = list(snap.glob("*.parquet"))
    if not parquets:
        raise FileNotFoundError(f"No RIR parquet under {snap}")
    paths = ", ".join(f"'{p}'" for p in parquets)
    conn.execute(f"""
        CREATE OR REPLACE TABLE rir_prefix AS
        SELECT
            prefix,
            start_address,
            end_address,
            rname,
            rdata,
            rtype,
            source AS rir_source
        FROM read_parquet([{paths}])
        WHERE af = 4   -- v4 only for Phase 1
    """)
    n = conn.execute("SELECT count(*) FROM rir_prefix").fetchone()[0]
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rir_start ON rir_prefix(start_address)")
    return n

def lookup_ip_to_rdns_sql(ip_col: str) -> str:
    """Return a SQL fragment that joins rir_prefix to an outer query's IP column.

    Usage:
        SELECT d.query_name, r.rname, r.rir_source
        FROM read_parquet([...]) d
        LEFT JOIN rir_prefix r
        ON {lookup_ip_to_rdns_sql('d.ip4_address')}
    """
    return f"""
    (
        host({ip_col}::INET) BETWEEN host(r.start_address::INET) AND host(r.end_address::INET)
    )
    """

def coverage_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Quick summary of the loaded RIR table."""
    r = conn.execute("SELECT count(*), count(DISTINCT rname), count(DISTINCT rir_source) FROM rir_prefix").fetchone()
    by_rir = dict(conn.execute("SELECT rir_source, count(*) FROM rir_prefix GROUP BY rir_source").fetchall())
    return {"rows": r[0], "unique_rnames": r[1], "rir_sources": r[2], "by_rir": by_rir}
