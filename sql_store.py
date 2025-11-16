
from __future__ import annotations
import os
import sqlite3
from typing import Optional, Iterable, Tuple

from time import time

# --- Force sql_store to use onto_server/onto.db -----------------------------
from pathlib import Path

# sql_store.py 与 onto_server 同级目录：<project_root>/sql_store.py 和 <project_root>/onto_server/
PROJECT_ROOT = Path(__file__).resolve().parent
ONTO_DB_DEFAULT = PROJECT_ROOT / "onto_server" / "onto2.db"

# 确保目录存在（若 onto_server/ 不存在这行也不会报错）
ONTO_DB_DEFAULT.parent.mkdir(parents=True, exist_ok=True)

# 提前设置统一的环境变量，让 storage.py 也用同一个 DB（非常关键）
os.environ.setdefault("ONTO_DB_PATH", str(ONTO_DB_DEFAULT))

# 供本模块 fallback 使用
DEFAULT_DB_PATH = os.environ["ONTO_DB_PATH"]
# ---------------------------------------------------------------------------

_SQLCACHE_DDL = """
CREATE TABLE IF NOT EXISTS sql_cache (
    sequence_id TEXT PRIMARY KEY,
    sql_text    TEXT NOT NULL,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sql_cache_created ON sql_cache(created_at);
"""

def _conn():
    db_path = DEFAULT_DB_PATH
    conn = sqlite3.connect(db_path)
    with conn:
        c = conn.cursor()
        # Safe PRAGMA for better durability/perf
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.executescript(_SQLCACHE_DDL)
    return conn

def init():
    """Explicit initializer (optional). Safe to call multiple times."""
    _ = _conn()
    try:
        _.close()
    except Exception:
        pass

def save_sql(sequence_id: str, sql_text: str) -> None:
    """Save/overwrite a SQL text for a given sequence_id."""
    from time import time as _now
    con = _conn()
    try:
        with con:
            con.execute(
                "INSERT OR REPLACE INTO sql_cache(sequence_id, sql_text, created_at) VALUES (?,?,?)",
                (str(sequence_id), str(sql_text), float(_now()))
            )
    finally:
        try:
            con.close()
        except Exception:
            pass

def get_sql(sequence_id: str) -> Optional[str]:
    """Return SQL text by sequence_id or None if absent."""
    con = _conn()
    try:
        cur = con.execute("SELECT sql_text FROM sql_cache WHERE sequence_id = ?", (str(sequence_id),))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        try:
            con.close()
        except Exception:
            pass

def list_recent_sql(limit: int = 50) -> Iterable[Tuple[str, str, float]]:
    """Return recent N rows: [(sequence_id, sql_text, created_at), ...]."""
    con = _conn()
    try:
        cur = con.execute(
            "SELECT sequence_id, sql_text, created_at FROM sql_cache ORDER BY created_at DESC LIMIT ?",
            (int(limit),)
        )
        return list(cur.fetchall())
    finally:
        try:
            con.close()
        except Exception:
            pass


def bulk_save_sql(sequence_sql_pairs):
    """
    sequence_sql_pairs: List[Tuple[str, str]]
    一次性批量写入 sql_cache (sequence_id, sql_text, created_at)
    """
    con = _conn()
    con.executemany(
        "INSERT OR REPLACE INTO sql_cache (sequence_id, sql_text, created_at) VALUES (?,?,?)",
        [(sid, sql, time()) for (sid, sql) in sequence_sql_pairs]
    )
    con.commit()
    con.close()