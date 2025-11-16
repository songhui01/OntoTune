import sqlite3
import json
import itertools
from datetime import datetime
from typing import Optional, Iterable, Tuple, List

from common import OntoException

def _onto2_db():
    conn = sqlite3.connect("onto2.db")
    return conn


def get_sql(sequence_id: str) -> Optional[str]:
    with _onto2_db() as con:
        cur = con.execute("SELECT sql_text FROM sql_cache WHERE sequence_id = ?", (str(sequence_id),))
        row = cur.fetchone()
        return row[0] if row else None

