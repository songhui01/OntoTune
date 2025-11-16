import sqlite3
import json
import itertools
from datetime import datetime
from typing import Optional, Iterable, Tuple, List

from common import OntoException

def _onto_db():
    conn = sqlite3.connect("onto.db")
    c = conn.cursor()
    c.execute("""
CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY,
    pg_pid INTEGER,
    plan TEXT, 
    reward REAL
)""")
    c.execute("""
CREATE TABLE IF NOT EXISTS experimental_query (
    id INTEGER PRIMARY KEY, 
    query TEXT UNIQUE
)""")
    c.execute("""
CREATE TABLE IF NOT EXISTS experience_for_experimental (
    experience_id INTEGER,
    experimental_id INTEGER,
    arm_idx INTEGER,
    FOREIGN KEY (experience_id) REFERENCES experience(id),
    FOREIGN KEY (experimental_id) REFERENCES experimental_query(id),
    PRIMARY KEY (experience_id, experimental_id, arm_idx)
)""")
    conn.commit()
    return conn


def get_sql(sequence_id: str) -> Optional[str]:
    """Return SQL text by sequence_id or None if absent."""
    with _onto_db() as con:
        cur = con.execute("SELECT sql_text FROM sql_cache WHERE sequence_id = ?", (str(sequence_id),))
        row = cur.fetchone()
        return row[0] if row else None

def record_reward(plan, reward, pid):
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO experience (plan, reward, pg_pid) VALUES (?, ?, ?)",
                  (json.dumps(plan), reward, pid))
        conn.commit()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{now} [INFO] [SERVER] Logged reward of {reward}")

def last_reward_from_pid(pid):
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM experience WHERE pg_pid = ? ORDER BY id DESC LIMIT 1",
                  (pid,))
        res = c.fetchall()
        if not res:
            return None
        return res[0][0]

def experience():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("SELECT plan, reward FROM experience")
        return c.fetchall()

def experiment_experience():
    all_experiment_experience = []
    for res in experiment_results():
        all_experiment_experience.extend(
            [(x["plan"], x["reward"]) for x in res]
        )
    return all_experiment_experience
    
def experience_size():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM experience")
        return c.fetchone()[0]

def clear_experience():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM experience")
        conn.commit()

def record_experimental_query(sql):
    try:
        with _onto_db() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO experimental_query (query) VALUES(?)",
                      (sql,))
            conn.commit()
    except sqlite3.IntegrityError as e:
        raise OntoException("Could not add experimental query. "
                           + "Was it already added?") from e

    print("Added new test query.")

def num_experimental_queries():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM experimental_query")
        return c.fetchall()[0][0]
    
def unexecuted_experiments():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("CREATE TEMP TABLE arms (arm_idx INTEGER)")
        c.execute("INSERT INTO arms (arm_idx) VALUES (0),(1),(2),(3),(4)")

        c.execute("""
SELECT eq.id, eq.query, arms.arm_idx 
FROM experimental_query eq, arms
LEFT OUTER JOIN experience_for_experimental efe 
     ON eq.id = efe.experimental_id AND arms.arm_idx = efe.arm_idx
WHERE efe.experience_id IS NULL
""")
        return [{"id": x[0], "query": x[1], "arm": x[2]}
                for x in c.fetchall()]

def experiment_results():
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("""
SELECT eq.id, e.reward, e.plan, efe.arm_idx
FROM experimental_query eq, 
     experience_for_experimental efe, 
     experience e 
WHERE eq.id = efe.experimental_id AND e.id = efe.experience_id
ORDER BY eq.id, efe.arm_idx;
""")
        for eq_id, grp in itertools.groupby(c, key=lambda x: x[0]):
            yield ({"reward": x[1], "plan": x[2], "arm": x[3]} for x in grp)
        

def record_experiment(experimental_id, experience_id, arm_idx):
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("""
INSERT INTO experience_for_experimental (experience_id, experimental_id, arm_idx)
VALUES (?, ?, ?)""", (experience_id, experimental_id, arm_idx))
        conn.commit()

def load_recent_batch_balanced(limit_per_arm=32, num_arms=7, include_experiments=True):
    """
    返回 (metas, rewards)，每个 arm 最多取 limit_per_arm 条，避免某些 arm 长期缺课被遗忘
    实现思路：
      1) 先从 experience 按 id DESC 扫描，按 arm 分桶
      2) 不足的 arm 再用实验数据补
    """
    buckets = {a: [] for a in range(int(num_arms))}

    # --- 从 experience 扫描 ---
    with _onto_db() as conn:
        c = conn.cursor()
        # 预取一个保守上限（例如 5000 条），避免全表扫描；也可视化你们的规模调整
        c.execute("SELECT id, plan, reward FROM experience ORDER BY id DESC LIMIT 20")
        rows = c.fetchall()

    for _id, plan_text, reward in rows:
        try:
            meta = json.loads(plan_text)['metadata']
        except Exception:
            continue

        arm_idx = None
        if isinstance(meta, dict):
            arm_idx = (meta.get('arm_config_json', {}) or {}).get('index', None)
            if arm_idx is None:
                for k in ('arm_idx', 'arm', 'chosen_arm'):
                    if k in meta and meta[k] is not None:
                        arm_idx = int(meta[k])
                        break

        if arm_idx is None:
            continue
        if not (0 <= int(arm_idx) < int(num_arms)):
            continue

        a = int(arm_idx)
        if len(buckets[a]) < int(limit_per_arm):
            meta.setdefault('arm_config_json', {})
            meta['arm_config_json']['index'] = a
            buckets[a].append((meta, float(reward)))

        # 如果所有桶都满了，就可以提前结束
        if all(len(buckets[a]) >= int(limit_per_arm) for a in buckets):
            break

    # --- 不足的 arm 用实验数据补齐 ---
    if include_experiments and not all(len(buckets[a]) >= int(limit_per_arm) for a in buckets):
        flat = []
        for grp in experiment_results():
            for x in grp:  # {"reward","plan","arm"}
                flat.append(x)

        # 逆序“近似最近”
        for x in reversed(flat):
            try:
                meta = json.loads(x["plan"])
            except Exception:
                continue
            arm_idx = x.get("arm", None)
            if arm_idx is None:
                continue
            a = int(arm_idx)
            if not (0 <= a < int(num_arms)):
                continue
            if len(buckets[a]) >= int(limit_per_arm):
                continue
            if isinstance(meta, dict):
                meta.setdefault('arm_config_json', {})
                meta['arm_config_json']['index'] = a
            buckets[a].append((meta, float(x["reward"])))

            if all(len(buckets[a]) >= int(limit_per_arm) for a in buckets):
                break

    # --- 拼接输出（按 arm 轮询或直接拼接都可；这里直接拼接） ---
    metas, rewards = [], []
    for a in range(int(num_arms)):
        for meta, rew in buckets[a]:
            metas.append(meta)
            rewards.append(rew)
    return metas, rewards



# select eq.id, efe.arm_idx, min(e.reward) from experimental_query eq, experience_for_experimental efe, experience e WHERE eq.id = efe.experimental_id AND e.id = efe.experience_id GROUP BY eq.id;

# ==== template tables & helpers ====
def _ensure_template_tables():
    with _onto_db() as conn:
        c = conn.cursor()
        # 模板总览表：模板键、出现次数、最近时间、可选摘要
        c.execute("""
            CREATE TABLE IF NOT EXISTS onto_templates (
                template_id   TEXT PRIMARY KEY,
                key_tuple_json TEXT NOT NULL,
                first_seen_ts TEXT NOT NULL,
                last_seen_ts  TEXT NOT NULL,
                sample_count  INTEGER NOT NULL DEFAULT 0
            )
            """)
        # 模板-臂统计：递推均值/方差（var_time 存 M2 累积，读取时可除以 n-1 得样本方差）
        c.execute("""
            CREATE TABLE IF NOT EXISTS onto_template_arm_stats (
                template_id  TEXT NOT NULL,
                arm          INTEGER NOT NULL,
                n            INTEGER NOT NULL DEFAULT 0,
                mean_time    REAL,
                var_time     REAL,
                last_update  TEXT NOT NULL,
                PRIMARY KEY (template_id, arm),
                FOREIGN KEY (template_id) REFERENCES onto_templates(template_id)
            )
            """)
        conn.commit()

# 确保在模块加载/第一次调用时建好表
_ensure_template_tables()


def upsert_template(template_id: str, key_tuple_json: str):
    """
    首次遇到模板：插入；再次遇到：sample_count+1 且更新 last_seen_ts。
    key_tuple_json 建议是 JSON.dumps 后的字符串；这里不做解析。
    """
    now = datetime.utcnow().isoformat()
    with _onto_db() as conn:
        c = conn.cursor()
        # SQLite UPSERT 语法（需要 SQLite >= 3.24）
        c.execute("""
            INSERT INTO onto_templates(template_id, key_tuple_json, first_seen_ts, last_seen_ts, sample_count)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(template_id) DO UPDATE SET
                last_seen_ts = excluded.last_seen_ts,
                sample_count = onto_templates.sample_count + 1
            """, (template_id, key_tuple_json, now, now))
        conn.commit()


def get_template_seen_n(template_id: str) -> int:
    """
    返回模板累计样本数（sample_count），若无则 0。
    """
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("SELECT sample_count FROM onto_templates WHERE template_id = ?", (template_id,))
        row = c.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def read_tpl_arm_stats(template_id: str) -> dict[int, dict]:
    """
    读取某模板下所有臂的统计：{arm: {"n":int, "mean_time":float, "var_time":float}}
    var_time 存的是 Welford 的 M2 累积；如需样本方差，可在外部用 M2/(n-1)。
    """
    with _onto_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT arm, n, mean_time, var_time
            FROM onto_template_arm_stats
            WHERE template_id = ?
            """, (template_id,))
        rows = c.fetchall() or []
    return {
        int(a): {
            "n": int(n or 0),
            "mean_time": float(m or 0.0),
            "var_time": float(v or 0.0)
        }
        for (a, n, m, v) in rows
    }


def update_tpl_arm_stats(template_id: str, arm: int, run_time: float):
    """
    递推更新模板-臂的均值/方差（Welford 算法）
    - mean_time: 递推均值
    - var_time:  累积平方差 M2（不是除以 n-1 的方差），便于后续继续更新
    """
    now = datetime.utcnow().isoformat()
    with _onto_db() as conn:
        c = conn.cursor()
        # 取旧值
        c.execute("""
            SELECT n, mean_time, var_time FROM onto_template_arm_stats
            WHERE template_id = ? AND arm = ?
            """, (template_id, arm))
        row = c.fetchone()
        if row:
            n, mean, m2 = int(row[0] or 0), float(row[1] or 0.0), float(row[2] or 0.0)
        else:
            n, mean, m2 = 0, 0.0, 0.0

        n_new = n + 1
        delta = run_time - mean
        mean_new = mean + delta / n_new
        delta2 = run_time - mean_new
        m2_new = m2 + delta * delta2  # 累积平方差（Welford 的 M2）

        # UPSERT
        c.execute("""
            INSERT INTO onto_template_arm_stats(template_id, arm, n, mean_time, var_time, last_update)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(template_id, arm) DO UPDATE SET
                n         = excluded.n,
                mean_time = excluded.mean_time,
                var_time  = excluded.var_time,
                last_update = excluded.last_update
            """, (template_id, int(arm), n_new, mean_new, m2_new, now))
        conn.commit()
