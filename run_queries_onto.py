import psycopg2
import os
import sys
import random
import uuid
import sql_store
from time import time, sleep

USE_ONTO = True
PG_CONNECTION_STR = "dbname=so_pg user=postgres host=localhost  options='-c statement_timeout=600000'"

# https://stackoverflow.com/questions/312443/
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def run_query(sql, sequence_id, onto_select=False, onto_reward=False):
    start = time()
    while True:
        try:
            conn = psycopg2.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute("SET onto_sequence_id TO %s", (str(sequence_id),))  
            cur.execute(f"SET enable_onto TO {onto_select or onto_reward}")
            cur.execute(f"SET enable_onto_selection TO {onto_select}")
            cur.execute(f"SET enable_onto_rewards TO {onto_reward}")
            cur.execute(f"SET pg_selection TO {False}") # True or False
            cur.execute("SET onto_num_arms TO 6")
            cur.execute("SET onto_num_queries_per_round TO 25")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(sql)
            cur.fetchall()
            conn.close()
            break
        except psycopg2.errors.QueryCanceled:
            print("Query timed out after 600 seconds.")
            conn.close()
            break
        except Exception as e:
            print("Retrying query due to error:", e)
            sleep(1)
            continue
    stop = time()
    return stop - start

        
query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read()
    queries.append((fp, query))
print("Read", len(queries), "queries.")
# Initialize the local SQL cache (SQLite)
sql_store.init()
print("Using Onto:", USE_ONTO)

random.seed(42)
query_sequence = random.choices(queries, k=500) #500

pg_chunks, *onto_chunks = list(chunks(query_sequence, 25)) #25 if this number is changed, remember to change line 29!!!
print("Total queries to run:", len(query_sequence))


# === 预分配所有 sequence_id，并一次性批量保存到 sql_cache ===
seq_pairs = []             # List[(sequence_id, sql)]
pg_entries = []            # List[{"fp","sql","sid"}]
onto_entries = []          # List[List[{"fp","sql","sid"}]] 逐 chunk

ts0 = int(time() * 1000)
# PG 预热块
for i, (fp, q) in enumerate(pg_chunks, 1):
    sid = f"pg-{ts0}-{i}"
    pg_entries.append({"fp": fp, "sql": q, "sid": sid})
    seq_pairs.append((sid, q))

# onto 块
base = ts0 + 1
for c_idx, chunk in enumerate(onto_chunks):
    cur = []
    for q_idx, (fp, q) in enumerate(chunk):
        # 保持你原先的命名风格
        sid = f"onto-{c_idx}-{q_idx}-{base}"
        cur.append({"fp": fp, "sql": q, "sid": sid})
        seq_pairs.append((sid, q))
        base += 1
    onto_entries.append(cur)

# ✅ 一次性批量落库（不再在 run_query 内逐条保存）
sql_store.bulk_save_sql(seq_pairs)
print(f"[INFO] Bulk-saved {len(seq_pairs)} SQLs into sql_cache.")

print("Executing queries using PG optimizer for initial training")

# === 执行：直接复用预生成的 sid ===
# PG 预热
for e in pg_entries:
    pg_time = run_query(e["sql"], e["sid"], onto_reward=True)
    print("x", "x", time(), e["fp"], pg_time, "PG", e["sid"], flush=True)

# onto 轮次
for c_idx, chunk in enumerate(onto_entries):
    if USE_ONTO:
        os.system("cd onto_server && python3 ontoctl.py --retrain")
        os.system("sync")
    for q_idx, e in enumerate(chunk):
        q_time = run_query(e["sql"], e["sid"], onto_reward=USE_ONTO, onto_select=USE_ONTO)
        print(c_idx, q_idx, time(), e["fp"], q_time, e["sid"], flush=True)