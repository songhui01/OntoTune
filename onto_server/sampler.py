from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
import json, hashlib, math, random
import numpy as np
from collections import defaultdict

# -------------------------- helpers --------------------------

def _safe_get_arm(meta: Dict[str, Any], num_arms: int) -> int:
    idx = meta.get("arm_config_json", {}).get("index", -1)
    if isinstance(idx, bool):
        idx = int(idx)
    if not isinstance(idx, int):
        return -1
    return idx if 0 <= idx < num_arms else -1

def _build_feature_matrix(meta: Dict[str, Any], num_arms: int) -> np.ndarray:
    # lazy import to avoid heavy deps at module import
    import featurize
    X = featurize.build_feature_matrix(meta, num_arms).T  # (C, R)
    return X

def _split_sql_vs_arm(X: np.ndarray, num_arms: int) -> Tuple[np.ndarray, np.ndarray]:
    """Return (sql_only, arm_onehot_rows) flattened."""
    # X shape (C, R). The last num_arms rows in featurize are arm rows by convention.
    C = X.shape[0]
    arm_rows = X[-num_arms:] if C >= num_arms else np.zeros((num_arms, X.shape[1]))
    sql_rows = X[:-num_arms] if C > num_arms else X * 0  # if not enough rows, zero
    return sql_rows.reshape(-1).astype(np.float32), arm_rows.reshape(-1).astype(np.float32)

def _cosine(a: np.ndarray, b: np.ndarray, eps: float = 1e-8) -> float:
    na = np.linalg.norm(a); nb = np.linalg.norm(b)
    if na < eps or nb < eps: 
        return 0.0
    return float(np.dot(a, b) / (na * nb))

def _hash_sql(sql_vec: np.ndarray) -> str:
    # Use a robust hash of the SQL-only vector (ignoring arm) to get a template bucket
    h = hashlib.sha1(sql_vec.tobytes()).hexdigest()
    return h

@dataclass
class Item:
    meta: Dict[str, Any]
    reward: float
    arm: int
    v_sql: np.ndarray  # flattened SQL-only embedding
    v_all: np.ndarray  # flattened whole embedding (with arm)
    template: str      # hashed sql-only template id
    raw: Tuple[str, float]  # (plan_json_str, reward)

# -------------------------- main selection --------------------------

def _prep_items(examples: List[Tuple[str, float]], num_arms: int) -> List[Item]:
    items: List[Item] = []
    for plan_str, reward in examples:
        try:
            plan = json.loads(plan_str)
            meta = plan.get("metadata", plan)  # tolerate different shapes
        except Exception:
            continue
        arm = _safe_get_arm(meta, num_arms)
        if arm < 0:
            continue
        try:
            X = _build_feature_matrix(meta, num_arms)
        except Exception:
            continue
        v_sql, v_arm = _split_sql_vs_arm(X, num_arms)
        v_all = np.concatenate([v_sql, v_arm], axis=0)
        template = _hash_sql(v_sql)
        items.append(Item(meta=meta, reward=float(reward), arm=arm, v_sql=v_sql, v_all=v_all, template=template, raw=(plan_str, float(reward))))
    return items

def _maxmin_diverse(items: List[Item], k: int, dup_cos=0.995) -> List[int]:
    """k-center greedy on v_sql to maximize diversity; drop near-duplicates by cosine."""
    if not items:
        return []
    # pick the farthest-from-mean first
    vecs = np.stack([it.v_sql for it in items], axis=0)
    mean = vecs.mean(axis=0, keepdims=True)
    d = np.sum((vecs - mean) ** 2, axis=1)
    selected = [int(np.argmax(d))]
    # distances to nearest selected
    nn_dist = np.linalg.norm(vecs - vecs[selected[0]], axis=1)
    while len(selected) < min(k, len(items)):
        nxt = int(np.argmax(nn_dist))
        # duplicate check
        is_dup = False
        for j in selected:
            if _cosine(vecs[nxt], vecs[j]) >= dup_cos:
                is_dup = True
                break
        if not is_dup:
            selected.append(nxt)
        # update dists
        nn_dist = np.minimum(nn_dist, np.linalg.norm(vecs - vecs[nxt], axis=1))
        # early break if no progress
        if len(selected) >= k:
            break
    return selected

def select_samples_budgeted(
    examples: List[Tuple[str, float]],
    num_arms: int = 7,
    budget: int = 100,
    per_template_cap: int = 10,
    arm_min_coverage: int = 3,
    hard_tail_ratio: float = 0.20,
    template_getter=None,
    hard_cap: int | None = None,
) -> List[Tuple[str, float]]:
    """
    Return a subset of examples within `budget` following coverage and diversity rules.
    - Include worst (largest) rewards top p% as "hard" set.
    - Enforce per-template cap.
    - Ensure each arm has at least `arm_min_coverage` if available.
    - Fill the rest via MaxMin diversity on SQL-only embeddings.
    """
    def _tpl_key(it):
        # 优先用外部提供的新模板键
        if template_getter is not None:
            try:
                k = template_getter(it)
                if k is not None:
                    return k
            except Exception:
                pass
        # 回退到旧模板：SQL-only 哈希/聚类
        return it.template   # ← 这里保持你原来的字段/算法
    items = _prep_items(examples, num_arms=num_arms)
    if len(items) <= budget:
        return [it.raw for it in items]

    # 1) Hard tail (largest reward are worst latency)
    n_hard = int(math.ceil(len(items) * hard_tail_ratio))
    if hard_cap is not None:
        n_hard = min(n_hard, hard_cap)
    hard_idx = np.argsort([it.reward for it in items])[-n_hard:].tolist()
    hard_set = set(hard_idx)

    # 2) Enforce per-template cap: within each template, keep up to cap prioritizing hard then diverse
    by_template: Dict[str, List[int]] = defaultdict(list)
    for i, it in enumerate(items):
        tkey = _tpl_key(it)
        by_template[tkey].append(i)
    kept = []
    for tpl, idxs in by_template.items():
        # split hard vs non-hard
        hard = [i for i in idxs if i in hard_set]
        non  = [i for i in idxs if i not in hard_set]
        chosen = hard[:per_template_cap]
        # fill with diverse among remaining of this template
        if len(chosen) < per_template_cap and non:
            sub = [items[i] for i in non]
            pick_rel = _maxmin_diverse(sub, k=per_template_cap - len(chosen))
            chosen.extend([non[j] for j in pick_rel])
        kept.extend(chosen)
    # de-duplicate
    kept = list(dict.fromkeys(kept))

    # 3) Arm coverage: ensure min coverage per arm by pulling additional examples if needed
    counts_by_arm = {a:0 for a in range(num_arms)}
    for i in kept:
        counts_by_arm[items[i].arm] += 1
    need_more = [(a, arm_min_coverage - c) for a, c in counts_by_arm.items() if arm_min_coverage - c > 0]
    if need_more:
        for a, need in need_more:
            candidates = [i for i, it in enumerate(items) if it.arm == a and i not in kept]
            if not candidates:
                continue
            # pick diverse within arm
            sub = [items[i] for i in candidates]
            pick_rel = _maxmin_diverse(sub, k=need)
            kept.extend([candidates[j] for j in pick_rel])

    # 4) Global budget fill with MaxMin across remaining pool
    if len(kept) < budget:
        remaining = [i for i in range(len(items)) if i not in kept]
        sub = [items[i] for i in remaining]
        need = budget - len(kept)
        pick_rel = _maxmin_diverse(sub, k=need)
        kept.extend([remaining[j] for j in pick_rel])

    # 5) If over budget (unlikely), trim by removing most redundant
    if len(kept) > budget:
        kept = kept[:budget]

    # Stable order for reproducibility
    kept = sorted(kept)
    return [items[i].raw for i in kept]
