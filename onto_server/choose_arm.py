# choose_arm.py
import math, numpy as np, random
from typing import Dict, Optional, Tuple, List

def _decayed_epsilon(n: int, eps0: float = 0.2, eps_min: float = 0.02) -> float:
    return max(eps0 / math.sqrt(max(1, n)), eps_min)

def _impact_gate(est_cost: Optional[float], cap: float = 5.0) -> float:
    if est_cost is None: return 1.0
    x = min(max(est_cost, 0.0), cap)
    return 1.0 / (1.0 + x)

def _optimistic_scores(scores: np.ndarray,
                       tpl_arm_stats: Dict[int, Dict[str, float]],
                       beta: float = 0.0,
                       higher_is_better: bool = False) -> np.ndarray:
    s = scores.astype(float).copy()
    if beta <= 0.0 or not tpl_arm_stats:
        return s
    for a, st in tpl_arm_stats.items():
        n = max(1.0, float(st.get("n", 0.0)))
        unc = 1.0 / math.sqrt(n)
        if not higher_is_better: s[a] -= beta * unc
        else:                    s[a] += beta * unc
    return s

def _to_py_int_list(arr: np.ndarray) -> List[int]:
    return [int(x) for x in np.ravel(arr).tolist()]

def choose_arm(template_id: str,
               model_scores,
               tpl_seen_n: int,
               min_seen_tpl: int,  
               tpl_arm_stats: Dict[int, Dict[str, float]],
               est_cost: Optional[float] = None,
               top_k: int = 3,
               avoid_bottom_m: int = 1,
               eps0: float = 0.2,
               eps_min: float = 0.02,
               optimism_beta: float = 0.0,
               higher_is_better: bool = False) -> Tuple[int, dict]:

    # 1) normalize the scores to be one dimensional float array 
    scores = np.asarray(model_scores, dtype=float).reshape(-1)

    K = max(1, min(int(top_k), scores.shape[0]))
    eps_base = _decayed_epsilon(int(tpl_seen_n), eps0, eps_min)
    gate = _impact_gate(est_cost)
    eps = max(eps_min, eps_base * gate)

    # 2) original sorting & argmin
    order_model = np.argsort(-scores) if higher_is_better else np.argsort(scores)
    order_model = _to_py_int_list(order_model)
    argmin_arm = order_model[0] if order_model else 0

    # 3) ranking after optimistic upper boundary
    s_opt = _optimistic_scores(scores, tpl_arm_stats, optimism_beta, higher_is_better)
    order = np.argsort(-s_opt) if higher_is_better else np.argsort(s_opt)
    order = _to_py_int_list(order)
    
    seen_map = {a: int(tpl_arm_stats.get(a, {}).get("n", 0)) for a in range(scores.shape[0])}

    # 4) reverse logic, and remove the last m
    m = 0
    if avoid_bottom_m and len(order) > 1:
        m = max(0, min(int(avoid_bottom_m), len(order) - 1))
    banned = set(order[-m:]) if m > 0 else set()

    # 4.1) filter out the arms that not within the threshold -- not let them in the candidates but can be explored later
    topk = order[:K]
    eligible = [a for a in topk if seen_map.get(a, 0) >= min_seen_tpl]
    if not eligible:
        eligible = [topk[0]]  # at least one

    # 5) candidate = eligible minus banned
    candidates = [a for a in eligible if a not in banned] or [eligible[0]]
    unseen_pool = [a for a in range(len(scores)) if seen_map.get(a, 0) < min_seen_tpl]

    # 6) ε exploration / exploitation
    if (len(candidates) > 1) and (random.random() < eps):
        mode = "explore"
        chosen = random.choice(candidates)
    else:
        mode = "exploit"
        chosen = candidates[0]

    trace = {
        "template_id": str(template_id),
        "tpl_seen_n": int(tpl_seen_n),
        "est_cost": float(est_cost) if est_cost is not None else None,
        "eps_base": float(eps_base),
        "gate": float(gate),
        "eps": float(eps),
        "top_k": int(K),
        "order_model": order_model,
        "order_after_optimism": order,
        "argmin_arm": int(argmin_arm),
        "banned_bottom": sorted(list(banned)),
        "candidates": candidates,
        "chosen": int(chosen),
        "mode": mode,
        "higher_is_better": bool(higher_is_better),
        "optimism_beta": float(optimism_beta),
    }
    return int(chosen), trace
