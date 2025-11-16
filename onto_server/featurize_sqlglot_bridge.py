
"""
featurize_sqlglot_bridge.py
"""
from typing import Dict, List

def _ensure_attr_flag(meta: Dict, name: str) -> Dict:
    found = None
    for a in meta.get("attributes", []):
        if a.get("name") == name:
            found = a; break
    if not found:
        found = {"name": name, "inSQL": True, "inWhere": False, "inJoin": False,
                 "inGroup": False, "inSort": False, "isNumeric": False,
                 "hasIndex": False, "correlationAbove0.9": False}
        meta.setdefault("attributes", []).append(found)
    else:
        found["inSQL"] = True
    return found

def _touch_table(meta: Dict, tname: str) -> Dict:
    tf = None
    for t in meta.get("table-features", []):
        if t.get("name") == tname:
            tf = t; break
    if not tf:
        tf = {"name": tname, "inSQL": True, "hasInWhere": False, "hasInJoin": False,
              "hasInGroup": False, "hasInSort": False, "hasNumeric": False,
              "hasIndex": False, "hasCorr": False}
        meta.setdefault("table-features", []).append(tf)
    else:
        tf["inSQL"] = True
    return tf

def merge_parsed_sqlglot_into_meta(meta: Dict, parsed: Dict) -> Dict:
    tables: List[str] = parsed.get("tables", [])
    joins  = parsed.get("joins", [])
    where_cols: List[str] = parsed.get("where_cols", [])
    group_by: List[str] = parsed.get("group_by", [])
    order_by: List[str] = [x[0] for x in parsed.get("order_by", [])]
    aggregates: List[Dict] = parsed.get("aggregates", [])
    has_distinct = parsed.get("has_distinct", False)

    for t in tables:
        _touch_table(meta, t)

    for j in joins:
        for side in ("left_col","right_col"):
            col = j.get(side)
            if not col: continue
            if "." in col:
                t, c = col.split(".", 1)
                _touch_table(meta, t)["hasInJoin"] = True
                _ensure_attr_flag(meta, f"{t}.{c}")["inJoin"] = True

    for col in where_cols:
        if "." in col:
            t, c = col.split(".", 1)
            _touch_table(meta, t)["hasInWhere"] = True
            _ensure_attr_flag(meta, f"{t}.{c}")["inWhere"] = True

    for expr in group_by:
        if "." in expr:
            t, c = expr.split(".", 1)
            _touch_table(meta, t)["hasInGroup"] = True
            _ensure_attr_flag(meta, f"{t}.{c}")["inGroup"] = True

    for expr in order_by:
        if "." in expr:
            t, c = expr.split(".", 1)
            _touch_table(meta, t)["hasInSort"] = True
            _ensure_attr_flag(meta, f"{t}.{c}")["inSort"] = True

    for a in aggregates:
        arg = a.get("arg")
        if not arg: continue
        if "." in arg or arg == "*":
            if arg != "*":
                t, c = arg.split(".", 1)
                af = _ensure_attr_flag(meta, f"{t}.{c}")
                af[f"has_{a['agg']}"] = True
                if a.get("is_distinct"):
                    af["has_count_distinct"] = True
                _touch_table(meta, t)["hasInGroup"] = True
            else:
                # COUNT(*) -> global flag
                meta.setdefault("sql_flags", {})["has_count_star"] = True

    meta.setdefault("sql_flags", {})["has_distinct"] = bool(has_distinct)
    return meta

def merge_semantics_into_meta(meta: dict, sem: dict) -> dict:
    tf = meta.setdefault("template_features", {})
    def seti(k, v): tf[k] = int(bool(v))

    seti("has_window",      sem.get("num_window", 0) > 0)
    seti("has_like",        sem.get("has_like", False))
    seti("has_between",     sem.get("has_between", False))
    seti("has_in",          sem.get("has_in", False))
    seti("has_isnull",      sem.get("has_isnull", False))
    seti("has_case",        sem.get("has_case", False))
    meta.setdefault("sql_flags", {})["limit_val"] = sem.get("limit_val")

    meta["template_features"]["num_join_bucket"]     = int(0 if sem["num_join"] == 0 else (1 if sem["num_join"] <= 2 else 2))
    meta["template_features"]["num_aggs_bucket"]     = int(0 if sem["num_aggs"] == 0 else (1 if sem["num_aggs"] <= 2 else 2))
    meta["template_features"]["num_cte_bucket"]      = int(0 if sem["num_cte"] == 0 else 1)
    meta["template_features"]["num_subquery_bucket"] = int(0 if sem["num_subquery"] == 0 else 1)
    meta["template_features"]["num_agg_distinct"]    = int(sem.get("num_agg_distinct", 0))

    meta["join_edges"] = sem.get("join_edges", []) 
    return meta
