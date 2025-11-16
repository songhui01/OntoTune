# onto_server/onto_utils_template.py
import hashlib
import json

def _b(meta, k):         return bool(meta.get(k, False))
def _i(meta, k, d=0):    return int(meta.get(k, d))

def _count_ops(plan_node):
    typ = (plan_node.get("Node Type") or "?").upper()
    kids = plan_node.get("Plans", []) or []
    res = {"JOIN":0,"NL":0,"HASH":0,"MERGE":0,"SCAN":0,"SORT":0,"AGG":0,"SUBQ":0}
    if "JOIN" in typ:
        res["JOIN"] += 1
        if "NESTED" in typ: res["NL"] += 1
        if "HASH"   in typ: res["HASH"] += 1
        if "MERGE"  in typ: res["MERGE"] += 1
    elif "SCAN" in typ:
        res["SCAN"] += 1
    elif "SORT" in typ:
        res["SORT"] += 1
    elif "AGG" in typ or "AGGREGATE" in typ or "GROUP" in typ:
        res["AGG"] += 1
    elif "SUBQUERY" in typ:
        res["SUBQ"] += 1
    for k in kids:
        c = _count_ops(k)
        for key in res: res[key] += c[key]
    for key in res: res[key] = min(res[key], 4)
    return res

def _bucket_from_tables(table_features, key, cuts):
    vals = [float(tf.get(key, 0.0)) for tf in (table_features or [])]
    if not vals: return 0
    v = max(vals)
    if v < cuts[0]: return 0
    if v <= cuts[1]: return 1
    return 2

def template_from_plan_meta(plan_json: dict, meta: dict) -> str:

    tables_ordered, _, summary = extract_tables_flags(meta)
    tables_in_sql = summary["tables_in_sql"]  # 只含 inSQL=True 的表

    plan_root = plan_json.get("Plan", {}) if plan_json else {}
    shape = _count_ops(plan_root)
    sem = {
        "has_distinct": bool(meta.get("has_distinct", False)),
        "has_exists": bool(meta.get("has_exists", False)),
        "has_not_exists": bool(meta.get("has_not_exists", False)),
        "has_non_equi_pred": bool(meta.get("has_non_equi_pred", False)),
        "post_link_present": bool(meta.get("post_link_present", False)),
        "post_link_occurs_2plus": bool(meta.get("post_link_occurs_2plus", False)),

        "group_by_cols_bucket": int(meta.get("group_by_cols_bucket", 0)),  # 0/1/2/3
        "rows_bucket": int(meta.get("rows_bucket", 0)),                    # 0/1/2
    }

    key_obj = {
        "tables_in_sql": tables_in_sql,
        "shape": {
            "JOIN": int(shape.get("JOIN", 0)),
            "SCAN": int(shape.get("SCAN", 0)),
            "SORT": int(shape.get("SORT", 0)),
            "AGG":  int(shape.get("AGG", 0)),
        },
        "sem": sem,
    }
    key_str = json.dumps(key_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(key_str.encode("utf-8")).hexdigest()

def extract_tables_flags(meta: dict):
    tf_list = meta.get("table-features", [])
    tf_map = {tf.get("name"): tf for tf in tf_list if tf.get("name")}

    tables_ordered = meta.get("tables")
    if not tables_ordered:
        tables_ordered = sorted(tf_map.keys())

    flags_by_table = {}
    for t in tables_ordered:
        tf = tf_map.get(t, {})
        flags_by_table[t] = {
            "inSQL":      bool(tf.get("inSQL", False)),
            "inWhere":    bool(tf.get("hasInWhere", False)),
            "inJoin":     bool(tf.get("hasInJoin", False)),
            "inGroup":    bool(tf.get("hasInGroup", False)),
            "inSort":     bool(tf.get("hasInSort", False)),
            "hasNumeric": bool(tf.get("hasNumeric", False)),
            "hasIndex":   bool(tf.get("hasIndex", False)),
            "hasCorr":    bool(tf.get("hasCorr", False)),
        }

    tables_in_sql = [t for t in tables_ordered if flags_by_table.get(t, {}).get("inSQL", False)]
    summary = {
        "tables_in_sql": tables_in_sql,
        "n_in_join":  sum(1 for t in tables_ordered if flags_by_table[t]["inJoin"]),
        "n_in_group": sum(1 for t in tables_ordered if flags_by_table[t]["inGroup"]),
        "n_in_sort":  sum(1 for t in tables_ordered if flags_by_table[t]["inSort"]),
        "n_in_where": sum(1 for t in tables_ordered if flags_by_table[t]["inWhere"]),
    }
    return tables_ordered, flags_by_table, summary

