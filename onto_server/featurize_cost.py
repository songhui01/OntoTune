
import numpy as np

GCS_CANON_KEYS = [
  "cost_L1_share","cost_L2_share","cost_L3plus_share",
  "cost_op_NestedLoop_share","cost_op_HashJoin_share","cost_op_MergeJoin_share",
  "cost_op_SeqScan_share","cost_op_IndexScan_share","cost_op_IndexOnlyScan_share",
  "cost_op_BitmapIndexScan_share","cost_op_Sort_share","cost_op_Aggregate_share","cost_op_Unique_share"
]

def _safe_div(a, b, eps=1e-9):
    return float(a) / float(b + eps)

def _sum_total_cost(plan_root):
    total = 0.0
    def walk(p):
        nonlocal total
        if not isinstance(p, dict):
            return
        total += float(p.get("Total Cost", 0.0))
        for ch in p.get("Plans", []) or []:
            walk(ch)
    walk(plan_root)
    return total

def _iter_nodes(plan_root):
    stack = [plan_root] if isinstance(plan_root, dict) else []
    while stack:
        n = stack.pop()
        yield n
        ch = n.get("Plans") or []
        for c in ch:
            if isinstance(c, dict):
                stack.append(c)

def _collect_cols_from_keys(keys):
    cols = set()
    for k in keys or []:
        if isinstance(k, dict):
            relid = k.get("relid")
            attnum = k.get("attnum")
            if relid is not None and attnum is not None:
                cols.add((int(relid), int(attnum)))
    return cols

def _add(d, key, val):
    d[key] = d.get(key, 0.0) + float(val)

# ---------- Main cost distribution ----------

def distribute_costs_to_tables_and_columns(meta, plan_root):
    total_cost_all = _sum_total_cost(plan_root)
    meta.setdefault("_table_costs", {})
    meta.setdefault("_col_costs", {})

    def add_table(relid, tag, val):
        bucket = meta["_table_costs"].setdefault(int(relid), {})
        _add(bucket, tag, val)

    def add_col(relid, attnum, tag, val):
        bucket = meta["_col_costs"].setdefault((int(relid), int(attnum)), {})
        _add(bucket, tag, val)

    # layer aggregation
    layer_cost = {}
    op_cost = {}

    for n in _iter_nodes(plan_root):
        c = float(n.get("Total Cost", 0.0))
        depth = int(n.get("Plan Depth", n.get("_depth", 0)))
        node_type = n.get("Node Type", "")
        layer_cost[depth] = layer_cost.get(depth, 0.0) + c
        op_cost[node_type] = op_cost.get(node_type, 0.0) + c

        if node_type in ("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Bitmap Index Scan"):
            relid = n.get("Relation ID") or n.get("Relation Oid") or n.get("relid") or n.get("on_relid")
            if relid is not None:
                add_table(relid, "table_scan_cost", c)
            # Quals columns
            quals = n.get("Quals") or n.get("Filter Quals") or []
            used = set()
            for q in quals:
                if not isinstance(q, dict):
                    continue
                col = q.get("column")
                if isinstance(col, dict) and "relid" in col and "attnum" in col:
                    used.add((col["relid"], col["attnum"]))
            for (r,a) in used:
                add_col(r, a, "col_cost_from_scan", c / max(1, len(used)))

        # Join-like
        elif node_type in ("Hash Join", "Merge Join", "Nested Loop"):
            keys = n.get("Join Keys") or n.get("join_keys") or []
            L = set()
            R = set()
            for key in keys:
                if not isinstance(key, dict):
                    continue
                if "left" in key and isinstance(key["left"], dict):
                    L.add((key["left"].get("relid"), key["left"].get("attnum")))
                if "right" in key and isinstance(key["right"], dict):
                    R.add((key["right"].get("relid"), key["right"].get("attnum")))
            L = {(r,a) for (r,a) in L if r is not None and a is not None}
            R = {(r,a) for (r,a) in R if r is not None and a is not None}

            # 40% to join key cols, split half/half
            key_share = 0.4 * c
            if L:
                each = (key_share * 0.5) / len(L)
                for (r,a) in L:
                    add_col(r, a, "col_cost_from_join_share_build", each)
            if R:
                each = (key_share * 0.5) / len(R)
                for (r,a) in R:
                    add_col(r, a, "col_cost_from_join_share_probe", each)

            # 60% to tables (half/half if unknown proportions)
            table_share = 0.6 * c
            L_tables = {r for (r,_) in L}
            R_tables = {r for (r,_) in R}
            for r in L_tables:
                add_table(r, "table_join_cost", table_share * 0.5 / max(1, len(L_tables)))
            for r in R_tables:
                add_table(r, "table_join_cost", table_share * 0.5 / max(1, len(R_tables)))

        elif node_type == "Sort":
            keys = n.get("Sort Keys") or n.get("sort_keys") or []
            if keys:
                each = c / len(keys)
                for k in keys:
                    if not isinstance(k, dict): 
                        continue
                    r = k.get("relid"); a = k.get("attnum")
                    if r is not None and a is not None:
                        add_table(r, "table_sort_cost", each)
                        add_col(r, a, "col_cost_from_sort", each)

        elif node_type in ("Aggregate", "GroupAggregate", "HashAggregate"):
            keys = n.get("Group Keys") or n.get("agg_keys") or []
            aggs = n.get("Aggs") or n.get("aggs") or []
            g_share = 0.5 * c; a_share = 0.5 * c
            if keys:
                each = g_share / len(keys)
                for k in keys:
                    if not isinstance(k, dict): continue
                    r = k.get("relid"); a = k.get("attnum")
                    if r is not None and a is not None:
                        add_table(r, "table_agg_cost", each)
                        add_col(r, a, "col_cost_from_agg", each)
            arg_cols = []
            for a in aggs:
                ar = a.get("arg_relid")
                at = a.get("arg_attnum")
                if ar is not None and at is not None:
                    arg_cols.append((ar, at))
            if arg_cols:
                each = a_share / len(arg_cols)
                for (r, a) in arg_cols:
                    add_table(r, "table_agg_cost", each)
                    add_col(r, a, "col_cost_from_agg", each)

    # normalize to shares
    for r, d in meta["_table_costs"].items():
        for k in list(d.keys()):
            if not k.endswith("_share"):
                d[k + "_share"] = _safe_div(d[k], total_cost_all)
    for k, d in meta["_col_costs"].items():
        for kk in list(d.keys()):
            if not kk.endswith("_share"):
                d[kk + "_share"] = _safe_div(d[kk], total_cost_all)

    l1 = layer_cost.get(0, 0.0)
    l2 = layer_cost.get(1, 0.0)
    l3p = sum(v for dep, v in layer_cost.items() if dep >= 2)
    global_cost = {
        "cost_L1_share": _safe_div(l1, total_cost_all),
        "cost_L2_share": _safe_div(l2, total_cost_all),
        "cost_L3plus_share": _safe_div(l3p, total_cost_all),
    }
    # Op shares
    for optype, val in op_cost.items():
        key = f"cost_op_{optype.replace(' ', '')}_share"
        global_cost[key] = _safe_div(val, total_cost_all)

    meta.setdefault("global_cost_shares", {}).update(global_cost)
    return meta

# ---------- matrix extension ----------

def append_cost_rows_to_matrix(feature_matrix, row_names, metadata_json, table_list, attr_list):
    table_index_map = {name: idx for idx, name in enumerate(table_list)}
    attr_index_map  = {name: idx + len(table_list) for idx, name in enumerate(attr_list)}

    new_rows = [
        "col_cost_from_scan_share",
        "col_cost_from_join_share_build_share",
        "col_cost_from_join_share_probe_share",
        "col_cost_from_sort_share",
        "col_cost_from_agg_share",
    ]

    import numpy as _np
    fm = feature_matrix
    rows_old, cols = fm.shape
    rows_new = rows_old + len(new_rows)
    out = _np.zeros((rows_new, cols), dtype=fm.dtype)
    out[:rows_old, :] = fm
    row_map = {name: idx for idx, name in enumerate(row_names)}
    # assign new row indices
    for i, rn in enumerate(new_rows):
        row_map[rn] = rows_old + i

    # Write column-level costs
    col_costs = metadata_json.get("_col_costs", {})
    for key, d in col_costs.items():
        relid, attnum = key
        found = None
        for a in metadata_json.get("attributes", []):
            if a.get("relid") == relid and a.get("attnum") == attnum:
                found = a.get("name")
                break
        if not found:
            # try table.column
            continue
        col_name = found
        if col_name not in attr_index_map:
            continue
        j = attr_index_map[col_name]
        for rn in new_rows:
            val = d.get(rn, 0.0)
            out[row_map[rn], j] = 1 if isinstance(val, (bool, int)) else float(val)

    new_row_names = row_names + new_rows
    return out, new_row_names
