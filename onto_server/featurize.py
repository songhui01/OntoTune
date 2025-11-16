import numpy as np
import general

from featurize_cost import (
    distribute_costs_to_tables_and_columns,
    append_cost_rows_to_matrix,
    GCS_CANON_KEYS,
)

JOIN_TYPES = ["Nested Loop", "Hash Join", "Merge Join"]
LEAF_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Index Scan"]
ALL_TYPES = JOIN_TYPES + LEAF_TYPES

def build_adjacency_matrix(metadata_json):
    table_list = metadata_json.get("tables", [])

    attr_list = []
    table_attr_map = {} # table_name -> list of attribute full names

    for table_name in table_list:
        cols = metadata_json.get(table_name, [])
        full_attr_names = [f"{table_name}.{col}" for col in cols]
        attr_list.extend(full_attr_names)
        table_attr_map[table_name] = full_attr_names

    # total node number = no. of tables + no. of attributes
    num_nodes = len(table_list) + len(attr_list)
    adjacency_matrix = np.zeros((num_nodes, num_nodes), dtype=int)


    # table index: 0 ~ len(table_list)-1
    # attr index:  len(table_list) ~ num_nodes-1

    table_index_map = {name: idx for idx, name in enumerate(table_list)}
    attr_index_map = {name: idx + len(table_list) for idx, name in enumerate(attr_list)}

    # attribute is in a table
    for table, attrs in table_attr_map.items():
        t_idx = table_index_map[table]
        for attr in attrs:
            a_idx = attr_index_map[attr]
            adjacency_matrix[t_idx][a_idx] = 1
            adjacency_matrix[a_idx][t_idx] = 1 # undirected

    # two attributes has an edge if they are in the same table
    for attrs in table_attr_map.values():
        for i in range(len(attrs)):
            for j in range(i + 1, len(attrs)):
                a_i = attr_index_map[attrs[i]]
                a_j = attr_index_map[attrs[j]]
                adjacency_matrix[a_i][a_j] = 1
                adjacency_matrix[a_j][a_i] = 1  # undirected

    return adjacency_matrix

def build_feature_matrix(metadata_json, num_arms=5, plan=None):
    """
    Build a binary feature matrix with both SQL metadata and arm configuration.

    Rows:
        0: Attribute is numeric
        1: Table or Attribute appears in SQL
        2: Attribute appears in WHERE
        3: Attribute appears in JOIN
        4: Attribute has index
        5: Attribute has in GROUP BY
        6: Attribute has correlation > 0.9
        7: Attribute has in ORDER BY
        ......

    Columns = [tables + attributes]
    """
    table_list = metadata_json.get("tables", [])

    attr_list = []
    for table_name in table_list:
        attr_list.extend([f"{table_name}.{col}" for col in metadata_json.get(table_name, [])])

    num_cols = len(table_list) + len(attr_list)

    # current rows
    row_names = [
        "isNumeric", "inSQL", "inWhere", "inJoin", "hasIndex", "inGroup",
        "correlationAbove0.9", "inSort"
    ]

    # template rows
    tpl_flag_rows = [
        "tpl_has_distinct",
        "tpl_has_exists",
        "tpl_has_not_exists",
        "tpl_has_non_equi_pred",
        "tpl_need_sort_for_merge",
        "tpl_post_link_present",
        "tpl_post_link_occurs_2plus",
    ]
    tpl_bucket_defs = {
        "tpl_group_by_cols_bucket": 4,  # 0..3
        "tpl_rows_bucket": 3,           # 0..2
    }
    template_feature_rows = tpl_flag_rows[:]
    for base, K in tpl_bucket_defs.items():
        for b in range(K):
            template_feature_rows.append(f"{base}_{b}")

    # rows and matrixs
    row_names = row_names + template_feature_rows
    num_rows = len(row_names)
    feature_matrix = np.zeros((num_rows, num_cols), dtype=np.float32)
    row_map = {name: idx for idx, name in enumerate(row_names)}

    # Table-level feature mapping
    table_feature_map = {tf["name"]: tf for tf in metadata_json.get("table-features", [])}
    for i, table in enumerate(table_list):
        tf = table_feature_map.get(table, {})
        feature_matrix[row_map["isNumeric"]][i] = int(tf.get("hasNumeric", False))
        feature_matrix[row_map["inSQL"]][i] = int(tf.get("inSQL", False))
        feature_matrix[row_map["inWhere"]][i] = int(tf.get("hasInWhere", False))
        feature_matrix[row_map["inJoin"]][i] = int(tf.get("hasInJoin", False))
        feature_matrix[row_map["hasIndex"]][i] = int(tf.get("hasIndex", False))
        feature_matrix[row_map["inGroup"]][i] = int(tf.get("hasInGroup", False))
        feature_matrix[row_map["correlationAbove0.9"]][i] = int(tf.get("hasCorr", False))
        feature_matrix[row_map["inSort"]][i] = int(tf.get("hasInSort", False))

    # Attribute-level feature mapping
    attr_map = {attr["name"]: attr for attr in metadata_json.get("attributes", [])}
    for j, full_name in enumerate(attr_list):
        col_idx = len(table_list) + j
        attr = attr_map.get(full_name, {})
        feature_matrix[row_map["isNumeric"]][col_idx] = int(attr.get("isNumeric", False))
        feature_matrix[row_map["inSQL"]][col_idx] = int(attr.get("inSQL", False))
        feature_matrix[row_map["inWhere"]][col_idx] = int(attr.get("inWhere", False))
        feature_matrix[row_map["inJoin"]][col_idx] = int(attr.get("inJoin", False))
        feature_matrix[row_map["hasIndex"]][col_idx] = int(attr.get("hasIndex", False))
        feature_matrix[row_map["inGroup"]][col_idx] = int(attr.get("inGroup", False))
        feature_matrix[row_map["correlationAbove0.9"]][col_idx] = int(attr.get("correlationAbove0.9", False))
        feature_matrix[row_map["inSort"]][col_idx] = int(attr.get("inSort", False))

    # template feature 
    tf = metadata_json.get("template_features", {}) or {}
    def _get(k, default=0):
        return tf.get(k, metadata_json.get(k, default))

    def _set_all(row_name, v):
        feature_matrix[row_map[row_name], :] = int(bool(v))

    _set_all("tpl_has_distinct",         _get("has_distinct", 0))
    _set_all("tpl_has_exists",           _get("has_exists", 0))
    _set_all("tpl_has_not_exists",       _get("has_not_exists", 0))
    _set_all("tpl_has_non_equi_pred",    _get("has_non_equi_pred", 0))
    _set_all("tpl_need_sort_for_merge",  _get("need_sort_for_merge", 0))
    _set_all("tpl_post_link_present",    _get("post_link_present", 0))
    _set_all("tpl_post_link_occurs_2plus", _get("post_link_occurs_2plus", 0))

    # 5) bucket one-hot
    def _onehot_all(base, K, idx):
        idx = int(idx if idx is not None else 0)
        idx = max(0, min(K-1, idx))
        feature_matrix[row_map[f"{base}_{idx}"], :] = 1

    _onehot_all("tpl_group_by_cols_bucket", tpl_bucket_defs["tpl_group_by_cols_bucket"],
                _get("group_by_cols_bucket", 0))
    _onehot_all("tpl_rows_bucket", tpl_bucket_defs["tpl_rows_bucket"],
                _get("rows_bucket", 0))

    extra_rows = [
        "plan_cost_share",     # broadcast
        "plan_rows_share",     # broadcast
        "sql_has_window",
        "sql_has_like",
        "sql_has_between",
        "sql_has_in",
        "sql_has_isnull",
        "sql_has_case",
        "sql_num_join_bucket_0",
        "sql_num_join_bucket_1",
        "sql_num_join_bucket_2",
        "sql_num_aggs_bucket_0",
        "sql_num_aggs_bucket_1",
        "sql_num_aggs_bucket_2",
        "sql_num_cte_bucket_0",
        "sql_num_cte_bucket_1",
        "sql_num_subquery_bucket_0",
        "sql_num_subquery_bucket_1",
        "sql_num_agg_distinct"   # COUNT DISTINCT（
    ]
    row_names = row_names + extra_rows
    feature_matrix = np.pad(feature_matrix, ((0, len(extra_rows)), (0, 0)), mode="constant")
    row_map.update({name: idx for idx, name in enumerate(row_names)})

    try:
        distribute_costs_to_tables_and_columns(metadata_json, plan.get("Plan", plan))
    except Exception:
        pass

    cost_by_table, rows_by_table, tc, tr = compute_plan_table_shares(plan or {})
    cost_share = {k: (v / tc) for k, v in cost_by_table.items()}
    rows_share = {k: (v / tr) for k, v in rows_by_table.items()}
    vec_cost = broadcast_table_metric_to_columns(metadata_json, cost_share, default=0.0)
    vec_rows = broadcast_table_metric_to_columns(metadata_json, rows_share, default=0.0)
    feature_matrix[row_map["plan_cost_share"], :] = vec_cost
    feature_matrix[row_map["plan_rows_share"], :] = vec_rows

    try:
        feature_matrix, row_names = append_cost_rows_to_matrix(
            feature_matrix, row_names, metadata_json, table_list, attr_list
        )
        row_map = {name: idx for idx, name in enumerate(row_names)} 
    except Exception:
        pass

    # global cost and broadcast
    gcs = metadata_json.get("global_cost_shares", {})
    keys = GCS_CANON_KEYS
    gcs_row_names = [f"plan_{k}" for k in keys]

    # update row_names / row_map
    feature_matrix = np.pad(feature_matrix, ((0, len(gcs_row_names)), (0, 0)), mode="constant")
    row_names.extend(gcs_row_names)
    row_map = {name: idx for idx, name in enumerate(row_names)}

    for k in keys:
        v = float(gcs.get(k, 0.0)) if isinstance(gcs, dict) else 0.0
        feature_matrix[row_map[f"plan_{k}"], :] = v

    # from meta.template_features
    tf = metadata_json.get("template_features", {})
    def set_all(row, v):
        feature_matrix[row_map[row], :] = int(bool(v))

    set_all("sql_has_window",  tf.get("has_window", 0))
    set_all("sql_has_like",    tf.get("has_like", 0))
    set_all("sql_has_between", tf.get("has_between", 0))
    set_all("sql_has_in",      tf.get("has_in", 0))
    set_all("sql_has_isnull",  tf.get("has_isnull", 0))
    set_all("sql_has_case",    tf.get("has_case", 0))

    def one_hot_all(prefix, K, idx):
        idx = max(0, min(K-1, int(idx or 0)))
        feature_matrix[row_map[f"{prefix}_{idx}"], :] = 1

    one_hot_all("sql_num_join_bucket",     3, tf.get("num_join_bucket", 0))
    one_hot_all("sql_num_aggs_bucket",     3, tf.get("num_aggs_bucket", 0))
    one_hot_all("sql_num_cte_bucket",      2, tf.get("num_cte_bucket", 0))
    one_hot_all("sql_num_subquery_bucket", 2, tf.get("num_subquery_bucket", 0))

    set_all("sql_num_agg_distinct", tf.get("num_agg_distinct", 0))


    return feature_matrix

def get_arm_index(metadata_json, default_index=0):
    return int(metadata_json.get("arm_config_json", {}).get("index", default_index))

def get_arm_one_hot(metadata_json, num_arms, default_index=0):
    import numpy as np
    idx = get_arm_index(metadata_json, default_index)
    if idx < 0 or idx >= int(num_arms):
        idx = default_index
    v = np.zeros((int(num_arms),), dtype=np.float32)
    v[idx] = 1.0
    return v

def augment_meta_from_plan(plan: dict, meta: dict) -> dict:
    # traverse the plan tree, collect the features, particularly for the merge, hash and cost
    def walk(p, acc):
        if not isinstance(p, dict):
            return
        node_type = p.get("Node Type", "")
        join_type = p.get("Join Type", "")

        if node_type in ("Unique", "HashAggregate", "Aggregate"):
            acc["has_distinct"] = True
            if "Group Key" in p or "Group Keys" in p:
                keys = p.get("Group Key", p.get("Group Keys", [])) or []
                acc["group_by_cols_count"] = max(acc["group_by_cols_count"], len(keys))

        if join_type == "Semi":
            acc["has_exists"] = True
        if join_type == "Anti":
            acc["has_not_exists"] = True

        if node_type == "Merge Join":
            acc["need_sort_for_merge"] = True

        if "Hash Cond" not in p and "Merge Cond" not in p and "Join Filter" in p:
            acc["has_non_equi_pred"] = True

        if p.get("Relation Name", "") == "post_link":
            acc["post_link_occurs"] += 1

        if "Plan Rows" in p:
            try:
                acc["estimated_rows_max"] = max(acc["estimated_rows_max"], float(p["Plan Rows"]))
            except Exception:
                pass

        for child in p.get("Plans", []) or []:
            walk(child, acc)

    acc = dict(
        has_distinct=False, has_exists=False, has_not_exists=False,
        has_non_equi_pred=False, need_sort_for_merge=False,
        post_link_occurs=0, group_by_cols_count=0, estimated_rows_max=0.0,
    )
    walk(plan, acc)

    meta["has_distinct"]          = bool(acc["has_distinct"])
    meta["has_exists"]            = bool(acc["has_exists"])
    meta["has_not_exists"]        = bool(acc["has_not_exists"])
    meta["has_non_equi_pred"]     = bool(acc["has_non_equi_pred"])
    meta["need_sort_for_merge"]   = bool(acc["need_sort_for_merge"])

    meta["post_link_present"]       = bool(acc["post_link_occurs"] > 0)
    meta["post_link_occurs_2plus"]  = bool(acc["post_link_occurs"] >= 2)

    g = int(acc["group_by_cols_count"])
    meta["group_by_cols_bucket"] = 0 if g == 0 else (1 if g == 1 else (2 if g == 2 else 3))

    r = float(acc["estimated_rows_max"])
    meta["rows_bucket"] = 0 if r < 1e3 else (1 if r < 1e5 else 2)  # 0/1/2

    meta["template_features"] = {
        "has_distinct":            int(meta["has_distinct"]),
        "has_exists":              int(meta["has_exists"]),
        "has_not_exists":          int(meta["has_not_exists"]),
        "has_non_equi_pred":       int(meta["has_non_equi_pred"]),
        "need_sort_for_merge":     int(meta["need_sort_for_merge"]),
        "post_link_present":       int(meta["post_link_present"]),
        "post_link_occurs_2plus":  int(meta["post_link_occurs_2plus"]),
        "group_by_cols_bucket":    int(meta["group_by_cols_bucket"]),
        "rows_bucket":             int(meta["rows_bucket"]),
    }
    return meta

def compute_plan_table_shares(plan: dict):

    cost_by_table = {}
    rows_by_table = {}
    def walk(p):
        if not isinstance(p, dict): return
        rel = p.get("Relation Name")
        if rel:
            cost_by_table[rel] = cost_by_table.get(rel, 0.0) + float(p.get("Total Cost", 0.0))
            rows_by_table[rel] = rows_by_table.get(rel, 0.0) + float(p.get("Plan Rows", 0.0))
        for ch in p.get("Plans", []) or []:
            walk(ch)
    walk(plan or {})
    total_cost = sum(cost_by_table.values()) or 1.0
    total_rows = sum(rows_by_table.values()) or 1.0
    return cost_by_table, rows_by_table, total_cost, total_rows

def broadcast_table_metric_to_columns(metadata_json, table_metric: dict, default=0.0):
    tables = metadata_json.get("tables", [])
    attrs  = []
    for t in tables:
        attrs.extend([f"{t}.{c}" for c in metadata_json.get(t, [])])

    vec = []
    # tables
    for t in tables:
        vec.append(float(table_metric.get(t, default)))
    # attributes
    for name in attrs:
        t = name.split(".", 1)[0]
        vec.append(float(table_metric.get(t, default)))
    return np.array(vec, dtype=np.float32)

def inject_sql_join_edges_into_adjacency(adj, metadata_json):
    tables = metadata_json.get("tables", [])
    attrs = []
    for t in tables:
        attrs.extend([f"{t}.{c}" for c in metadata_json.get(t, [])])
    attr_index_map = {name: idx + len(tables) for idx, name in enumerate(attrs)}

    for lcol, rcol, _jt in metadata_json.get("join_edges", []):
        if lcol in attr_index_map and rcol in attr_index_map:
            i = attr_index_map[lcol]; j = attr_index_map[rcol]
            adj[i, j] = 1; adj[j, i] = 1
    return adj
