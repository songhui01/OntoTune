from typing import Dict, List, Tuple
from dataclasses import dataclass

# NOTE: imports happen only when you call parse_sqlglot()
def _import_sqlglot():
    import sqlglot
    from sqlglot import parse_one, exp
    return sqlglot, parse_one, exp

def _col_to_str(col) -> str:
    # col is sqlglot.expressions.Column
    # render as table.column if table qualifier exists
    t = col.table
    n = col.name
    if t:
        return f"{t}.{n}"
    return n

def _gather_columns(expr, exp_mod) -> List[str]:
    return list(dict.fromkeys(_col_to_str(c) for c in expr.find_all(exp_mod.Column)))

def parse_sqlglot(sql: str, read_dialect: str = "postgres") -> Dict:
    sqlglot, parse_one, exp = _import_sqlglot()
    # Parse with dialect-awareness
    tree = parse_one(sql, read=read_dialect)

    # Tables
    tables = []
    for t in tree.find_all(exp.Table):
        # Use only the table identifier (ignore schema for now; can extend to schema.table if needed)
        name = ".".join(p.name for p in t.find_all(exp.Identifier)) or t.alias_or_name
        # Better: prefer t.alias_or_name when alias absent returns actual table name
        name = t.alias_or_name
        if name and name not in tables:
            tables.append(name)

    # DISTINCT / DISTINCT ON
    has_distinct = bool(tree.args.get("distinct"))
    distinct_on = []
    darg = tree.args.get("distinct")
    if isinstance(darg, exp.Distinct):
        exprs = darg.expressions or []
        for e in exprs:
            # collect columns under each distinct expression
            distinct_on += _gather_columns(e, exp)

    # SELECT list & aggregates
    select_cols = []
    aggregates = []
    proj = tree.expressions or []
    for e in proj:
        raw = e.sql(dialect="postgres")
        agg = None
        arg = None
        is_distinct = False
        # COUNT/SUM/AVG/MIN/MAX detection
        if isinstance(e, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            agg = e.key.lower()
            # COUNT(*) case
            if isinstance(e, exp.Count) and isinstance(e.this, exp.Star):
                arg = "*"
            else:
                # if argument is a column, render "t.c"; else keep its SQL
                if e.this is not None:
                    cols = _gather_columns(e.this, exp)
                    arg = cols[0] if cols else e.this.sql(dialect="postgres")
            is_distinct = bool(e.args.get("distinct"))
            aggregates.append({"agg": agg, "arg": arg, "is_distinct": is_distinct})
        else:
            # expression may contain nested aggregates
            for a in e.find_all((exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
                agg = a.key.lower()
                if isinstance(a, exp.Count) and isinstance(a.this, exp.Star):
                    arg = "*"
                else:
                    if a.this is not None:
                        cols = _gather_columns(a.this, exp)
                        arg = cols[0] if cols else a.this.sql(dialect="postgres")
                is_distinct = bool(a.args.get("distinct"))
                aggregates.append({"agg": agg, "arg": arg, "is_distinct": is_distinct})
        select_cols.append({"raw": raw, "agg": agg, "arg": arg, "is_distinct": is_distinct})

    # WHERE columns
    where_cols = []
    if tree.args.get("where"):
        where_cols = _gather_columns(tree.args["where"], exp)

    # GROUP BY columns
    group_by = []
    if tree.args.get("group"):
        group = tree.args["group"]
        for e in group.expressions or []:
            group_by += _gather_columns(e, exp)

    # ORDER BY columns
    order_by: List[Tuple[str, str]] = []
    if tree.args.get("order"):
        order = tree.args["order"]
        for s in order.expressions or []:
            cols = _gather_columns(s, exp)
            expr_sql = s.sql(dialect="postgres")
            direction = "DESC" if s.args.get("desc") else "ASC"
            if cols:
                # prefer first column name form
                order_by.append((cols[0], direction))
            else:
                order_by.append((expr_sql, direction))

    # JOINs with equi-keys (extract a.b = c.d)
    joins = []
    for j in tree.find_all(exp.Join):
        jtype = (j.args.get("kind") or "JOIN").upper()
        on_exp = j.args.get("on")
        if on_exp is None:
            continue
        # Collect simple equality predicates
        for comp in on_exp.find_all(exp.EQ):
            left_cols = _gather_columns(comp.left, exp)
            right_cols = _gather_columns(comp.right, exp)
            if left_cols and right_cols:
                joins.append({
                    "type": jtype,
                    "left_col": left_cols[0],
                    "right_col": right_cols[0],
                    "is_equi": True
                })

        # Handle USING(k) by rewriting to equality columns if present
        using = j.args.get("using")
        if using:
            for u in using.expressions or []:
                if isinstance(u, exp.Identifier):
                    colname = u.name
                    # Heuristic: join between two most recent/available tables on same column
                    # We'll record symmetric pair "t.col = u.col" for each adjacent pair if available.
                    if len(tables) >= 2:
                        t1, t2 = tables[-2], tables[-1]
                        joins.append({
                            "type": jtype,
                            "left_col": f"{t1}.{colname}",
                            "right_col": f"{t2}.{colname}",
                            "is_equi": True
                        })

    return {
        "tables": tables,
        "joins": joins,
        "select_cols": select_cols,
        "where_cols": list(dict.fromkeys(where_cols)),
        "group_by": list(dict.fromkeys(group_by)),
        "order_by": order_by,
        "has_distinct": has_distinct,
        "distinct_on": list(dict.fromkeys(distinct_on)),
        "aggregates": aggregates,
    }

# sqlglot_parse.py 里新增
def enrich_sql_semantics(sql: str, read_dialect: str = "postgres"):
    """
    返回更丰富的 SQL 语义，供后续桥接和矩阵/邻接使用。
    """
    _, parse_one, exp = _import_sqlglot()
    t = parse_one(sql, read=read_dialect)

    def has(node_cls): 
        return any(True for _ in t.find_all(node_cls))

    # 统计
    num_cte      = len(list(t.find_all(exp.CTE)))
    num_subquery = len(list(t.find_all(exp.Subquery)))
    num_window   = len(list(t.find_all(exp.Window)))
    num_join     = len(list(t.find_all(exp.Join)))
    num_aggs     = len(list(t.find_all((exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max))))
    num_agg_distinct = len([a for a in t.find_all(exp.Count) if a.args.get("distinct")])

    # 谓词存在性
    has_like    = has(exp.Like)
    has_between = has(exp.Between)
    has_in      = has(exp.In)
    has_isnull  = has(exp.Is)
    has_case    = has(exp.Case)

    # LIMIT/OFFSET
    limit_val = None
    if t.args.get("limit"):
        try:
            lit = t.args["limit"].expression
            if isinstance(lit, exp.Literal) and lit.is_number:
                limit_val = int(lit.name)
        except Exception:
            pass

    # JOIN 边（列-列），与 parse_sqlglot 的 joins 一致，这里也产出全量列表，方便直接喂邻接
    join_edges = []  # [(left_col, right_col, join_type), ...]
    for j in t.find_all(exp.Join):
        jtype = (j.args.get("kind") or "JOIN").upper()
        on_exp = j.args.get("on")
        if on_exp is not None:
            for comp in on_exp.find_all(exp.EQ):
                left_cols  = [c.sql() for c in comp.left.find_all(exp.Column)]
                right_cols = [c.sql() for c in comp.right.find_all(exp.Column)]
                if left_cols and right_cols:
                    join_edges.append((left_cols[0], right_cols[0], jtype))
        using = j.args.get("using")
        if using:
            cols = [u.name for u in (using.expressions or []) if isinstance(u, exp.Identifier)]
            # 简单策略：以最近的两张表形成对
            tables = [x.alias_or_name for x in t.find_all(exp.Table)]
            if len(tables) >= 2:
                t1, t2 = tables[-2], tables[-1]
                for c in cols:
                    join_edges.append((f"{t1}.{c}", f"{t2}.{c}", jtype))

    return {
        "num_cte": num_cte,
        "num_subquery": num_subquery,
        "num_window": num_window,
        "num_join": num_join,
        "num_aggs": num_aggs,
        "num_agg_distinct": num_agg_distinct,
        "has_like": has_like,
        "has_between": has_between,
        "has_in": has_in,
        "has_isnull": has_isnull,
        "has_case": has_case,
        "limit_val": limit_val,
        "join_edges": join_edges,
    }
