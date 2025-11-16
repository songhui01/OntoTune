
#include "postgres.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "access/htup_details.h"

#include "onto_cost.h"

static void append_json_string(StringInfo dst, const char *key, const char *val)
{
    appendStringInfo(dst, "\"%s\":\"%s\"", key, val ? val : "");
}

static void append_json_float(StringInfo dst, const char *key, double val)
{
    appendStringInfo(dst, "\"%s\":%.6f", key, val);
}

static void append_json_int(StringInfo dst, const char *key, long val)
{
    appendStringInfo(dst, "\"%s\":%ld", key, val);
}

static const char* node_type_name(Plan *plan)
{
    if (!plan) return "Unknown";
    switch (nodeTag(plan))
    {
        case T_SeqScan: return "Seq Scan";
        case T_IndexScan: return "Index Scan";
        case T_IndexOnlyScan: return "Index Only Scan";
        case T_BitmapHeapScan: return "Bitmap Heap Scan";
        case T_BitmapIndexScan: return "Bitmap Index Scan";
        case T_NestLoop: return "Nested Loop";
        case T_HashJoin: return "Hash Join";
        case T_MergeJoin: return "Merge Join";
        case T_Sort: return "Sort";
        case T_Agg: return "Aggregate";
        case T_Hash: return "Hash";
        case T_Material: return "Material";
        case T_Limit: return "Limit";
        default: return "Plan";
    }
}

/* weak stub to extract relid for scan nodes */
static Oid extract_scan_relid(Plan *plan)
{
    if (!plan) return InvalidOid;
    if (IsA(plan, SeqScan))
        return ((SeqScan*)plan)->scanrelid;
    if (IsA(plan, IndexScan))
        return ((IndexScan*)plan)->scan.scanrelid;
    if (IsA(plan, IndexOnlyScan))
        return ((IndexOnlyScan*)plan)->scan.scanrelid;
    if (IsA(plan, BitmapHeapScan))
        return ((BitmapHeapScan*)plan)->scan.scanrelid;
    return InvalidOid;
}

static void onto_serialize_plan_json_internal(StringInfo dst, Plan *plan);

static void append_children(StringInfo dst, Plan *plan)
{
    if (!plan || plan->initPlan) { /* ignore initPlans here */ }
    appendStringInfoString(dst, ",\"Plans\":[");
    bool first = true;
    if (outerPlan(plan))
    {
        if (!first) appendStringInfoChar(dst, ',');
        onto_serialize_plan_json_internal(dst, outerPlan(plan));
        first = false;
    }
    if (innerPlan(plan))
    {
        if (!first) appendStringInfoChar(dst, ',');
        onto_serialize_plan_json_internal(dst, innerPlan(plan));
        first = false;
    }
    /* Append other children if any (e.g., subplans) — omitted for brevity */
    appendStringInfoChar(dst, ']');
}

static void onto_serialize_plan_json_internal(StringInfo dst, Plan *plan)
{
    appendStringInfoChar(dst, '{');

    append_json_string(dst, "Node Type", node_type_name(plan));

    appendStringInfoChar(dst, ',');
    append_json_float(dst, "Startup Cost", plan ? plan->startup_cost : 0.0);
    appendStringInfoChar(dst, ',');
    append_json_float(dst, "Total Cost", plan ? plan->total_cost : 0.0);
    appendStringInfoChar(dst, ',');
    append_json_float(dst, "Plan Rows", plan ? plan->plan_rows : 0.0);
    appendStringInfoChar(dst, ',');
    append_json_int(dst, "Plan Width", plan ? plan->plan_width : 0);

    /* Scan relid */
    Oid relid = extract_scan_relid(plan);
    if (OidIsValid(relid))
    {
        appendStringInfoChar(dst, ',');
        append_json_int(dst, "Relation ID", relid);
    }

    /* Placeholders for keys (replace with real Var walkers in your project) */
    if (IsA(plan, MergeJoin) || IsA(plan, Sort))
    {
        appendStringInfoString(dst, ",\"Sort Keys\":[]");
    }
    if (IsA(plan, HashJoin) || IsA(plan, MergeJoin) || IsA(plan, NestLoop))
    {
        appendStringInfoString(dst, ",\"Join Keys\":[]");
    }
    if (IsA(plan, Agg))
    {
        appendStringInfoString(dst, ",\"Group Keys\":[]");
        appendStringInfoString(dst, ",\"Aggs\":[]");
    }

    /* children */
    append_children(dst, plan);

    appendStringInfoChar(dst, '}');
}

void onto_serialize_plan_json(StringInfo dst, Plan *plan)
{
    if (!plan)
    {
        appendStringInfoString(dst, "null");
        return;
    }
    initStringInfo(dst);
    onto_serialize_plan_json_internal(dst, plan);
}
