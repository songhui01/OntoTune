#ifndef ONTO_META_H
#define ONTO_META_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "catalog/pg_statistic.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "catalog/pg_operator.h"
#include "commands/explain.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "parser/parse_expr.h"
#include <regex.h>
#include "parser/parse_node.h"
#include "executor/spi.h"
#include "utils/builtins.h"

#include "optimizer/planner.h"
#include "parser/parse_clause.h"
#include "utils/hsearch.h"
#include "nodes/print.h"

#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "onto_compat.h"

#include "utils/rel.h"
#include "utils/relcache.h"
#include "catalog/namespace.h"

#include "onto_configs.h"


/*
 * the development of some helper functions in this onto_meta.h, along with other utilities, are fast-prototyped by AI tools, 
 * by prompting with the design details, and then examined and tested by the authors.
 *
 * this helper function is to ensemble the sql related information, and the db statistics into the metadata info, that will be
 * used in the ontoserver to form the feature matrix
 */

TargetEntry* get_sortgroupclause_tle(SortGroupClause *sgClause, List *targetList);

#define MAX_NAME_LEN 128

// === data structure ===
typedef struct AttributeFeature {
    char name[256];  // e.g., "table.attr"
    bool inSQL;
    bool inWhere;
    bool inJoin;
    bool inGroup;
    bool inSort;
    bool isNumeric;
    bool hasIndex;
    bool correlationAbove0_9;
} AttributeFeature;

typedef struct TableFeature {
    char name[64];
    List* attrList; 
    bool inSQL;
    bool hasInWhere;
    bool hasInJoin;
    bool hasInGroup;
    bool hasInSort;
    bool hasNumeric;
    bool hasIndex;
    bool hasCorr;
} TableFeature;

typedef struct MetadataSkeleton {
    List* tableNames; 
    HTAB* tableAttributes; 
    List* tableFeatureList; 
    List* attributeFeatureList;
    HTAB* aliasMap; 
} MetadataSkeleton;

typedef struct TableKey {
    char name[MAX_NAME_LEN];
} TableKey;

typedef struct TableAttrEntry {
    TableKey key;  
    List* attrList; // List of char*
} TableAttrEntry;

typedef struct FieldMatchEntry {
    char full_name[256];
    List* match_names;
} FieldMatchEntry;

typedef struct AliasEntry {
    char alias[MAX_NAME_LEN];
    char realname[MAX_NAME_LEN];
} AliasEntry;

typedef struct {
    MetadataSkeleton* skel;
    Query* query;
} JoinCondContext;

typedef struct CorrKey {
    char relname[MAX_NAME_LEN];
    char attname[MAX_NAME_LEN];
} CorrKey;

typedef struct CorrEntry {
    CorrKey key;
    float4 correlation;
} CorrEntry;

static HTAB* corr_table = NULL;

// planner_hook_type get_prev_planner_hook(void);

// === interfances ===
MetadataSkeleton* create_metadata_skeleton(void);
void add_table(MetadataSkeleton* skel, const char* tablename);
void add_attribute(MetadataSkeleton* skel, const char* tablename, const char* attrname);
void add_attribute_feature(MetadataSkeleton* skel, AttributeFeature* feat);
void finalize_table_features(MetadataSkeleton* skel);
char* export_metadata_json(MetadataSkeleton* skel);
char* generate_metadata_json(Query* parse);


// ===divide and conquer ===
void extract_tables_and_attributes(MetadataSkeleton* skel, Query* parse);
void extract_global_schema(MetadataSkeleton* skel);
void analyze_attribute_metadata(MetadataSkeleton* skel, Query* parse);
void analyze_query_structure(MetadataSkeleton* skel, Query* parse);
void analyze_query_context_from_sql_string(MetadataSkeleton* skel, const char* queryString);


List* extract_all_subqueries(const char* sql);
void match_table_field_appearance(MetadataSkeleton* skel, const char* subquery, List* matchEntries);

List* generate_field_match_entries(MetadataSkeleton* skel, const char* queryString);


void init_alias_map(MetadataSkeleton* skel) {
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = MAX_NAME_LEN;
    ctl.entrysize = sizeof(AliasEntry);
    skel->aliasMap = hash_create("aliasMap", 64, &ctl, HASH_ELEM);
}

void insert_into_alias_map(HTAB* aliasMap, const char* alias, const char* realname) {
    bool found;
    AliasEntry* entry = (AliasEntry*) hash_search(aliasMap, alias, HASH_ENTER, &found);
    strncpy(entry->alias, alias, MAX_NAME_LEN);
    strncpy(entry->realname, realname, MAX_NAME_LEN);
}

const char* lookup_alias_realname(HTAB* aliasMap, const char* alias) {
    AliasEntry* entry = (AliasEntry*) hash_search(aliasMap, alias, HASH_FIND, NULL);
    return entry ? entry->realname : NULL;
}

static bool list_member_str(List* list, const char* str) {
    ListCell* cell;
    foreach(cell, list) {
        char* item = (char*) lfirst(cell);
        if (strcmp(item, str) == 0)
            return true;
    }
    return false;
}

// === metadata skeleton ===
MetadataSkeleton* create_metadata_skeleton(void) {
    MetadataSkeleton* skel = (MetadataSkeleton*) palloc0(sizeof(MetadataSkeleton));
    skel->tableNames = NIL;
    skel->tableFeatureList = NIL;
    skel->attributeFeatureList = NIL;

    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(TableKey);
    ctl.entrysize = sizeof(TableAttrEntry);
    ctl.hcxt = CurrentMemoryContext;
    skel->tableAttributes = hash_create("tableAttributes", 64, &ctl,
                                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    init_alias_map(skel);
    return skel;
}

void add_table(MetadataSkeleton* skel, const char* tablename) {
    if (!list_member_str(skel->tableNames, tablename))
        skel->tableNames = lappend(skel->tableNames, pstrdup(tablename));

    TableKey key;
    memset(key.name, 0, MAX_NAME_LEN);
    strncpy(key.name, tablename, MAX_NAME_LEN - 1);
    key.name[MAX_NAME_LEN - 1] = '\0';

    bool found;
    TableAttrEntry* entry = hash_search(skel->tableAttributes, &key, HASH_ENTER, &found);
    if (!found)
        entry->attrList = NIL;
}

void add_attribute(MetadataSkeleton* skel, const char* tablename, const char* attrname) {
    TableKey key;
    memset(&key, 0, sizeof(TableKey));
    strncpy(key.name, tablename, MAX_NAME_LEN - 1);
    key.name[MAX_NAME_LEN - 1] = '\0';
    
    TableAttrEntry* entry = hash_search(skel->tableAttributes, &key, HASH_FIND, NULL);
    if (entry && !list_member_str(entry->attrList, attrname)) {
        entry->attrList = lappend(entry->attrList, pstrdup(attrname));
    }
}

void add_attribute_feature(MetadataSkeleton* skel, AttributeFeature* feat) {
    skel->attributeFeatureList = lappend(skel->attributeFeatureList, feat);
}

// find and update
void finalize_table_features(MetadataSkeleton* skel) {
    ListCell* lc;

    foreach(lc, skel->tableNames) {
        const char* tablename = (const char*) lfirst(lc);
        TableFeature* tf = NULL;

        ListCell* lc2;
        foreach(lc2, skel->tableFeatureList) {
            TableFeature* existing = (TableFeature*) lfirst(lc2);
            if (strncmp(existing->name, tablename, MAX_NAME_LEN) == 0) {
                tf = existing;
                break;
            }
        }

        // new TableFeature
        if (!tf) {
            tf = (TableFeature*) palloc0(sizeof(TableFeature));
            tf->attrList = NIL;
            strncpy(tf->name, tablename, sizeof(tf->name));
            tf->name[sizeof(tf->name) - 1] = '\0';
            skel->tableFeatureList = lappend(skel->tableFeatureList, tf);
        }

        // reset
        tf->hasInWhere = false;
        tf->hasInJoin = false;
        tf->hasInGroup = false;
        tf->hasInSort = false;
        tf->hasNumeric = false;
        tf->hasIndex = false;
        tf->hasCorr = false;

        // learn from attributes
        ListCell* fc;
        foreach(fc, skel->attributeFeatureList) {
            AttributeFeature* af = (AttributeFeature*) lfirst(fc);
            if (strncmp(af->name, tf->name, strlen(tf->name)) == 0 && af->name[strlen(tf->name)] == '.') {
                tf->hasInWhere |= af->inWhere;
                tf->hasInJoin |= af->inJoin;
                tf->hasInGroup |= af->inGroup;
                tf->hasInSort |= af->inSort;
                tf->hasNumeric |= af->isNumeric;
                tf->hasIndex |= af->hasIndex;
                tf->hasCorr |= af->correlationAbove0_9;
            }
        }
    }
}

static void append_json_string(StringInfo dst, const char *s)
{
    const unsigned char *p;

    if (s == NULL) {
        appendStringInfoString(dst, "\"\"");
        return;
    }

    appendStringInfoChar(dst, '"');
    for (p = (const unsigned char *) s; *p; p++)
    {
        unsigned char ch = *p;
        switch (ch)
        {
            case '\"': appendStringInfoString(dst, "\\\""); break;
            case '\\': appendStringInfoString(dst, "\\\\"); break;
            case '\b': appendStringInfoString(dst, "\\b");  break;
            case '\f': appendStringInfoString(dst, "\\f");  break;
            case '\n': appendStringInfoString(dst, "\\n");  break;
            case '\r': appendStringInfoString(dst, "\\r");  break;
            case '\t': appendStringInfoString(dst, "\\t");  break;
            default:
                if (ch < 0x20)
                    appendStringInfo(dst, "\\u%04x", ch);
                else
                    appendStringInfoChar(dst, ch);
        }
    }
    appendStringInfoChar(dst, '"');
}



char* export_metadata_json(MetadataSkeleton* skel) {
    StringInfoData buf;
    initStringInfo(&buf);

    appendStringInfoString(&buf, "{\n");

    appendStringInfoString(&buf, "  \"sequence_id\": ");
    append_json_string(&buf, (onto_sequence_id && onto_sequence_id[0]) ? onto_sequence_id : "");
    appendStringInfoString(&buf, ",\n");  

    appendStringInfoString(&buf, "  \"tables\": [");
    for (int i = 0; i < list_length(skel->tableNames); ++i) {
        const char* tname = (const char*) list_nth(skel->tableNames, i);
        appendStringInfo(&buf, "%s\"%s\"", i > 0 ? ", " : "", tname);
    }
    appendStringInfoString(&buf, "],\n");

    ListCell* lc;
    foreach(lc, skel->tableNames) {
        const char* tname = (const char*) lfirst(lc);
        TableKey key;
        strncpy(key.name, tname, MAX_NAME_LEN);
        TableAttrEntry* entry = hash_search(skel->tableAttributes, &key, HASH_FIND, NULL);
        appendStringInfo(&buf, "  \"%s\": [", tname);

        if (entry && entry->attrList) {
            for (int i = 0; i < list_length(entry->attrList); ++i) {
                const char* attr = (const char*) list_nth(entry->attrList, i);
                appendStringInfo(&buf, "%s\"%s\"", i > 0 ? ", " : "", attr);
            }
        }
        appendStringInfoString(&buf, "],\n");
    }

    appendStringInfoString(&buf, "  \"table-features\": [\n");
    bool first = true;
    foreach(lc, skel->tableFeatureList) {
        TableFeature* tf = (TableFeature*) lfirst(lc);
        if (!first) appendStringInfoString(&buf, ",\n");
        first = false;
        appendStringInfo(&buf,
          "    {\"name\": \"%s\", \"inSQL\": %s, \"hasInWhere\": %s, \"hasInJoin\": %s, \"hasInGroup\": %s, \"hasInSort\": %s, \"hasNumeric\": %s, \"hasIndex\": %s, \"hasCorr\": %s}",
          tf->name,
          tf->inSQL ? "true" : "false",
          tf->hasInWhere ? "true" : "false",
          tf->hasInJoin ? "true" : "false",
          tf->hasInGroup ? "true" : "false",
          tf->hasInSort ? "true" : "false",
          tf->hasNumeric ? "true" : "false",
          tf->hasIndex ? "true" : "false",
          tf->hasCorr ? "true" : "false");
    }
    appendStringInfoString(&buf, "\n  ],\n");

    appendStringInfoString(&buf, "  \"attributes\": [\n");
    first = true;
    foreach(lc, skel->attributeFeatureList) {
        AttributeFeature* af = (AttributeFeature*) lfirst(lc);
        if (!first) appendStringInfoString(&buf, ",\n");
        first = false;
        appendStringInfo(&buf,
          "    {\"name\": \"%s\", \"inSQL\": %s, \"inWhere\": %s, \"inJoin\": %s, \"inGroup\": %s, \"inSort\": %s, \"isNumeric\": %s, \"hasIndex\": %s, \"correlationAbove0.9\": %s}",
          af->name,
          af->inSQL ? "true" : "false",
          af->inWhere ? "true" : "false",
          af->inJoin ? "true" : "false",
          af->inGroup ? "true" : "false",
          af->inSort ? "true" : "false",
          af->isNumeric ? "true" : "false",
          af->hasIndex ? "true" : "false",
          af->correlationAbove0_9 ? "true" : "false");
    }
    appendStringInfoString(&buf, "\n  ]\n}");

    char* result = pstrdup(buf.data);  // deep copy; use pfree to free later
    pfree(buf.data);  // clean up
    return result;
}

// tester function
void test_generate_metadata_json(Query* parse, const char* queryString) {
    char* json = generate_metadata_json(parse);
    elog(INFO, "Generated Metadata JSON:\n%s", json);
}

void mark_table_flag(MetadataSkeleton* skel, const char* table, const char* context) {
    if (!table) return;

    TableFeature* found = NULL;
    ListCell* lc;
    foreach(lc, skel->tableFeatureList) {
        TableFeature* tf = (TableFeature*) lfirst(lc);
        if (strcmp(tf->name, table) == 0) {
            found = tf;
            break;
        }
    }

    if (!found) {
        TableFeature* tf = (TableFeature*) palloc0(sizeof(TableFeature));
        tf->attrList = NIL;  // add this!
        strncpy(tf->name, table, sizeof(tf->name));
        tf->name[sizeof(tf->name) - 1] = '\0'; // ensure null-termination
        skel->tableFeatureList = lappend(skel->tableFeatureList, tf);
        found = tf;
    }

    // inSQL
    found->inSQL = true;
}


void mark_columnref(MetadataSkeleton* skel, ColumnRef* cref, const char* context) {

    //elog(DEBUG1, "[FEATURE333] %s:", "test000");
    if (!cref || !cref->fields) return;

    char* table = NULL;
    char* attr = NULL;

    if (list_length(cref->fields) == 2) {
        table = strVal(list_nth(cref->fields, 0));
        attr = strVal(list_nth(cref->fields, 1));

        // find aliasMap，and change to real table names
        char** mapped = (char**) hash_search(skel->aliasMap, table, HASH_FIND, NULL);
        if (mapped)
            table = *mapped;

    } else if (list_length(cref->fields) == 1) {
        attr = strVal(list_nth(cref->fields, 0));
    }
    if (!attr) return;

    char full_name[256];
    if (table)
        snprintf(full_name, sizeof(full_name), "%s.%s", table, attr);
    else
        snprintf(full_name, sizeof(full_name), "%s", attr);

    // find or create corresponding AttributeFeature
    AttributeFeature* found = NULL;
    ListCell* lc;
    foreach(lc, skel->attributeFeatureList) {
        AttributeFeature* af = (AttributeFeature*) lfirst(lc);
        if (strcmp(af->name, full_name) == 0) {
            found = af;
            break;
        }
    }

    if (!found) {
        AttributeFeature* af = (AttributeFeature*) palloc0(sizeof(AttributeFeature));
        strncpy(af->name, full_name, sizeof(af->name));
        skel->attributeFeatureList = lappend(skel->attributeFeatureList, af);
        found = af;
    }

    if (strcmp(context, "where") == 0) found->inWhere = true;
    else if (strcmp(context, "join") == 0) found->inJoin = true;
    else if (strcmp(context, "group") == 0) found->inGroup = true;
    else if (strcmp(context, "sort") == 0) found->inSort = true;

    found->inSQL = true; // all inSQL
}

static void extract_columnrefs_from_expr(Node* node, MetadataSkeleton* skel, const char* context) {
    if (!node)
        return;

    switch (nodeTag(node)) {

        case T_ColumnRef: {
            ColumnRef* cref = (ColumnRef*) node;
            mark_columnref(skel, cref, context);
            break;
        }

        case T_A_Expr: {
            A_Expr* expr = (A_Expr*) node;
            extract_columnrefs_from_expr((Node*) expr->lexpr, skel, context);
            extract_columnrefs_from_expr((Node*) expr->rexpr, skel, context);
            break;
        }

        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*) node;
            ListCell* lc;
            foreach(lc, expr->args) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        case T_FuncCall: {
            FuncCall* fcall = (FuncCall*) node;
            ListCell* lc;
            foreach(lc, fcall->args) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        case T_SubLink: {
            SubLink* sublink = (SubLink*) node;
            extract_columnrefs_from_expr((Node*) sublink->testexpr, skel, context);
            extract_columnrefs_from_expr((Node*) sublink->operName, skel, context);
            extract_columnrefs_from_expr((Node*) sublink->subselect, skel, context);
            break;
        }

        case T_CaseExpr: {
            CaseExpr* expr = (CaseExpr*) node;
            extract_columnrefs_from_expr((Node*) expr->arg, skel, context);
            ListCell* lc;
            foreach(lc, expr->args) {
                CaseWhen* when = (CaseWhen*) lfirst(lc);
                extract_columnrefs_from_expr((Node*) when->expr, skel, context);
                extract_columnrefs_from_expr((Node*) when->result, skel, context);
            }
            extract_columnrefs_from_expr((Node*) expr->defresult, skel, context);
            break;
        }

        case T_OpExpr: {
            OpExpr* expr = (OpExpr*) node;
            ListCell* lc;
            foreach(lc, expr->args) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        case T_NullTest: {
            NullTest* nt = (NullTest*) node;
            extract_columnrefs_from_expr((Node*) nt->arg, skel, context);
            break;
        }

        case T_BooleanTest: {
            BooleanTest* bt = (BooleanTest*) node;
            extract_columnrefs_from_expr((Node*) bt->arg, skel, context);
            break;
        }

        case T_CoalesceExpr: {
            CoalesceExpr* ce = (CoalesceExpr*) node;
            ListCell* lc;
            foreach(lc, ce->args) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        case T_MinMaxExpr: {
            MinMaxExpr* mm = (MinMaxExpr*) node;
            ListCell* lc;
            foreach(lc, mm->args) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        case T_NamedArgExpr: {
            NamedArgExpr* nae = (NamedArgExpr*) node;
            extract_columnrefs_from_expr((Node*) nae->arg, skel, context);
            break;
        }

        case T_List: {
            ListCell* lc;
            foreach(lc, (List*) node) {
                extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
            }
            break;
        }

        default: {
            break;
        }
    }
}

void extract_columnrefs_from_expr_2(Node* node, MetadataSkeleton* skel, const char* context) {

    if (!node) return;

    if (IsA(node, List)) {
        ListCell* lc;
        foreach(lc, (List*) node) {
            extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
        }
        return;
    }

    if (IsA(node, ColumnRef)) {
        mark_columnref(skel, (ColumnRef*)node, context);
    } else if (IsA(node, A_Expr)) {
        A_Expr* expr = (A_Expr*) node;

        if (expr->kind == AEXPR_OP &&
            list_length(expr->name) == 1 &&
            strcmp(strVal(linitial(expr->name)), "=") == 0 &&
            IsA(expr->lexpr, ColumnRef) &&
            IsA(expr->rexpr, ColumnRef)) {

            // this is also considered as using join 
            mark_columnref(skel, (ColumnRef*) expr->lexpr, "join");
            mark_columnref(skel, (ColumnRef*) expr->rexpr, "join");

        } else {

            extract_columnrefs_from_expr(expr->lexpr, skel, context);
            extract_columnrefs_from_expr(expr->rexpr, skel, context);
        }
    } else if (IsA(node, BoolExpr)) {
        ListCell* lc;
        foreach(lc, ((BoolExpr*)node)->args) {
            extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
        }
    } else if (IsA(node, NullTest)) {
        extract_columnrefs_from_expr(((NullTest*) node)->arg, skel, context);
    } else if (IsA(node, FuncCall)) {
        ListCell* lc;
        foreach(lc, ((FuncCall*)node)->args) {
            extract_columnrefs_from_expr((Node*) lfirst(lc), skel, context);
        }
    } else if (IsA(node, SubLink)) {
        SubLink* sub = (SubLink*) node;
        if (sub->subselect && IsA(sub->subselect, Query)) {
            analyze_query_structure(skel, (Query*) sub->subselect);
        }
    }
}


void analyze_query_structure_1(MetadataSkeleton* skel, Query* parse) {
    ListCell* lc;

    // ============ SELECT ============
    foreach(lc, parse->targetList) {
        ResTarget* res = (ResTarget*) lfirst(lc);
        if (res->val && IsA(res->val, SubLink)) {
            // recursively analyze select part
            SubLink* sub = (SubLink*) res->val;
            if (IsA(sub->subselect, Query)) {
                analyze_query_structure(skel, (Query*) sub->subselect);
            }
        }
    }

    // ============ FROM ============
    foreach(lc, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*) lfirst(lc);

        if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
            analyze_query_structure(skel, rte->subquery);
        }
    }

    // ============ JOIN ON ============
    if (parse->jointree && parse->jointree->fromlist) {
        foreach(lc, parse->jointree->fromlist) {
            Node* node = (Node*) lfirst(lc);
            if (IsA(node, JoinExpr)) {
                JoinExpr* join = (JoinExpr*) node;
                if (join->quals) {
                    extract_columnrefs_from_expr(join->quals, skel, "join");
                }
            }
        }
    }

    // ============ WHERE ============
    if (parse->jointree && parse->jointree->quals) {
        extract_columnrefs_from_expr(parse->jointree->quals, skel, "where");
    }

    // ============ GROUP BY ============
    foreach(lc, parse->groupClause) {
        Node* groupExpr = (Node*) lfirst(lc);
        extract_columnrefs_from_expr(groupExpr, skel, "group");
    }

    // ============ ORDER BY ============
    foreach(lc, parse->sortClause) {
        SortGroupClause* sgc = (SortGroupClause*) lfirst(lc);
        TargetEntry* tle = get_sortgroupclause_tle(sgc, parse->targetList);
        if (tle && tle->expr) {
            extract_columnrefs_from_expr((Node*) tle->expr, skel, "sort");
        }
    }
}

// === Using sql string -- not used now ===
void analyze_query_context_from_sql_string(MetadataSkeleton* skel, const char* queryString) {
    List* all_queries = extract_all_subqueries(queryString);
    List* match_entries = generate_field_match_entries(skel, queryString); 

    ListCell* lc;
    foreach(lc, all_queries) {
        char* subquery = (char*) lfirst(lc);
        match_table_field_appearance(skel, subquery, match_entries);
    }
}

// === find all queries or sub queries ===
List* extract_all_subqueries(const char* sql) {
    List* queries = NIL;
    queries = lappend(queries, pstrdup(sql)); // main query

    const char* ptr = sql;
    while ((ptr = strstr(ptr, "SELECT")) != NULL) {
        const char* start = ptr;
        int depth = 0;
        bool found = false;
        while (*ptr) {
            if (*ptr == '(') depth++;
            else if (*ptr == ')') depth--;
            else if (depth == 0 && strncasecmp(ptr, "SELECT", 6) == 0) {
                found = true;
                break;
            }
            ptr++;
        }
        if (found) {
            char* sub = pnstrdup(start, ptr - start);
            queries = lappend(queries, sub);
        } else break;
    }
    return queries;
}

static char* str_tolower(const char* input) {
    size_t len = strlen(input);
    char* result = palloc(len + 1);

    for (size_t i = 0; i < len; i++) {
        result[i] = tolower((unsigned char) input[i]);
    }
    result[len] = '\0';
    return result;
}

// === check attributes in WHERE or not ===
void match_table_field_appearance(MetadataSkeleton* skel, const char* subquery, List* matchEntries) {
    char* lower_sql = str_tolower(subquery);

    ListCell* lc;
    foreach(lc, matchEntries) {
        FieldMatchEntry* entry = (FieldMatchEntry*) lfirst(lc);

        ListCell* mc;
        foreach(mc, entry->match_names) {
            const char* name = (const char*) lfirst(mc);
            if (strcasestr(lower_sql, name)) {
                ListCell* afc;
                foreach(afc, skel->attributeFeatureList) {
                    AttributeFeature* af = (AttributeFeature*) lfirst(afc);
                    if (strcmp(af->name, entry->full_name) == 0) {
                        if (strcasestr(lower_sql, "where") && strcasestr(strcasestr(lower_sql, "where"), name))
                            af->inWhere = true;
                    }
                }
            }
        }
    }
}

List* generate_field_match_entries(MetadataSkeleton* skel, const char* queryString) {
    List* matchList = NIL;
    ListCell* lc_attr;
    ListCell* lc_table;

    foreach(lc_attr, skel->attributeFeatureList) {
        AttributeFeature* af = (AttributeFeature*) lfirst(lc_attr);

        FieldMatchEntry* entry = palloc0(sizeof(FieldMatchEntry));
        strncpy(entry->full_name, af->name, sizeof(entry->full_name) - 1);
        entry->match_names = NIL;

        char* dot = strchr(af->name, '.');
        if (!dot) continue;
        char table[128];
        char attr[128];
        strncpy(table, af->name, dot - af->name);
        table[dot - af->name] = '\0';
        strncpy(attr, dot + 1, sizeof(attr) - 1);
        attr[sizeof(attr) - 1] = '\0';

        entry->match_names = lappend(entry->match_names, pstrdup(attr));
        entry->match_names = lappend(entry->match_names, psprintf("u.%s", attr));
        entry->match_names = lappend(entry->match_names, psprintf("%s.%s", table, attr));

        matchList = lappend(matchList, entry);
    }

    // select *, x.*
    foreach(lc_table, skel->tableNames) {
        const char* table = (const char*) lfirst(lc_table);

        TableKey key;
        strncpy(key.name, table, MAX_NAME_LEN);
        TableAttrEntry* entry = hash_search(skel->tableAttributes, &key, HASH_FIND, NULL);
        if (entry) {
            ListCell* lc_attrname;
            foreach(lc_attrname, entry->attrList) {
                const char* attr = (const char*) lfirst(lc_attrname);

                FieldMatchEntry* fentry = palloc0(sizeof(FieldMatchEntry));
                snprintf(fentry->full_name, sizeof(fentry->full_name), "%s.%s", table, attr);
                fentry->match_names = NIL;

                fentry->match_names = lappend(fentry->match_names, pstrdup("*"));
                fentry->match_names = lappend(fentry->match_names, psprintf("%s.*", table));
                fentry->match_names = lappend(fentry->match_names, psprintf("u.*"));

                matchList = lappend(matchList, fentry);
            }
        }
    }

    return matchList;
}

void extract_global_schema(MetadataSkeleton* skel) {
    int ret;

    MemoryContext oldctx = CurrentMemoryContext;  // save current context

    SPI_connect();

    // get all the user side tables
    const char* sql = "SELECT relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')";
    ret = SPI_execute(sql, true, 0);

    if (ret != SPI_OK_SELECT) {
        elog(ERROR, "Failed to fetch table names");
    }

    for (int i = 0; i < SPI_processed; i++) {
        char* relname_raw = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        if (!relname_raw)
            continue;

        MemoryContextSwitchTo(oldctx);   
        char* relname = pstrdup(relname_raw);
        MemoryContextSwitchTo(oldctx);   

        add_table(skel, relname);

        Oid relid = RelnameGetRelid(relname);
        if (!OidIsValid(relid))
            continue;

        Relation rel = heap_open(relid, AccessShareLock);
        TupleDesc tupdesc = RelationGetDescr(rel);

        for (int j = 0; j < tupdesc->natts; j++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
            if (attr->attisdropped)
                continue;
            
            MemoryContextSwitchTo(oldctx);
            char* attrname = pstrdup(NameStr(attr->attname));
            MemoryContextSwitchTo(CurrentMemoryContext);

            add_attribute(skel, relname, attrname);
        }

        heap_close(rel, AccessShareLock);
    }

    SPI_finish();
}


// === extract_tables_and_attributes ===
void extract_tables_and_attributes(MetadataSkeleton* skel, Query* parse) {
    ListCell* lc;
    List* rtable = parse->rtable;

    foreach(lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*) lfirst(lc);
        if (rte->rtekind != RTE_RELATION)
            continue;

        const char* relname = get_rel_name(rte->relid);
        if (!relname)
            continue;

        // process alias information
        const char* alias = rte->eref->aliasname;
        if (alias && strcmp(alias, relname) != 0) {
            insert_into_alias_map(skel->aliasMap, alias, relname);
        }

        add_table(skel, relname);

        Relation rel = heap_open(rte->relid, AccessShareLock);
        TupleDesc tupdesc = RelationGetDescr(rel);

        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped)
                continue;
            add_attribute(skel, pstrdup(relname), pstrdup(NameStr(attr->attname)));
        }

        heap_close(rel, AccessShareLock);
    }
}

static void build_corr_table(List* tableNames) {

    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(CorrKey);
    ctl.entrysize = sizeof(CorrEntry);
    ctl.hcxt = CurrentMemoryContext;
    corr_table = hash_create("Correlation Table", 256, &ctl, HASH_ELEM | HASH_CONTEXT);

    if (SPI_connect() != SPI_OK_CONNECT)
        return;

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT tablename, attname, correlation FROM pg_stats WHERE tablename IN (");

    bool first = true;
    ListCell* lc;
    foreach(lc, tableNames) {
        const char* tname = (const char*) lfirst(lc);
        if (!first) appendStringInfoString(&query, ", ");
        first = false;
        appendStringInfo(&query, "'%s'", tname);
    }
    appendStringInfoChar(&query, ')');

    if (SPI_execute(query.data, true, 0) == SPI_OK_SELECT) {
        SPITupleTable* tuptable = SPI_tuptable;
        TupleDesc tupdesc = tuptable->tupdesc;

        for (uint64 i = 0; i < SPI_processed; i++) {
            HeapTuple tup = tuptable->vals[i];

            bool isnull1, isnull2, isnull3;
            Datum val1 = SPI_getbinval(tup, tupdesc, 1, &isnull1); // tablename
            Datum val2 = SPI_getbinval(tup, tupdesc, 2, &isnull2); // attname
            Datum val3 = SPI_getbinval(tup, tupdesc, 3, &isnull3); // correlation

            if (isnull1 || isnull2 || isnull3)
                continue;

            // 修复：用 Name 类型来解码 name 字段（而非 text）
            char* relname = NameStr(*DatumGetName(val1));
            char* attname = NameStr(*DatumGetName(val2));
            float4 corr = DatumGetFloat4(val3);


            CorrKey key;
            strncpy(key.relname, relname, MAX_NAME_LEN);
            strncpy(key.attname, attname, MAX_NAME_LEN);

            bool found;
            CorrEntry* entry = hash_search(corr_table, &key, HASH_ENTER, &found);
            entry->correlation = corr;
        }
    }

    SPI_finish();
}

static bool correlation_above_0_9(const char* relname, const char* attname) {
    if (!corr_table) return false;

    CorrKey key;
    strncpy(key.relname, relname, MAX_NAME_LEN);
    strncpy(key.attname, attname, MAX_NAME_LEN);

    CorrEntry* entry = hash_search(corr_table, &key, HASH_FIND, NULL);
    return (entry && entry->correlation > 0.9);
}

static void free_corr_table() {
    if (corr_table)
        hash_destroy(corr_table);
    corr_table = NULL;
}

static List* getTableNames(Query* parse) {
    ListCell* lc;
    List* rtable = parse->rtable;
    List* tableNames = NIL;

    foreach(lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*) lfirst(lc);
        if (rte->rtekind != RTE_RELATION)
            continue;

        const char* relname = get_rel_name(rte->relid);
        tableNames = lappend(tableNames, pstrdup(relname));
    }
    return tableNames;
}

// === analyze_attribute_metadata ===
void analyze_attribute_metadata(MetadataSkeleton* skel, Query* parse) {
    ListCell* lc;
    List* rtable = parse->rtable;

    List* tableNames = getTableNames(parse);

    build_corr_table(tableNames);

    foreach(lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*) lfirst(lc);
        if (rte->rtekind != RTE_RELATION)
            continue;

        const char* relname = get_rel_name(rte->relid);
        Relation rel = heap_open(rte->relid, AccessShareLock);
        TupleDesc tupdesc = RelationGetDescr(rel);
        List* indexlist = RelationGetIndexList(rel);

        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped)
                continue;

            AttributeFeature* af = (AttributeFeature*) palloc0(sizeof(AttributeFeature));
            snprintf(af->name, sizeof(af->name), "%s.%s", relname, NameStr(attr->attname));
            af->inSQL = true;
            af->inWhere = false;
            af->inJoin = false;
            af->inGroup = false;
            af->inSort = false;
            af->isNumeric = (attr->atttypid == INT4OID || attr->atttypid == FLOAT4OID || attr->atttypid == FLOAT8OID);

            // check index
            af->hasIndex = false;
            ListCell* ilc;
            foreach(ilc, indexlist) {
                Oid indexOid = lfirst_oid(ilc);
                HeapTuple indexTup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
                if (HeapTupleIsValid(indexTup)) {
                    Form_pg_index indexStruct = (Form_pg_index) GETSTRUCT(indexTup);
                    for (int j = 0; j < indexStruct->indnatts; j++) {
                        if (indexStruct->indkey.values[j] == attr->attnum) {
                            af->hasIndex = true;
                            break;
                        }
                    }
                    ReleaseSysCache(indexTup);
                }
                if (af->hasIndex) break;
            }

            // check correlationAbove0.9
            af->correlationAbove0_9 = correlation_above_0_9(relname, NameStr(attr->attname));


            add_attribute_feature(skel, af);
        }

        list_free(indexlist);
        heap_close(rel, AccessShareLock);
    }
    free_corr_table();
    // free tableNames (list of pstrdup'ed char*)
    ListCell* lc2;
    foreach(lc2, tableNames) {
        char* name = (char*) lfirst(lc2);
        pfree(name);
    }
    list_free(tableNames);
}

// PG12 does not have optimizer/var.h, thus it needs to add this
extern List* pull_var_clause(Node* node, int flags);

static AttributeFeature* find_attribute_feature(MetadataSkeleton* skel, const char* attrName) {
    ListCell* lc;
    foreach(lc, skel->attributeFeatureList) {
        AttributeFeature* af = (AttributeFeature*) lfirst(lc);
        if (strncmp(af->name, attrName, sizeof(af->name)) == 0) {
            return af;
        }
    }
    return NULL;
}

static bool resolve_attr_name(MetadataSkeleton* skel, Var* var, Query* query, char* outName) {
    if (!IsA(var, Var) || var->varlevelsup != 0) return false;
    RangeTblEntry* rte = rt_fetch(var->varno, query->rtable);
    const char* attname = get_rte_attribute_name(rte, var->varattno);

    const char* alias = rte->eref->aliasname;
    const char* mapped = lookup_alias_realname(skel->aliasMap, alias);
    const char* tablename = NULL;

    if (mapped) {
        tablename = mapped;
    } else if (rte->rtekind == RTE_RELATION) {
        tablename = get_rel_name(rte->relid);
    } else {
        tablename = alias;  // fallback
    }
    if (!tablename || !attname) return false;

    snprintf(outName, 256, "%s.%s", tablename, attname);
    return true;
}


static void mark_var_list(MetadataSkeleton* skel, Query* query, List* varList, const char* field) {
    ListCell* lc;
    foreach(lc, varList) {
        Var* var = (Var*) lfirst(lc);
        if (!IsA(var, Var)) continue;
        char attrName[256];

        if (!resolve_attr_name(skel, var, query, attrName)) continue;

        AttributeFeature* af = find_attribute_feature(skel, attrName);
        if (!af) continue;

        if (strcmp(field, "inSQL") == 0)   af->inSQL = true;
        if (strcmp(field, "inWhere") == 0) af->inWhere = true;
        if (strcmp(field, "inGroup") == 0) af->inGroup = true;
        if (strcmp(field, "inSort") == 0)  af->inSort = true;
        if (strcmp(field, "inJoin") == 0)  af->inJoin = true;
    }
}

static List* extract_group_vars(Query* query) {
    List* groupVars = NIL;
    ListCell* lc;

    foreach(lc, query->groupClause) {
        SortGroupClause* sgc = (SortGroupClause*) lfirst(lc);

        ListCell* lc2;
        foreach(lc2, query->targetList) {
            TargetEntry* tle = (TargetEntry*) lfirst(lc2);
            if (tle->ressortgroupref == sgc->tleSortGroupRef) {
                List* vars = pull_var_clause((Node *)tle->expr, PVC_RECURSE_AGGREGATES);
                groupVars = list_concat(groupVars, vars);
                break;
            }
        }
    }

    return groupVars;
}

static List* extract_sort_vars(Query* query) {
    List* sortVars = NIL;
    ListCell* lc;

    foreach(lc, query->sortClause) {
        SortGroupClause* sgc = (SortGroupClause*) lfirst(lc);

        ListCell* lc2;
        foreach(lc2, query->targetList) {
            TargetEntry* tle = (TargetEntry*) lfirst(lc2);
            if (tle->ressortgroupref == sgc->tleSortGroupRef) {
                List* vars = pull_var_clause((Node *)tle->expr, PVC_RECURSE_AGGREGATES);
                sortVars = list_concat(sortVars, vars);
                break;
            }
        }
    }

    return sortVars;
}

static void extract_join_vars(Node* node, List** joinVars) {
    if (node == NULL) return;

    if (IsA(node, JoinExpr)) {
        JoinExpr* join = (JoinExpr*) node;

        List* vars = pull_var_clause(join->quals, PVC_RECURSE_AGGREGATES);
        *joinVars = list_concat(*joinVars, vars);

        extract_join_vars(join->larg, joinVars);
        extract_join_vars(join->rarg, joinVars);
    } else if (IsA(node, FromExpr)) {
        FromExpr* fe = (FromExpr*) node;
        ListCell* lc;
        foreach(lc, fe->fromlist) {
            extract_join_vars((Node *) lfirst(lc), joinVars);
        }
    }
}

static const char* get_node_tag_name(NodeTag tag) {
    switch (tag) {
        case T_Var: return "Var";
        case T_Const: return "Const";
        case T_Param: return "Param";
        case T_FuncExpr: return "FuncExpr";
        case T_OpExpr: return "OpExpr";
        case T_BoolExpr: return "BoolExpr";
        case T_RelabelType: return "RelabelType";
        case T_NullTest: return "NullTest";
        case T_CoerceToDomain: return "CoerceToDomain";
        case T_CoerceToDomainValue: return "CoerceToDomainValue";
        case T_CoerceViaIO: return "CoerceViaIO";
        default: return "Unknown";
    }
}

static Var* unwrap_var(Node* node) {
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
        return (Var*) node;

    if (IsA(node, RelabelType)) {
        RelabelType* rt = (RelabelType*) node;
        return unwrap_var((Node*) rt->arg); 
    }

    return NULL;
}

static void log_opexpr_details(Node* node) {
    if (!IsA(node, OpExpr)) {
        return;
    }

    OpExpr* op = (OpExpr*) node;

    ListCell* lc;
    int idx = 0;
    foreach(lc, op->args) {
        Node* arg = (Node*) lfirst(lc);
        idx++;
    }
}

static bool mark_join_condition_walker(Node* node, void* context) {
    if (node == NULL)
        return false;

    JoinCondContext* ctx = (JoinCondContext*) context;
    MetadataSkeleton* skel = ctx->skel;
    Query* query = ctx->query;
    if (IsA(node, OpExpr)) {
        OpExpr* op = (OpExpr*) node;
        log_opexpr_details((Node*) node);
        if (list_length(op->args) == 2) {
            Node* arg1 = linitial(op->args);
            Node* arg2 = lsecond(op->args);

            Var* var1 = unwrap_var(arg1);
            Var* var2 = unwrap_var(arg2);

            if (var1 && var2 && var1->varno != var2->varno) {
                char name1[256], name2[256];
                if (resolve_attr_name(skel, var1, query, name1)) {
                    AttributeFeature* af1 = find_attribute_feature(skel, name1);
                    if (af1) af1->inJoin = true;
                }
                if (resolve_attr_name(skel, var2, query, name2)) {
                    AttributeFeature* af2 = find_attribute_feature(skel, name2);
                    if (af2) af2->inJoin = true;
                }
            }
        }
    }

    return expression_tree_walker(node, mark_join_condition_walker, context);;
}

// using Var
void analyze_query_structure(MetadataSkeleton* skel, Query* query) {
    List* targetVars = pull_var_clause((Node *)query->targetList, PVC_RECURSE_AGGREGATES);
    List* whereVars  = pull_var_clause((Node *)query->jointree ? query->jointree->quals : NULL, PVC_RECURSE_AGGREGATES);
    List* groupVars = extract_group_vars(query);
    List* sortVars  = extract_sort_vars(query);
    mark_var_list(skel, query, targetVars, "inSQL");
    mark_var_list(skel, query, whereVars,  "inWhere");
    mark_var_list(skel, query, groupVars,  "inGroup");
    mark_var_list(skel, query, sortVars,   "inSort");

    JoinCondContext ctx = { .skel = skel, .query = query };

    if (query->jointree && query->jointree->quals) {
        mark_join_condition_walker((Node*) query->jointree->quals, &ctx);
    }

    ListCell* lc;
    foreach(lc, query->jointree->fromlist) {
        Node* n = (Node*) lfirst(lc);
        if (IsA(n, JoinExpr)) {
            JoinExpr* join = (JoinExpr*) n;
            if (join->quals)
                mark_join_condition_walker((Node*) join->quals, &ctx);
        }
    }


    list_free(targetVars);
    list_free(whereVars);
    list_free(groupVars);
    list_free(sortVars);
}


void free_metadata_skeleton(MetadataSkeleton* skel) {
    if (skel->tableNames) {
        ListCell* lc;
        foreach(lc, skel->tableNames) {
            char* name = (char*) lfirst(lc);
            pfree(name);
        }
        list_free(skel->tableNames);
    }

    if (skel->tableAttributes) {
        HASH_SEQ_STATUS status;
        TableAttrEntry* entry;
        hash_seq_init(&status, skel->tableAttributes);
        while ((entry = hash_seq_search(&status)) != NULL) {
            if (entry->attrList) {
                ListCell* lc;
                foreach(lc, entry->attrList) {
                    char* attr = (char*) lfirst(lc);
                    pfree(attr);
                }
                list_free(entry->attrList);
            }
        }
        hash_destroy(skel->tableAttributes);
    }

    if (skel->attributeFeatureList) {
        ListCell* lc;
        foreach(lc, skel->attributeFeatureList) {
            AttributeFeature* af = (AttributeFeature*) lfirst(lc);
            pfree(af);
        }
        list_free(skel->attributeFeatureList);
    }

    if (skel->tableFeatureList) {
        ListCell* lc;
        foreach(lc, skel->tableFeatureList) {
            TableFeature* tf = (TableFeature*) lfirst(lc);
            // Optional: free tf->attrList if used
            list_free(tf->attrList);
            pfree(tf);
        }
        list_free(skel->tableFeatureList);
    }

    if (skel->aliasMap) {
        hash_destroy(skel->aliasMap);
    }

    pfree(skel);
}

// === main entry ===
char* generate_metadata_json(Query* parse) {

    MetadataSkeleton* skel = create_metadata_skeleton(); // prepare the data structure

    // extract_tables_and_attributes(skel, parse); // only get tables and attributes in the db occured in the script

    extract_global_schema(skel);   // parepare all tables and attributes in the db

    analyze_attribute_metadata(skel, parse);        // attribute features from parse, index, numberic...

    analyze_query_structure(skel, parse); // features: where, join, group etc

    finalize_table_features(skel); // use attribute features to update table features

    char* result = export_metadata_json(skel);
    free_metadata_skeleton(skel); 
    return result;
}


#endif