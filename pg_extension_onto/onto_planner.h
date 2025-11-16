#ifndef ONTO_PLANNER_H
#define ONTO_PLANNER_H

#include <unistd.h>
#include "onto_configs.h"
#include "onto_util.h"
#include "onto_meta.h"
#include "onto_sharedmem.h"
#include "onto_bufferstate.h"
#include "storage/lockdefs.h"

#include "catalog/pg_type.h"        // INT4OID, FLOAT4OID, etc
#include "catalog/pg_index.h"       // Form_pg_index
#include "catalog/index.h"          // INDEXRELID
#include "utils/syscache.h"         // SearchSysCache1, ReleaseSysCache
#include "utils/catcache.h"         // catalog caches
#include "utils/rel.h"              // Relation, RelationGetIndexList

#include "access/heapam.h"
#include <float.h>
#include <sys/time.h>
#include "nodes/plannodes.h"
#include <math.h>

#define REWARD_EPSILON 300.0

#define save_arm_options(x) { \
  bool hj = enable_hashjoin;\
  bool mj = enable_mergejoin;\
  bool nl = enable_nestloop;\
  bool is = enable_indexscan;\
  bool ss = enable_seqscan;\
  bool io = enable_indexonlyscan;\
  { x } \
  enable_hashjoin = hj;\
  enable_mergejoin = mj;\
  enable_nestloop = nl;\
  enable_indexscan = is;\
  enable_seqscan = ss;\
  enable_indexonlyscan = io; }


// Connect to a Onto server, construct plans for each arm, have the server
// select a plan. Has the same signature as the PG optimizer.
OntoPlan *plan_query(Query *parse, int cursorOptions, ParamListInfo boundParams);

// Translate an arm index into SQL statements to give the hint (used for EXPLAIN).
char* arm_to_hint(int arm);


// Set the planner hint options to the correct one for the passed-in arm
// index. Should be called with the `save_arm_options` macro so we don't
// blast-away the user's config.
static void set_arm_options(int arm) {
  enable_hashjoin = false;
  enable_mergejoin = false;
  enable_nestloop = false;
  enable_indexscan = false;
  enable_seqscan = false;
  enable_indexonlyscan = false;
  
  switch (arm) {
  case 0:
      enable_hashjoin = true;
      enable_indexscan = true;
      enable_mergejoin = true;
      enable_nestloop = true;
      enable_seqscan = true;
      enable_indexonlyscan = true;
      break;

  // begin bao top 5 hint sets
  case 1: 
    enable_hashjoin = true;
    enable_indexscan = true;
    enable_mergejoin = true;
    enable_nestloop = false;
    enable_seqscan = true;
    enable_indexonlyscan = true;
    break;
  case 2: 
    enable_hashjoin = true;
    enable_indexscan = false;
    enable_mergejoin = false;
    enable_nestloop = true;
    enable_seqscan = true;
    enable_indexonlyscan = true;
    break;
  case 3: 
    enable_hashjoin = true;
    enable_indexscan = false;
    enable_mergejoin = false;
    enable_nestloop = false;
    enable_seqscan = true;
    enable_indexonlyscan = true;
    break;
  case 4: 
    enable_hashjoin = false;
    enable_indexscan = true;
    enable_mergejoin = true;
    enable_nestloop = true;
    enable_seqscan = true;
    enable_indexonlyscan = true;
    break;
  case 5: 
    enable_hashjoin = true;
    enable_indexscan = true;
    enable_mergejoin = false;
    enable_nestloop = true;
    enable_seqscan = true;
    enable_indexonlyscan = true;
    break;
  // end bao top 5 hint sets

  // // begin testing arms
  // case 0:
  //   enable_hashjoin = true;
  //   enable_indexscan = true;
  //   enable_mergejoin = true;
  //   enable_nestloop = true;
  //   enable_seqscan = true;
  //   enable_indexonlyscan = true;
  //   break;

  // case 1: 
  //   enable_hashjoin = false;
  //   enable_indexscan = true;
  //   enable_mergejoin = true;
  //   enable_nestloop = true;
  //   enable_seqscan = true;
  //   enable_indexonlyscan = true;
  //   break;
  // case 2: 
  //   enable_hashjoin = true;
  //   enable_indexscan = false;
  //   enable_mergejoin = true;
  //   enable_nestloop = true;
  //   enable_seqscan = true;
  //   enable_indexonlyscan = true;
  //   break;
  // case 3: 
  //   enable_hashjoin = true;
  //   enable_indexscan = true;
  //   enable_mergejoin = false;
  //   enable_nestloop = true;
  //   enable_seqscan = true;
  //   enable_indexonlyscan = true;
  //   break;
  // case 4: 
  //   enable_hashjoin = true;
  //   enable_indexscan = true;
  //   enable_mergejoin = true;
  //   enable_nestloop = true;
  //   enable_seqscan = false;
  //   enable_indexonlyscan = true;
  //   break;
  // case 5: 
  //   enable_hashjoin = true;
  //   enable_indexscan = true;
  //   enable_mergejoin = true;
  //   enable_nestloop = true;
  //   enable_seqscan = true;
  //   enable_indexonlyscan = false;
  //   break;
  //   // end testing arms
    
  case 7: 
    enable_indexonlyscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    break;
  case 8: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    break;
  case 9: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_nestloop = true; 
    break;
  case 10:
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_seqscan = true; 
    break;
  case 11: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    enable_seqscan = true; 
    break;
  case 12: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_mergejoin = true; 
    enable_seqscan = true; 
    break;
  case 13: 
    enable_hashjoin = true; 
    enable_indexscan = true; 
    enable_nestloop = true; 
    break;
  case 14: 
    enable_indexscan = true; 
    enable_nestloop = true; 
    break;
  case 15: 
    enable_indexscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    enable_seqscan = true; 
    break;
  case 16: 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_nestloop = true; 
    break;
  case 17: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    break;
  case 18: 
    enable_indexscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    break;
  case 19: 
    enable_indexonlyscan = true; 
    enable_mergejoin = true; 
    enable_nestloop = true; 
    enable_seqscan = true; 
    break;
  case 20: 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_nestloop = true; 
    enable_seqscan = true; 
    break;
  case 21: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_indexscan = true; 
    enable_mergejoin = true; 
    break;
  case 22: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_mergejoin = true; 
    break;
  case 23: 
    enable_hashjoin = true; 
    enable_indexscan = true; 
    enable_nestloop = true; 
    enable_seqscan = true; 
    break;
  case 24: 
    enable_hashjoin = true; 
    enable_indexscan = true; 
    break;
  case 25: 
    enable_hashjoin = true; 
    enable_indexonlyscan = true; 
    enable_nestloop = true; 
    break;
  default:
    elog(ERROR, "Invalid arm index %d selected.", arm);
    break;
  }
}

// Get json data of arm
char* get_arm_config_json(int arm) {
  char* buf;
  size_t size;
  FILE* stream;

  bool hj = enable_hashjoin;
  bool mj = enable_mergejoin;
  bool nl = enable_nestloop;
  bool is = enable_indexscan;
  bool ss = enable_seqscan;
  bool io = enable_indexonlyscan;

  set_arm_options(arm);

  stream = open_memstream(&buf, &size);
  fprintf(stream, "{ \"enable_hashjoin\": %s, \"enable_mergejoin\": %s, \"enable_nestloop\": %s, "
                  "\"enable_indexscan\": %s, \"enable_seqscan\": %s, \"enable_indexonlyscan\": %s, "
                  "\"index\": %d }",
          enable_hashjoin ? "true" : "false",
          enable_mergejoin ? "true" : "false",
          enable_nestloop ? "true" : "false",
          enable_indexscan ? "true" : "false",
          enable_seqscan ? "true" : "false",
          enable_indexonlyscan ? "true" : "false",
          arm);
  fclose(stream);

  enable_hashjoin = hj;
  enable_mergejoin = mj;
  enable_nestloop = nl;
  enable_indexscan = is;
  enable_seqscan = ss;
  enable_indexonlyscan = io;

  return buf;
}


// Get a query plan for a particular arm.
static PlannedStmt* plan_arm(int arm, Query* parse,
                             int cursorOptions, ParamListInfo boundParams) {

  // altered
  char *query_tree_str = nodeToString(parse);
  FILE *f = fopen("/tmp/query_tree.log", "a");  // any path that your can access
  if (f) {
      fprintf(f, "=== New Query Tree ===\n%s\n", query_tree_str);
      fclose(f);
  }

  PlannedStmt* plan = NULL;
  Query* query_copy = copyObject(parse); // create a copy of the query plan

  if (arm == -1) {
    // Use whatever the user has set as the current configuration.
    plan = standard_planner(query_copy, cursorOptions, boundParams);
    return plan;
  }
  
  // Preserving the user's options, set the config to match the arm index
  // and invoke the PG planner.
  save_arm_options({
      set_arm_options(arm);
      plan = standard_planner(query_copy, cursorOptions, boundParams);
    });

  return plan;
}

// A struct to represent a query plan before we transform it into JSON.
typedef struct OntoPlanNode {
  // An integer representation of the PG NodeTag.
  unsigned int node_type;

  // The optimizer cost for this node (total cost).
  double optimizer_cost;

  // The cardinality estimate (plan rows) for this node.
  double cardinality_estimate;

  // If this is a scan or index lookup, the name of the underlying relation.
  char* relation_name;

  // Left child.
  struct OntoPlanNode* left;


  // Right child.
  struct OntoPlanNode* right;

  // bool freed;
} OntoPlanNode;

// Transform the operator types we care about from their PG tag to a
// string. Call other operators "Other".
static const char* node_type_to_string(NodeTag tag) {
  switch (tag) {
  case T_SeqScan:
    return "Seq Scan";
  case T_IndexScan:
    return "Index Scan";
  case T_IndexOnlyScan:
    return "Index Only Scan";
  case T_BitmapIndexScan:
    return "Bitmap Index Scan";
  case T_NestLoop:
    return "Nested Loop";
  case T_MergeJoin:
    return "Merge Join";
  case T_HashJoin:
    return "Hash Join";
  default:
    return "Other";
  }
}

// Allocate an empty OntoPlanNode.
static OntoPlanNode* new_onto_plan() {
  return (OntoPlanNode*) malloc(sizeof(OntoPlanNode));
}

// Free (recursively) an entire OntoPlanNode. Frees children as well.
static void free_onto_plan_node(OntoPlanNode* node) {
  if (node->left) free_onto_plan_node(node->left);
  if (node->right) free_onto_plan_node(node->right);
  free(node);
}

// Emit a JSON representation of the given OntoPlanNode to the stream given.
// Recursive function, the entry point is `plan_to_json`.
static void emit_json(OntoPlanNode* node, FILE* stream) {
  fprintf(stream, "{\"Node Type\": \"%s\",", node_type_to_string(node->node_type));
  fprintf(stream, "\"Node Type ID\": \"%d\",", node->node_type);
  if (node->relation_name)
    // TODO need to escape the relation name for JSON...
    fprintf(stream, "\"Relation Name\": \"%s\",", node->relation_name);
  fprintf(stream, "\"Total Cost\": %f,", node->optimizer_cost);
  fprintf(stream, "\"Plan Rows\": %f", node->cardinality_estimate);
  if (!node->left && !node->right) {
    fprintf(stream, "}");
    return;
  }

  fprintf(stream, ", \"Plans\": [");
  if (node->left) emit_json(node->left, stream);
  if (node->right) {
    fprintf(stream, ", ");
    emit_json(node->right, stream);
  }
  fprintf(stream, "]}");
}

char *onto_sequence_id = NULL;
// Transform a PostgreSQL PlannedStmt into a OntoPlanNode tree.
static OntoPlanNode* transform_plan(PlannedStmt* stmt, Plan* node) {
  OntoPlanNode* result = new_onto_plan();

  result->node_type = node->type;
  result->optimizer_cost = node->total_cost;
  result->cardinality_estimate = node->plan_rows;
  result->relation_name = get_relation_name(stmt, node);

  result->left = NULL;
  result->right = NULL;
  if (node->lefttree) result->left = transform_plan(stmt, node->lefttree);
  if (node->righttree) result->right = transform_plan(stmt, node->righttree);

  return result;
}

// Given a PostgreSQL PlannedStmt, produce the JSON representation we need to
// send to the Onto server.
static char* plan_to_json(PlannedStmt* plan) {
  char* buf;
  size_t json_size;
  FILE* stream;
  OntoPlanNode* transformed_plan;

  transformed_plan = transform_plan(plan, plan->planTree);
  
  stream = open_memstream(&buf, &json_size);
  fprintf(stream, "{\"Plan\": ");
  emit_json(transformed_plan, stream);
  fprintf(stream, "}\n");
  fclose(stream);

  free_onto_plan_node(transformed_plan);
  
  return buf;
}

double estimate_reward(PlannedStmt* stmt) {
    if (!stmt || !stmt->planTree) {
        elog(WARNING, "[OntoPlanner] estimate_reward received null plan");
        return DBL_MAX;
    }

    Plan* root_plan = stmt->planTree;

    // You can also combine startup_cost and total_cost if you wish.
    double reward = root_plan->total_cost;

    elog(DEBUG1, "[OntoPlanner] Estimated reward (cost) = %.4f", reward);
    return reward;
}

static inline void free_planned_stmt(PlannedStmt* plan) {
    // no-op; PostgreSQL memory context will clean it up automatically
}

int select_best_arm_greedy_balanced(PlannedStmt **best_plan,
                                     Query *parse,
                                     int cursorOptions,
                                     ParamListInfo boundParams) {
    double reward_list[ONTO_MAX_ARMS];
    PlannedStmt *plan_list[ONTO_MAX_ARMS] = {0};

    double best_reward = DBL_MAX;
    int candidate_arms[ONTO_MAX_ARMS];
    int num_candidates = 0;

    struct timeval t_start, t_end;
    gettimeofday(&t_start, NULL);

    for (int i = 0; i < onto_num_arms; i++) {
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        PlannedStmt *stmt = plan_arm(i, parse, cursorOptions, boundParams);

        gettimeofday(&t1, NULL);
        long plan_us = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);

        double reward = estimate_reward(stmt);
        reward_list[i] = reward;
        plan_list[i] = stmt;
        
        elog(WARNING, "[OntoPlanner] Arm %d: reward = %.4f, plan_time = %.2fms",
             i, reward, plan_us / 1000.0);

        if (reward < best_reward)
            best_reward = reward;
    }

    for (int i = 0; i < onto_num_arms; i++) {
        if (fabs(reward_list[i] - best_reward) < REWARD_EPSILON) {
            candidate_arms[num_candidates++] = i;
        } else {
            if (plan_list[i]) free_planned_stmt(plan_list[i]);
        }
    }

    int chosen = candidate_arms[0];
    int min_use = shared_arm_schedule->arm_usage_count[chosen];
    for (int j = 1; j < num_candidates; j++) {
        int arm = candidate_arms[j];
        int use = shared_arm_schedule->arm_usage_count[arm];
        if (use < min_use) {
            min_use = use;
            chosen = arm;
        }
    }

    gettimeofday(&t_end, NULL);
    long total_us = (t_end.tv_sec - t_start.tv_sec) * 1000000 + (t_end.tv_usec - t_start.tv_usec);

    elog(WARNING, "[OntoPlanner] Greedy (balanced): best_reward = %.2f, %d candidates, chosen arm = %d (usage = %d), total_time = %.2fms",
         best_reward, num_candidates, chosen, min_use, total_us / 1000.0);

    shared_arm_schedule->arm_usage_count[chosen]++;
    *best_plan = plan_list[chosen];

    return chosen;
}

// Primary planning function. Invokes the PG planner for each arm, sends the
// results to the Onto server, gets the response, and returns the corrosponding
// query plan (as a OntoPlan).
OntoPlan* plan_query(Query *parse, int cursorOptions, ParamListInfo boundParams) {
  OntoPlan* plan;
  PlannedStmt* plan_for_arm[ONTO_MAX_ARMS];
  char* json_for_arm[ONTO_MAX_ARMS];
  Query* query_copy;
  int conn_fd;

  // Prepare the plan object to store a OntoQueryInfo instance.
  plan = (OntoPlan*) malloc(sizeof(OntoPlan));
  plan->query_info = (OntoQueryInfo*) malloc(sizeof(OntoQueryInfo));
  plan->selection = 0;
  
  // Connect this buffer state with the query.
  plan->query_info->buffer_json = buffer_state();

  // Connect the metadata with the query
  plan->query_info->metadata_json = generate_metadata_json(parse);

  char* arm_config_jsons[ONTO_MAX_ARMS];
  for (int i = 0; i < onto_num_arms; i++) {
      arm_config_jsons[i] = strdup(get_arm_config_json(i)); 
  }

  elog(WARNING, "[OntoPlanner] onto_sequence_id= %s", onto_sequence_id);
  if (!enable_onto_selection) {

      if (pg_selection) {
        elog(WARNING, "pg selection is enabled, only pg is used to select plans.");
        plan->plan = plan_arm(-1, parse, cursorOptions, boundParams);
        plan->query_info->plan_json = plan_to_json(plan->plan);
        return plan;
      }

      uint32 query_index = get_next_query_index();
      PlannedStmt* best_plan = NULL;
      int selected_arm = -1;

      if (query_index < 0) {
          // Cold-start phase
          selected_arm = query_index % onto_num_arms;  // even distribution
          best_plan = plan_arm(selected_arm, parse, cursorOptions, boundParams);
      } else {
          selected_arm = select_best_arm_greedy_balanced(&best_plan, parse, cursorOptions, boundParams);
      }

      plan->plan = best_plan;
      plan->query_info->plan_json = plan_to_json(best_plan);
      plan->query_info->arm_config_json = strdup(arm_config_jsons[selected_arm]);

      for (int i = 0; i < onto_num_arms; i++) {
          free(arm_config_jsons[i]);
      }
      return plan;
  }
  
  conn_fd = connect_to_onto(onto_host, onto_port);
  if (conn_fd == -1) {
    elog(WARNING, "Unable to connect to Onto server.");
    return NULL;
  }

  memset(plan_for_arm, 0, ONTO_MAX_ARMS*sizeof(PlannedStmt*));

  send_json_with_length(conn_fd, START_QUERY_MESSAGE);
  for (int i = 0; i < onto_num_arms; i++) {
    // Plan the query for this arm.
    query_copy = copyObject(parse);
    plan_for_arm[i] = plan_arm(i, query_copy, cursorOptions, boundParams);

    // Transform it into JSON, transmit it to the Onto server.
    json_for_arm[i] = plan_to_json(plan_for_arm[i]);
    send_json_with_length(conn_fd, json_for_arm[i]);
    send_json_with_length(conn_fd, arm_config_jsons[i]);
  }
  
  send_json_with_length(conn_fd, plan->query_info->buffer_json);
  send_json_with_length(conn_fd, plan->query_info->metadata_json);  
  send_json_with_length(conn_fd, TERMINAL_MESSAGE);
  shutdown(conn_fd, SHUT_WR);
  // Read the response.
  int bytes_read = read(conn_fd, &plan->selection, sizeof(unsigned int));
  if (bytes_read != sizeof(unsigned int)) {
      elog(WARNING, "Onto could not read the response from the server.");
      plan->selection = 0;
  }

  shutdown(conn_fd, SHUT_RDWR);

  if (plan->selection >= ONTO_MAX_ARMS) {
    elog(ERROR, "Onto server returned arm index %d, which is outside the range.",
         plan->selection);
    plan->selection = 0;
  }

  // Keep the plan the Onto server selected, and associate the JSON representation
  // of the plan with the OntoPlan. Free everything else.
  // change line number
  plan->plan = plan_for_arm[plan->selection];
  for (int i = 0; i < onto_num_arms; i++) {
    if (i == plan->selection) {
      plan->query_info->plan_json = json_for_arm[i];
      plan->query_info->selected_arm = plan->selection;
      const char* arm_config = arm_config_jsons[i] ? arm_config_jsons[i] : "{}";
      plan->query_info->arm_config_json = strdup(arm_config);
      // free(arm_config_jsons[i]);
      if (arm_config_jsons[i]) {
          elog(WARNING, "arm_config_json[%d] = %s", i, arm_config_jsons[i]);
      } else {
          elog(WARNING, "arm_config_json[%d] = NULL", i);
      }
      char* original = plan->query_info->metadata_json;
      int len = strlen(original) + strlen(arm_config) + 64;
      char* new_json = (char*) palloc(len); 
      char* insert_pos = strrchr(original, '}');
      if (!insert_pos) {
          elog(ERROR, "Invalid JSON in metadata_json");
      }
      int head_len = insert_pos - original;
      strncpy(new_json, original, head_len);
      new_json[head_len] = '\0';
      snprintf(new_json + head_len, len - head_len,
               ", \"arm_config_json\": %s}", plan->query_info->arm_config_json);
      char* old_metadata = plan->query_info->metadata_json;
      plan->query_info->metadata_json = new_json;
      pfree(old_metadata); 
      // free(plan->query_info->metadata_json);
      // plan->query_info->metadata_json = new_json;

    } else {
      free(json_for_arm[i]);
      // free(arm_config_jsons[i]);
    }
  }
    
  return plan;
}

// Given an arm index, produce the SQL statements that would cause PostgreSQL to
// select the same query plan as Onto would.
char* arm_to_hint(int arm) {
  char* buf;
  size_t size;
  FILE* stream;
  
  stream = open_memstream(&buf, &size);

  save_arm_options({
      set_arm_options(arm);
      if (!enable_nestloop) fprintf(stream, "SET enable_nestloop TO off; ");
      if (!enable_hashjoin) fprintf(stream, "SET enable_hashjoin TO off; ");
      if (!enable_mergejoin) fprintf(stream, "SET enable_mergejoin TO off; ");
      if (!enable_seqscan) fprintf(stream, "SET enable_seqscan TO off; ");
      if (!enable_indexscan) fprintf(stream, "SET enable_indexscan TO off; ");
      if (!enable_indexonlyscan) fprintf(stream, "SET enable_indexonlyscan TO off; ");
    });

  fclose(stream);
  
  if (size == 0) return NULL;
  return buf;
}

#endif
