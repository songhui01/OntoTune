#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include <math.h>

#include "onto_configs.h"
#include "onto_util.h"
#include "onto_bufferstate.h"
#include "onto_sharedmem.h"
#include "onto_planner.h"
#include "postgres.h"
#include "fmgr.h"
#include "parser/parsetree.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "utils/guc.h"
#include "commands/explain.h"
#include "tcop/tcopprot.h"

PG_MODULE_MAGIC;
void _PG_init(void);
void _PG_fini(void);


// Onto works by integrating with PostgreSQL's hook functionality.
// 1) The onto_planner hook intercepts a query before the PG optimizer handles
//    it, and communicates with the Onto server.
// 2) The onto_ExecutorStart hook sets up time recording for the given query.
//    static PlannedStmt* onto_planner(Query* parse,
// 3) The onto_ExecutorEnd hook gets the query timing and sends the reward
//    for the query back to the Onto server.
// 4) The onto_ExplainOneQuery hook adds the Onto suggested hint and the reward
//    prediction to the EXPLAIN output of a query.
static PlannedStmt* onto_planner(Query *parse,
                                int cursorOptions, ParamListInfo boundParams);
static void onto_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void onto_ExecutorEnd(QueryDesc *queryDesc);
static void onto_ExplainOneQuery(Query* query, int cursorOptions, IntoClause* into,
                                ExplainState* es, const char* queryString,
                                ParamListInfo params, QueryEnvironment *queryEnv);

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;

// planner_hook_type get_prev_planner_hook(void) {
//     return prev_planner_hook;
// }

void _PG_init(void) {
  // install each Onto hook
  prev_ExecutorStart = ExecutorStart_hook;
  ExecutorStart_hook = onto_ExecutorStart;

  prev_ExecutorEnd = ExecutorEnd_hook;
  ExecutorEnd_hook = onto_ExecutorEnd;

  prev_planner_hook = planner_hook;
  planner_hook = onto_planner;

  prev_ExplainOneQuery = ExplainOneQuery_hook;
  ExplainOneQuery_hook = onto_ExplainOneQuery;

  // define Onto user-visible variables
  DefineCustomBoolVariable(
    "enable_onto",
    "Enable the Onto optimizer",
    "Enables the Onto optimizer. When enabled, the variables enable_onto_rewards"
    " and enable_onto_selection can be used to control whether or not Onto records"
    " query latency or selects query plans.",
    &enable_onto,
    false,
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomBoolVariable(
    "pg_selection",
    "Explictly enforce the pg selection",
    "If it is set to true, then the running will only use pg to select plans",
    &pg_selection,
    false,
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomBoolVariable(
    "enable_onto_rewards",
    "Send reward info to Onto",
    "Enables reward collection. When enabled, and when enable_onto is true, query latencies"
    " are sent to the Onto server after execution.",
    &enable_onto_rewards,
    true,
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomBoolVariable(
    "enable_onto_selection",
    "Use Onto to select query plans",
    "Enables Onto query plan selection. When enabled, and when enable_onto is true, Onto"
    " will choose a query plan according to its learned model.",
    &enable_onto_selection,
    true,
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomStringVariable(
    "onto_host",
    "Onto server host", NULL,
    &onto_host,
    "localhost",
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomIntVariable(
    "onto_port",
    "Onto server port", NULL,
    &onto_port,
    9381, 1, 65536, 
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomIntVariable(
    "onto_num_arms",
    "Number of arms to consider",
    "The number of arms to consider for each query plan. Each arm represents "
    "a planner configuration. Higher values give better plans, but higher "
    "optimization times. The standard planner is always considered.",
    &onto_num_arms,
    6, 1, ONTO_MAX_ARMS, 
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomIntVariable(
    "onto_num_queries_per_round",
    "Number of queries per round used to schedule arms",
    "The number of arms to consider for each query plan. Each arm represents "
    "a planner configuration. Higher values give better plans, but higher "
    "optimization times. The standard planner is always considered.",
    &onto_num_queries_per_round,
    200, 1, 500, 
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

  DefineCustomBoolVariable(
    "onto_include_json_in_explain",
    "Includes Onto's JSON representation in EXPLAIN output.",
    "Includes Onto's JSON representation of a query plan in the "
    "output of EXPLAIN commands. Used by the Onto server.",
    &onto_include_json_in_explain,
    false,
    PGC_USERSET,
    0,
    NULL, NULL, NULL);

    DefineCustomStringVariable(
      "onto_sequence_id",
      "Sequence ID passed from client to extension for metadata packaging.",
      NULL,
      &onto_sequence_id,
      "",
      PGC_USERSET,
      0,
      NULL, NULL, NULL);


    RequestAddinShmemSpace(sizeof(SharedArmSchedule));
    if (!shmem_startup_hook)
        shmem_startup_hook = onto_shmem_startup;
    else {
        prev_shmem_startup_hook = shmem_startup_hook;
        shmem_startup_hook = onto_shmem_startup;
    }
}


void _PG_fini(void) {
  //elog(DEBUG1, "finished extension");
}

static PlannedStmt* onto_planner(Query *parse,
                                int cursorOptions,
                                ParamListInfo boundParams) {
  // The plan returned by the Onto planner, containing the PG plan,
  // the JSON query plan (for reward tracking), and the arm selected.
  OntoPlan* plan;

  // For timing Onto's overhead.
  clock_t t_start, t_final;
  double plan_time_ms;

  // Final PG plan to execute.
  PlannedStmt* to_return;

  if (prev_planner_hook) {
    elog(WARNING, "Skipping Onto hook, another planner hook is installed.");
    return prev_planner_hook(parse, cursorOptions,
                             boundParams);
  }


  // Skip optimizing this query if it is not a SELECT statement (checked by
  // `should_onto_optimize`), or if Onto is not enabled. We do not check
  // enable_onto_selection here, because if enable_onto is on, we still need
  // to attach a query plan to the query to record the reward later.
  if (!should_onto_optimize(parse) || !enable_onto) {
    return standard_planner(parse, cursorOptions,
                            boundParams);
  }


  t_start = clock();

  // Call Onto query planning routine (in `onto_planner.h`).
  plan = plan_query(parse, cursorOptions, boundParams);

  if (plan == NULL) {
    // something went wrong, default to the PG plan.
    return standard_planner(parse, cursorOptions, boundParams);
  }

  // We need some way to associate this query with the OntoQueryInfo data.
  // Hack: connect the Onto plan info to this plan via the queryId field.
  to_return = plan->plan;
  to_return->queryId = (uint64_t)(void*) plan->query_info;
  plan->query_info = NULL;
  
  t_final = clock();
  plan_time_ms = ((double)(t_final - t_start)
                  / (double)CLOCKS_PER_SEC) * (double)1000.0;

  // Free the OntoPlan* object now that we have gotten the OntoQueryInfo
  // and after we have gotten the PG plan out of it.
  free_onto_plan(plan);
  
  return to_return;
}


static void onto_ExecutorStart(QueryDesc *queryDesc, int eflags) {
  // Code from pg_stat_statements. If needed, setup query timing
  // to use as Onto's reward signal.

  if (prev_ExecutorStart)
    prev_ExecutorStart(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);

  if (enable_onto_rewards
      && queryDesc->plannedstmt->queryId != 0) {
    if (queryDesc->totaltime == NULL) {
      MemoryContext oldcxt;
      
      oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
      queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER);
      MemoryContextSwitchTo(oldcxt);
    }
  }

}

static void onto_ExecutorEnd(QueryDesc *queryDesc) {
  // A query has finished. We need to check if it was a query Onto could optimize,
  // and if so, report the reward to the Onto server.
  OntoQueryInfo* onto_query_info;
  char* r_json;
  int conn_fd;

  if (enable_onto_rewards && should_report_reward(queryDesc)) {
    // We are tracking rewards for queries, and this query was
    // eligible for optimization by Onto.
    conn_fd = connect_to_onto(onto_host, onto_port);
    if (conn_fd < 0) {
      elog(WARNING, "Unable to connect to Onto server, reward for query will be dropped.");
      return;
    }
    if (!queryDesc->totaltime) {
      elog(WARNING, "Onto could not read instrumentation result, reward for query will be dropped.");
      return;
    }
    // Finalize the instrumentation so we can read the final time.
    InstrEndLoop(queryDesc->totaltime);

    // Generate a JSON blob with our reward.
    r_json = reward_json(queryDesc->totaltime->total * 1000.0);
    // Extract the OntoQueryInfo, which we hid inside the queryId of the
    // PlannedStmt. `should_report_reward` ensures it is set.
    onto_query_info = (OntoQueryInfo*)(void*)queryDesc->plannedstmt->queryId;
    queryDesc->plannedstmt->queryId = 0;

    // Write out the query plan, buffer information, and reward to the Onto
    // server.
    send_json_with_length(conn_fd, START_FEEDBACK_MESSAGE);
    send_json_with_length(conn_fd, onto_query_info->plan_json);
    send_json_with_length(conn_fd, onto_query_info->buffer_json);
    send_json_with_length(conn_fd, onto_query_info->metadata_json); 
    // send_json_with_length(conn_fd, onto_query_info->arm_config_json); the first round will not generate arm info
    if (onto_query_info->arm_config_json != NULL) {
        send_json_with_length(conn_fd, onto_query_info->arm_config_json);
    } else {
        const char* default_config = "{ \
            \"enable_hashjoin\": true, \
            \"enable_mergejoin\": true, \
            \"enable_nestloop\": true, \
            \"enable_seqscan\": true, \
            \"enable_indexscan\": true, \
            \"enable_indexonlyscan\": true, \
            \"index\": 0 \
        }";
        send_json_with_length(conn_fd, default_config);
    }

    send_json_with_length(conn_fd, r_json);
    send_json_with_length(conn_fd, TERMINAL_MESSAGE);
    shutdown(conn_fd, SHUT_RDWR);

    free_onto_query_info(onto_query_info);
  }
  
  if (prev_ExecutorEnd) {
    prev_ExecutorEnd(queryDesc);
  } else {
    standard_ExecutorEnd(queryDesc);
  }
}

static void onto_ExplainOneQuery(Query* query, int cursorOptions, IntoClause* into,
                                ExplainState* es,  const char* queryString,
                                ParamListInfo params, QueryEnvironment* queryEnv) {
  PlannedStmt* plan;
  OntoPlan* onto_plan;
  instr_time plan_start, plan_duration;
  int conn_fd;
  char* buffer_json;
  char* plan_json;
  double prediction;
  char* hint_text;
  bool old_selection_val;
  bool connected = false;
  char* metadata_json;
  char* arm_config_json;

  // If there are no other EXPLAIN hooks, add to the EXPLAIN output Onto's estimate
  // of this query plan's execution time, as well as what hints would be used
  // by Onto.
  
  if (prev_ExplainOneQuery) {
    prev_ExplainOneQuery(query, cursorOptions, into, es,
                        queryString, params, queryEnv);
  }

  // There should really be a standard_ExplainOneQuery, but there
  // isn't, so we will do our best. We will replicate some PG code
  // here as a consequence.
  

  INSTR_TIME_SET_CURRENT(plan_start);
  plan = (planner_hook ? planner_hook(query, cursorOptions, params)
          : standard_planner(query, cursorOptions, params));
  INSTR_TIME_SET_CURRENT(plan_duration);
  INSTR_TIME_SUBTRACT(plan_duration, plan_start);
    
  if (!enable_onto) {
    // Onto is disabled, do the deault explain thing.
    ExplainOnePlan(plan, into, es, queryString,
                   params, queryEnv, &plan_duration);
    return;
  }

  buffer_json = buffer_state();
  plan_json = plan_to_json(plan);
  onto_plan = plan_query(query, cursorOptions, params);
  if (!onto_plan || !onto_plan->query_info) {
    elog(WARNING, "Onto plan or query_info is NULL during explain.");
    ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &plan_duration);
    return;
  }

  metadata_json = onto_plan->query_info->metadata_json;
  arm_config_json = onto_plan->query_info->arm_config_json;

  // Ask the Onto server for an estimate for this plan.
  conn_fd = connect_to_onto(onto_host, onto_port);
  if (conn_fd < 0) {
    elog(WARNING, "Unable to connect to Onto server, no prediction provided.");
    prediction = NAN;
  } else {
    send_json_with_length(conn_fd, START_PREDICTION_MESSAGE);
    send_json_with_length(conn_fd, plan_json);
    send_json_with_length(conn_fd, buffer_json);
    send_json_with_length(conn_fd, metadata_json);
    send_json_with_length(conn_fd, arm_config_json);
    send_json_with_length(conn_fd, TERMINAL_MESSAGE);
    shutdown(conn_fd, SHUT_RDWR);

    // send_json_array(conn_fd,
    //         START_PREDICTION_MESSAGE,
    //         plan_json, buffer_json, metadata_json, arm_config_json);
    // shutdown(conn_fd, SHUT_WR);

    // Read the response from the Onto server.
    if (read(conn_fd, &prediction, sizeof(double)) != sizeof(double)) {
      elog(WARNING, "Onto could not read the response from the server during EXPLAIN.");
      prediction = NAN;
    }

    connected = true;
    shutdown(conn_fd, SHUT_RDWR);
  }

  // Open a new explain group called "Onto" and add our prediction into it.
  ExplainOpenGroup("OntoProps", NULL, true, es);
  ExplainOpenGroup("Onto", "Onto", true, es);

  if (connected) {
    // The Onto server will (correctly) give a NaN if no model is available,
    // but PostgreSQL will dump that NaN into the raw JSON, causing parse bugs.
    if (isnan(prediction))
      ExplainPropertyText("Onto prediction", "NaN", es);
    else
      ExplainPropertyFloat("Onto prediction", "ms", prediction, 3, es);
  }
  
  if (onto_include_json_in_explain) {
    ExplainPropertyText("Onto plan JSON", plan_json, es);
    ExplainPropertyText("Onto buffer JSON", buffer_json, es);
  }

  free(plan_json);
  free(buffer_json);

  // Next, plan the query so that we can suggest a hint. If enable_onto_selection
  // was on, this repeats some work, as the query will be planned twice. That's OK
  // since EXPLAIN should still be fast.
  old_selection_val = enable_onto_selection;
  enable_onto_selection = true;
  // onto_plan = plan_query(query, cursorOptions, params);
  enable_onto_selection = old_selection_val;
  
  if (!onto_plan) {
    elog(WARNING, "Could not plan query with Onto during explain, omitting hint.");
  } else {
    hint_text = arm_to_hint(onto_plan->selection);
    ExplainPropertyText("Onto recommended hint",
                        (hint_text ? hint_text : "(no hint)"),
                        es);
    free(hint_text);
    free_onto_plan(onto_plan);
  }
    
  ExplainCloseGroup("Onto", "Onto", true, es);
  ExplainCloseGroup("OntoProps", NULL, true, es);
  
  // Do the deault explain thing.
  ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &plan_duration);
}
