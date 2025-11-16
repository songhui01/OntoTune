#ifndef ONTO_UTIL_H
#define ONTO_UTIL_H

#include <arpa/inet.h>
#include <unistd.h>

#include "postgres.h"
#include "optimizer/planner.h"
#include "optimizer/cost.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "executor/execdesc.h"

#include <stdio.h>
#include <string.h>

// Utility functions and common structs used throughout Onto.

// JSON tags for sending to the Onto server.
static const char* START_QUERY_MESSAGE = "{\"type\": \"query\"}\n";
static const char *START_FEEDBACK_MESSAGE = "{\"type\": \"reward\"}\n";
static const char* START_PREDICTION_MESSAGE = "{\"type\": \"predict\"}\n";
static const char* TERMINAL_MESSAGE = "{\"final\": true}\n";


// Onto-specific information associated with a query plan.
typedef struct OntoQueryInfo {
  // A JSON representation of the query plan we can send to the Onto server.
  char* plan_json;

  // A JSON representation of the buffer state when the query was planned.
  char* buffer_json;

  // A JSON representation of the ontology for query, db, statistics
  char* metadata_json;

  // Arm information
  int selected_arm;
  char* arm_config_json;  

} OntoQueryInfo;


// A struct containing a PG query plan and the related Onto-specific information.
typedef struct OntoPlan {
  OntoQueryInfo* query_info;

  // The PostgreSQL plan.
  PlannedStmt* plan;

  // The arm index we used to generate this plan.
  unsigned int selection;
} OntoPlan;

// Free a OntoQueryInfo struct.
static void free_onto_query_info(OntoQueryInfo* info) {
  if (!info) return;
  if (info->plan_json) free(info->plan_json);
  if (info->buffer_json) free(info->buffer_json);
  if (info->metadata_json) pfree(info->metadata_json);
  if (info->arm_config_json) free(info->arm_config_json);

  free(info);
}

// Free a OntoPlan (including the contained OntoQueryInfo).
static void free_onto_plan(OntoPlan* plan) {
  if (!plan) return;
  if (plan->query_info) free_onto_query_info(plan->query_info);
  free(plan);
}

// Determine if we should report the reward of this query or not.
static bool should_report_reward(QueryDesc* queryDesc) {
  // before reporting a reward, check that:
  // (1) that the query ID is not zero (query ID is left as 0 for INSERT, UPDATE, etc.)
  // (2) that the query actually executed (e.g., was not an EXPLAIN).
  // (3) the the instrument_options is zero (e.g., was not an EXPLAIN ANALYZE)
  return (queryDesc->plannedstmt->queryId != 0
          && queryDesc->already_executed
          && queryDesc->instrument_options == 0);
}

// Determine if we should optimize this query or not.
static bool should_onto_optimize(Query* parse) {
  Oid relid;
  char* namespace;

  // Don't try and optimize anything that isn't a SELECT query.
  if (parse->commandType != CMD_SELECT) return false; 

  // Iterate over all the relations in this query.
  for (int i = 0; i < list_length(parse->rtable); i++) {
    relid = rt_fetch(i, parse->rtable)->relid;
    // A relid of zero seems to have a special meaning, and it causes
    // get_rel_namespace or get_namespace_name to crash. Relid of zero
    // doesn't seem to appear in "normal" queries though.
    if (!relid) return false;

    // Ignore queries that involve the pg_catalog (internal data used by PostgreSQL).
    namespace = get_namespace_name(get_rel_namespace(relid));
    if (strcmp(namespace, "pg_catalog") == 0) return false;
  }

  return true;

}


// https://stackoverflow.com/a/4770992/1464282
static bool starts_with(const char *str, const char *pre) {
  return strncmp(pre, str, strlen(pre)) == 0;
}

// Create a JSON object containing the reward, suitable to send to the Onto
// server.
static char* reward_json(double reward) {
  char* buf;
  size_t json_size;
  FILE* stream;
  pid_t pid = getpid();
  
  stream = open_memstream(&buf, &json_size);

  fprintf(stream, "{\"reward\": %f , \"pid\": %d }\n", reward, pid);
  fclose(stream);

  return buf;

}

// Write the entire string to the given socket.
static void write_all_to_socket(int conn_fd, const char* json) {
  size_t json_length;
  ssize_t written, written_total;
  json_length = strlen(json);
  written_total = 0;
  
  while (written_total != json_length) {
    written = write(conn_fd,
                    json + written_total,
                    json_length - written_total);
    written_total += written;
  }
}

// Connect to the Onto server.
static int connect_to_onto(const char* host, int port) {
  int ret, conn_fd;
  struct sockaddr_in server_addr = { 0 };

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, host, &server_addr.sin_addr);
  conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (conn_fd < 0) {
    return conn_fd;
  }
  
  ret = connect(conn_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
  if (ret == -1) {
    return ret;
  }

  return conn_fd;

}

// Get the relation name of a particular plan node with a PostgreSQL
// PlannedStmt.
static char* get_relation_name(PlannedStmt* stmt, Plan* node) {
  Index rti;

  switch (node->type) {
  case T_SeqScan:
  case T_SampleScan:
  case T_IndexScan:
  case T_IndexOnlyScan:
  case T_BitmapHeapScan:
  case T_BitmapIndexScan:
  case T_TidScan:
  case T_ForeignScan:
  case T_CustomScan:
  case T_ModifyTable:
    rti = ((Scan*)node)->scanrelid;
    return get_rel_name(rt_fetch(rti, stmt->rtable)->relid);
    break;
  default:
    return NULL;
  }
}

static char* squash_newlines(char *s) {
    for (char *p = s; *p; p++) if (*p == '\n') *p = ' ';
    return s;
}


static void send_json_array(int fd,
                            const char *type_msg,
                            char *plan_json,
                            char *buffer_json,
                            char *metadata_json,
                            char *arm_config_json)
{
    squash_newlines((char*)type_msg);
    squash_newlines(plan_json);
    squash_newlines(buffer_json);
    squash_newlines(metadata_json);
    squash_newlines(arm_config_json);

    char big[1<<20];
    int n = snprintf(big, sizeof(big),
        "[\n"
        "  %s,\n"   // e.g. {"type":"predict"}
        "  %s,\n"   // the plan
        "  %s,\n"   // the buffer state
        "  %s,\n"   // the metadata
        "  %s,\n"   // the arm_config
        "  {\"final\":true}\n"
        "]\n",
        type_msg,
        plan_json,
        buffer_json,
        metadata_json,
        arm_config_json
    );

    if (n < 0 || n >= sizeof(big)) {
        elog(WARNING, "send_json_array: buffer too small");
        return;
    }
    write_all_to_socket(fd, big);
}


static void send_json_with_length(int sockfd, const char* json_str) {
    uint32_t len = strlen(json_str);           // JSON length
    uint32_t net_len = htonl(len);

    // length
    write(sockfd, &net_len, sizeof(net_len));

    // json content
    write(sockfd, json_str, len);
}

#endif
