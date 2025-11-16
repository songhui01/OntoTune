#ifndef ONTO_CONFIGS_H
#define ONTO_CONFIGS_H

#include "c.h"

#define ONTO_MAX_ARMS 26

// Each onto config variable is linked to a PostgreSQL session variable.
// See the string docs provided to the PG functions in main.c.
static bool enable_onto = false;
static bool enable_onto_rewards = false;
static bool enable_onto_selection = false;
static bool pg_selection = false;
static char* onto_host = NULL;
static int onto_port = 9381;
static int onto_num_arms = 6;
static int onto_num_queries_per_round = 300;
static bool onto_include_json_in_explain = false;
extern char *onto_sequence_id;
#endif
