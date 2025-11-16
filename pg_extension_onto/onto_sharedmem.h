#pragma once

#include "postgres.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "lib/stringinfo.h"
#include "onto_atomic_compat.h"

#define ONTO_MAX_QUERIES 500

typedef struct {
    onto_atomic_uint32 current_query_index;
    int initialized;
    int schedule[ONTO_MAX_QUERIES];
    int arm_usage_count[ONTO_MAX_ARMS];
} SharedArmSchedule;

static SharedArmSchedule *shared_arm_schedule = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void log_arm_schedule(int num_queries) {
    StringInfoData buf;
    initStringInfo(&buf);

    appendStringInfoString(&buf, "[OntoSharedMem] arm_schedule = [");
    for (int i = 0; i < num_queries; i++) {
        appendStringInfo(&buf, "%s%d", (i > 0 ? ", " : ""), shared_arm_schedule->schedule[i]);
    }
    appendStringInfoString(&buf, "]");

    elog(WARNING, "%s", buf.data);
}


static void initialize_shared_schedule(int num_arms, int num_queries) {
    elog(WARNING, "[OntoSharedMem] Initializing arm_schedule with num_arms=%d, num_queries=%d", num_arms, num_queries);

    srand(42);
    int i = 0;

    for (int j = 0; j < num_arms; j++) {
        int repeat = num_queries / num_arms;
        for (int k = 0; k < repeat; k++) {
            shared_arm_schedule->schedule[i++] = j;
        }
    }

    while (i < num_queries) {
        shared_arm_schedule->schedule[i++] = rand() % num_arms;
    }

    for (int k = num_queries - 1; k > 0; k--) {
        int j = rand() % (k + 1);
        int tmp = shared_arm_schedule->schedule[k];
        shared_arm_schedule->schedule[k] = shared_arm_schedule->schedule[j];
        shared_arm_schedule->schedule[j] = tmp;
    }

    onto_atomic_init_u32(&shared_arm_schedule->current_query_index, 0);
    shared_arm_schedule->initialized = 1;
}

static void onto_shmem_startup(void) {
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    shared_arm_schedule = ShmemInitStruct("OntoSharedArmSchedule",
                                          sizeof(SharedArmSchedule),
                                          &found);
    if (!found) {
        memset(shared_arm_schedule, 0, sizeof(SharedArmSchedule));
        initialize_shared_schedule(onto_num_arms, onto_num_queries_per_round);
        log_arm_schedule(onto_num_queries_per_round);
    } else {
        elog(LOG, "[OntoSharedMem] Shared arm_schedule already exists");
    }
}

static int get_next_arm() {
    uint32 index = onto_atomic_fetch_add_u32(&shared_arm_schedule->current_query_index, 1);
    if (index >= ONTO_MAX_QUERIES) {
        elog(WARNING, "[OntoSharedMem] Index %u out of bounds (max %d), fallback to 0", index, ONTO_MAX_QUERIES);
        return 0;
    }
    return shared_arm_schedule->schedule[index];
}

static int get_next_query_index(void) {
    return onto_atomic_fetch_add_u32(&shared_arm_schedule->current_query_index, 1);
}

