
#ifndef ONTO_COST_H
#define ONTO_COST_H

#include "postgres.h"
#include "nodes/plannodes.h"
#include "lib/stringinfo.h"


extern void onto_serialize_plan_json(StringInfo dst, Plan *plan);

#endif /* ONTO_COST_H */
