#ifndef ONTO_COMPAT_H
#define ONTO_COMPAT_H

#include "postgres.h"

#if PG_VERSION_NUM < 130000

// --- pull_var_clause flags ---
#define PVC_INCLUDE_AGGREGATES      0x0001
#define PVC_RECURSE_AGGREGATES      0x0002
#define PVC_INCLUDE_WINDOWFUNCS     0x0004
#define PVC_RECURSE_WINDOWFUNCS     0x0008
#define PVC_INCLUDE_PLACEHOLDERS    0x0010
#define PVC_RECURSE_PLACEHOLDERS    0x0020

// --- declare pull_var_clause ---
static inline List* local_pull_var_clause(Node* node, int flags) {
    extern List* pull_var_clause(Node* node, int flags);  // real function in var.c
    return pull_var_clause(node, flags);
}

#else

// #include "optimizer/var.h"
#define local_pull_var_clause pull_var_clause

#endif

#endif  // ONTO_COMPAT_H
