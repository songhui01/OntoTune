#pragma once

/*
 * onto_atomic_compat.h
 * Portable atomic add implementation compatible with PostgreSQL 12.
 * Does NOT require pg_atomic.h.
 * Uses GCC __sync builtins which are process/thread-safe.
 */

#include <stdint.h>

typedef struct {
    volatile uint32_t value;
} onto_atomic_uint32;

static inline void onto_atomic_init_u32(onto_atomic_uint32 *ptr, uint32_t val) {
    ptr->value = val;
}

static inline uint32_t onto_atomic_fetch_add_u32(onto_atomic_uint32 *ptr, uint32_t add) {
    return __sync_fetch_and_add(&(ptr->value), add);
}

static inline void onto_atomic_write_u32(onto_atomic_uint32 *ptr, uint32_t val) {
    __sync_lock_test_and_set(&(ptr->value), val);
}

static inline uint32_t onto_atomic_read_u32(onto_atomic_uint32 *ptr) {
    return __sync_fetch_and_add(&(ptr->value), 0);
}
