/*
 * Copyright 2021 Stanislav Paskalev <spaskalev@protonmail.com>
 *
 * A binary buddy memory allocator
 *
 * To include and use it in your project do the following
 * 1. Add buddy_alloc.h (this file) to your include directory
 * 2. Include the header in places where you need to use the allocator
 * 3. In one of your source files #define BUDDY_ALLOC_IMPLEMENTATION
 *    and then import the header. This will insert the implementation.
 *
 * Latest version is available at https://github.com/spaskalev/buddy_alloc
 */

#pragma once

#ifndef BUDDY_HEADER
// #include <limits.h>
// #include <stdbool.h>
// #include <stddef.h>
// #include <stdint.h>
// #include <string.h>
// #include <sys/types.h>
#include <defines/defines.hpp>
#include <std/mem.hpp>

#ifndef BUDDY_PRINTF
// #include <stdio.h>
#endif
#endif

#ifdef __cplusplus
#ifndef BUDDY_CPP_MANGLED
extern "C" {
#endif
#endif

struct buddy;

/* Returns the size of a buddy required to manage a block of the specified size */
size_t buddy_sizeof(size_t memory_size);

/*
 * Returns the size of a buddy required to manage a block of the specified size
 * using a non-default alignment.
 */
size_t buddy_sizeof_alignment(size_t memory_size, size_t alignment);

/* Initializes a binary buddy memory allocator at the specified location */
struct buddy *buddy_init(unsigned char *at, unsigned char *main, size_t memory_size);

/* Initializes a binary buddy memory allocator at the specified location using a non-default alignment */
struct buddy *buddy_init_alignment(unsigned char *at, unsigned char *main, size_t memory_size, size_t alignment);

/*
 * Initializes a binary buddy memory allocator embedded in the specified arena.
 * The arena's capacity is reduced to account for the allocator metadata.
 */
struct buddy *buddy_embed(unsigned char *main, size_t memory_size);

/*
 * Returns the address of a previously-created buddy allocator at the arena.
 * Use to get a new handle to the allocator when the arena is moved or copied.
 */
struct buddy *buddy_get_embed_at(unsigned char *main, size_t memory_size);

/*
 * Initializes a binary buddy memory allocator embedded in the specified arena
 * using a non-default alignment.
 * The arena's capacity is reduced to account for the allocator metadata.
 */
struct buddy *buddy_embed_alignment(unsigned char *main, size_t memory_size, size_t alignment);

/*
 * Returns the address of a previously-created buddy allocator at the arena.
 * Use to get a new handle to the allocator when the arena is moved or copied.
 */
struct buddy *buddy_get_embed_at_alignment(unsigned char *main, size_t memory_size, size_t alignment);

/* Resizes the arena and metadata to a new size. */
struct buddy *buddy_resize(struct buddy *buddy, size_t new_memory_size);

/* Tests if the allocator can be shrunk in half */
bool buddy_can_shrink(struct buddy *buddy);

/* Tests if the allocator is completely empty */
bool buddy_is_empty(struct buddy *buddy);

/* Tests if the allocator is completely full */
bool buddy_is_full(struct buddy *buddy);

/* Reports the arena size */
size_t buddy_arena_size(struct buddy *buddy);

/* Reports the arena's free size. Note that this is (often) not a continuous size
   but the sum of all free slots in the buddy. */
size_t buddy_arena_free_size(struct buddy *buddy);

/*
 * Allocation functions
 */

/* Use the specified buddy to allocate memory. See malloc. */
void *buddy_malloc(struct buddy *buddy, size_t requested_size);

/* Use the specified buddy to allocate zeroed memory. See calloc. */
void *buddy_calloc(struct buddy *buddy, size_t members_count, size_t member_size);

/* Realloc semantics are a joke. See realloc. */
void *buddy_realloc(struct buddy *buddy, void *ptr, size_t requested_size, bool ignore_data);

/* Realloc-like behavior that checks for overflow. See reallocarray*/
void *buddy_reallocarray(struct buddy *buddy, void *ptr,
    size_t members_count, size_t member_size, bool ignore_data);

/* Use the specified buddy to free memory. See free. */
void buddy_free(struct buddy *buddy, void *ptr);

/* A (safer) free with a size. Will not free unless the size fits the target span. */
void buddy_safe_free(struct buddy *buddy, void *ptr, size_t requested_size);

/*
 * Reservation functions
 */

/* Reserve a range by marking it as allocated. Useful for dealing with physical memory. */
void buddy_reserve_range(struct buddy *buddy, void *ptr, size_t requested_size);

/* Release a reserved memory range. Unsafe, this can mess up other allocations if called with wrong parameters! */
void buddy_unsafe_release_range(struct buddy *buddy, void *ptr, size_t requested_size);

/*
 * Iteration functions
 */

/*
 * Iterate through the free and allocated slots and call the provided function for each of them.
 *
 * If the provided function returns a non-NULL result the iteration stops and the result
 * is returned to called. NULL is returned upon completing iteration without stopping.
 *
 * The iteration order is implementation-defined and may change between versions.
 */
void *buddy_walk(struct buddy *buddy, void *(fp)(void *ctx, void *addr, size_t slot_size, size_t allocated), void *ctx);

/*
 * Miscellaneous functions
 */

/*
 * Calculates the fragmentation in the allocator in a 0 - 255 range.
 * NOTE: if you are using a non-power-of-two sized arena the maximum upper bound can be lower.
 */
unsigned char buddy_fragmentation(struct buddy *buddy);

#ifdef __cplusplus
#ifndef BUDDY_CPP_MANGLED
}
#endif
#endif