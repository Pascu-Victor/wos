#pragma once

// #include "slab.hpp"
#include <minimalist_malloc/mini_malloc.hpp>

#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"

namespace ker::mod::mm::dyn::kmalloc {
static const uint64_t KMALLOC_NOGROW = 1;

void init();
void *malloc(uint64_t size);
void free(void *ptr);
void *realloc(void *ptr, size_t size);
void *calloc(size_t nmemb, size_t size);

// slab::SlabCache* createCache(uint64_t objectSize, const char* name, uint64_t align);
// void* cacheAlloc(slab::SlabCache* cache, uint64_t flags);
// void cacheFree(slab::SlabCache* cache, void* ptr);
// void cacheDestroy(slab::SlabCache* cache);

template <typename T>
inline static T *malloc(void) {
    return (T *)malloc(sizeof(T));
}

template <typename T>
inline static void free(T *ptr) {
    free(ptr, sizeof(T));
}
}  // namespace ker::mod::mm::dyn::kmalloc

void *operator new(size_t sz);
void *operator new[](size_t sz);
// void operator delete(void* ptr) noexcept;
void operator delete(void *ptr, size_t size) noexcept;
// void operator delete[](void* ptr) noexcept;
void operator delete[](void *ptr, size_t size) noexcept;

void operator delete(void *ptr) noexcept;
void operator delete[](void *ptr) noexcept;
