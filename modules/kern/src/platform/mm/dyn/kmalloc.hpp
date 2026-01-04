#pragma once
#include <cstddef>
#include <cstdint>
#include <minimalist_malloc/mini_malloc.hpp>

namespace ker::mod::mm::dyn::kmalloc {
void init();

// Dump tracked large allocations for kmalloc (safe to call from OOM)
void dumpTrackedAllocations();

// Fill outCount and outBytes with totals for tracked large allocations
void getTrackedAllocTotals(uint64_t& outCount, uint64_t& outBytes);
auto malloc(uint64_t size) -> void*;
void free(void* ptr);
auto realloc(void* ptr, size_t size) -> void*;
auto calloc(size_t nmemb, size_t size) -> void*;

template <typename T>
inline static auto malloc() -> T* {
    return (T*)malloc(sizeof(T));
}

template <typename T, int = sizeof(T)>
inline static void free(T* ptr) {
    ::ker::mod::mm::dyn::kmalloc::free(static_cast<void*>(ptr));
}
}  // namespace ker::mod::mm::dyn::kmalloc

auto operator new(size_t sz) -> void*;
auto operator new[](size_t sz) -> void*;
// void operator delete(void* ptr) noexcept;
void operator delete(void* ptr, size_t size) noexcept;
// void operator delete[](void* ptr) noexcept;
void operator delete[](void* ptr, size_t size) noexcept;

void operator delete(void* ptr) noexcept;
void operator delete[](void* ptr) noexcept;
