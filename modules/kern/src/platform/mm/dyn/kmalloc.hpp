#pragma once
#include <cstddef>
#include <cstdint>
#include <minimalist_malloc/mini_malloc.hpp>
#include <new>

namespace ker::mod::mm::dyn::kmalloc {
void init();
void enablePerCpuAllocations();  // Call after cpuParamInit to enable per-CPU allocators

// Dump tracked large allocations for kmalloc (safe to call from OOM)
void dumpTrackedAllocations();

// Fill outCount and outBytes with totals for tracked large allocations
void getTrackedAllocTotals(uint64_t& outCount, uint64_t& outBytes);
auto malloc(uint64_t size) -> void*;
void free(void* ptr);
auto realloc(void* ptr, size_t size) -> void*;
auto calloc(size_t nmemb, size_t size) -> void*;

#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
// Typed allocation: captures call-site address and a compile-time type string for OOM dumps.
// Called by the malloc<T>() template; may also be called directly with a custom tag.
auto malloc_tagged(uint64_t size, uintptr_t caller, const char* tag) -> void*;
#endif

template <typename T>
inline static auto malloc() -> T* {
#if defined(WOS_KASAN) || defined(WOS_KUBSAN)
    return static_cast<T*>(malloc_tagged(sizeof(T), reinterpret_cast<uintptr_t>(__builtin_return_address(0)), __PRETTY_FUNCTION__));
#else
    return static_cast<T*>(malloc(sizeof(T)));
#endif
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

auto operator new(unsigned long size, std::nothrow_t const&) noexcept -> void*;
auto operator new[](unsigned long size, std::nothrow_t const&) noexcept -> void*;
