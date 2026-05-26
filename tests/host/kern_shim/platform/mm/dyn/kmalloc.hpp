#pragma once

// Host shim: replaces kernel slab allocator with libc malloc/free.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace ker::mod::mm::dyn::kmalloc {

inline void init() {}
inline void enablePerCpuAllocations() {}
inline void dumpTrackedAllocations() {}
inline void getTrackedAllocTotals(uint64_t& outCount, uint64_t& outBytes) {
    outCount = 0;
    outBytes = 0;
}

inline auto malloc(uint64_t size) -> void* { return ::malloc(static_cast<size_t>(size)); }
inline void free(void* ptr) { ::free(ptr); }
inline auto realloc(void* ptr, size_t size) -> void* { return ::realloc(ptr, size); }
inline auto calloc(size_t nmemb, size_t size) -> void* { return ::calloc(nmemb, size); }

template <typename T>
inline static auto malloc() -> T* {
    return static_cast<T*>(::malloc(sizeof(T)));
}

template <typename T, int = sizeof(T)>
inline static void free(T* ptr) {
    ::free(static_cast<void*>(ptr));
}

}  // namespace ker::mod::mm::dyn::kmalloc

// Global operator new/delete — forward to libc (these are normally provided
// by the host C++ runtime, so we only define them if they're missing).
// On the host the default new/delete already use malloc/free, so nothing
// special is needed.
