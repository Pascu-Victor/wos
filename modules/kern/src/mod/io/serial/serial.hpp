#pragma once

#include <extern/limine.h>

#include <cstddef>
#include <cstdint>
#include <mod/io/port/port.hpp>

namespace ker::mod::io::serial {
void init();
void write(const char* str);
void write(const char* str, uint64_t len);
void write(char c);
void write(uint64_t num);
void write_hex(uint64_t num);
void write_bin(uint64_t num);

// Lock management for atomic multi-write operations
void acquire_lock();
void release_lock();

// Call this after per-CPU data is initialized to enable proper CPU ID tracking
// for the early-boot owner fallback before current-task tracking is available.
void mark_cpu_id_available();

// Enter panic mode - switches to a simple non-reentrant raw spinlock that
// cannot deadlock on CPU-ID logic. Must be called before panic log output.
// Safe to call from multiple CPUs; only the first caller wins the panic lock.
// Enter panic mode - makes acquireLock()/releaseLock() no-ops so that the panic
// owner CPU (which already holds acquireLock()) is never deadlocked by other CPUs.
// Returns true if this CPU is the first caller (the panic owner).
auto enter_panic_mode() -> bool;

// Returns true if panic mode is active.
auto is_panic_mode() -> bool;

// Returns true if this CPU is the one that won the panic lock (first caller of
// enterPanicMode()). Use this to distinguish re-entrant calls on the owner CPU
// (e.g. ubsan → panic_handler) from other CPUs entering panic paths.
auto is_panic_owner() -> bool;

// Unlocked write variants - caller must hold lock via acquireLock()/releaseLock()
void write_unlocked(const char* str);
void write_unlocked(const char* str, uint64_t len);
void write_unlocked(char c);
void write_unlocked(uint64_t num);
void write_hex_unlocked(uint64_t num);
void write_bin_unlocked(uint64_t num);

// RAII-style scoped lock for multiple writes
class ScopedLock {
   public:
    ScopedLock() { acquire_lock(); }
    ~ScopedLock() { release_lock(); }
    ScopedLock(const ScopedLock&) = delete;
    ScopedLock& operator=(const ScopedLock&) = delete;
};
}  // namespace ker::mod::io::serial
