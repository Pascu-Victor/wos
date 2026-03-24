#pragma once

#include <extern/limine.h>

#include <cstddef>
#include <mod/io/port/port.hpp>

extern "C" __attribute__((noreturn)) void hcf() noexcept;

namespace ker::mod::io::serial {
void init();
void write(const char* str);
void write(const char* str, uint64_t len);
void write(char c);
void write(uint64_t num);
void writeHex(uint64_t num);
void writeBin(uint64_t num);

// Lock management for atomic multi-write operations
void acquireLock();
void releaseLock();

// Call this after per-CPU data is initialized to enable proper CPU ID tracking
void markCpuIdAvailable();

// Enter panic mode - switches to a simple non-reentrant raw spinlock that
// cannot deadlock on CPU-ID logic. Must be called before panic log output.
// Safe to call from multiple CPUs; only the first caller wins the panic lock.
// Enter panic mode — makes acquireLock()/releaseLock() no-ops so that the panic
// owner CPU (which already holds acquireLock()) is never deadlocked by other CPUs.
// Returns true if this CPU is the first caller (the panic owner).
bool enterPanicMode();

// Returns true if panic mode is active.
bool isPanicMode();

// Unlocked write variants - caller must hold lock via acquireLock()/releaseLock()
void writeUnlocked(const char* str);
void writeUnlocked(const char* str, uint64_t len);
void writeUnlocked(char c);
void writeUnlocked(uint64_t num);
void writeHexUnlocked(uint64_t num);
void writeBinUnlocked(uint64_t num);

// RAII-style scoped lock for multiple writes
class ScopedLock {
   public:
    ScopedLock() { acquireLock(); }
    ~ScopedLock() { releaseLock(); }
    ScopedLock(const ScopedLock&) = delete;
    ScopedLock& operator=(const ScopedLock&) = delete;
};
}  // namespace ker::mod::io::serial
