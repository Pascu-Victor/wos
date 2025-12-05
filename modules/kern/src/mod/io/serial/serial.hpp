#pragma once

#include <limine.h>

#include <cstddef>
#include <mod/io/port/port.hpp>

extern "C" __attribute__((noreturn)) void hcf() noexcept;

namespace ker::mod::io {
namespace serial {
void init(void);
void write(const char* str);
void write(const char* str, uint64_t len);
void write(const char c);
void write(uint64_t num);
void writeHex(uint64_t num);
void writeBin(uint64_t num);

// Lock management for atomic multi-write operations
void acquireLock();
void releaseLock();

// Call this after per-CPU data is initialized to enable proper CPU ID tracking
void markCpuIdAvailable();

// Unlocked write variants - caller must hold lock via acquireLock()/releaseLock()
void writeUnlocked(const char* str);
void writeUnlocked(const char* str, uint64_t len);
void writeUnlocked(const char c);
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
}  // namespace serial
}  // namespace ker::mod::io
