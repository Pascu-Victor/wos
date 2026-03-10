#pragma once

// Read-Write Lock — multiple concurrent readers, exclusive writer.
//
// XFS uses these for inode i_lock (concurrent reads on the same file)
// and per-allocation-group locks.
//
// Implementation: atomic reader counter + writer flag.  Readers increment
// the counter; a writer sets the flag and waits for readers to drain.
// Contended paths fall back to kern_yield() (same pattern as Mutex).
//
// Reference: Linux kernel/locking/rwsem.c (simplified)

#include <atomic>
#include <cstdint>

namespace ker::mod::sys {

class RwLock {
   public:
    RwLock() = default;

    // Non-copyable, non-movable
    RwLock(const RwLock&) = delete;
    RwLock(RwLock&&) = delete;
    auto operator=(const RwLock&) -> RwLock& = delete;
    auto operator=(RwLock&&) -> RwLock& = delete;

    // --- Reader API ---

    // Acquire a shared (read) lock.  Blocks if a writer is active or waiting.
    void read_lock();

    // Try to acquire a shared lock without blocking.
    auto read_try_lock() -> bool;

    // Release a shared lock.
    void read_unlock();

    // --- Writer API ---

    // Acquire an exclusive (write) lock.  Blocks until all readers and the
    // current writer (if any) have released.
    void write_lock();

    // Try to acquire an exclusive lock without blocking.
    auto write_try_lock() -> bool;

    // Release an exclusive lock.
    void write_unlock();

   private:
    // State encoding (single 32-bit atomic):
    //   bits [30:0]  — active reader count
    //   bit  31      — writer-active flag
    // A separate `write_waiters_` counter prevents reader starvation of writers:
    // when a writer is waiting, new readers block until the writer is served.
    static constexpr uint32_t WRITER_BIT = 1u << 31;
    static constexpr uint32_t READER_MASK = ~WRITER_BIT;

    std::atomic<uint32_t> state_{0};
    std::atomic<uint32_t> write_waiters_{0};
};

// RAII guards

class ReadGuard {
   public:
    explicit ReadGuard(RwLock& lk) : lk_(lk) { lk_.read_lock(); }
    ~ReadGuard() { lk_.read_unlock(); }

    ReadGuard(const ReadGuard&) = delete;
    ReadGuard(ReadGuard&&) = delete;
    auto operator=(const ReadGuard&) -> ReadGuard& = delete;
    auto operator=(ReadGuard&&) -> ReadGuard& = delete;

   private:
    RwLock& lk_;
};

class WriteGuard {
   public:
    explicit WriteGuard(RwLock& lk) : lk_(lk) { lk_.write_lock(); }
    ~WriteGuard() { lk_.write_unlock(); }

    WriteGuard(const WriteGuard&) = delete;
    WriteGuard(WriteGuard&&) = delete;
    auto operator=(const WriteGuard&) -> WriteGuard& = delete;
    auto operator=(WriteGuard&&) -> WriteGuard& = delete;

   private:
    RwLock& lk_;
};

}  // namespace ker::mod::sys
