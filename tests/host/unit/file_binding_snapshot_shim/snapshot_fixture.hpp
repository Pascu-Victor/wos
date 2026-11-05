#pragma once

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>

// Finite Host endpoints only. The complete production family/binding/index TU
// supplies every ownership and translation-retirement operation under test.
namespace snapshot_fixture {
inline thread_local unsigned lock_depth{}, index_depth{};
inline unsigned puts{}, closes{}, callbacks{};
inline int fail_reserve_after{-1};
inline int last_callback_result{};
auto run_one() -> bool;
void drain();
auto queue_empty() -> bool;
inline void check(bool condition) {
    if (!condition) std::abort();
}
struct Spinlock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
    constexpr Spinlock() noexcept = default;
    auto lock_irqsave() noexcept -> uint64_t {
        while (flag.test_and_set(std::memory_order_acquire)) {
        }
        ++lock_depth;
        return 0;
    }
    void unlock_irqrestore(uint64_t) noexcept {
        check(lock_depth != 0);
        --lock_depth;
        flag.clear(std::memory_order_release);
    }
};
inline Spinlock index_lock{};
struct IndexGuard {
    uint64_t flags;
    IndexGuard() : flags(index_lock.lock_irqsave()) { ++index_depth; }
    ~IndexGuard() {
        --index_depth;
        index_lock.unlock_irqrestore(flags);
    }
    IndexGuard(const IndexGuard&) = delete;
    auto operator=(const IndexGuard&) -> IndexGuard& = delete;
};
}  // namespace snapshot_fixture

namespace ker::mod::sys {
using Spinlock = snapshot_fixture::Spinlock;
}
namespace ker::vfs {
struct File {
    std::atomic<int> refcount{1};
    int close_result{};
};
inline auto vfs_put_file_result(File* file) -> int {
    snapshot_fixture::check(file != nullptr && snapshot_fixture::lock_depth == 0 && snapshot_fixture::index_depth == 0);
    int const BEFORE = file->refcount.fetch_sub(1);
    snapshot_fixture::check(BEFORE > 0);
    ++snapshot_fixture::puts;
    if (BEFORE != 1) return 0;
    ++snapshot_fixture::closes;
    return file->close_result;
}
inline void vfs_put_file(File* file) { static_cast<void>(vfs_put_file_result(file)); }
}  // namespace ker::vfs
