#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <object_pages.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/sched/shutdown_ownership.hpp>

namespace vm_fixture {
inline unsigned cases{}, assertions{}, failures{}, puts{}, closes{}, flushes{}, table_attempts{}, yields{};
inline int fail_table_after{-1};
inline bool access_window{}, worker{}, panic_mode{};
inline void expect(bool value, const char* text) {
    ++assertions;
    if (!value && failures++ < 20) std::fprintf(stderr, "FAIL %s\n", text);
}
inline void require(bool value, const char* text) {
    expect(value, text);
    if (!value) std::abort();
}
auto allocate_table() -> ker::mod::mm::paging::PageTable*;
void release_table(ker::mod::mm::paging::PageTable*);
auto table_pages() -> size_t;
}
namespace ker::mod::sys {
struct Spinlock {
    std::atomic_flag held = ATOMIC_FLAG_INIT;
    constexpr Spinlock() noexcept = default;
    auto lock_irqsave() noexcept -> uint64_t {
        uint64_t const before = object_fixture::irq_depth++;
        while (held.test_and_set(std::memory_order_acquire)) {}
        ++object_fixture::lock_depth;
        return before;
    }
    void unlock_irqrestore(uint64_t before) noexcept {
        object_fixture::check(object_fixture::lock_depth != 0);
        --object_fixture::lock_depth;
        held.clear(std::memory_order_release);
        object_fixture::irq_depth = static_cast<unsigned>(before);
    }
};
struct Mutex {
    std::mutex lock_;
    void lock() {
        object_fixture::check(object_fixture::irq_depth == 0 && object_fixture::pub_depth == 0);
        lock_.lock();
        ++object_fixture::pub_depth;
        ++object_fixture::mutex_depth;
    }
    void unlock() {
        object_fixture::check(object_fixture::irq_depth == 0 && object_fixture::pub_depth == 1);
        --object_fixture::pub_depth;
        --object_fixture::mutex_depth;
        lock_.unlock();
    }
};
}
namespace ker::vfs {
struct File { std::atomic<int> refcount{1}; };
inline auto vfs_put_file_result(File* file) -> int {
    object_fixture::check(file && object_fixture::irq_depth == 0 && object_fixture::lock_depth == 0);
    int const old = file->refcount.fetch_sub(1);
    object_fixture::check(old > 0);
    ++vm_fixture::puts;
    if (old == 1) ++vm_fixture::closes;
    return 0;
}
inline void vfs_put_file(File* file) { (void)vfs_put_file_result(file); }
}
namespace ker::mod::sched::task {
enum class TaskState : uint8_t { ACTIVE, EXITING, DEAD };
enum class TaskType : uint8_t { PROCESS, DAEMON, OTHER };
struct LazyVmemRange { uint64_t start{}, end{}; };
struct Task {
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> exit_in_progress{}, usercopy_pagemap_closing{}, scheduler_transition_active{};
    std::atomic<unsigned> usercopy_pagemap_accesses{};
    TaskType type{TaskType::PROCESS};
    uint64_t syscall_account_start_us{1};
    bool wants_block{}, deferred_task_switch{};
    mm::paging::PageTable* pagemap{};
    sys::Spinlock lazy_vmem_lock;
    std::vector<LazyVmemRange> lazy_vmem_ranges;
    ShutdownOwnership kernel_shutdown_ownership;
    auto try_acquire_usercopy_pagemap(mm::paging::PageTable*& output) -> bool {
        if (state.load() != TaskState::ACTIVE || usercopy_pagemap_closing.load() || pagemap == nullptr) return false;
        ++usercopy_pagemap_accesses;
        output = pagemap;
        return true;
    }
    void release_usercopy_pagemap() { object_fixture::check(usercopy_pagemap_accesses.fetch_sub(1) != 0); }
};
}
namespace vm_fixture { inline thread_local ker::mod::sched::task::Task* current{}; }
namespace ker::mod::sched {
inline auto can_query_current_task() -> bool { return true; }
inline auto get_current_task() -> task::Task* { return vm_fixture::current; }
inline auto has_run_queues() -> bool { return object_fixture::ordinary; }
inline auto interrupts_enabled() -> bool { return object_fixture::irq_depth == 0; }
inline auto preempt_count() -> uint32_t { return 0; }
inline void kern_yield() { ++vm_fixture::yields; }
}
namespace ker::mod::io::serial {
inline auto is_panic_mode() -> bool { return vm_fixture::panic_mode; }
}
namespace ker::mod::sys::usercopy {
inline constexpr uint64_t USER_ADDR_LIMIT = uint64_t{1} << 47;
inline auto access_window_active() -> bool { return vm_fixture::access_window; }
}
namespace ker::mod::mm::phys {
auto can_wait_for_reclaim() -> bool;
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void*;
auto page_ref_try_inc(void*) -> bool;
auto page_ref_dec(void*) -> uint32_t;
auto page_ref_get(void*) -> uint32_t;
auto page_kind_get(void*) -> PageKind;
}
namespace ker::mod::mm::numa { struct AllocationContext; struct AllocationResult; }
namespace ker::mod::mm::virt {
class PreparedAnonymousSwapFork;
class ForkPrivateChildRoot;
class ForkSourceOwner;
struct ForkCopyRefusalWitness;
class BindingRootMutationLease;
}
