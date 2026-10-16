// Host endpoints are explicit primitives; all metadata algorithms are actual extracted bodies.
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <utility>
#include <vector>
namespace fixture {
unsigned cases{}, checks{}, pub{}, spin{}, mutex{}, allocations{}, retains{}, puts{};
bool irq = true, ready = true, query = true, try_busy = false;
unsigned preempt{};
int fail_after = -1;
void check(bool b, const char* why) {
    ++checks;
    if (!b) {
        std::fprintf(stderr, "ASSERT: %s\n", why);
        std::abort();
    }
}
bool fail_alloc() {
    check(!pub && !spin && !mutex && irq && !preempt, "allocation outside final owners");
    ++allocations;
    if (fail_after == 0) return true;
    if (fail_after > 0) --fail_after;
    return false;
}
struct Mutex {
    bool held{};
    void lock() {
        check(!held, "nonrecursive registry mutex");
        held = true;
        ++mutex;
    }
    bool try_lock() {
        if (try_busy) return false;
        lock();
        return true;
    }
    void unlock() {
        check(held && mutex, "unlock owned registry");
        held = false;
        --mutex;
    }
};
struct Spin {
    bool held{};
    uint64_t lock_irqsave() {
        check(!held, "nonrecursive metadata lock");
        uint64_t f = irq;
        irq = false;
        held = true;
        ++spin;
        return f;
    }
    void unlock_irqrestore(uint64_t f) {
        check(held && spin, "unlock metadata");
        held = false;
        --spin;
        irq = f;
    }
};
}  // namespace fixture
void* operator new(std::size_t n) {
    if (fixture::fail_alloc()) return nullptr;
    return std::malloc(n ? n : 1);
}
void* operator new[](std::size_t n) {
    if (fixture::fail_alloc()) return nullptr;
    return std::malloc(n ? n : 1);
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    if (fixture::fail_alloc()) return nullptr;
    return std::malloc(n ? n : 1);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    if (fixture::fail_alloc()) return nullptr;
    return std::malloc(n ? n : 1);
}
namespace fixture {
void release(void* p) {
    if (p) check(!pub && !spin && !mutex && irq && !preempt, "heap return outside all final owners");
    std::free(p);
}
}  // namespace fixture
void operator delete(void* p) noexcept { fixture::release(p); }
void operator delete[](void* p) noexcept { fixture::release(p); }
void operator delete(void* p, std::size_t) noexcept { fixture::release(p); }
void operator delete[](void* p, std::size_t) noexcept { fixture::release(p); }
namespace ker::mod::mm::paging {
constexpr uint64_t PAGE_SIZE = 4096;
struct PageTable {
    uint64_t serial{1};
};
}  // namespace ker::mod::mm::paging
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, DEAD };
struct Task {
    TaskType type = TaskType::PROCESS;
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> scheduler_transition_active{}, usercopy_pagemap_closing{};
    uint64_t syscall_account_start_us = 1, pid = 1;
    bool has_run{}, is_thread{};
    ker::mod::mm::paging::PageTable* pagemap{};
    Task* owner{};
};
auto process_pid(const Task& t) -> uint64_t { return t.owner ? t.owner->pid : t.pid; }
}  // namespace ker::mod::sched::task
namespace ker::mod::sched {
task::Task* current{};
auto can_query_current_task() -> bool { return fixture::query; }
auto get_current_task() -> task::Task* { return current; }
auto preempt_count() -> unsigned { return fixture::preempt; }
auto interrupts_enabled() -> bool { return fixture::irq; }
}  // namespace ker::mod::sched
namespace ker::mod::sys {
class SyscallPreparationScope {
   public:
    auto ready() const -> bool { return fixture::ready; }
};
}  // namespace ker::mod::sys
namespace ker::syscall::vmem {
class SharedVmemPublicationGuard {
    bool acquired_ = true;

   public:
    SharedVmemPublicationGuard() {
        fixture::check(!fixture::pub, "single PUB owner");
        ++fixture::pub;
    }
    ~SharedVmemPublicationGuard() { --fixture::pub; }
    auto acquired() const -> bool { return acquired_; }
};
}  // namespace ker::syscall::vmem
namespace ker::mod::mm::virt {
ker::mod::mm::paging::PageTable kernel;
auto get_kernel_pagemap() -> ker::mod::mm::paging::PageTable* { return &kernel; }
auto capture_user_root_incarnation(ker::mod::mm::paging::PageTable* p, uint64_t& out) -> bool {
    if (!p || !p->serial || p == &kernel) return false;
    out = p->serial;
    return true;
}
}  // namespace ker::mod::mm::virt
namespace ker::test::numa_lifecycle {
enum class CounterField { LEGACY_FILE_REGISTRY, REGISTRY_RETURNS, SWAP_MAPPINGS, SWAP_OBJECTS, SWAP_RETURNS };
namespace mm_debt {
long values[5]{};
void begin_change() {}
void end_change() {}
void add(CounterField f) { ++values[static_cast<unsigned>(f)]; }
void drop(CounterField f) {
    fixture::check(values[static_cast<unsigned>(f)] > 0, "debt cannot underflow");
    --values[static_cast<unsigned>(f)];
}
}  // namespace mm_debt
}  // namespace ker::test::numa_lifecycle
namespace fixture {
using Task = ker::mod::sched::task::Task;
using Root = ker::mod::mm::paging::PageTable;
using Scope = ker::mod::sys::SyscallPreparationScope;
using Pub = ker::syscall::vmem::SharedVmemPublicationGuard;
struct Actors {
    Root source{11}, childroot{12}, foreign{13};
    Task actor, child;
    Scope scope;
    Actors() {
        check(!pub && !spin && !mutex, "no previous owners");
        actor.pagemap = &source;
        child.pagemap = &childroot;
        child.pid = 2;
        ker::mod::sched::current = &actor;
    }
    ~Actors() {
        check(!pub && !spin && !mutex, "all owners released");
        fail_after = -1;
        irq = true;
        try_busy = false;
        ready = true;
        preempt = 0;
    }
};
}  // namespace fixture
