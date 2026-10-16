#include "futex.hpp"

#include <abi/callnums/futex.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>
#include <util/hashtable.hpp>

#ifdef WOS_SELFTEST
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <utility>
#endif

namespace ker::syscall::futex {
namespace {
// ============================================================================
// Private words follow an address-space incarnation and VA; genuine shared
// leaves follow their retained common frame. Hashes only choose a bucket.
// ============================================================================

enum class FutexKeyKind : uint8_t { PRIVATE, SHARED_FRAME };
struct FutexKey {
    FutexKeyKind kind{};
    uint64_t owner{};
    uint64_t word{};
};

auto shared_futex_key(uint64_t physical_word) -> FutexKey {
    return {.kind = FutexKeyKind::SHARED_FRAME, .owner = 0, .word = physical_word};
}

auto futex_key(const mod::mm::virt::UserFutexIdentity& identity) -> FutexKey {
    if (identity.shared) return shared_futex_key(identity.physical_word);
    return {.kind = FutexKeyKind::PRIVATE, .owner = identity.root_incarnation, .word = identity.virtual_word};
}

struct FutexKeyHash {
    auto operator()(const FutexKey& key) const -> uint64_t {
        uint64_t const VALUE = ker::util::IntHash{}(key.owner) ^ ker::util::IntHash{}(key.word) ^ static_cast<uint64_t>(key.kind);
        return ker::util::IntHash{}(VALUE ^ (VALUE >> 32U));
    }
};
struct FutexKeyEqual {
    auto operator()(const FutexKey& left, const FutexKey& right) const -> bool {
        return left.kind == right.kind && left.owner == right.owner && left.word == right.word;
    }
};

struct FutexWaiter {
    uint64_t phys_addr{};              // Pinned physical word; private identity is independent of this address.
    uint64_t task_pid{};               // PID of the waiting task
    uint64_t task_cpu{};               // CPU the task was running on
    FutexWaiter* hash_next = nullptr;  // intrusive chain for hash table
    // A successfully published waiter has two independent owners: its table
    // entry and Task::futex_waiter. Wake and cancellation paths can retire
    // those owners concurrently, so deletion belongs to the final releaser.
    std::atomic<uint32_t> ref_count{2};
    // The pinned frame cannot be recycled while either published owner lives.
    mod::sys::usercopy::UserPageFrameRef frame;
    FutexKey key{};
};

struct FutexKeyExtract {
    auto operator()(const FutexWaiter& w) const -> FutexKey { return w.key; }
};

constexpr long NSEC_PER_SEC = 1000000000L;
constexpr uint64_t USEC_PER_SEC = 1000000ULL;

// 256 buckets — per-bucket spinlocks replace the single global lock.
// Default-constructed (no allocation); init() called on first use.
ker::util::IntrHashTable<FutexWaiter, FutexKeyExtract, FutexKeyHash, FutexKeyEqual> futex_table;
ker::mod::sys::Spinlock futex_init_lock;
std::atomic<bool> futex_table_initialized{false};

[[nodiscard]] auto ensure_futex_table() -> bool {
    if (__builtin_expect(static_cast<long>(!futex_table_initialized.load(std::memory_order_acquire)), 0) == 0) {
        return true;
    }

    uint64_t const FLAGS = futex_init_lock.lock_irqsave();
    bool initialized = futex_table_initialized.load(std::memory_order_relaxed);
    if (!initialized) {
        initialized = futex_table.init(256);
        futex_table_initialized.store(initialized, std::memory_order_release);
    }
    futex_init_lock.unlock_irqrestore(FLAGS);
    return initialized;
}

auto relative_timeout_us(mod::sched::task::Task& task, uint64_t timeout_user_addr, uint64_t& out_us) -> int64_t {
    out_us = 0;
    if (timeout_user_addr == 0) {
        return 0;
    }

    timespec ts{};
    if (!mod::sys::usercopy::copy_from_current_task_partial(task, timeout_user_addr, &ts, sizeof(ts)).complete(sizeof(ts))) {
        return -EFAULT;
    }
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -EINVAL;
    }

    auto const NSEC_US = (static_cast<uint64_t>(ts.tv_nsec) + 999ULL) / 1000ULL;
    auto const SEC = static_cast<uint64_t>(ts.tv_sec);
    if (SEC > (UINT64_MAX - NSEC_US) / USEC_PER_SEC) {
        return -EINVAL;
    }

    out_us = (SEC * USEC_PER_SEC) + NSEC_US;
    return 0;
}

auto deadline_from_now_us(uint64_t timeout_us) -> uint64_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    if (UINT64_MAX - NOW_US < timeout_us) {
        return UINT64_MAX;
    }
    return NOW_US + timeout_us;
}

auto futex_addr_is_aligned(uint64_t addr) -> bool { return (addr % alignof(int)) == 0; }

auto futex_wake_limit_from_count(int count, size_t& out_limit) -> int64_t {
    out_limit = 0;
    if (count < 0) {
        return -EINVAL;
    }
    out_limit = static_cast<size_t>(count);
    return 0;
}

auto claim_task_waiter(mod::sched::task::Task* task, FutexWaiter* waiter) -> bool {
    if (task == nullptr || waiter == nullptr) {
        return false;
    }

    void* expected_waiter = waiter;
    return task->futex_waiter.compare_exchange_strong(expected_waiter, nullptr, std::memory_order_acq_rel, std::memory_order_acquire);
}

auto release_waiter_ref(FutexWaiter* waiter) -> bool {
    if (waiter == nullptr) {
        return false;
    }

    uint32_t const PREVIOUS = waiter->ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (PREVIOUS != 1) {
        return false;
    }
    delete waiter;
    return true;
}

void retire_task_waiter_ref(FutexWaiter* waiter) {
    if (waiter == nullptr) {
        return;
    }

    if (futex_table.remove(waiter)) {
        static_cast<void>(release_waiter_ref(waiter));  // Table reference.
    }
    static_cast<void>(release_waiter_ref(waiter));  // Task-slot reference.
}

}  // namespace

// ============================================================================
// Syscall dispatcher
// ============================================================================

uint64_t sys_futex(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3) {
    auto futex_op = static_cast<abi::futex::futex_ops>(op);

    switch (futex_op) {
        case abi::futex::futex_ops::FUTEX_WAIT:
            return static_cast<uint64_t>(futex_wait(a1, static_cast<int>(a2), a3));

        case abi::futex::futex_ops::FUTEX_WAKE:
            return static_cast<uint64_t>(futex_wake(a1, static_cast<int>(a2)));

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

// ============================================================================
// futex_wait - Block until woken or value changes
// ============================================================================

int64_t futex_wait(uint64_t user_addr, int expected, uint64_t timeout_user_addr) {
    if (!futex_addr_is_aligned(user_addr)) {
        return -EINVAL;
    }
    if (!ensure_futex_table()) {
        return -ENOMEM;
    }

    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    if (!mod::sys::usercopy::range_valid(user_addr, sizeof(int)) ||
        (user_addr & (mod::mm::paging::PAGE_SIZE - 1)) > mod::mm::paging::PAGE_SIZE - sizeof(int)) {
        return -EFAULT;
    }

    // Stage timeout usercopy without retaining a first root-gate reader.
    // Futex mapping failure keeps priority over the staged timeout error.
    uint64_t timeout_us = 0;
    int64_t const TIMEOUT_STATUS = relative_timeout_us(*current_task, timeout_user_addr, timeout_us);

    mod::sys::usercopy::StableUserPage futex_page{};
    if (!mod::sys::usercopy::pin_current_task_user_page(*current_task, user_addr, false, futex_page)) {
        return -EFAULT;
    }
    uint64_t const PHYS_ADDR = futex_page.physical_address();
    auto const* kernel_addr = static_cast<const int*>(futex_page.kernel_address());
    if (TIMEOUT_STATUS != 0) {
        return TIMEOUT_STATUS;
    }

    if (timeout_user_addr != 0 && timeout_us == 0) {
        mod::sys::usercopy::UserAccessGuard const ACCESS{};
        int const CURRENT_VALUE = __atomic_load_n(kernel_addr, __ATOMIC_ACQUIRE);
        if (!futex_page.still_mapped()) {
            return -EFAULT;
        }
        return CURRENT_VALUE == expected ? -ETIMEDOUT : -EAGAIN;
    }

    // Allocate a waiter node
    auto* waiter = new (std::nothrow) FutexWaiter{};
    if (waiter == nullptr) {
        return -ENOMEM;
    }
    waiter->phys_addr = PHYS_ADDR;
    waiter->task_pid = current_task->pid;
    waiter->task_cpu = current_task->load_cpu();
    waiter->hash_next = nullptr;

    if (!futex_table.valid()) {
        delete waiter;
        return -ENOMEM;
    }
    if (!futex_page.still_mapped()) {
        delete waiter;
        return -EFAULT;
    }
    if (!futex_page.retain_frame(waiter->frame)) {
        delete waiter;
        return -ENOMEM;
    }
    mod::mm::virt::UserFutexIdentity identity{};
    if (!futex_page.capture_futex_identity(identity)) {
        delete waiter;
        return -EFAULT;
    }
    waiter->key = futex_key(identity);

    void* previous_waiter = nullptr;
    bool const INSERTED = futex_table.insert_if(
        waiter,
        [kernel_addr, expected]() -> bool {
            mod::sys::usercopy::UserAccessGuard const ACCESS{};
            int const CURRENT_VALUE = __atomic_load_n(kernel_addr, __ATOMIC_ACQUIRE);
            return CURRENT_VALUE == expected;
        },
        [current_task, waiter, timeout_user_addr, timeout_us, &previous_waiter]() {
            current_task->set_wait_channel("futex_wait", ker::mod::sched::task::WaitChannelKind::FUTEX);
            current_task->wake_at_us = timeout_user_addr != 0 ? deadline_from_now_us(timeout_us) : 0;
            current_task->deferred_task_switch = true;
            previous_waiter = current_task->futex_waiter.exchange(waiter, std::memory_order_acq_rel);
        });
    bool const MAPPING_STILL_VALID = futex_page.futex_identity_still_mapped(identity);
    if (!INSERTED) {
        delete waiter;
        return MAPPING_STILL_VALID ? -EAGAIN : -EFAULT;
    }

    if (previous_waiter != nullptr) {
        retire_task_waiter_ref(static_cast<FutexWaiter*>(previous_waiter));
    }

    if (!MAPPING_STILL_VALID) {
        if (claim_task_waiter(current_task, waiter)) {
            current_task->deferred_task_switch = false;
            current_task->wake_at_us = 0;
            current_task->clear_wait_channel();
            retire_task_waiter_ref(waiter);
        }
        return -EFAULT;
    }

    return 0;
}

// ============================================================================
// futex_wake - Wake one or more waiters
// ============================================================================

int64_t futex_wake(uint64_t user_addr, int count) {
    size_t wake_limit = 0;
    int64_t const COUNT_STATUS = futex_wake_limit_from_count(count, wake_limit);
    if (COUNT_STATUS != 0 || wake_limit == 0) {
        return COUNT_STATUS;
    }

    if (!futex_addr_is_aligned(user_addr)) {
        return -EINVAL;
    }

    if (!ensure_futex_table()) {
        return -ENOMEM;
    }
    auto* current_task = mod::sched::get_current_task();
    if (current_task == nullptr) {
        return -EINVAL;
    }

    if (!mod::sys::usercopy::range_valid(user_addr, sizeof(int)) ||
        (user_addr & (mod::mm::paging::PAGE_SIZE - 1)) > mod::mm::paging::PAGE_SIZE - sizeof(int)) {
        return -EFAULT;
    }

    mod::sys::usercopy::StableUserPage futex_page{};
    if (!mod::sys::usercopy::pin_current_task_user_page(*current_task, user_addr, false, futex_page)) {
        return -EFAULT;
    }
    if (!futex_page.still_mapped()) {
        return -EFAULT;
    }
    mod::mm::virt::UserFutexIdentity identity{};
    if (!futex_page.capture_futex_identity(identity)) return -EFAULT;

    int woken_count = 0;

    futex_table.remove_by_key_limit(futex_key(identity), wake_limit, [&](FutexWaiter* waiter) -> bool {
        bool claimed_waiter = false;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            if (claim_task_waiter(waiter_task, waiter)) {
                mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
                claimed_waiter = true;
                woken_count++;
                static_cast<void>(release_waiter_ref(waiter));  // Task-slot reference.
            }
            waiter_task->release();
        }
        static_cast<void>(release_waiter_ref(waiter));  // Table reference.
        return claimed_waiter;
    });

    return woken_count;
}

void futex_wait_cleanup_for_task(mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    auto* waiter = static_cast<FutexWaiter*>(task->futex_waiter.exchange(nullptr, std::memory_order_acq_rel));
    if (waiter == nullptr) {
        return;
    }

    if (futex_table.remove(waiter)) {
        static_cast<void>(release_waiter_ref(waiter));  // Table reference.
    }
    static_cast<void>(release_waiter_ref(waiter));  // Task-slot reference.
}

int64_t futex_wake_by_phys(uint64_t phys_addr, int count) {
    size_t wake_limit = 0;
    int64_t const COUNT_STATUS = futex_wake_limit_from_count(count, wake_limit);
    if (COUNT_STATUS != 0 || wake_limit == 0) {
        return COUNT_STATUS;
    }

    if (phys_addr == 0) {
        return -EINVAL;
    }

    if (!ensure_futex_table()) {
        return -ENOMEM;
    }

    int woken_count = 0;
    futex_table.remove_by_key_limit(shared_futex_key(phys_addr), wake_limit, [&](FutexWaiter* waiter) -> bool {
        bool claimed_waiter = false;
        auto* waiter_task = mod::sched::find_task_by_pid_safe(waiter->task_pid);
        if (waiter_task != nullptr) {
            if (claim_task_waiter(waiter_task, waiter)) {
                mod::sched::wake_task_from_event_on_cpu(waiter_task, waiter->task_cpu, mod::sched::EventWakeDeferredSwitch::CANCEL);
                claimed_waiter = true;
                woken_count++;
                static_cast<void>(release_waiter_ref(waiter));  // Task-slot reference.
            }
            waiter_task->release();
        }
        static_cast<void>(release_waiter_ref(waiter));  // Table reference.
        return claimed_waiter;
    });
    return woken_count;
}

#ifdef WOS_SELFTEST
namespace {
// Exclusively owned, never active or scheduler-published. Mapping retirement
// runs after every lexical stable pin has ended, while a queued frame may live.
struct SelftestFutexFrame {
    static constexpr uint64_t ADDRESS = 0x49000040;
    mod::sched::task::Task task{};
    mod::mm::paging::PageTable* root{};
    void* frame{};
    bool oracle_owned{};

    ~SelftestFutexFrame() {
        retire_mapping();
        static_cast<void>(release_oracle());
    }

    auto initialize() -> bool {
        root = mod::mm::virt::create_pagemap_may_fail();
        if (root == nullptr) return false;
        task.type = mod::sched::task::TaskType::PROCESS;
        task.pagemap = root;
        frame = mod::mm::phys::page_alloc_with_reclaim_may_fail(mod::mm::PhysicalPageOwner::SELFTEST, mod::mm::paging::PAGE_SIZE,
                                                                "futex-frame-owner");
        if (frame == nullptr) return false;
        if (!mod::mm::phys::page_ref_try_inc(frame)) {
            mod::mm::phys::page_ref_dec(frame);
            frame = nullptr;
            return false;
        }
        oracle_owned = true;
        uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(frame)));
        // map_page transfers this initial frame reference into the owned root.
        mod::mm::virt::map_page(root, ADDRESS & ~(mod::mm::paging::PAGE_SIZE - 1), PHYSICAL,
                                mod::mm::paging::page_types::USER | mod::mm::paging::PAGE_SHARED);
        // The following real stable pin verifies this mapping before retain;
        // the fixture needs no raw translation at the syscall entrypoint.
        return true;
    }

    void retire_mapping() {
        if (root == nullptr) return;
        auto* detached = task.detach_pagemap_after_usercopy_quiescence();
        mod::mm::virt::destroy_user_space(detached, 0, "futex_ktest", "frame-owner");
        mod::mm::virt::release_pagemap(detached);
        root = nullptr;
    }

    auto release_oracle() -> bool {
        if (!oracle_owned) return false;
        oracle_owned = false;
        // Observe the final decrement itself; never query an already free
        // frame that another CPU could have allocated in the meantime.
        return mod::mm::phys::page_ref_dec(frame) == 0;
    }
};

struct SelftestFutexWaiterOwners {
    decltype(futex_table)& table;
    mod::sched::task::Task& task;
    FutexWaiter* waiter;
    bool table_owned{true};
    bool slot_owned{true};

    ~SelftestFutexWaiterOwners() {
        // This local table and unpublished Task have no external actors.
        // Retire both known fixture owners even when a test predicate fails.
        if (slot_owned) {
            task.futex_waiter.store(nullptr, std::memory_order_release);
            static_cast<void>(release_waiter_ref(waiter));
        }
        if (table_owned) {
            static_cast<void>(table.remove(waiter));
            static_cast<void>(release_waiter_ref(waiter));
        }
    }
};

auto selftest_publish_key_waiter(decltype(futex_table)& table, SelftestFutexFrame& subject) -> FutexWaiter* {
    auto* waiter = new (std::nothrow) FutexWaiter{};
    if (waiter == nullptr) return nullptr;
    bool published = false;
    {
        mod::sys::usercopy::StableUserPage pin;
        mod::mm::virt::UserFutexIdentity identity{};
        if (mod::sys::usercopy::pin_task_user_page(subject.task, subject.ADDRESS, false, false, pin) &&
            pin.capture_futex_identity(identity) && pin.retain_frame(waiter->frame)) {
            waiter->phys_addr = pin.physical_address();
            waiter->key = futex_key(identity);
            auto* word = static_cast<const int*>(pin.kernel_address());
            published = table.insert_if(
                waiter,
                [word] {
                    mod::sys::usercopy::UserAccessGuard const ACCESS{};
                    return __atomic_load_n(word, __ATOMIC_ACQUIRE) == 0;
                },
                [&] { subject.task.futex_waiter.store(waiter, std::memory_order_release); });
        }
    }
    if (!published) {
        delete waiter;
        return nullptr;
    }
    return waiter;
}

auto selftest_remove_key(decltype(futex_table)& table, const FutexKey& key, SelftestFutexWaiterOwners& owners) -> size_t {
    return table.remove_by_key_limit(key, 1, [&](FutexWaiter* waiter) {
        if (waiter != owners.waiter) return false;
        owners.table_owned = false;
        static_cast<void>(release_waiter_ref(waiter));
        return true;
    });
}

auto selftest_capture_key(SelftestFutexFrame& subject, uint64_t address, mod::mm::virt::UserFutexIdentity& identity) -> bool {
    mod::sys::usercopy::StableUserPage pin;
    return mod::sys::usercopy::pin_task_user_page(subject.task, address, false, false, pin) && pin.capture_futex_identity(identity) &&
           pin.futex_identity_still_mapped(identity);
}
}  // namespace

auto futex_selftest_queued_frame_survives_root_retirement() -> bool {
    for (bool slot_first : {false, true}) {
        SelftestFutexFrame subject;
        if (!subject.initialize()) return false;
        decltype(futex_table) table;
        if (!table.init(8)) return false;
        auto* waiter = new (std::nothrow) FutexWaiter{};
        if (waiter == nullptr) return false;
        bool published = false;
        {
            mod::sys::usercopy::StableUserPage pin;
            mod::mm::virt::UserFutexIdentity identity{};
            if (mod::sys::usercopy::pin_task_user_page(subject.task, subject.ADDRESS, false, false, pin) && pin.still_mapped() &&
                pin.retain_frame(waiter->frame) && pin.capture_futex_identity(identity)) {
                waiter->phys_addr = pin.physical_address();
                waiter->key = futex_key(identity);
                published = table.insert_if(
                    waiter, [] { return true; }, [&] { subject.task.futex_waiter.store(waiter, std::memory_order_release); });
            }
        }
        if (!published) {
            delete waiter;
            return false;
        }
        SelftestFutexWaiterOwners owners{table, subject.task, waiter};
        bool ok =
            subject.task.usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0 && mod::mm::phys::page_ref_get(subject.frame) == 3;
        subject.retire_mapping();
        ok = subject.task.pagemap == nullptr && mod::mm::phys::page_ref_get(subject.frame) == 2 && ok;
        if (slot_first) {
            bool const CLAIMED = claim_task_waiter(&subject.task, waiter);
            ok = CLAIMED && ok;
            // This unpublished Task and local table have no external actors;
            // the fixture owns both references even when an assertion fails.
            subject.task.futex_waiter.store(nullptr, std::memory_order_release);
            owners.slot_owned = false;
            bool const FIRST_DELETED = release_waiter_ref(waiter);
            ok = !FIRST_DELETED && ok;
            // Final deletion happens inside the real bucket IRQ-lock callback.
            size_t const REMOVED = table.remove_by_key_limit(waiter->key, 1, [&](FutexWaiter* entry) {
                bool const FRAME_HELD = mod::mm::phys::page_ref_get(subject.frame) == 2;
                owners.table_owned = false;
                bool const DELETED = release_waiter_ref(entry);
                ok = FRAME_HELD && DELETED && ok;
                return true;
            });
            ok = REMOVED == 1 && ok;
        } else {
            bool const REMOVED = table.remove(waiter);
            ok = REMOVED && ok;
            if (REMOVED) {
                owners.table_owned = false;
                ok = !release_waiter_ref(waiter) && ok;
            }
            bool const CLAIMED = claim_task_waiter(&subject.task, waiter);
            ok = CLAIMED && ok;
            subject.task.futex_waiter.store(nullptr, std::memory_order_release);
            owners.slot_owned = false;
            bool const DELETED = release_waiter_ref(waiter);
            ok = DELETED && ok;
        }
        ok = mod::mm::phys::page_ref_get(subject.frame) == 1 && ok;
        ok = subject.release_oracle() && ok;
        if (!ok) return false;
    }
    return true;
}

auto futex_selftest_frame_ref_moves_preserve_ownership() -> bool {
    SelftestFutexFrame subject;
    if (!subject.initialize()) return false;
    bool ok = true;
    {
        mod::sys::usercopy::StableUserPage pin;
        mod::sys::usercopy::StableUserPage empty_pin;
        mod::sys::usercopy::UserPageFrameRef first;
        mod::sys::usercopy::UserPageFrameRef empty;
        ok = mod::sys::usercopy::pin_task_user_page(subject.task, subject.ADDRESS, false, false, pin) && pin.retain_frame(first) &&
             mod::mm::phys::page_ref_get(subject.frame) == 4;
        ok = !pin.retain_frame(first) && first.valid() && !empty_pin.retain_frame(empty) && !empty.valid() &&
             mod::mm::phys::page_ref_get(subject.frame) == 4 && ok;
        mod::sys::usercopy::UserPageFrameRef moved(std::move(first));
        ok = !first.valid() && moved.valid() && mod::mm::phys::page_ref_get(subject.frame) == 4 && ok;
        auto* same = &moved;
        moved = std::move(*same);
        ok = moved.valid() && mod::mm::phys::page_ref_get(subject.frame) == 4 && ok;
        ok = pin.retain_frame(first) && mod::mm::phys::page_ref_get(subject.frame) == 5 && ok;
        moved = std::move(first);
        ok = !first.valid() && moved.valid() && mod::mm::phys::page_ref_get(subject.frame) == 4 && ok;
        moved.reset();
        moved.reset();
        ok = !moved.valid() && mod::mm::phys::page_ref_get(subject.frame) == 3 && ok;
    }
    ok = subject.task.usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0 && mod::mm::phys::page_ref_get(subject.frame) == 2 &&
         ok;
    subject.retire_mapping();
    ok = mod::mm::phys::page_ref_get(subject.frame) == 1 && ok;
    return subject.release_oracle() && ok;
}

auto futex_selftest_private_keys_separate_physical_aliases() -> bool {
    SelftestFutexFrame first;
    SelftestFutexFrame second;
    if (!first.initialize() || !second.initialize()) return false;
    uint64_t const PHYSICAL = reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(first.frame)));
    uint64_t const PRIVATE_FLAGS = mod::mm::paging::page_types::USER_READONLY;
    mod::mm::virt::map_page(first.root, first.ADDRESS, PHYSICAL, PRIVATE_FLAGS);  // Same-frame flag change owns no extra ref.
    if (!mod::mm::phys::page_ref_try_inc(first.frame)) return false;
    mod::mm::virt::map_page(second.root, second.ADDRESS, PHYSICAL, PRIVATE_FLAGS);
    uint64_t const ALIAS = first.ADDRESS + mod::mm::paging::PAGE_SIZE;
    if (!mod::mm::phys::page_ref_try_inc(first.frame)) return false;
    mod::mm::virt::map_page(first.root, ALIAS, PHYSICAL, PRIVATE_FLAGS);
    mod::mm::virt::UserFutexIdentity own{};
    mod::mm::virt::UserFutexIdentity other_root{};
    mod::mm::virt::UserFutexIdentity other_word{};
    if (!selftest_capture_key(first, first.ADDRESS, own) || !selftest_capture_key(second, second.ADDRESS, other_root) ||
        !selftest_capture_key(first, ALIAS, other_word) || own.shared || other_root.shared || other_word.shared ||
        own.physical_word != other_root.physical_word || own.physical_word != other_word.physical_word ||
        own.root_incarnation == other_root.root_incarnation || own.root_incarnation != other_word.root_incarnation) {
        return false;
    }
    decltype(futex_table) table;
    if (!table.init(8)) return false;
    auto* waiter = selftest_publish_key_waiter(table, first);
    if (waiter == nullptr) return false;
    SelftestFutexWaiterOwners owners{table, first.task, waiter};
    return selftest_remove_key(table, futex_key(other_root), owners) == 0 &&
           selftest_remove_key(table, futex_key(other_word), owners) == 0 &&
           selftest_remove_key(table, shared_futex_key(own.physical_word), owners) == 0 &&
           selftest_remove_key(table, futex_key(own), owners) == 1;
}

auto futex_selftest_shared_alias_and_private_replacement_keys() -> bool {
    SelftestFutexFrame first;
    SelftestFutexFrame second;
    if (!first.initialize() || !second.initialize()) return false;
    uint64_t const FIRST_PHYSICAL = reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(first.frame)));
    uint64_t const SECOND_PHYSICAL = reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(second.frame)));
    if (!mod::mm::phys::page_ref_try_inc(first.frame)) return false;
    mod::mm::virt::map_page(second.root, second.ADDRESS, FIRST_PHYSICAL, mod::mm::paging::page_types::USER | mod::mm::paging::PAGE_SHARED);
    mod::mm::virt::UserFutexIdentity shared_own{};
    mod::mm::virt::UserFutexIdentity shared_peer{};
    if (!selftest_capture_key(first, first.ADDRESS, shared_own) || !selftest_capture_key(second, second.ADDRESS, shared_peer) ||
        !shared_own.shared || !shared_peer.shared || shared_own.root_incarnation == shared_peer.root_incarnation ||
        !FutexKeyEqual{}(futex_key(shared_own), futex_key(shared_peer))) {
        return false;
    }
    {
        decltype(futex_table) table;
        if (!table.init(8)) return false;
        auto* waiter = selftest_publish_key_waiter(table, first);
        if (waiter == nullptr) return false;
        SelftestFutexWaiterOwners owners{table, first.task, waiter};
        if (selftest_remove_key(table, futex_key(shared_peer), owners) != 1) return false;
    }
    mod::mm::virt::map_page(first.root, first.ADDRESS, FIRST_PHYSICAL, mod::mm::paging::page_types::USER_READONLY);
    mod::mm::virt::UserFutexIdentity before{};
    if (!selftest_capture_key(first, first.ADDRESS, before) || before.shared) return false;
    decltype(futex_table) table;
    if (!table.init(8)) return false;
    auto* waiter = selftest_publish_key_waiter(table, first);
    if (waiter == nullptr) return false;
    SelftestFutexWaiterOwners owners{table, first.task, waiter};
    if (!mod::mm::phys::page_ref_try_inc(second.frame)) return false;
    mod::mm::virt::map_page(first.root, first.ADDRESS, SECOND_PHYSICAL, mod::mm::paging::page_types::USER_READONLY);
    mod::mm::virt::UserFutexIdentity after{};
    if (!selftest_capture_key(first, first.ADDRESS, after) || after.shared || after.physical_word == before.physical_word ||
        !FutexKeyEqual{}(futex_key(before), futex_key(after))) {
        return false;
    }
    return selftest_remove_key(table, shared_futex_key(before.physical_word), owners) == 0 &&
           selftest_remove_key(table, futex_key(after), owners) == 1;
}

auto futex_selftest_table_init_is_serialized() -> bool {
    bool const FIRST = ensure_futex_table();
    bool const SECOND = ensure_futex_table();
    return FIRST && SECOND && futex_table_initialized.load(std::memory_order_acquire) && futex_table.valid();
}

auto futex_selftest_addr_alignment_guard() -> bool { return futex_addr_is_aligned(0x1000) && !futex_addr_is_aligned(0x1001); }

auto futex_selftest_stale_wake_does_not_claim_waiter() -> bool {
    mod::sched::task::Task task{};
    FutexWaiter waiter{};
    FutexWaiter stale{};

    task.futex_waiter.store(&waiter, std::memory_order_release);
    if (!claim_task_waiter(&task, &waiter)) {
        return false;
    }
    if (task.futex_waiter.load(std::memory_order_acquire) != nullptr) {
        return false;
    }

    task.futex_waiter.store(&stale, std::memory_order_release);
    if (claim_task_waiter(&task, &waiter)) {
        return false;
    }

    return task.futex_waiter.load(std::memory_order_acquire) == &stale;
}

auto futex_selftest_waiter_two_owner_retirement() -> bool {
    auto* waiter = new (std::nothrow) FutexWaiter{};
    if (waiter == nullptr) {
        return false;
    }

    bool const FIRST_OWNER_DELETED = release_waiter_ref(waiter);
    bool const FINAL_OWNER_DELETED = release_waiter_ref(waiter);
    return !FIRST_OWNER_DELETED && FINAL_OWNER_DELETED;
}

auto futex_selftest_wake_count_limit() -> bool {
    size_t limit = 99;
    if (futex_wake_limit_from_count(-1, limit) != -EINVAL || limit != 0) {
        return false;
    }
    if (futex_wake_limit_from_count(0, limit) != 0 || limit != 0) {
        return false;
    }
    if (futex_wake_limit_from_count(3, limit) != 0 || limit != 3) {
        return false;
    }
    return true;
}
#endif

}  // namespace ker::syscall::futex
