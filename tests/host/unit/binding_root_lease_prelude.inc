// Finite Host boundary ports. Every marked body below comes unchanged from the
// selected production source; the complete family/binding/index TU is linked.
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <type_traits>
#include <utility>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <snapshot_fixture.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sched/shutdown_ownership.hpp>

namespace lease_fixture {
inline unsigned assertions{}, failures{}, cases{}, deaths{}, publication_depth{}, control_lookups{}, shootdown_services{};
inline bool run_queues{true}, queryable{true}, interrupts{true}, panic{}, access_window{}, retirement_worker{};
inline uint32_t preempt{};
void check(bool value, const char* message) {
    ++assertions;
    if (!value && failures++ < 24) std::fprintf(stderr, "FAIL %s\n", message);
}
void require(bool value, const char* message) {
    check(value, message);
    if (!value) std::abort();
}
// A finite ordinary mutex seam, not an emulation of scheduler blocking.
struct Mutex {
    bool held{};
    void lock() {
        require(!held && publication_depth == 0 && snapshot_fixture::lock_depth == 0, "PUB primitive lock boundary");
        held = true;
        ++publication_depth;
    }
    void unlock() {
        require(held && publication_depth == 1 && snapshot_fixture::lock_depth == 0, "PUB primitive unlock boundary");
        --publication_depth;
        held = false;
    }
};
}
namespace ker::mod::sys { using Mutex = lease_fixture::Mutex; }
namespace ker::mod::sched::task {
enum class TaskState : uint8_t { NEW, ACTIVE, EXITING, DEAD };
enum class TaskType : uint8_t { PROCESS, DAEMON, OTHER };
struct Task {
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> scheduler_transition_active{}, exit_in_progress{}, usercopy_pagemap_closing{};
    bool deferred_task_switch{}, wants_block{};
    TaskType type{TaskType::PROCESS};
    uint64_t syscall_account_start_us{1};
    ker::mod::sched::ShutdownOwnership kernel_shutdown_ownership{};
};
}
namespace lease_fixture { inline thread_local ker::mod::sched::task::Task* current{}; }
namespace ker::mod::sched {
auto has_run_queues() -> bool { return lease_fixture::run_queues; }
auto can_query_current_task() -> bool { return lease_fixture::queryable; }
auto preempt_count() -> uint32_t { return lease_fixture::preempt; }
auto interrupts_enabled() -> bool { return lease_fixture::interrupts; }
auto get_current_task() -> task::Task* { return lease_fixture::current; }
}
namespace ker::mod::io::serial {
auto is_panic_mode() -> bool { return lease_fixture::panic; }
}
namespace ker::mod::sys::usercopy {
auto access_window_active() noexcept -> bool { return lease_fixture::access_window; }
}
namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::is_current_worker() noexcept -> bool { return lease_fixture::retirement_worker; }
}

namespace ker::syscall::vmem {
@PUB_DECL@
namespace {
inline auto get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }
@PUB_STORAGE@
@PUB_MATCH@
}
@PUB_CTOR@
@PUB_DTOR@
@PUB_ACQUIRED@
@PUB_EXECUTOR@
}
namespace ker::mod::mm::phys {
@CAN_WAIT_IMPL@
@CAN_WAIT@
}
namespace ker::mod::mm::virt {
using PageTable = paging::PageTable;
@LEASE_ENUM@
@LEASE_DECL@
static_assert(!std::is_copy_constructible_v<BindingRootMutationLease> && !std::is_move_constructible_v<BindingRootMutationLease>);

// Root-control lookup is intentionally only a pointer-key metadata port. A
// PROT_NONE key must never be read; no root allocator/page-reference API exists
// in this Host translation unit. The real index and real gate handle admission.
struct HostRootControl { PageTable* root{}; uint64_t serial{}; };
std::array<HostRootControl, 8> host_root_controls{};
sys::Spinlock& user_mapping_pin_lock = snapshot_fixture::index_lock;
file_mapping::FileMappingIndex file_mapping_index;
PageTable* kernel_pagemap = reinterpret_cast<PageTable*>(uintptr_t{0x1000});
auto find_user_root_control_locked(PageTable* root) -> const HostRootControl* {
    lease_fixture::require(snapshot_fixture::lock_depth != 0, "metadata lookup holds actual admission pin seam");
    ++lease_fixture::control_lookups;
    for (const auto& control : host_root_controls)
        if (control.root == root && root != nullptr) return &control;
    return nullptr;
}
void service_pending_tlb_shootdowns() { ++lease_fixture::shootdown_services; }
@GATE_STORAGE@
@GATE_INDEX@
@GATE_SERVICE@
@READER_ADMIT@
@READER_ACQUIRE@
@READER_RELEASE@
@GATE_TRY@
@GATE_END@
@CONTEXT@
@LEASE_ACQUIRED@
@LEASE_FINISH@
@LEASE_ACQUIRE@
@LEASE_FACTORY@
}
