#pragma once

// Finite Host endpoints for actual resource accounting and copied bridge bodies.
// No endpoint can issue a PinCharge or provide page, root or DMA authority.
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <net/wki/resource_contract.hpp>
#include <optional>
#include <platform/sched/resource_lifetime.hpp>
#include <platform/sched/resource_model.hpp>
#include <platform/sched/resource_pin.hpp>
#include <string>

namespace pin_host {
inline thread_local std::array<unsigned, 8> ranks{};
inline thread_local unsigned depth{}, publication_depth{};
inline std::atomic<unsigned> usercopies{};
}  // namespace pin_host
namespace ker::mod::sys {
class Spinlock {
   public:
    constexpr explicit Spinlock(unsigned rank = 2) : rank_(rank) {}
    auto lock_irqsave() -> uint64_t {
        assert(pin_host::depth < pin_host::ranks.size());
        assert(pin_host::depth == 0 || pin_host::ranks[pin_host::depth - 1] <= rank_);
        mutex_.lock();
        auto const before = pin_host::depth;
        pin_host::ranks[pin_host::depth++] = rank_;
        return before;
    }
    void unlock_irqrestore(uint64_t flags) {
        assert(pin_host::depth == flags + 1 && pin_host::ranks[pin_host::depth - 1] == rank_);
        --pin_host::depth;
        mutex_.unlock();
    }

   private:
    unsigned rank_;
    std::mutex mutex_;
};
using Mutex = std::mutex;
using MutexGuard = std::lock_guard<Mutex>;
}  // namespace ker::mod::sys
namespace ker::mod::security {
enum class Authority { RESOURCE_LIMIT };
struct Snapshot {
    bool privileged = true;
    auto allows(Authority) const -> bool { return privileged; }
};
namespace compartment {
inline auto host_namespace_admission(void*) -> int { return 0; }
}  // namespace compartment
}  // namespace ker::mod::security
namespace ker::mod::sched::task {
enum class TaskType { DAEMON, PROCESS };
enum class TaskState { ACTIVE, EXITING, DEAD };
struct HostNamespace {
    auto closed() const -> bool { return false; }
    auto is_host() const -> bool { return true; }
};
struct HostDescriptors {
    auto size() const -> unsigned { return 0; }
    auto reserved_count() const -> unsigned { return 0; }
    auto lookup(unsigned) const -> void* { return nullptr; }
};
// Layout seam: only fields used by the actual selected Task/resource methods.
// The actual Owner/LifetimeOwner/reference methods below supply canonical binding.
struct Task {
    static constexpr unsigned FD_TABLE_SIZE = 256;
    TaskType type = TaskType::DAEMON;
    std::atomic<TaskState> state{TaskState::ACTIVE};
    bool is_thread{};
    uint64_t pid = 77, owner_pid{};
    uint64_t child_user_time_us{}, child_system_time_us{};
    std::atomic<uint32_t> ref_count{1}, resource_cpu_threads{}, resource_thread_owners{};
    std::atomic<bool> exit_in_progress{}, scheduler_published{};
    bool resource_cpu_admission_closed{}, resource_exec_admission_closed{};
    std::atomic<uint64_t> resource_exec_quiescence_epoch{};
    ker::mod::sys::Spinlock resource_lock{1}, fd_table_lock{1};
    ker::mod::sched::resource::State resources{};
    ker::mod::sched::resource::PinAccountOwner pin_account;
    ker::mod::sched::resource::LifetimeOwner resource_owner;
    HostDescriptors fd_table;
    HostNamespace space;
    // ACTUAL_TASK_REFERENCE_METHODS
    auto credentials() const -> ker::mod::security::Snapshot { return {}; }
    auto pid_namespace() const -> const HostNamespace* { return &space; }
    auto ipc_namespace() const -> const HostNamespace* { return &space; }
    auto mount_namespace() const -> const HostNamespace* { return &space; }
    auto initialize_pid_namespace(const HostNamespace&) -> bool { return true; }
    auto initialize_ipc_namespace(const HostNamespace&) -> bool { return true; }
    auto initialize_mount_namespace(const HostNamespace&) -> bool { return true; }
    auto get_fd_cloexec(unsigned) const -> bool { return false; }
};
}  // namespace ker::mod::sched::task
namespace ker::syscall::vmem {
class SharedVmemPublicationGuard {
   public:
    SharedVmemPublicationGuard() {
        mutex_.lock();
        ++pin_host::publication_depth;
    }
    ~SharedVmemPublicationGuard() {
        --pin_host::publication_depth;
        mutex_.unlock();
    }

   private:
    static inline std::mutex mutex_;
};
}  // namespace ker::syscall::vmem
namespace ker::mod::sys::usercopy {
template <class T>
auto copy_value_from_task(ker::mod::sched::task::Task&, uint64_t address, T& value) -> bool {
    assert(pin_host::depth == 0 && pin_host::publication_depth == 0);
    ++pin_host::usercopies;
    if (address == 0) return false;
    std::memcpy(&value, reinterpret_cast<const void*>(address), sizeof(value));
    return true;
}
template <class T>
auto copy_value_to_task(ker::mod::sched::task::Task&, uint64_t address, const T& value) -> bool {
    assert(pin_host::depth == 0 && pin_host::publication_depth == 0);
    ++pin_host::usercopies;
    if (address == 0) return false;
    std::memcpy(reinterpret_cast<void*>(address), &value, sizeof(value));
    return true;
}
}  // namespace ker::mod::sys::usercopy
namespace ker::mod::sched::resource {
inline void inherit_service(task::Task&, task::Task&) {}
inline auto observe_address_space(task::Task&) -> bool { return true; }
inline void copy_address_space_observation(task::Task&, Usage&) {}
inline void copy_process_observation(task::Task&, Usage&) {}
}  // namespace ker::mod::sched::resource
namespace ker::syscall::process::child_events {
struct Observation {
    uint64_t user_time_us{}, system_time_us{};
};
inline auto read_reaped_cpu_times(ker::mod::sched::task::Task&) -> Observation { return {}; }
}  // namespace ker::syscall::process::child_events
namespace pin_host {
using Task = ker::mod::sched::task::Task;
using ProcTask = Task;
struct ProcFileData {
    Task* target{};
    ker::mod::sys::Mutex limits_content_lock;
    char* content{};
    size_t content_len{};
    ~ProcFileData() { delete[] content; }
};
inline auto procfs_view_policy(const ProcFileData&) -> int { return 0; }
inline void int_to_str(uint64_t value, char* out, size_t bytes) {
    std::snprintf(out, bytes, "%llu", static_cast<unsigned long long>(value));
}
// ACTUAL_PROCFS_LIMITS
// ACTUAL_WKI_EXPORT_DECLARATIONS
}  // namespace pin_host
