#include "process.hpp"

#include <bits/posix/posix_string.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <mod/io/serial/serial.hpp>
#include <new>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/session.hpp>
#include <platform/security/authorization.hpp>
#include <platform/sys/mutex.hpp>
#include <utility>

#include "abi/callnums/process.h"
#include "abi/ptrace.hpp"
#include "net/wki/remote_compute.hpp"
#include "net/wki/wki.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/debug/ptrace.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/reclaim.hpp"
#include "platform/mm/virt.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/power/power.hpp"
#include "platform/sched/frame_class.hpp"
#include "platform/sched/resource.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/smt/smt.hpp"
#include "platform/sys/context_switch.hpp"
#include "platform/sys/signal.hpp"
#include "platform/sys/usercopy.hpp"
#include "release.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/getpid.hpp"
#include "syscalls_impl/process/getppid.hpp"
#include "syscalls_impl/process/init_control.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "util/hostname.hpp"
#include "util/smallvec.hpp"
#include "vfs/file.hpp"
#include "vfs/vfs.hpp"

// Signal constants (matching Linux ABI from abi-bits/signal.h)
static constexpr int WOS_SIGKILL = 9;
static constexpr int WOS_SIGSTOP = 19;
static constexpr uint64_t WOS_SA_RESTORER = 0x04000000;
static constexpr uint64_t WOS_CLONE_VM = 0x00000100;
static constexpr uint64_t WOS_CLONE_FS = 0x00000200;
static constexpr uint64_t WOS_CLONE_FILES = 0x00000400;
static constexpr uint64_t WOS_CLONE_UNTRACED = 0x00800000;
static constexpr uint64_t WOS_PERSONALITY_QUERY = 0xffffffff;
static constexpr uint64_t WOS_PERSONALITY_QUERY_ALL_BITS = ~0ULL;
static constexpr int WOS_PRIO_PROCESS = 0;
static constexpr int WOS_PRIO_KERNEL_ENCODE_BIAS = 20;
static constexpr int WOS_PR_SET_PDEATHSIG = 1;
static constexpr int WOS_PR_GET_PDEATHSIG = 2;
static constexpr int WOS_PR_GET_DUMPABLE = 3;
static constexpr int WOS_PR_SET_DUMPABLE = 4;
static constexpr int WOS_PR_SET_NAME = 15;
static constexpr int WOS_PR_GET_NAME = 16;
static constexpr int WOS_PR_SET_PTRACER = 0x59616d61;
static constexpr int WOS_PR_SET_VMA = 0x53564d41;
static constexpr int WOS_PR_SET_VMA_ANON_NAME = 0;
static constexpr int WOS_ARCH_SET_GS = 0x1001;
static constexpr int WOS_ARCH_SET_FS = 0x1002;
static constexpr int WOS_ARCH_GET_FS = 0x1003;
static constexpr int WOS_ARCH_GET_GS = 0x1004;
static constexpr uint32_t WOS_SS_ONSTACK = 1;
static constexpr uint32_t WOS_SS_DISABLE = 2;
static constexpr size_t WOS_MINSIGSTKSZ = 2048;

namespace ker::syscall::process {

// Owns lookup/snapshot references through ordinary return and fatal syscall
// cleanup. A group keeps the unvisited cohort alive across each remote wait.
class PendingSignalTarget {
   public:
    explicit PendingSignalTarget(ker::mod::sched::task::Task* registration_owner = ker::mod::sched::get_current_task())
        : owner(registration_owner) {
        if (owner != nullptr) {
            next = owner->pending_signal_targets;
            owner->pending_signal_targets = this;
        }
    }
    explicit PendingSignalTarget(ker::mod::sched::task::Task& target,
                                 ker::mod::sched::task::Task* registration_owner = ker::mod::sched::get_current_task())
        : PendingSignalTarget(registration_owner) {
        targets[0] = &target;
        count = 1;
    }
    PendingSignalTarget(const PendingSignalTarget&) = delete;
    auto operator=(const PendingSignalTarget&) -> PendingSignalTarget& = delete;
    ~PendingSignalTarget() { cancel(); }
    void set_cleanup(LocalSignalCleanup cleanup, void* context) {
        callback_cleanup = cleanup;
        callback_context = context;
    }
    auto capture(ker::mod::sched::ActiveTaskPredicate predicate, void* context) -> int {
        return capture_with(predicate, context, ker::mod::sched::snapshot_active_task_lifetime_refs_if);
    }
    void retain_if(ker::mod::sched::ActiveTaskPredicate predicate, void* context) {
        size_t i = 0;
        while (i < count) {
            auto* target = targets[cursor + i];
            // The predicate may wait for publication. Keep every remaining
            // reference in a dense registered array at each such wait.
            if (predicate(target, context))
                ++i;
            else {
                --count;
                targets[cursor + i] = targets[cursor + count];
                targets[cursor + count] = nullptr;
                target->release();
            }
        }
    }
    auto view() const -> std::span<ker::mod::sched::task::Task* const> { return {targets + cursor, count}; }
    // Transfer one reference immediately into signal_as's nested owner. No
    // blocking or allocation may separate this transfer from its adoption.
    auto take() -> ker::mod::sched::task::Task* {
        if (count == 0) return nullptr;
        auto* target = targets[cursor];
        targets[cursor++] = nullptr;
        --count;
        return target;
    }
    auto admit_local(ker::mod::sched::task::Task& target) -> ker::mod::sched::task::Task::SignalAdmissionIdentity {
        auto identity = target.begin_local_signal_admission();
        if (identity.local_lease) local_admission = &target;
        return identity;
    }
    void cancel() {
        if (owner != nullptr) {
            owner->pending_signal_targets = next;
            owner = nullptr;
        }
        auto cleanup = std::exchange(callback_cleanup, nullptr);
        auto* context = std::exchange(callback_context, nullptr);
        if (cleanup != nullptr) cleanup(context);
        // Fatal syscall cleanup owns this registration too. A transfer must
        // not wait forever for a lease left on an abandoned caller stack.
        if (local_admission != nullptr) {
            local_admission->end_local_signal_admission();
            local_admission = nullptr;
        }
        while (auto* target = take()) target->release();
        if (targets != inline_targets.data()) delete[] targets;
        targets = inline_targets.data();
        capacity = inline_targets.size();
        cursor = 0;
    }
#ifdef WOS_SELFTEST
    static auto selftest() -> bool;
#endif

   private:
    using Task = ker::mod::sched::task::Task;
    using Snapshot = size_t (*)(ker::mod::sched::ActiveTaskPredicate, void*, Task**, size_t);
    auto capture_with(ker::mod::sched::ActiveTaskPredicate predicate, void* context, Snapshot snapshot, bool fail_allocation = false)
        -> int {
        if (count != 0) return -EINVAL;
        // Concurrent forks may grow the registry between the size query and
        // retry. Bound retries; every failure occurs before any signal effect.
        constexpr unsigned ATTEMPTS = 4;
        for (unsigned attempt = 0; attempt < ATTEMPTS; ++attempt) {
            size_t const REQUIRED = snapshot(predicate, context, targets, capacity);
            if (REQUIRED <= capacity) {
                count = REQUIRED;
                cursor = 0;
                return 0;
            }
            if (attempt + 1 == ATTEMPTS) break;
            auto** larger = fail_allocation ? nullptr : new (std::nothrow) Task*[REQUIRED];
            if (larger == nullptr) return -ENOMEM;
            auto** previous = targets;
            targets = larger;
            capacity = REQUIRED;
            if (previous != inline_targets.data()) delete[] previous;
        }
        return -EAGAIN;
    }
    ker::mod::sched::task::Task* owner = nullptr;
    PendingSignalTarget* next = nullptr;
    Task* local_admission = nullptr;
    LocalSignalCleanup callback_cleanup = nullptr;
    void* callback_context = nullptr;
    std::array<Task*, 32> inline_targets{};
    Task** targets = inline_targets.data();
    size_t capacity = inline_targets.size();
    size_t count = 0;
    size_t cursor = 0;
};

void cleanup_signal_targets(ker::mod::sched::task::Task& owner) {
    while (owner.pending_signal_targets != nullptr) owner.pending_signal_targets->cancel();
}

#ifdef WOS_SELFTEST
auto PendingSignalTarget::selftest() -> bool {
    // Boot KTEST runs after setup_queues, before a current task exists.
    // Exercise the same registration/cleanup path with an explicit owner.
    Task caller{};
    auto* current = &caller;
    Task target{};
    struct Fixture {
        Task* target;
        size_t required;
        unsigned calls = 0;
        bool grow = false;
    } fixture{&target, 2};
    auto snapshot = [](ker::mod::sched::ActiveTaskPredicate, void* context, Task** out, size_t capacity) -> size_t {
        auto& test = *static_cast<Fixture*>(context);
        ++test.calls;
        size_t const REQUIRED = test.grow ? capacity + 1 : test.required;
        if (REQUIRED > capacity) return REQUIRED;
        for (size_t i = 0; i < REQUIRED; ++i) {
            if (!test.target->try_acquire_lifetime_ref()) return i;
            out[i] = test.target;
        }
        return REQUIRED;
    };
    bool ok = true;
    for (size_t required : {size_t{2}, size_t{40}}) {
        fixture = {&target, required};
        {
            PendingSignalTarget cohort(current);
            ok &= cohort.capture_with(nullptr, &fixture, snapshot) == 0 && cohort.count == required &&
                  target.ref_count.load() == required + 1;
            ok &= cohort.capture_with(nullptr, &fixture, snapshot) == -EINVAL;
            auto* selected = cohort.take();
            ok &= selected == &target && cohort.count == required - 1;
            if (selected != nullptr) {
                PendingSignalTarget active(*selected, current);
                ok &= current->pending_signal_targets == &active && active.next == &cohort;
            }
            ok &= current->pending_signal_targets == &cohort && target.ref_count.load() == required;
        }
        ok &= current->pending_signal_targets == nullptr && target.ref_count.load() == 1;
    }
    fixture = {&target, 40};
    {
        PendingSignalTarget cohort(current);
        ok &= cohort.capture_with(nullptr, &fixture, snapshot) == 0;
        struct Selection {
            PendingSignalTarget* cohort;
            unsigned visited = 0;
            bool dense = true;
        } selection{&cohort};
        cohort.retain_if(
            [](Task*, void* context) {
                auto& state = *static_cast<Selection*>(context);
                for (auto* retained : state.cohort->view()) state.dense &= retained != nullptr;
                return ++state.visited % 2 == 0;
            },
            &selection);
        ok &= selection.visited == 40 && selection.dense && cohort.view().size() == 20 && target.ref_count.load() == 21;
        cleanup_signal_targets(*current);
        ok &= cohort.view().empty() && target.ref_count.load() == 1 && current->pending_signal_targets == nullptr;
    }
    fixture = {&target, 40};
    {
        PendingSignalTarget cohort(current);
        ok &= cohort.capture_with(nullptr, &fixture, snapshot, true) == -ENOMEM && cohort.count == 0 && fixture.calls == 1;
        ok &= target.ref_count.load() == 1;
    }
    fixture = {&target, 40, 0, true};
    {
        PendingSignalTarget cohort(current);
        ok &= cohort.capture_with(nullptr, &fixture, snapshot) == -EAGAIN && cohort.count == 0 && fixture.calls == 4;
        ok &= cohort.targets != cohort.inline_targets.data() && target.ref_count.load() == 1;
        cohort.cancel();
        ok &= cohort.targets == cohort.inline_targets.data() && cohort.capacity == cohort.inline_targets.size();
    }
    fixture = {&target, 40};
    {
        PendingSignalTarget cohort(current);
        ok &= cohort.capture_with(nullptr, &fixture, snapshot) == 0;
        auto* selected = cohort.take();
        if (selected == nullptr) return false;
        PendingSignalTarget active(*selected, current);
        // Fatal cleanup must release both the in-flight target and all
        // unvisited references, then tolerate normal destructor reentry.
        cleanup_signal_targets(*current);
        ok &= current->pending_signal_targets == nullptr && target.ref_count.load() == 1 && cohort.count == 0 && active.count == 0 &&
              cohort.targets == cohort.inline_targets.data();
    }
    {
        PendingSignalTarget first(current);
        PendingSignalTarget second(current);
        auto const INITIAL = first.admit_local(target);
        auto const ALSO_LOCAL = second.admit_local(target);
        ok &= INITIAL.local_lease && ALSO_LOCAL.local_lease && !target.local_signal_admissions_drained();
        target.publish_accepted_signal_route(0x7B710001, 77);
        PendingSignalTarget after_cutover(current);
        auto const REMOTE = after_cutover.admit_local(target);
        ok &= REMOTE.remote && !REMOTE.local_lease;
        target.signal_generate_pending_mask(uint64_t{1} << 11);
        after_cutover.cancel();
        second.cancel();
        ok &= !target.local_signal_admissions_drained();
        // Exercise actual fatal-owner cleanup, then normal destructors. The
        // remaining local effect drains exactly once before target release.
        cleanup_signal_targets(*current);
        ok &= target.local_signal_admissions_drained() && target.signal_pending_bits() == (uint64_t{1} << 11);
        ok &= current->pending_signal_targets == nullptr && target.ref_count.load() == 1;
    }
    target.signal_pending_store(0);
    struct CallbackStorage {
        unsigned releases = 0;
        uint64_t* values = nullptr;
    } storage;
    auto release_storage = [](void* opaque) noexcept {
        auto& state = *static_cast<CallbackStorage*>(opaque);
        ++state.releases;
        delete[] std::exchange(state.values, nullptr);
    };
    for (bool fatal : {false, true}) {
        storage = {};
        {
            PendingSignalTarget cohort(current);
            cohort.set_cleanup(release_storage, &storage);
            storage.values = new (std::nothrow) uint64_t[40];
            if (storage.values == nullptr) return false;
            fixture = {&target, 40};
            ok &= cohort.capture_with(nullptr, &fixture, snapshot) == 0;
            auto* selected = cohort.take();
            if (selected == nullptr) return false;
            PendingSignalTarget active(*selected, current);
            if (fatal) {
                cleanup_signal_targets(*current);
                ok &= storage.releases == 1 && storage.values == nullptr && current->pending_signal_targets == nullptr;
            }
        }
        ok &= storage.releases == 1 && storage.values == nullptr && target.ref_count.load() == 1;
    }
    return ok && current->pending_signal_targets == nullptr && target.ref_count.load() == 1 && target.signal_pending_bits() == 0;
}

auto process_selftest_signal_snapshot_ownership() -> bool { return PendingSignalTarget::selftest(); }
#endif

namespace {
using fork_log = ker::mod::dbg::logger<"fork">;
using process_log = ker::mod::dbg::logger<"process">;
constexpr uint8_t FORK_ADMISSION_ORDER = 0;

#ifdef WOS_SELFTEST
std::atomic<bool> g_process_selftest_force_fd_clone_insert_failure{false};
#endif

inline auto mutable_hostname_char(ker::mod::sched::task::Task::HostnameBuffer& hostname, size_t index) -> char& {
    // Hostname setters validate len against hostname.size() before indexing.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return hostname[index];
}

inline auto local_wki_hostname() -> const char* { return std::begin(ker::net::wki::g_wki.local_hostname); }

inline auto is_ptrace_launch_stop_signal(int sig) -> bool { return sig == WOS_SIGSTOP; }

inline auto is_live_signal_target(const ker::mod::sched::task::Task& task) -> bool {
    return task.state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE && !task.has_exited;
}

auto process_pid_for_task(const ker::mod::sched::task::Task& task) -> uint64_t { return ker::mod::sched::task::process_pid(task); }

void release_cloned_fd_table_refs(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->fd_table.for_each([](uint64_t /*key*/, void* val) {
        if (val != nullptr) {
            static_cast<ker::vfs::File*>(val)->refcount.fetch_sub(1, std::memory_order_relaxed);
        }
    });
}

auto clone_fd_table_shared_checked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return false;
    }

    ker::mod::sched::resource::Owner fd_owner(parent);
    parent = fd_owner.get();
    if (parent == nullptr) {
        return false;
    }

    bool ok = true;
    uint64_t const IRQF = parent->fd_table_lock.lock_irqsave();
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (!ok || val == nullptr) {
            return;
        }

        auto* file = static_cast<ker::vfs::File*>(val);
        file->refcount.fetch_add(1, std::memory_order_relaxed);
#ifdef WOS_SELFTEST
        if (g_process_selftest_force_fd_clone_insert_failure.load(std::memory_order_relaxed)) {
            file->refcount.fetch_sub(1, std::memory_order_relaxed);
            ok = false;
            return;
        }
#endif
        if (!child->fd_table.insert(key, file)) {
            file->refcount.fetch_sub(1, std::memory_order_relaxed);
            ok = false;
        }
    });
    child->fd_cloexec = parent->fd_cloexec;
    parent->fd_table_lock.unlock_irqrestore(IRQF);

    return ok;
}

auto find_process_leader_safe(uint64_t pid) -> ker::mod::sched::task::Task* {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return nullptr;
    }

    uint64_t const PROCESS_PID = process_pid_for_task(*task);
    if (PROCESS_PID == task->pid) {
        return task;
    }

    task->release();
    return ker::mod::sched::find_task_by_pid_safe(PROCESS_PID);
}

auto wos_proc_getgroups(size_t size, uint64_t list_addr) -> uint64_t {
    constexpr size_t GETGROUPS_SIZE_MAX = 0x7FFFFFFFU;

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (size > GETGROUPS_SIZE_MAX) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto const CREDENTIALS = task->credentials();
    size_t const COUNT = CREDENTIALS.groups().size();
    if (size == 0) {
        return COUNT;
    }
    if (list_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (size < COUNT) {
        return static_cast<uint64_t>(-EINVAL);
    }

    std::array<uint32_t, ker::mod::sched::task::Task::SUPPLEMENTARY_GROUPS_MAX> groups{};
    for (size_t i = 0; i < COUNT; ++i) {
        groups.at(i) = CREDENTIALS.groups()[i];
    }
    size_t const BYTES = COUNT * sizeof(groups.at(0));
    if (!ker::mod::sys::usercopy::ensure_writable(*task, list_addr, BYTES) ||
        !ker::mod::sys::usercopy::copy_to_task(*task, list_addr, groups.data(), BYTES)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    return COUNT;
}

auto wos_proc_setgroups(size_t size, uint64_t list_addr) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (!task->credentials().allows(ker::mod::security::Authority::SET_GID)) {
        return static_cast<uint64_t>(-EPERM);
    }
    if (size > ker::mod::sched::task::Task::SUPPLEMENTARY_GROUPS_MAX) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (size > 0 && list_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    std::array<uint32_t, ker::mod::sched::task::Task::SUPPLEMENTARY_GROUPS_MAX> copied_groups{};
    size_t const BYTES = size * sizeof(copied_groups.at(0));
    if (BYTES != 0 && !ker::mod::sys::usercopy::copy_from_task(*task, list_addr, copied_groups.data(), BYTES)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    return static_cast<uint64_t>(ker::mod::sched::resource::change_credentials(
        *task, {.change = ker::mod::security::Change::SET_GROUPS, .groups = std::span(copied_groups).first(size)}));
}

inline auto clamp_perf_aux(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

struct LocalProcStage {
    uint32_t correlation{};
    uint64_t started_us{};
    uint64_t subject_pid{};
    uint64_t capture_session = ker::mod::perf::PERF_ANY_SESSION;
    bool active{};
};

auto begin_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
    -> LocalProcStage {
    if (task == nullptr) {
        return {};
    }
    uint64_t const SESSION =
        ker::mod::perf::current_wki_capture_session(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op));
    if (SESSION == ker::mod::perf::PERF_ANY_SESSION) {
        return {};
    }
    LocalProcStage const STAGE = {
        .correlation = ker::mod::perf::next_wki_trace_correlation(),
        .started_us = ker::mod::time::get_us(),
        .subject_pid = task->pid,
        .capture_session = SESSION,
        .active = true,
    };
    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), STAGE.subject_pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, 0, 0, STAGE.correlation, 0, aux, callsite, SESSION);
    return STAGE;
}

void record_fork_child(const LocalProcStage& stage, uint64_t child_pid, uint64_t child_cpu, uint64_t callsite) {
    using namespace ker::mod;
    if (!stage.active ||
        stage.capture_session !=
            perf::current_wki_capture_session(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::FORK_CHILD))) {
        return;
    }
    // Keep the legacy independent child POINT while adding an explicit link.
    // Child first scheduling may precede either parent-side publication POINT.
    perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), child_pid, perf::WkiPerfScope::LOCAL_PROC,
                           static_cast<uint8_t>(perf::WkiPerfLocalProcOp::FORK), perf::WkiPerfPhase::POINT, 0, 0,
                           perf::next_wki_trace_correlation(), 0, clamp_perf_aux(child_cpu), callsite, stage.capture_session);
    perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), child_pid, perf::WkiPerfScope::LOCAL_PROC,
                           static_cast<uint8_t>(perf::WkiPerfLocalProcOp::FORK_CHILD), perf::WkiPerfPhase::POINT, 0, 0, stage.correlation,
                           0, clamp_perf_aux(child_cpu), callsite, stage.capture_session);
}

auto end_local_proc_stage(ker::mod::sched::task::Task* /*task*/, ker::mod::perf::WkiPerfLocalProcOp op, LocalProcStage& stage,
                          int32_t status, uint64_t bytes, uint64_t callsite) -> uint32_t {
    if (!std::exchange(stage.active, false)) {
        return 0;
    }
    if (stage.capture_session !=
        ker::mod::perf::current_wki_capture_session(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op))) {
        return 0;
    }
    uint64_t const NOW = ker::mod::time::get_us();
    uint32_t const ELAPSED_US = clamp_perf_aux(NOW >= stage.started_us ? NOW - stage.started_us : 0);
    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), stage.subject_pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::END,
                                     0, 0, stage.correlation, status, ELAPSED_US, callsite, stage.capture_session);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), 0, 0, status, ELAPSED_US, true,
                                       0, bytes, stage.capture_session);
    return ELAPSED_US;
}

inline void log_unmapped_child_resume_state(const ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child,
                                            uint64_t saved_rip, uint64_t saved_rsp, uint64_t saved_flags) {
    if (child == nullptr || child->pagemap == nullptr) {
        return;
    }

    const uint64_t RIP_PHYS = ker::mod::sys::usercopy::mapped_physical_address(*child, child->context.frame.rip);
    const uint64_t RSP_PHYS = ker::mod::sys::usercopy::mapped_physical_address(*child, child->context.frame.rsp);
    const bool RIP_BAD =
        child->context.frame.rip == 0 || child->context.frame.rip >= 0x0000800000000000ULL || RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID;
    const bool RSP_BAD =
        child->context.frame.rsp == 0 || child->context.frame.rsp >= 0x0000800000000000ULL || RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID;
    if (!RIP_BAD && !RSP_BAD) {
        return;
    }

    fork_log::warn(
        "fork child resume anomaly: parent=%lu child=%lu pagemap=%p rip=0x%llx rip_phys=0x%llx rsp=0x%llx rsp_phys=0x%llx gs_rip=0x%llx "
        "gs_rsp=0x%llx gs_rflags=0x%llx parent_pagemap=%p",
        parent != nullptr ? parent->pid : 0, child->pid, static_cast<void*>(child->pagemap),
        static_cast<unsigned long long>(child->context.frame.rip),
        static_cast<unsigned long long>((RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RIP_PHYS),
        static_cast<unsigned long long>(child->context.frame.rsp),
        static_cast<unsigned long long>((RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RSP_PHYS),
        static_cast<unsigned long long>(saved_rip), static_cast<unsigned long long>(saved_rsp),
        static_cast<unsigned long long>(saved_flags), parent != nullptr ? static_cast<void*>(parent->pagemap) : nullptr);
}

auto alloc_fork_kernel_stack_with_reclaim() -> uint64_t {
    return reinterpret_cast<uint64_t>(ker::mod::mm::phys::kernel_stack_alloc("fork_kstack"));
}

auto reclaim_fork_stack_from_syscall(ker::mod::sched::task::Task* parent, bool wait_for_retirement = false) -> bool {
    using namespace ker::mod;
    // FORK enters with IF cleared by IA32_FMASK. Keep the generic allocator
    // predicate strict; only this syscall boundary can permit blocking GC.
    if (!sched::has_run_queues() || !sched::can_query_current_task() || parent == nullptr || parent != sched::get_current_task() ||
        parent->type != sched::task::TaskType::PROCESS || parent->syscall_account_start_us == 0 || sched::preempt_count() != 0 ||
        io::serial::is_panic_mode() || parent->scheduler_transition_active.load(std::memory_order_acquire) ||
        parent->deferred_task_switch || parent->wants_block || parent->is_voluntary_blocked()) {
        return false;
    }

    // Initial pool allocation has released its lock, and no child or VM
    // publication lock exists yet. Reuse GC's exclusive bounded pressure pass
    // with every epoch/current/refcount guard intact, then restore caller IF.
    bool const INTERRUPTS_WERE_ENABLED = sched::interrupts_enabled();
    asm volatile("sti" ::: "memory");
    if (wait_for_retirement) {
        // waitpid can consume the exit event before the child finishes its
        // address-space cleanup and switches off its stack. Give that executor
        // a safe scheduling opportunity before starting another owned GC pass.
        sched::request_gc_memory_pressure();
        sched::kern_yield_impl(WOS_PERF_CALLSITE());
    }
    sched::GcPressureDiagnostics diagnostics{};
    uint32_t const RECLAIMED = sched::reclaim_memory_pressure(&diagnostics);
    // NOLINTNEXTLINE(bugprone-branch-clone)
    if (INTERRUPTS_WERE_ENABLED) {
        asm volatile("sti" ::: "memory");
    } else {
        asm volatile("cli" ::: "memory");
    }
    auto const POOL = mm::phys::get_kernel_stack_pool_stats();
    process_log::info(
        "fork_stack_pressure_v1 parent=%lu entry_if=%u reclaimed=%u ran=%u scanned=%lu epoch=%lu young=%lu current=%lu ref=%lu "
        "zombie_busy=%lu retained=%lu invalid=%lu time_limit=%u deferred=%u stack_free=%lu stack_cap=%lu",
        parent->pid, INTERRUPTS_WERE_ENABLED ? 1U : 0U, RECLAIMED, diagnostics.ran ? 1U : 0U, diagnostics.scanned, diagnostics.epoch_skips,
        diagnostics.epoch_still_young, diagnostics.current_skips, diagnostics.refcount_skips, diagnostics.zombie_busy_skips,
        diagnostics.retained_zombie_skips, diagnostics.invalid_tasks, diagnostics.time_budget_exhausted ? 1U : 0U,
        diagnostics.deferred_pending ? 1U : 0U, POOL.free_slots, POOL.capacity);
    return true;
}

auto alloc_fork_kernel_stack_from_syscall(ker::mod::sched::task::Task* parent) -> uint64_t {
    auto stack = alloc_fork_kernel_stack_with_reclaim();
    if (stack != 0) {
        return stack;
    }

    uint64_t const START_US = ker::mod::time::get_us();
    constexpr uint64_t RETIREMENT_BUDGET_US = 50000;
    for (uint32_t attempt = 0; attempt < ker::mod::mm::reclaim::DIRECT_RECLAIM_MAX_PASSES; ++attempt) {
        uint64_t const NOW_US = ker::mod::time::get_us();
        if (attempt != 0 && (NOW_US < START_US || NOW_US - START_US >= RETIREMENT_BUDGET_US)) {
            break;
        }
        if (!reclaim_fork_stack_from_syscall(parent, attempt != 0)) {
            break;
        }
        // Retry once per bounded pass, also accepting a concurrent free when
        // another GC owner was busy. Persistent or reader-pinned shortage
        // still returns NULL; never bypass reclamation's lifetime guards.
        stack = alloc_fork_kernel_stack_with_reclaim();
        if (stack != 0) {
            break;
        }
    }
    return stack;
}

auto throttle_fork_for_reclaim_pressure(uint64_t callsite) -> bool {
    if (!ker::mod::mm::phys::can_wait_for_reclaim()) {
        return true;
    }

    constexpr uint64_t STACK_PAGES = ker::mod::mm::KERNEL_STACK_SIZE / ker::mod::mm::paging::PAGE_SIZE;
    for (uint32_t attempt = 0; attempt < ker::mod::mm::reclaim::DIRECT_RECLAIM_MAX_PASSES; ++attempt) {
        // Admission protects global page headroom. Guarded stacks come only
        // from the reserved pool; physical headroom does not imply a free slot.
        auto const PRESSURE = ker::mod::mm::reclaim::pressure_for_order(FORK_ADMISSION_ORDER);
        if (PRESSURE == ker::mod::mm::reclaim::PressureLevel::NONE) {
            return true;
        }

        auto const RESULT = ker::mod::mm::reclaim::reclaim_for_allocation(FORK_ADMISSION_ORDER, STACK_PAGES);
        if (!RESULT.made_progress) {
            ker::mod::mm::reclaim::request_background(FORK_ADMISSION_ORDER, STACK_PAGES);
            ker::mod::sched::kern_yield_impl(callsite);
        }
    }
    return ker::mod::mm::reclaim::pressure_for_order(FORK_ADMISSION_ORDER) == ker::mod::mm::reclaim::PressureLevel::NONE;
}

void snapshot_fpu_state_for_fork(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) {
    ker::mod::sys::context_switch::save_fpu_state(parent);
    std::memcpy(child->fx_state.aligned(), parent->fx_state.aligned(), ker::mod::sched::task::FxState::XSAVE_AREA_SIZE);
    child->fx_state.saved = parent->fx_state.saved;
    child->fx_state.live_saved = false;
    child->fx_state.initialized = parent->fx_state.initialized;
}

auto fork_child_return_regs(ker::mod::cpu::GPRegs entry_regs) -> ker::mod::cpu::GPRegs {
    entry_regs.rax = 0;
    return entry_regs;
}

void note_fork_failure(ker::mod::sched::task::Task* parent, const char* stage, int32_t status) {
    using namespace ker::mod;
    // Observe after the existing failure cleanup. Pool fields are coherent,
    // but queue/physical snapshots are later, independent observations; they
    // cannot prove the allocator's state at the moment it rejected the fork.
    auto const STACK_POOL = mm::phys::get_kernel_stack_pool_stats();
    uint64_t const PHYS_FREE_PAGES = mm::phys::get_free_mem_pages();
    uint64_t const CPU_COUNT = smt::get_core_count();
    uint64_t dead = 0;
    uint64_t runnable = 0;
    uint64_t waiting = 0;
    for (uint64_t cpu_no = 0; cpu_no < CPU_COUNT; ++cpu_no) {
        auto const QUEUES = sched::get_run_queue_stats(cpu_no);
        dead += QUEUES.expired_task_count;
        runnable += QUEUES.active_task_count;
        waiting += QUEUES.wait_queue_count;
    }
    uint64_t const GC_DEFERRED = CPU_COUNT != 0 ? sched::get_scheduler_trace_stats(0).gc_deferred_depth : 0;
    // No pool or run-queue lock remains held when the journal wakes readers.
    process_log::warn(
        "fork_failure_v1 stage=%s status=%d parent=%lu cpu=%u cpus=%lu stack_cap=%lu stack_free=%lu stack_high=%lu "
        "phys_free_pages=%lu dead=%lu runnable=%lu waiting=%lu gc_deferred=%lu",
        stage, status, parent->pid, cpu::current_cpu(), CPU_COUNT, STACK_POOL.capacity, STACK_POOL.free_slots, STACK_POOL.high_watermark,
        PHYS_FREE_PAGES, dead, runnable, waiting, GC_DEFERRED);
}

auto wos_proc_fork(ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }

    LocalProcStage FORK_STAGE = begin_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, 0, WOS_PERF_CALLSITE());
    auto finish_fork = [&](int64_t result, uint64_t bytes = 0, const char* failure_stage = nullptr) -> uint64_t {
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
        end_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, FORK_STAGE, STATUS, bytes, WOS_PERF_CALLSITE());
        if (result == -ENOMEM && failure_stage != nullptr) {
            note_fork_failure(parent, failure_stage, STATUS);
        }
        return static_cast<uint64_t>(result);
    };

    auto const PARENT_CREDENTIALS = parent->credentials();
    sched::resource::ProcessReservation process_reservation(parent, PARENT_CREDENTIALS.user().real);
    if (process_reservation.result() < 0) {
        return finish_fork(process_reservation.result());
    }
    if (!throttle_fork_for_reclaim_pressure(WOS_PERF_CALLSITE())) {
        return finish_fork(-ENOMEM, 0, "pressure-admission");
    }

    // Snapshot the syscall-entry register block after any pressure-reclaim
    // yield, but before fork allocation/COW work can be timer-preempted. A
    // kernel-mode timer preemption may replace parent->context.regs with the
    // interrupted kernel context; it must never become the child's user
    // return state.
    ker::mod::cpu::GPRegs const FORK_ENTRY_REGS = gpr;
    parent->context.regs = FORK_ENTRY_REGS;

    // --- Allocate child kernel stack ---
    auto const KERNEL_STACK_BASE = alloc_fork_kernel_stack_from_syscall(parent);
    if (KERNEL_STACK_BASE == 0) {
        return finish_fork(-ENOMEM, 0, "kernel-stack");
    }
    uint64_t const KERNEL_RSP = KERNEL_STACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    // --- Allocate child Task without the ELF-loading constructor ---
    auto* child = new sched::task::Task{};
    if (child == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        return finish_fork(-ENOMEM, 0, "task");
    }
    if (!sched::resource::inherit(*child, *parent)) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ESRCH);
    }
    snapshot_fpu_state_for_fork(parent, child);

    // --- Initialize child task fields ---
    // Copy name
    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        child->name = name_copy;
    }

    child->pid = sched::task::get_next_pid();
    child->type = sched::task::TaskType::PROCESS;
    child->cpu = cpu::current_cpu();
    child->has_run = false;
    child->exit_status = 0;
    child->exit_in_progress = false;
    child->has_exited = false;
    child->exit_notify_ready.store(false, std::memory_order_relaxed);
    child->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    child->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    child->deferred_task_switch = false;
    child->yield_switch = false;
    child->set_voluntary_blocked(false);
    child->kthread_entry = nullptr;
    child->elf_buffer = nullptr;
    child->elf_buffer_size = 0;
    child->is_elf_buffer_shared = false;
    child->jobctl_stopped.store(false, std::memory_order_relaxed);
    child->jobctl_stop_pending.store(false, std::memory_order_relaxed);
    child->jobctl_stop_publish_deferred.store(false, std::memory_order_relaxed);
    child->jobctl_stop_signal = 0;

    // EEVDF scheduling fields - start fresh
    child->vruntime = 0;
    child->vdeadline = 0;
    child->sched_weight = parent->sched_weight;
    child->sched_nice = parent->sched_nice;
    child->slice_ns = parent->slice_ns;
    child->slice_used_ns = 0;
    child->heap_index = -1;
    child->sched_queue = sched::task::Task::sched_queue::NONE;
    child->sched_next = nullptr;

    // Copy fixed per-process path storage.
    {
        auto* filesystem_parent = parent->filesystem_owner();
        uint64_t const PATH_IRQF = filesystem_parent->path_metadata_lock.lock_irqsave();
        child->cwd = filesystem_parent->cwd;
        child->cwd_len = filesystem_parent->cwd_len;
        child->root = filesystem_parent->root;
        child->root_len = filesystem_parent->root_len;
        child->root_anchor = filesystem_parent->root_anchor;
        child->cwd_anchor = filesystem_parent->cwd_anchor;
        filesystem_parent->path_metadata_lock.unlock_irqrestore(PATH_IRQF);
    }
    child->exe_path = parent->exe_path;

    // Copy WKI spawn configuration. NOINHERIT applies when a process creates
    // a child: the parent keeps its policy, but the child starts automatic.
    if ((parent->wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_NOINHERIT) != 0) {
        child->wki_target_hostname.front() = '\0';
        child->wki_target_flags = 0;
    } else {
        child->wki_target_hostname = parent->wki_target_hostname;
        child->wki_target_flags = parent->wki_target_flags;
    }
    child->wki_submitter_hostname = parent->wki_execution_identity().submitter_hostname;
    child->wki_remote_pid =
        (child->wki_submitter_hostname.front() != '\0' && std::strcmp(child->wki_submitter_hostname.data(), local_wki_hostname()) != 0)
            ? child->pid
            : 0;
    if (!child->wki_vfs_rules.clone_from(parent->wki_vfs_rules)) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "vfs-rules");
    }
    child->wki_skip_legacy_placement = false;

    // Copy POSIX credentials
    (void)child->exchange_creation_mask(parent->creation_mask());
    child->personality = parent->personality;
    if (sched::resource::initialize_credentials(*child, PARENT_CREDENTIALS) != 0) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "credentials");
    }

    // Copy session, process group, and controlling terminal
    auto child_job = parent->job_identity();
    child_job.exec_completed = false;
    child->initialize_job_identity(child_job);

    // Copy signal dispositions from parent (fork inherits signal handlers)
    child->signal_pending_store(0, std::memory_order_relaxed);  // Pending signals are NOT inherited
    child->signal_mask_store(parent->signal_mask_bits(), std::memory_order_relaxed);
    child->sigsuspend_saved_mask = 0;
    child->sigaltstack_sp = parent->sigaltstack_sp;
    child->sigaltstack_size = parent->sigaltstack_size;
    child->sigaltstack_flags = parent->sigaltstack_flags;
    child->parent_death_signal = 0;
    child->sigsuspend_active = false;
    child->in_signal_handler = false;
    child->do_sigreturn = false;
    child->initialize_signal_handlers(parent->signal_handlers_snapshot());

    // --- Create child pagemap with COW ---
    child->pagemap = mm::virt::create_pagemap();
    if (child->pagemap == nullptr) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "pagemap");
    }

    // Copy kernel mappings
    mm::virt::copy_kernel_mappings(child);

    // Snapshot every address-space surface under one publication boundary.
    // Cleanup is deliberately deferred until after the sleepable guard drops:
    // file-range release may perform VFS writeback.
    bool fork_vm_snapshot_ok = false;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        child->entry = parent->entry;
        child->program_header_addr = parent->program_header_addr;
        child->elf_header_addr = parent->elf_header_addr;
        child->image_load_base = parent->image_load_base;
        child->image_vaddr_start = parent->image_vaddr_start;
        child->image_vaddr_end = parent->image_vaddr_end;
        child->program_header_count = parent->program_header_count;
        child->program_header_ent_size = parent->program_header_ent_size;
        child->interp_base = parent->interp_base;
        child->interp_vaddr_start = parent->interp_vaddr_start;
        child->interp_vaddr_end = parent->interp_vaddr_end;
        child->at_random_addr = parent->at_random_addr;
        child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
        fork_vm_snapshot_ok = sched::task::clone_lazy_vmem_ranges(*child, *parent) &&
                              mm::virt::deep_copy_user_pagemap_cow(parent->pagemap, child->pagemap) &&
                              ker::syscall::shm::shm_clone_for_fork_locked(parent, child) &&
                              ker::syscall::vmem::clone_file_mmap_ranges_for_pagemap_locked(parent->pagemap, child->pagemap);
    }
    if (!fork_vm_snapshot_ok) {
        ker::syscall::shm::shm_cleanup_for_task(child);
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
        sched::task::release_lazy_vmem_ranges(*child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-vm-snapshot-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "vm-snapshot");
    }

    (void)sched::resource::observe_address_space(*child);

    // --- Clone thread metadata ---
    // The child shares the same user-space layout (stack, TLS) via COW.
    // Allocate a Thread struct for the child that mirrors the parent's.
    if (parent->thread != nullptr) {
        auto* child_thread = new sched::threading::Thread();
        if (child_thread == nullptr) {
            ker::syscall::shm::shm_cleanup_for_task(child);
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
            sched::task::release_lazy_vmem_ranges(*child);
            mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-thread-alloc-fail");
            mm::phys::page_free(child->pagemap);
            delete[] child->name;
            mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
            delete child;
            return finish_fork(-ENOMEM, 0, "thread");
        }
        // Copy all fields - virtual addresses are the same (same address space layout via COW)
        *child_thread = *parent->thread;
        // The physical pointers are now shared via COW so the child shouldn't free them
        // on thread destroy - we zero them to prevent double-free
        child_thread->tls_phys_ptr = 0;
        child_thread->stack_phys_ptr = 0;
        child->thread = child_thread;
        sched::threading::record_stack_usage(*child);
        ker::mod::sys::signal::sync_task_signal_mask_cache(child);
    } else {
        child->thread = nullptr;
    }

    // --- Set up child context ---
    // Child's kernel stack and per-CPU scratch area
    child->context.syscall_kernel_stack = KERNEL_RSP;

    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = KERNEL_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    child->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

    // Resume from the immutable syscall-entry state, not the parent's mutable
    // scheduler context, which may now describe an in-kernel preemption.
    child->context.regs = fork_child_return_regs(FORK_ENTRY_REGS);
    child->context.int_no = 0;
    child->context.error_code = 0;

    // Build the child's interrupt frame from the PerCpu scratch area.
    // parent->context.frame is STALE - it was saved during the last timer
    // preemption / context switch, NOT during this syscall.  The real
    // syscall return state lives in the scratch area populated by the
    // syscall entry path in syscall.asm:
    //   gs:0x28 = RCX at entry = user return RIP
    //   gs:0x30 = R11 at entry = user RFLAGS
    //   gs:0x08 = user RSP at entry
    // NOLINTBEGIN(misc-const-correctness)
    uint64_t return_rip = 0;
    uint64_t return_flags = 0;
    uint64_t user_rsp = 0;
    // NOLINTEND(misc-const-correctness)
    {
        asm volatile("movq %%gs:0x28, %0" : "=r"(return_rip));
        asm volatile("movq %%gs:0x30, %0" : "=r"(return_flags));
        asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));

        child->context.frame.rip = return_rip;
        child->context.frame.rsp = user_rsp;
        child->context.frame.flags = return_flags;
        child->context.frame.cs = desc::gdt::GDT_USER_CS;
        child->context.frame.ss = desc::gdt::GDT_USER_DS;
        child->context.frame.int_num = 0;
        child->context.frame.err_code = 0;
    }
    ker::mod::sys::context_switch::record_saved_frame_class(child, child->context.frame,
                                                            ker::mod::sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);

    log_unmapped_child_resume_state(parent, child, return_rip, user_rsp, return_flags);

    // --- Clone file descriptors ---
    if (!clone_fd_table_shared_checked(parent, child)) {
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
        sched::task::release_lazy_vmem_ranges(*child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-fd-clone-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "fd-table");
    }

    // Establish authoritative process ownership before either scheduler or
    // legacy WKI placement can make the child visible.
    if (!child_events::begin_publication(*parent, *child)) {
        child_events::abort_publication(*child);
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
        sched::task::release_lazy_vmem_ranges(*child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-child-publication-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "child-publication");
    }

    // --- Enqueue child ---
    if (!process_reservation.attach(*child) || !sched::post_task_balanced(child)) {
        child_events::abort_publication(*child);
        // Undo FD refcount increments
        release_cloned_fd_table_refs(child);

        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
        sched::task::release_lazy_vmem_ranges(*child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-post-task-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM, 0, "scheduler-publication");
    }
    if (!child_events::commit_publication(*child)) [[unlikely]] {
        // Scheduler publication is irreversible at this call site. Preserve
        // the successful fork result so userspace cannot retry and duplicate
        // a child that is already runnable (or already exited/reaped).
        process_log::error("fork: lifecycle commit completed concurrently for published child PID %x", child->pid);
    }
    record_fork_child(FORK_STAGE, child->pid, child->cpu, WOS_PERF_CALLSITE());

    // Return child PID to parent
    return finish_fork(static_cast<int64_t>(child->pid), child->cpu);
}

// --- Signal infrastructure ---

// Userspace sigaction struct layout (must match abi-bits/signal.h)
struct KernelSigaction {
    uint64_t handler;   // sa_handler / sa_sigaction (union, 8 bytes)
    uint64_t flags;     // sa_flags (unsigned long)
    uint64_t restorer;  // sa_restorer (function pointer)
    // sigset_t sa_mask is 128 bytes (unsigned long[16]) but we only use first word
    uint64_t mask;  // First word of sa_mask
    // Remaining 120 bytes of sa_mask are unused padding
};

struct KernelStackT {
    void* ss_sp;
    int ss_flags;
    size_t ss_size;
};

struct KernelUtsname {
    char sysname[65];
    char nodename[65];
    char release[65];
    char version[65];
    char machine[65];
    char domainname[65];
};

struct CloneVmArgs {
    uint64_t fn;
    uint64_t child_stack;
    uint64_t flags;
    uint64_t arg;
    uint64_t parent_tidptr;
    uint64_t newtls;
    uint64_t child_tidptr;
};

void copy_cstr_field(char* dst, size_t cap, const char* src) {
    if (dst == nullptr || cap == 0) {
        return;
    }
    if (src == nullptr) {
        dst[0] = '\0';
        return;
    }
    size_t len = 0;
    while (len + 1 < cap && src[len] != '\0') {
        dst[len] = src[len];
        ++len;
    }
    dst[len] = '\0';
}

auto wos_proc_uname(KernelUtsname* buf) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (buf == nullptr) {
        return static_cast<uint64_t>(-EFAULT);
    }

    KernelUtsname uts{};
    copy_cstr_field(uts.sysname, sizeof(uts.sysname), ker::release::NAME);
    auto const HOSTNAME = ker::util::hostname::snapshot();
    copy_cstr_field(uts.nodename, sizeof(uts.nodename), HOSTNAME.data());
    copy_cstr_field(uts.release, sizeof(uts.release), ker::release::VERSION);
    copy_cstr_field(uts.version, sizeof(uts.version), ker::release::COMPILER);
    copy_cstr_field(uts.machine, sizeof(uts.machine), "x86_64");
    copy_cstr_field(uts.domainname, sizeof(uts.domainname), "localdomain");
    return ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(buf), uts) ? 0 : static_cast<uint64_t>(-EFAULT);
}

auto wos_proc_sigaction(int signum, uint64_t act_ptr, uint64_t oldact_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    // Signal numbers are 1-based, array is 0-based
    if (signum < 1 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS)) {
        return static_cast<uint64_t>(-EINVAL);
    }

    // SIGKILL and SIGSTOP cannot have their handlers changed
    if (signum == WOS_SIGKILL || signum == WOS_SIGSTOP) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto const IDX = static_cast<unsigned>(signum - 1);

    KernelSigaction act{};
    if (act_ptr != 0 && !ker::mod::sys::usercopy::copy_value_from_task(*task, act_ptr, act)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    // Serialize the old-action result and publication across all workers.
    // Usercopy can fault or sleep, so it must never hold the IRQ-safe store lock.
    sys::MutexGuard action_guard(task->signal_action_mutex());
    auto handler = task->signal_handler_snapshot(IDX);

    // Return old handler if requested
    if (oldact_ptr != 0) {
        KernelSigaction old{};
        old.handler = handler.handler;
        old.flags = handler.flags;
        old.restorer = handler.restorer;
        old.mask = handler.mask;
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, oldact_ptr, old)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    // Set new handler if provided
    if (act_ptr != 0) {
        handler.handler = act.handler;
        handler.flags = act.flags;
        handler.mask = act.mask;
        // Store restorer if SA_RESTORER flag is set
        if ((act.flags & WOS_SA_RESTORER) != 0U) {
            handler.restorer = act.restorer;
        }
        task->publish_signal_handler(IDX, handler);
        uint64_t const SIGNAL_BIT = uint64_t{1} << IDX;
        if (handler.handler == 1 && (sched::task::Task::TIMER_SIGNALS & SIGNAL_BIT) != 0) {
            // An expiry may have sampled the old disposition before publication.
            // Serialize its pending store with this discard. The action mutex
            // also excludes handler restoration until discard has finished.
            // Keep claim reservations intact: commit observes the cleared bit
            // and abandons delivery, then releases its own reservation.
            auto* owner = task->credential_owner();
            uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
            owner->timer_sig_pending.fetch_and(~SIGNAL_BIT, std::memory_order_acq_rel);
            owner->resource_lock.unlock_irqrestore(FLAGS);
        }
    }

    return 0;
}

auto wos_proc_sigaltstack(const KernelStackT* ss, KernelStackT* old_ss, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    (void)gpr;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    KernelStackT new_ss{};
    if (ss != nullptr && !ker::mod::sys::usercopy::copy_value_from_task(*task, reinterpret_cast<uint64_t>(ss), new_ss)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    uint64_t user_rsp = 0;
    asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));
    bool const ON_STACK = (task->sigaltstack_flags & WOS_SS_DISABLE) == 0 && ker::mod::sys::signal::is_on_alt_stack(*task, user_rsp);

    if (old_ss != nullptr) {
        KernelStackT old{};
        old.ss_sp = reinterpret_cast<void*>(task->sigaltstack_sp);
        old.ss_size = task->sigaltstack_size;
        old.ss_flags = ON_STACK ? WOS_SS_ONSTACK : static_cast<int>(task->sigaltstack_flags);
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(old_ss), old)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    if (ss == nullptr) {
        return 0;
    }
    if (ON_STACK) {
        return static_cast<uint64_t>(-EPERM);
    }
    auto const FLAGS = static_cast<uint32_t>(new_ss.ss_flags);
    if ((FLAGS & ~WOS_SS_DISABLE) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if ((FLAGS & WOS_SS_DISABLE) == 0 && new_ss.ss_size < WOS_MINSIGSTKSZ) {
        return static_cast<uint64_t>(-ENOMEM);
    }

    if ((FLAGS & WOS_SS_DISABLE) != 0) {
        task->sigaltstack_sp = 0;
        task->sigaltstack_size = 0;
        task->sigaltstack_flags = WOS_SS_DISABLE;
        return 0;
    }

    auto const STACK_ADDR = reinterpret_cast<uint64_t>(new_ss.ss_sp);
    if (STACK_ADDR == 0 || !ker::mod::sys::usercopy::range_valid(STACK_ADDR, new_ss.ss_size)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    uint64_t const ALT_STACK_TOP = STACK_ADDR + new_ss.ss_size;
    uint64_t const FRAME_ADDR = ker::mod::sys::signal::signal_frame_address(ALT_STACK_TOP);
    if (!ker::mod::sys::usercopy::ensure_writable(*task, FRAME_ADDR, sizeof(ker::mod::sys::signal::SignalFrame))) {
        return static_cast<uint64_t>(-EFAULT);
    }

    task->sigaltstack_sp = STACK_ADDR;
    task->sigaltstack_size = new_ss.ss_size;
    task->sigaltstack_flags = 0;
    return 0;
}

auto wos_proc_sigprocmask(int how, uint64_t set_ptr, uint64_t oldset_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    uint64_t set = 0;
    if (set_ptr != 0 && !ker::mod::sys::usercopy::copy_value_from_task(*task, set_ptr, set)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    // Return old mask if requested (sigset_t first word)
    if (oldset_ptr != 0) {
        uint64_t const OLD_MASK = task->signal_mask_bits();
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, oldset_ptr, OLD_MASK)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    // Apply new mask if provided
    if (set_ptr != 0) {
        // SIGKILL and SIGSTOP can never be blocked
        uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
        set &= ~UNBLOCKABLE;

        uint64_t new_mask = task->signal_mask_bits();
        switch (how) {
            case 0:  // SIG_BLOCK
                new_mask |= set;
                break;
            case 1:  // SIG_UNBLOCK
                new_mask &= ~set;
                break;
            case 2:  // SIG_SETMASK
                new_mask = set;
                break;
            default:
                return static_cast<uint64_t>(-EINVAL);
        }
        if (new_mask != task->signal_mask_bits()) {
            task->signal_mask_store(new_mask);
            ker::mod::sys::signal::sync_task_signal_mask_cache(task);
        }
    }

    return 0;
}

auto wos_proc_sigpending(uint64_t set_ptr) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (set_ptr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    std::array<uint64_t, 16> set{};
    set.at(0) = task->signal_delivery_pending_bits() & task->signal_mask_bits();
    bool const COPIED = ker::mod::sys::usercopy::copy_to_task(*task, set_ptr, set.data(), set.size() * sizeof(set.at(0)));
    return COPIED ? 0 : static_cast<uint64_t>(-EFAULT);
}

auto wos_proc_personality(uint64_t persona) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    uint64_t const OLD_PERSONALITY = task->personality;
    if (persona != WOS_PERSONALITY_QUERY && persona != WOS_PERSONALITY_QUERY_ALL_BITS) {
        task->personality = persona;
    }
    return OLD_PERSONALITY;
}

auto wos_proc_sigsuspend(uint64_t set_ptr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (set_ptr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    uint64_t set = 0;
    if (!ker::mod::sys::usercopy::copy_value_from_task(*task, set_ptr, set)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
    set &= ~UNBLOCKABLE;

    task->context.regs = gpr;
    task->sigsuspend_saved_mask = task->signal_mask_bits();
    task->sigsuspend_active = true;
    task->signal_mask_store(set);
    ker::mod::sys::signal::sync_task_signal_mask_cache(task);

    if (task->signal_deliverable_bits() != 0) {
        return static_cast<uint64_t>(-EINTR);
    }

    task->set_wait_channel("sigsuspend", ker::mod::sched::task::WaitChannelKind::SIGSUSPEND);
    task->deferred_task_switch = true;
    return 0;
}

auto wos_proc_clone_vm(uint64_t args_addr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (args_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    CloneVmArgs args{};
    if (!sys::usercopy::copy_value_from_task(*parent, args_addr, args)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    if (args.fn == 0 || args.child_stack == 0 || (args.flags & WOS_CLONE_VM) == 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    uint64_t const SUPPORTED_FLAGS = WOS_CLONE_VM | WOS_CLONE_FS | WOS_CLONE_FILES | WOS_CLONE_UNTRACED | 0xFF;
    if ((args.flags & ~SUPPORTED_FLAGS) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (args.parent_tidptr != 0 && !sys::usercopy::ensure_writable(*parent, args.parent_tidptr, sizeof(int))) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (args.child_tidptr != 0 && !sys::usercopy::ensure_writable(*parent, args.child_tidptr, sizeof(int))) {
        return static_cast<uint64_t>(-EFAULT);
    }

    auto const PARENT_CREDENTIALS = parent->credentials();
    sched::resource::ProcessReservation process_reservation(parent, PARENT_CREDENTIALS.user().real);
    if (process_reservation.result() < 0) {
        return static_cast<uint64_t>(process_reservation.result());
    }
    uint64_t child_fsbase = parent->thread != nullptr ? parent->thread->fsbase : 0;
    if (args.newtls != 0) {
        child_fsbase = args.newtls;
    }
    if (child_fsbase != 0 && !sys::usercopy::ensure_writable(*parent, child_fsbase, sys::signal::WOS_TCB_SIGNAL_CACHE_BYTES)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    auto const KERNEL_STACK_BASE = alloc_fork_kernel_stack_with_reclaim();
    if (KERNEL_STACK_BASE == 0) {
        return static_cast<uint64_t>(-ENOMEM);
    }
    uint64_t const KERNEL_RSP = KERNEL_STACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* child = new sched::task::Task{};
    if (child == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        return static_cast<uint64_t>(-ENOMEM);
    }

    if (!sched::resource::inherit(*child, *parent)) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return static_cast<uint64_t>(-ESRCH);
    }

    auto cleanup_child = [&]() {
        child_events::abort_publication(*child);
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        child->pagemap = nullptr;
        delete child;
    };

    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        if (name_copy == nullptr) {
            cleanup_child();
            return static_cast<uint64_t>(-ENOMEM);
        }
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        child->name = name_copy;
    }

    child->pid = sched::task::get_next_pid();
    child->type = sched::task::TaskType::PROCESS;
    child->cpu = cpu::current_cpu();

    // Publish user-visible TID values before the child can become runnable.
    // A later construction failure can leave these output slots changed, but
    // it cannot leave an unreported live child behind after returning an
    // error. This copy is deliberately before the shared-vmem publication
    // guard, which is a subsystem publication mutex.
    int const CHILD_PID = static_cast<int>(child->pid);
    if ((args.parent_tidptr != 0 && !sys::usercopy::copy_value_to_task(*parent, args.parent_tidptr, CHILD_PID)) ||
        (args.child_tidptr != 0 && !sys::usercopy::copy_value_to_task(*parent, args.child_tidptr, CHILD_PID))) {
        cleanup_child();
        return static_cast<uint64_t>(-EFAULT);
    }

    // Keep the shared-address-space metadata clone and scheduler publication
    // atomic with respect to mmap/munmap/mprotect propagation. Otherwise a
    // WOS_CLONE_VM child can clone the old range set after an update's task
    // snapshot and then publish with permanently stale lazy-fault metadata.
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    child->pagemap = parent->pagemap;
    child->entry = parent->entry;
    child->program_header_addr = parent->program_header_addr;
    child->elf_header_addr = parent->elf_header_addr;
    child->image_load_base = parent->image_load_base;
    child->image_vaddr_start = parent->image_vaddr_start;
    child->image_vaddr_end = parent->image_vaddr_end;
    child->program_header_count = parent->program_header_count;
    child->program_header_ent_size = parent->program_header_ent_size;
    child->interp_base = parent->interp_base;
    child->interp_vaddr_start = parent->interp_vaddr_start;
    child->interp_vaddr_end = parent->interp_vaddr_end;
    child->at_random_addr = parent->at_random_addr;
    child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    child->sched_weight = parent->sched_weight;
    child->sched_nice = parent->sched_nice;
    child->slice_ns = parent->slice_ns;
    child->heap_index = -1;
    child->sched_queue = sched::task::Task::sched_queue::NONE;
    {
        auto* filesystem_parent = parent->filesystem_owner();
        uint64_t const PATH_IRQF = filesystem_parent->path_metadata_lock.lock_irqsave();
        child->cwd = filesystem_parent->cwd;
        child->cwd_len = filesystem_parent->cwd_len;
        child->root = filesystem_parent->root;
        child->root_len = filesystem_parent->root_len;
        child->root_anchor = filesystem_parent->root_anchor;
        child->cwd_anchor = filesystem_parent->cwd_anchor;
        filesystem_parent->path_metadata_lock.unlock_irqrestore(PATH_IRQF);
    }
    child->exe_path = parent->exe_path;
    auto child_job = parent->job_identity();
    child_job.exec_completed = false;
    child->initialize_job_identity(child_job);
    (void)child->exchange_creation_mask(parent->creation_mask());
    child->personality = parent->personality;
    child->signal_pending_store(0, std::memory_order_relaxed);
    child->signal_mask_store(parent->signal_mask_bits(), std::memory_order_relaxed);
    child->initialize_signal_handlers(parent->signal_handlers_snapshot());
    child->sigaltstack_sp = parent->sigaltstack_sp;
    child->sigaltstack_size = parent->sigaltstack_size;
    child->sigaltstack_flags = parent->sigaltstack_flags;
    child->wki_skip_legacy_placement = true;
    child->wki_target_flags = sched::task::Task::WKI_TARGET_FLAG_LOCAL;

    if (sched::resource::initialize_credentials(*child, PARENT_CREDENTIALS) != 0 ||
        !child->wki_vfs_rules.clone_from(parent->wki_vfs_rules) || !sched::task::clone_lazy_vmem_ranges(*child, *parent)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    parent->shares_user_pagemap.store(true, std::memory_order_release);
    child->shares_user_pagemap.store(true, std::memory_order_release);
    (void)sched::resource::observe_address_space(*child);

    auto* child_thread = new sched::threading::Thread();
    if (child_thread == nullptr) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (parent->thread != nullptr) {
        *child_thread = *parent->thread;
        child_thread->tls_phys_ptr = 0;
        child_thread->stack_phys_ptr = 0;
    }
    child_thread->fsbase = child_fsbase;
    child_thread->stack = args.child_stack;
    child_thread->stack_size = 0;
    child_thread->stack_base_virt = 0;
    child_thread->stack_lowest_backed = 0;
    child->thread = child_thread;
    ker::mod::sys::signal::sync_task_signal_mask_cache_mapped(child);

    child->context.syscall_kernel_stack = KERNEL_RSP;
    auto* per_cpu = new cpu::PerCpu();
    if (per_cpu == nullptr) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    per_cpu->syscall_stack = KERNEL_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    per_cpu->user_rsp = args.child_stack;
    per_cpu->syscall_ret_rip = args.fn;
    per_cpu->syscall_ret_flags = 0x202;
    child->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);
    child_thread->gsbase = reinterpret_cast<uint64_t>(per_cpu);

    child->context.regs = {};
    child->context.regs.rdi = args.arg;
    child->context.frame.rip = args.fn;
    child->context.frame.rsp = args.child_stack;
    child->context.frame.flags = 0x202;
    child->context.frame.cs = desc::gdt::GDT_USER_CS;
    child->context.frame.ss = desc::gdt::GDT_USER_DS;
    ker::mod::sys::context_switch::record_saved_frame_class(child, child->context.frame,
                                                            ker::mod::sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);

    if (!clone_fd_table_shared_checked(parent, child)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }

    if (!child_events::begin_publication(*parent, *child)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }

    if (!process_reservation.attach(*child) || !sched::post_task_balanced(child)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (!child_events::commit_publication(*child)) [[unlikely]] {
        // The scheduler has already accepted the child, so returning failure
        // would hide a live process and invite a duplicate userspace retry.
        process_log::error("clone-vm: lifecycle commit completed concurrently for published child PID %x", child->pid);
    }

    return child->pid;
}

auto wos_proc_prctl(int option, uint64_t arg2, uint64_t arg3, uint64_t arg4, uint64_t arg5) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    switch (option) {
        case static_cast<int>(abi::process::CredentialControl::GET_NO_NEW_PRIVILEGES):
            if (arg2 != 0 || arg3 != 0 || arg4 != 0 || arg5 != 0) return static_cast<uint64_t>(-EINVAL);
            return task->credentials().no_new_privileges() ? 1 : 0;
        case static_cast<int>(abi::process::CredentialControl::GET_AUTHORITY_CEILING):
            if (arg2 != 0 || arg3 != 0 || arg4 != 0 || arg5 != 0) return static_cast<uint64_t>(-EINVAL);
            return task->credentials().authority_ceiling();
        case static_cast<int>(abi::process::CredentialControl::GET_SERVICE_PRIVILEGES): {
            if (arg2 != 0 || arg3 != 0 || arg4 != 0 || arg5 != 0) return static_cast<uint64_t>(-EINVAL);
            auto const CREDENTIALS = task->credentials();
            return CREDENTIALS.service_privileges() |
                   (CREDENTIALS.service_privileges_locked() ? abi::process::SERVICE_PRIVILEGES_LOCKED : 0);
        }
        case static_cast<int>(abi::process::CredentialControl::SET_SERVICE_PRIVILEGES): {
            if ((arg2 & ~abi::process::SERVICE_PRIVILEGE_ALL) != 0 || arg3 != 0 || arg4 != 0 || arg5 != 0)
                return static_cast<uint64_t>(-EINVAL);
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            return static_cast<uint64_t>(ker::mod::sched::resource::change_credentials(
                *task, {.change = ker::mod::security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = arg2}));
        }
        case static_cast<int>(abi::process::CredentialControl::SET_NO_NEW_PRIVILEGES): {
            if (arg2 != 1 || arg3 != 0 || arg4 != 0 || arg5 != 0) return static_cast<uint64_t>(-EINVAL);
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            return static_cast<uint64_t>(
                ker::mod::sched::resource::change_credentials(*task, {.change = ker::mod::security::Change::NO_NEW_PRIVILEGES}));
        }
        case static_cast<int>(abi::process::CredentialControl::DROP_AUTHORITY_CEILING): {
            if ((arg2 & ~abi::process::CREDENTIAL_AUTHORITY_ALL) != 0 || arg3 != 0 || arg4 != 0 || arg5 != 0)
                return static_cast<uint64_t>(-EINVAL);
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            return static_cast<uint64_t>(ker::mod::sched::resource::change_credentials(
                *task, {.change = ker::mod::security::Change::DROP_AUTHORITY, .authority_ceiling = arg2}));
        }
        case WOS_PR_SET_PDEATHSIG:
            if (arg2 > ker::mod::sched::task::Task::MAX_SIGNALS) {
                return static_cast<uint64_t>(-EINVAL);
            }
            task->parent_death_signal.store(static_cast<uint32_t>(arg2), std::memory_order_release);
            return 0;
        case WOS_PR_GET_PDEATHSIG:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                int const SIGNAL = static_cast<int>(task->parent_death_signal.load(std::memory_order_acquire));
                return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, SIGNAL) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        case WOS_PR_GET_DUMPABLE:
            return task->credentials().dumpable() ? 1 : 0;
        case WOS_PR_SET_DUMPABLE:
            if (arg2 > 1) {
                return static_cast<uint64_t>(-EINVAL);
            }
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                return static_cast<uint64_t>(ker::mod::sched::resource::change_credentials(
                    *task, {.change = ker::mod::security::Change::SET_DUMPABLE, .dumpable = arg2 != 0}));
            }
        case WOS_PR_SET_NAME:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                constexpr size_t TASK_NAME_MAX = 16;
                auto* name = new char[TASK_NAME_MAX];
                if (name == nullptr) {
                    return static_cast<uint64_t>(-ENOMEM);
                }
                if (!ker::mod::sys::usercopy::copy_cstring_from_task(*task, arg2, name, TASK_NAME_MAX)) {
                    delete[] name;
                    return static_cast<uint64_t>(-EFAULT);
                }
                const char* retired = nullptr;
                {
                    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                    retired = task->name;
                    task->name = name;
                }
                delete[] retired;
            }
            return 0;
        case WOS_PR_GET_NAME:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                std::array<char, 16> name{};
                {
                    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                    copy_cstr_field(name.data(), name.size(), task->name != nullptr ? task->name : "");
                }
                return ker::mod::sys::usercopy::copy_to_task(*task, arg2, name.data(), name.size()) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        case WOS_PR_SET_PTRACER: {
            // WOS has no Yama relationship restriction to relax. Preserve the
            // valid request as a no-op; never overwrite an active tracer PID.
            if (arg2 != 0 && arg2 != UINT64_MAX) {
                auto* candidate = ker::mod::sched::find_task_by_pid_safe(arg2);
                if (candidate == nullptr) {
                    return static_cast<uint64_t>(-EINVAL);
                }
                bool const PROCESS = candidate->type == ker::mod::sched::task::TaskType::PROCESS;
                candidate->release();
                if (!PROCESS) {
                    return static_cast<uint64_t>(-EINVAL);
                }
            }
            return 0;
        }
        case WOS_PR_SET_VMA:
            return arg2 == WOS_PR_SET_VMA_ANON_NAME ? 0 : static_cast<uint64_t>(-EINVAL);
        default:
            (void)arg3;
            (void)arg4;
            (void)arg5;
            return static_cast<uint64_t>(-EINVAL);
    }
}

auto wos_proc_arch_prctl(int option, uint64_t arg2) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->thread == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    switch (option) {
        case WOS_ARCH_SET_FS:
            task->thread->fsbase = arg2;
            ker::mod::cpu::wrfsbase(arg2);
            return 0;
        case WOS_ARCH_GET_FS:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, task->thread->fsbase) ? 0 : static_cast<uint64_t>(-EFAULT);
        case WOS_ARCH_SET_GS:
            (void)arg2;
            return 0;
        case WOS_ARCH_GET_GS:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                uint64_t const GS_BASE = 0;
                return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, GS_BASE) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        default:
            return static_cast<uint64_t>(-EINVAL);
    }
}

auto wos_proc_kill(int64_t pid, int sig) -> uint64_t {
    using namespace ker::mod;

    if (sig < 0 || std::cmp_greater(sig, sched::task::Task::MAX_SIGNALS)) {
        return static_cast<uint64_t>(-EINVAL);
    }
    auto* self = sched::get_current_task();
    if (self == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    auto const CALLER = self->credentials();
    auto const CALLER_JOB = self->job_identity();
    auto deliver = [&](sched::task::Task* target) -> int { return signal_as(*target, CALLER, CALLER_JOB, sig); };

    if (pid <= 0) {
        // Preserve WOS's existing pid=-1 behavior: the caller's process group.
        uint64_t const PGRP = pid < -1 ? static_cast<uint64_t>(-(pid + 1)) + 1 : CALLER_JOB.pgid;
        // Native init keeps its service group's origin after the leader exits.
        // Migrated members remain reachable without selecting a foreign alias.
        bool const INIT_LOCAL_GROUP = sched::task::process_pid(*self) == 1 && CALLER_JOB.group_origin.local() &&
                                      (self->wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0;
        return static_cast<uint64_t>(ker::net::wki::wki_group_signal_as(CALLER, CALLER_JOB, PGRP, pid >= -1 || INIT_LOCAL_GROUP, sig));
    }

    auto* target = sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
    if (target == nullptr) {
        target = ker::net::wki::wki_proxy_task_find_by_remote_pid_safe(static_cast<uint64_t>(pid));
    }
    if (target == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    int const RESULT = deliver(target);
    return static_cast<uint64_t>(RESULT);
}

auto wos_proc_setpriority(int which, int64_t who, int prio) -> uint64_t {
    if (which != WOS_PRIO_PROCESS) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* self = mod::sched::get_current_task();
    if (self == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if (who == 0) {
        who = static_cast<int64_t>(self->pid);
    }
    if (who < 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* target = mod::sched::find_task_by_pid_safe(static_cast<uint64_t>(who));
    if (target == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    int result = 0;
    {
        // Serialize policy and scheduler commit with every process nice/ID
        // change. Trusted daemon priority setters cannot affect process tasks.
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        auto const CALLER = self->credentials();
        auto const TARGET = target->credentials();
        int const NICE = ker::mod::security::clamp_priority(prio);
        if (!is_live_signal_target(*target)) {
            result = -ESRCH;
        } else if (target->type != mod::sched::task::TaskType::PROCESS && !CALLER.allows(ker::mod::security::Authority::PRIORITY)) {
            result = -EPERM;
        } else {
            result = ker::mod::security::authorize_priority(CALLER, TARGET, target->sched_nice, NICE);
            if (result == 0) {
                mod::sched::set_task_nice(target, NICE);
            }
        }
    }
    target->release();
    return static_cast<uint64_t>(result);
}

auto wos_proc_getpriority(int which, int64_t who) -> uint64_t {
    if (which != WOS_PRIO_PROCESS) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* self = mod::sched::get_current_task();
    if (self == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if (who == 0) {
        who = static_cast<int64_t>(self->pid);
    }
    if (who < 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* target = mod::sched::find_task_by_pid_safe(static_cast<uint64_t>(who));
    if (target == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    int encoded_nice = 0;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        encoded_nice = static_cast<int>(target->sched_nice) + WOS_PRIO_KERNEL_ENCODE_BIAS;
    }
    target->release();
    return static_cast<uint64_t>(encoded_nice);
}

auto wos_proc_setwkitarget(uint64_t hostname_addr, size_t len, uint32_t flags) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if ((flags & ~ker::mod::sched::task::Task::WKI_TARGET_FLAGS_ALL) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    uint32_t const PLACEMENT_FLAGS =
        flags & (ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL | ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE |
                 ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED);
    if (PLACEMENT_FLAGS != 0 && (PLACEMENT_FLAGS & (PLACEMENT_FLAGS - 1U)) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (hostname_addr != 0 && len != 0 &&
        (flags & (ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL | ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED)) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    if (hostname_addr == 0 || len == 0) {
        task->wki_target_hostname.front() = '\0';
        task->wki_target_flags = flags;
        return 0;
    }

    if (len >= task->wki_target_hostname.size()) {
        return static_cast<uint64_t>(-ENAMETOOLONG);
    }

    ker::mod::sched::task::Task::HostnameBuffer hostname{};
    if (!ker::mod::sys::usercopy::copy_from_task(*task, hostname_addr, hostname.data(), len)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    mutable_hostname_char(hostname, len) = '\0';
    task->wki_target_hostname = hostname;
    task->wki_target_flags = flags;
    return 0;
}

auto wos_proc_getwkitarget(uint64_t hostname_out_addr, size_t hostname_out_size, uint64_t flags_out_addr) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    auto const HOSTNAME = task->wki_target_hostname;
    uint32_t const FLAGS = task->wki_target_flags;
    size_t const LEN = strnlen(HOSTNAME.data(), HOSTNAME.size());
    if (hostname_out_addr != 0) {
        if (hostname_out_size == 0 || LEN + 1 > hostname_out_size) {
            return static_cast<uint64_t>(-ENAMETOOLONG);
        }
        if (!ker::mod::sys::usercopy::ensure_writable(*task, hostname_out_addr, LEN + 1)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }
    if (flags_out_addr != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, flags_out_addr, sizeof(FLAGS))) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (hostname_out_addr != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, hostname_out_addr, HOSTNAME.data(), LEN + 1)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (flags_out_addr != 0 && !ker::mod::sys::usercopy::copy_value_to_task(*task, flags_out_addr, FLAGS)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    return LEN;
}
}  // namespace

namespace {
auto signal_local_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
                     const ker::mod::sched::session::Identity& caller_job, int signum, const ker::mod::sched::session::Identity* group)
    -> std::optional<int> {
    using namespace ker::mod;
    bool allowed = false;
    PendingSignalTarget admission;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!is_live_signal_target(target)) return -ESRCH;
        if (target.type != sched::task::TaskType::PROCESS) return -EPERM;
        // Native exec publishes the owner's replacement root under this same
        // guard. Old-image worker TIDs must stop admitting effects before the
        // execing worker's pending state is drained into that replacement.
        auto* owner = target.credential_owner();
        if (target.is_thread && (owner == nullptr || target.pagemap != owner->pagemap)) return -ESRCH;
        auto const ADMISSION = admission.admit_local(target);
        auto const& JOB = ADMISSION.job;
        // Membership, current target authority and session are one decision.
        // A group request never falls through to per-task proxy forwarding.
        if (ADMISSION.remote) return group != nullptr ? std::optional<int>{-ESRCH} : std::nullopt;
        if (!ADMISSION.local_lease) return -EAGAIN;
        if (group != nullptr && !sched::session::same_group(*group, JOB)) return -ESRCH;
        allowed = security::may_signal(caller, ADMISSION.credentials, sched::session::same_session(caller_job, JOB), signum == 18);
    }
    if (!allowed) return -EPERM;
    if (signum == 0) return 0;
    if (is_ptrace_launch_stop_signal(signum) && debug::ptrace::report_signal_stop(target, static_cast<uint32_t>(signum))) return 0;
    target.signal_generate_pending_mask(1ULL << (signum - 1));
    sched::wake_task_for_signal(&target);
    return 0;
}

auto matches_numeric_process_group(ker::mod::sched::task::Task* target, void* context) -> bool {
    return target->type == ker::mod::sched::task::TaskType::PROCESS && target->job_identity().pgid == *static_cast<uint64_t*>(context);
}
}  // namespace

auto signal_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
               const ker::mod::sched::session::Identity& caller_job, int signum) -> int {
    using namespace ker::mod;
    PendingSignalTarget retained(target);
    if (signum < 0 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    if (!is_live_signal_target(target)) return -ESRCH;
    if (target.type != sched::task::TaskType::PROCESS) return -EPERM;
    if (auto remote = ker::net::wki::wki_proxy_task_signal_as(target, caller, caller_job, signum); remote.has_value()) return *remote;
    if (auto local = signal_local_as(target, caller, caller_job, signum, nullptr); local.has_value()) return *local;
    // ACCEPT may publish a signal route between the first lookup and local
    // admission. Retry receiver routing once; never fall back to the old frame.
    return ker::net::wki::wki_proxy_task_signal_as(target, caller, caller_job, signum).value_or(-ESRCH);
}

auto signal_group_local_as(const ker::mod::security::Snapshot& caller, const ker::mod::sched::session::Identity& caller_job,
                           const ker::mod::sched::session::Identity& group, int signum) -> int {
    if (signum < 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    if (group.pgid == 0) return -ESRCH;
    uint64_t pgid = group.pgid;
    PendingSignalTarget cohort;
    int const CAPTURED = cohort.capture(matches_numeric_process_group, &pgid);
    if (CAPTURED != 0) return CAPTURED;
    size_t accepted = 0;
    int error = -ESRCH;
    while (auto* target = cohort.take()) {
        PendingSignalTarget retained(*target);
        if (target->is_thread && signum >= 18 && signum <= 22) continue;
        int const RESULT = signal_local_as(*target, caller, caller_job, signum, &group).value_or(-ESRCH);
        accepted += RESULT == 0 ? 1 : 0;
        if (RESULT != 0 && RESULT != -ESRCH && (error == -ESRCH || RESULT == -EPERM)) error = RESULT;
    }
    return accepted != 0 ? 0 : error;
}

auto with_local_signal_targets(LocalSignalCallback callback, void* context, LocalSignalCleanup cleanup) -> int {
    if (callback == nullptr) return -EINVAL;
    auto process = [](ker::mod::sched::task::Task* task, void*) -> bool {
        return task->type == ker::mod::sched::task::TaskType::PROCESS && !task->is_thread;
    };
    PendingSignalTarget cohort;
    cohort.set_cleanup(cleanup, context);
    int const CAPTURED = cohort.capture(process, nullptr);
    if (CAPTURED != 0) return CAPTURED;
    int error = 0;
    cohort.retain_if(
        [](ker::mod::sched::task::Task* target, void* opaque) -> bool {
            // Registry ownership is already released; no lock crosses the
            // callback. This is discovery, not authority to deliver later.
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (!is_live_signal_target(*target) || target->signal_admission_identity().remote) return false;
            if (target->signal_home_identity().serial == 0) {
                *static_cast<int*>(opaque) = -EOVERFLOW;
                return false;
            }
            return true;
        },
        &error);
    if (error != 0) return error;
    return callback(cohort.view(), context);
}

auto append_signal_receiver(ker::mod::sched::task::Task& target, uint64_t cursor, uint64_t ceiling,
                            ker::net::wki::GroupReceiverResponse& response) -> int {
    auto const HOME = target.signal_home_identity();
    uint64_t const SERIAL = target.signal_receiver_serial();
    if (HOME.serial == 0 || SERIAL == 0) return -EOVERFLOW;
    if (ceiling == 0) response.ceiling = std::max(response.ceiling, SERIAL);
    if (SERIAL <= cursor || (ceiling != 0 && SERIAL > ceiling)) return 0;
    size_t at = 0;
    while (at < response.count && response.receivers[at].serial < SERIAL) ++at;
    if (at == response.receivers.size() || (at < response.count && response.receivers[at].serial == SERIAL)) return 0;
    size_t const LAST = std::min<size_t>(response.count, response.receivers.size() - 1);
    for (size_t i = LAST; i > at; --i) response.receivers[i] = response.receivers[i - 1];
    auto const& WKI = ker::net::wki::g_wki;
    response.receivers[at] = {.serial = SERIAL,
                              .pid = target.pid,
                              .home = {.origin = {.node = HOME.origin.node == 0 ? WKI.my_node_id : HOME.origin.node,
                                                  .reserved = 0,
                                                  .boot = HOME.origin.node == 0 ? WKI.local_boot_epoch : HOME.origin.boot},
                                       .serial = HOME.serial}};
    if (response.count < response.receivers.size()) ++response.count;
    return 0;
}

auto enumerate_signal_receivers(uint64_t cursor, uint64_t ceiling, ker::net::wki::GroupReceiverResponse& response) -> int {
    struct Context {
        uint64_t cursor;
        uint64_t ceiling;
        ker::net::wki::GroupReceiverResponse& response;
    } context{cursor, ceiling, response};
    response.count = 0;
    response.ceiling = ceiling;
    return with_local_signal_targets(
        [](std::span<ker::mod::sched::task::Task* const> targets, void* opaque) {
            auto& input = *static_cast<Context*>(opaque);
            for (auto* target : targets) {
                int const RESULT = append_signal_receiver(*target, input.cursor, input.ceiling, input.response);
                if (RESULT != 0) return RESULT;
            }
            return 0;
        },
        &context);
}

#ifdef WOS_SELFTEST
auto process_selftest_signal_receiver_pages() -> bool {
    using ker::mod::sched::task::Task;
    std::unique_ptr<Task[]> tasks(new (std::nothrow) Task[40]);
    if (!tasks) return false;
    bool ok = true;
    for (size_t i = 0; i < 40; ++i) {
        tasks[i].pid = 1000 + i;
        ok &= tasks[i].adopt_signal_home({.origin = {.node = 7, .boot = 8}, .serial = 50 + i});
        ok &= tasks[i].signal_receiver_serial() != 0;
    }
    ker::net::wki::GroupReceiverResponse first{};
    for (size_t i = 40; i-- != 0;) ok &= append_signal_receiver(tasks[i], 0, 0, first) == 0;
    ok &= first.count == 32 && first.ceiling == tasks[39].signal_receiver_serial();
    for (size_t i = 0; i < first.count; ++i)
        ok &= first.receivers[i].pid == 1000 + i && first.receivers[i].home.origin.node == 7 && first.receivers[i].home.origin.boot == 8 &&
              first.receivers[i].home.serial == 50 + i;
    // Registry and running-table views may overlap as publication completes.
    ok &= append_signal_receiver(tasks[0], 0, 0, first) == 0 && first.count == 32;
    ker::net::wki::GroupReceiverResponse second{.ceiling = first.ceiling};
    uint64_t const CURSOR = first.receivers[31].serial;
    for (size_t i = 0; i < 40; ++i) ok &= append_signal_receiver(tasks[i], CURSOR, first.ceiling, second) == 0;
    ok &= second.count == 8 && second.ceiling == first.ceiling;
    for (size_t i = 0; i < second.count; ++i) ok &= second.receivers[i].pid == 1032 + i;
    Task arrived{};
    arrived.pid = 2000;
    ok &= arrived.adopt_signal_home({.origin = {.node = 9, .boot = 10}, .serial = 50});
    ok &= append_signal_receiver(arrived, CURSOR, first.ceiling, second) == 0 && second.count == 8;
    ker::net::wki::GroupReceiverResponse next{};
    ok &= append_signal_receiver(arrived, 0, 0, next) == 0 && next.count == 1 && next.receivers[0].home.origin.node == 9 &&
          next.receivers[0].serial > first.ceiling;
    return ok;
}
#endif

auto enumerate_signal_anchors(uint64_t cursor, uint64_t ceiling, ker::net::wki::GroupRouteResponse& response) -> int {
    auto anchor = [](ker::mod::sched::task::Task* task, void*) -> bool {
        return task->type == ker::mod::sched::task::TaskType::PROCESS && !task->is_thread;
    };
    PendingSignalTarget cohort;
    int const CAPTURED = cohort.capture(anchor, nullptr);
    if (CAPTURED != 0) return CAPTURED;
    response.count = 0;
    response.ceiling = ceiling;
    while (auto* target = cohort.take()) {
        PendingSignalTarget retained(*target);
        if (!is_live_signal_target(*target) || !target->is_signal_home_anchor()) continue;
        auto const HOME = target->signal_home_identity();
        if (HOME.serial == 0) return -EOVERFLOW;
        if (ceiling == 0) response.ceiling = std::max(response.ceiling, HOME.serial);
        if (HOME.serial <= cursor || (ceiling != 0 && HOME.serial > ceiling)) continue;
        size_t at = 0;
        while (at < response.count && response.anchors[at].serial < HOME.serial) ++at;
        if (at == response.anchors.size()) continue;
        size_t const LAST = std::min<size_t>(response.count, response.anchors.size() - 1);
        for (size_t i = LAST; i > at; --i) response.anchors[i] = response.anchors[i - 1];
        response.anchors[at] = {.serial = HOME.serial, .pid = target->pid};
        if (response.count < response.anchors.size()) ++response.count;
    }
    return 0;
}

auto signal_group_target_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
                            const ker::mod::sched::session::Identity& caller_job, const ker::mod::sched::session::Identity& group,
                            bool query, int signum, ker::mod::sched::session::Identity& current_job, uint32_t& route_task_id,
                            uint64_t& route_pid) -> std::optional<int> {
    using namespace ker::mod;
    PendingSignalTarget retained(target);
    PendingSignalTarget admission;
    PendingSignalTarget members;
    route_task_id = 0;
    route_pid = 0;
    if (signum < 0 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    if (!query && signum != 0 && (signum < 18 || signum > 22)) {
        auto member = [](sched::task::Task* candidate, void* owner) -> bool {
            return candidate->is_thread && candidate->credential_owner() == owner;
        };
        int const CAPTURED = members.capture(member, &target);
        if (CAPTURED != 0) return CAPTURED;
    }
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!is_live_signal_target(target)) return -ESRCH;
        if (target.type != sched::task::TaskType::PROCESS || target.is_thread) return -EPERM;
        auto const ADMISSION = admission.admit_local(target);
        if (ADMISSION.remote) {
            auto const ROUTE = target.wki_execution_identity();
            route_task_id = ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id;
            route_pid = ROUTE.remote_pid;
            return std::nullopt;
        }
        if (!ADMISSION.local_lease) return -EAGAIN;
        current_job = ADMISSION.job;
        if (query) return 0;
        if (!sched::session::same_group(group, current_job)) return -ESRCH;
        if (!security::may_signal(caller, ADMISSION.credentials, sched::session::same_session(caller_job, current_job), signum == 18))
            return -EPERM;
    }
    if (signum == 0) return 0;
    auto generate = [signum](sched::task::Task& recipient) {
        if (!is_live_signal_target(recipient)) return;
        if (recipient.group_signal_visible() && is_ptrace_launch_stop_signal(signum) &&
            debug::ptrace::report_signal_stop(recipient, static_cast<uint32_t>(signum)))
            return;
        if (recipient.generate_group_signal(1ULL << (signum - 1))) sched::wake_task_for_signal(&recipient);
    };
    // One canonical admission remains owned through every old-image effect.
    // A concurrent cutover may close its route, but must drain this lease.
    generate(target);
    while (auto* member = members.take()) {
        PendingSignalTarget owned(*member);
        generate(*member);
    }
    return 0;
}

namespace {
template <typename Admitted>
auto child_event_signal_local(ker::mod::sched::task::Task& target, Admitted&& admitted) -> std::optional<int> {
    if (!is_live_signal_target(target) || target.exit_in_progress) return -ESRCH;
    auto const ADMISSION = target.begin_local_signal_admission();
    if (ADMISSION.remote) return std::nullopt;
    if (!ADMISSION.local_lease) return -EAGAIN;
    // Bounded, nonblocking effect: no stack-backed fatal-wait registration is
    // needed. Exec drains this lease before capturing the final pending state.
    admitted();
    target.signal_add_pending_mask(uint64_t{1} << 16);
    ker::mod::sched::wake_task_from_event(&target);
    target.end_local_signal_admission();
    return 0;
}

template <typename Admitted>
auto parent_death_signal(ker::mod::sched::task::Task& target, uint16_t hops, Admitted&& admitted) -> int {
    using namespace ker::mod;
    if (hops > ker::net::wki::WKI_PARENT_DEATH_HOPS) return -ELOOP;
    if (!is_live_signal_target(target)) return -ESRCH;
    if (target.type != sched::task::TaskType::PROCESS) return -EPERM;
    PendingSignalTarget admission;
    auto const ADMISSION = admission.admit_local(target);
    if (ADMISSION.remote) return ker::net::wki::wki_proxy_task_parent_death(target, hops).value_or(-ESRCH);
    if (!ADMISSION.local_lease) return -EAGAIN;
    // The event belongs to this child, independent of its parent's credentials.
    // Exec must drain admission before taking its final registration/pending snapshot.
    admitted();
    uint32_t const SIGNAL = target.parent_death_signal.load(std::memory_order_acquire);
    if (SIGNAL > sched::task::Task::MAX_SIGNALS) return -EINVAL;
    if (SIGNAL != 0 && target.generate_group_signal(uint64_t{1} << (SIGNAL - 1))) sched::wake_task_for_signal(&target);
    return 0;
}

template <typename Admitted>
auto terminal_source_signal(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum,
                            bool revoked, Admitted&& admitted) -> int {
    using namespace ker::mod;
    if (signum <= 0 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    if (!is_live_signal_target(target)) return -ESRCH;
    if (target.type != sched::task::TaskType::PROCESS) return -EPERM;
    PendingSignalTarget admission;
    // Admission and accepted-route publication share path_metadata_lock. The
    // terminal object already granted this event; no credential or address-space
    // decision requires reacquiring the caller's possible publication lock.
    auto const ADMISSION = admission.admit_local(target);
    if (ADMISSION.remote) {
        // No local lease crosses the forwarder's cutover wait. A retired route
        // cannot restore authority over the parked source frame.
        return ker::net::wki::wki_proxy_task_terminal_signal(target, expected, signum).value_or(-ESRCH);
    }
    if (!ADMISSION.local_lease) return -EAGAIN;
    if (!sched::session::same_session(expected, ADMISSION.job) || !sched::session::same_group(expected, ADMISSION.job)) return -ESRCH;
    if (!revoked && (expected.terminal != ADMISSION.job.terminal || expected.terminal_generation != ADMISSION.job.terminal_generation))
        return -ESRCH;
    admitted();
    // Remote exec must drain this lease before capturing the pending state.
    // Keep it through job-control generation and every scheduler wake as well.
    if (target.generate_group_signal(1ULL << (signum - 1))) sched::wake_task_for_signal(&target);
    return 0;
}
}  // namespace

auto try_signal_child_event_local(ker::mod::sched::task::Task& target) -> std::optional<int> {
    return child_event_signal_local(target, [] {});
}

void signal_child_event(ker::mod::sched::task::Task& target, uint16_t hops) {
    auto const RESULT = try_signal_child_event_local(target);
    if (!RESULT.has_value() || *RESULT == -EAGAIN) ker::net::wki::wki_defer_child_signal(target, hops);
}

auto signal_parent_death(ker::mod::sched::task::Task& target, uint16_t hops) -> int {
    PendingSignalTarget retained(target);
    return parent_death_signal(target, hops, [] {});
}

auto signal_terminal_source_as(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum,
                               bool revoked) -> int {
    return terminal_source_signal(target, expected, signum, revoked, [] {});
}

auto signal_terminal_as(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum, uint16_t hops)
    -> int {
    using namespace ker::mod;
    PendingSignalTarget retained(target);
    if (signum <= 0 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS) || expected.session_id == 0 || expected.pgid == 0)
        return -EINVAL;
    if (!is_live_signal_target(target)) return -ESRCH;
    if (target.type != sched::task::TaskType::PROCESS || target.is_thread) return -EPERM;
    PendingSignalTarget members;
    if (signum < 18 || signum > 22) {
        auto member = [](sched::task::Task* candidate, void* owner) -> bool {
            return candidate->is_thread && candidate->credential_owner() == owner;
        };
        int const CAPTURED = members.capture(member, &target);
        if (CAPTURED != 0) return CAPTURED;
    }
    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        if (auto remote = ker::net::wki::wki_proxy_task_terminal_signal(target, expected, signum, hops); remote.has_value()) return *remote;
        PendingSignalTarget admission;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (!is_live_signal_target(target)) return -ESRCH;
            auto const ADMISSION = admission.admit_local(target);
            if (ADMISSION.remote) continue;
            if (!ADMISSION.local_lease) return -EAGAIN;
            if (!sched::session::same_session(expected, ADMISSION.job) || !sched::session::same_group(expected, ADMISSION.job))
                return -ESRCH;
        }
        auto generate = [signum](sched::task::Task& recipient) {
            if (!is_live_signal_target(recipient)) return;
            if (recipient.group_signal_visible() && is_ptrace_launch_stop_signal(signum) &&
                debug::ptrace::report_signal_stop(recipient, static_cast<uint32_t>(signum)))
                return;
            if (recipient.generate_group_signal(1ULL << (signum - 1))) sched::wake_task_for_signal(&recipient);
        };
        // The terminal grant is independent of UID. Membership and cutover
        // ownership remain stable through every admitted old-image effect.
        generate(target);
        while (auto* member = members.take()) {
            PendingSignalTarget owned(*member);
            generate(*member);
        }
        return 0;
    }
    return -EAGAIN;
}

auto query_group_local(uint64_t pgid, ker::mod::sched::session::Origin local_namespace,
                       const ker::mod::sched::session::Identity& caller_job, ker::mod::sched::session::GroupCandidates& result) -> int {
    using namespace ker::mod::sched;
    result = {};
    if (pgid == 0) return -ESRCH;
    PendingSignalTarget cohort;
    int const CAPTURED = cohort.capture(matches_numeric_process_group, &pgid);
    if (CAPTURED != 0) return CAPTURED;
    std::optional<session::Origin> const OWN = pgid == caller_job.pgid ? std::optional{caller_job.group_origin} : std::nullopt;
    while (auto* target = cohort.take()) {
        PendingSignalTarget retained(*target);
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!is_live_signal_target(*target)) continue;
        auto const ADMISSION = target->signal_admission_identity();
        if (ADMISSION.remote) continue;
        auto const& JOB = ADMISSION.job;
        if (JOB.pgid == pgid) result.observe(JOB.group_origin, local_namespace, OWN);
    }
    return 0;
}

#ifdef WOS_SELFTEST
auto process_selftest_group_signal_authority() -> bool {
    using namespace ker::mod;
    sched::task::Task target{};
    target.type = sched::task::TaskType::PROCESS;
    target.pid = 0x7B510001;
    sched::session::Identity group{.session_id = 41, .pgid = 77, .origin = {.node = 7, .boot = 8}, .group_origin = {.node = 8, .boot = 9}};
    target.initialize_job_identity(group);
    security::Snapshot caller;
    bool ok = security::Snapshot::bootstrap_root().prepare({.change = security::Change::SET_UID, .real = 100}, caller) == 0;
    sched::session::Identity const CALLER_JOB{.session_id = 42, .pgid = 78};
    auto call = [&](int signum) { return signal_local_as(target, caller, CALLER_JOB, signum, &group); };
    group.group_origin = {};
    ok &= call(0) == -ESRCH && call(10) == -ESRCH;
    group.group_origin = {.node = 8, .boot = 9};
    ok &= call(0) == -EPERM && call(10) == -EPERM && call(18) == -EPERM;
    ok &= sched::resource::initialize_credentials(target, caller) == 0;
    ok &= call(0) == 0;  // Same UID may signal across sessions.
    target.set_wki_proxy_task_id(42);
    ok &= call(0) == -ESRCH && call(10) == -ESRCH;
    target.set_wki_proxy_task_id(0);
    auto changed = group;
    changed.pgid = 88;
    target.initialize_job_identity(changed);
    ok &= call(0) == -ESRCH && call(10) == -ESRCH;
    target.initialize_job_identity(group);
    ok &= call(0) == 0;
    target.publish_accepted_signal_route(42, 99);
    ok &= target.wki_execution_identity().proxy_task_id == 0 && !target.wki_proxy_task;
    ok &= call(0) == -ESRCH && call(10) == -ESRCH;
    ok &= !signal_local_as(target, caller, CALLER_JOB, 0, nullptr).has_value();
    target.publish_accepted_signal_route(43, 100);
    ok &= target.wki_execution_identity().signal_task_id == 42 && target.wki_execution_identity().remote_pid == 99;
    // The routed path uses the same stable credential/group decision while
    // retaining the canonical admission through unpublished pending updates.
    sched::task::Task receiver{};
    receiver.type = sched::task::TaskType::PROCESS;
    receiver.initialize_job_identity(group);
    receiver.hide_group_signals_until_publication();
    sched::session::Identity observed{};
    uint32_t route_id = 0;
    uint64_t route_pid = 0;
    auto routed = [&](bool query, int signal) -> std::optional<int> {
        if (!receiver.try_acquire()) return -ESRCH;
        return signal_group_target_as(receiver, caller, CALLER_JOB, group, query, signal, observed, route_id, route_pid);
    };
    ok &= routed(false, 12) == -EPERM && receiver.signal_pending_bits() == 0;
    ok &= routed(true, 0) == 0 && observed.pgid == group.pgid;
    ok &= sched::resource::initialize_credentials(receiver, caller) == 0;
    ok &= routed(false, 12) == 0 && receiver.signal_pending_bits() == (uint64_t{1} << 11);
    ok &= !receiver.group_signal_visible() && receiver.local_signal_admissions_drained();
    receiver.publish_accepted_signal_route(44, 101);
    ok &= !routed(false, 12).has_value() && route_id == 44 && route_pid == 101;
    ok &= receiver.signal_pending_bits() == (uint64_t{1} << 11) && receiver.ref_count.load() == 1;
    return ok && target.signal_pending_bits() == 0 && target.ref_count.load() == 1;
}

auto process_selftest_child_signal_cutover() -> bool {
    using namespace ker::mod;
    auto target = std::make_unique<sched::task::Task>();
    target->type = sched::task::TaskType::PROCESS;
    target->hide_group_signals_until_publication();
    constexpr uint64_t CHILD = uint64_t{1} << 16;
    bool ok = true;
    for (uint32_t uid : {0U, 104U, 200U}) {
        security::Snapshot credentials;
        ok &= security::Snapshot::bootstrap_root().prepare({.change = security::Change::SET_UID, .real = uid}, credentials) == 0;
        ok &= sched::resource::initialize_credentials(*target, credentials) == 0;
        for (uint64_t mask : {uint64_t{0}, CHILD}) {
            target->signal_mask_store(mask);
            ok &= try_signal_child_event_local(*target) == 0;
            ok &= try_signal_child_event_local(*target) == 0;
            ok &= target->signal_pending_bits() == CHILD && target->signal_mask_bits() == mask;
            ok &= target->credentials() == credentials && target->ref_count.load() == 1 && target->local_signal_admissions_drained();
            target->signal_clear_pending_mask(~uint64_t{0});
        }
    }
    bool interleaved = false;
    ok &= child_event_signal_local(*target, [&] {
              interleaved = true;
              ok &= !target->local_signal_admissions_drained();
              target->publish_accepted_signal_route(0x7B7E001, 123);
              auto const AFTER = target->begin_local_signal_admission();
              ok &= AFTER.remote && !AFTER.local_lease;
          }) == 0;
    ok &= interleaved && ker::net::wki::wki_exec_signal_snapshot(*target).pending == CHILD;
    target->signal_clear_pending_mask(~uint64_t{0});
    ok &= !try_signal_child_event_local(*target).has_value() && target->signal_pending_bits() == 0;
    target->exit_in_progress = true;
    ok &= try_signal_child_event_local(*target) == -ESRCH;
    return ok && target->ref_count.load() == 1 && target->local_signal_admissions_drained();
}

auto process_selftest_parent_death_cutover() -> bool {
    using namespace ker::mod;
    auto target = std::make_unique<sched::task::Task>();
    target->type = sched::task::TaskType::PROCESS;
    target->hide_group_signals_until_publication();
    bool ok = true;
    auto call = [&] {
        if (!target->try_acquire()) return -ESRCH;
        return signal_parent_death(*target, ker::net::wki::WKI_PARENT_DEATH_HOPS);
    };
    for (uint32_t uid : {0U, 100U, 104U, 200U}) {
        security::Snapshot credentials;
        ok &= security::Snapshot::bootstrap_root().prepare({.change = security::Change::SET_UID, .real = uid}, credentials) == 0;
        ok &= sched::resource::initialize_credentials(*target, credentials) == 0;
        for (uint32_t registration : {0U, 10U, 12U, 64U, 65U}) {
            target->parent_death_signal.store(registration);
            ok &= call() == (registration <= 64 ? 0 : -EINVAL);
            uint64_t const EXPECTED = registration == 0 || registration > 64 ? 0 : uint64_t{1} << (registration - 1);
            ok &= target->signal_pending_bits() == EXPECTED && target->credentials() == credentials;
            ok &= target->ref_count.load() == 1 && target->local_signal_admissions_drained();
            target->signal_clear_pending_mask(~uint64_t{0});
        }
    }
    target->parent_death_signal.store(10);
    bool interleaved = false;
    ok &= parent_death_signal(*target, ker::net::wki::WKI_PARENT_DEATH_HOPS, [&] {
              interleaved = true;
              ok &= !target->local_signal_admissions_drained() && target->signal_pending_bits() == 0;
              target->publish_accepted_signal_route(0x7B7B0001, 99);
              ok &= !target->local_signal_admissions_drained();
              auto const AFTER = target->begin_local_signal_admission();
              ok &= AFTER.remote && !AFTER.local_lease;
          }) == 0;
    auto const SNAPSHOT = ker::net::wki::wki_exec_signal_snapshot(*target);
    ok &= interleaved && SNAPSHOT.parent_death_signal == 10 && SNAPSHOT.pending == (uint64_t{1} << 9);
    target->signal_clear_pending_mask(~uint64_t{0});
    // No submitted row: an accepted route never falls back to the source proxy.
    ok &= call() == -ESRCH && target->signal_pending_bits() == 0;
    target->parent_death_signal.store(0);
    ok &= call() == -ESRCH && target->signal_pending_bits() == 0;
    return ok && target->ref_count.load() == 1 && target->local_signal_admissions_drained();
}

auto process_selftest_terminal_source_cutover() -> bool {
    using namespace ker::mod;
    sched::session::Identity const EXPECTED{
        .session_id = 41, .pgid = 42, .terminal = {.value = 0x101}, .terminal_generation = 7, .group_origin = {.node = 18, .boot = 10}};
    bool ok = true;
    for (bool thread : {false, true}) {
        auto owner = std::make_unique<sched::task::Task>();
        auto worker = std::make_unique<sched::task::Task>();
        owner->type = worker->type = sched::task::TaskType::PROCESS;
        owner->pid = 0x7B7A0001;
        worker->pid = 0x7B7A0002;
        worker->is_thread = true;
        worker->owner_pid = owner->pid;
        if (!sched::resource::bind_thread_owner(*worker, *owner)) return false;
        owner->initialize_job_identity(EXPECTED);
        auto& target = thread ? *worker : *owner;
        target.hide_group_signals_until_publication();
        // The source callback may already own publication (leader detach).
        // These private Tasks never enter the scheduler or send a packet.
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        auto call = [&](bool revoked = false, int signal = 2) { return signal_terminal_source_as(target, EXPECTED, signal, revoked); };
        for (unsigned mutation = 0; mutation < 8; ++mutation) {
            auto changed = EXPECTED;
            switch (mutation) {
                case 0:
                    ++changed.session_id;
                    break;
                case 1:
                    ++changed.pgid;
                    break;
                case 2:
                    ++changed.origin.node;
                    break;
                case 3:
                    ++changed.origin.boot;
                    break;
                case 4:
                    ++changed.group_origin.node;
                    break;
                case 5:
                    ++changed.group_origin.boot;
                    break;
                case 6:
                    ++changed.terminal.value;
                    break;
                case 7:
                    ++changed.terminal_generation;
                    break;
            }
            owner->initialize_job_identity(changed);
            ok &= call() == -ESRCH && target.signal_pending_bits() == 0 && target.local_signal_admissions_drained();
        }
        owner->initialize_job_identity(EXPECTED);
        ok &= call(false, 0) == -EINVAL && call(false, 65) == -EINVAL;
        for (uint32_t uid : {0U, 100U, 104U, 200U}) {
            security::Snapshot credentials;
            ok &= security::Snapshot::bootstrap_root().prepare({.change = security::Change::SET_UID, .real = uid}, credentials) == 0;
            ok &= sched::resource::initialize_credentials(*owner, credentials) == 0;
            ok &= call() == 0 && target.signal_pending_bits() == 2 && target.credentials() == credentials;
            target.signal_clear_pending_mask(~uint64_t{0});
        }
        auto cleared = EXPECTED;
        cleared.terminal = {};
        cleared.terminal_generation = 0;
        owner->initialize_job_identity(cleared);
        ok &= call() == -ESRCH && call(true, 1) == 0 && target.signal_pending_bits() == 1;
        target.signal_clear_pending_mask(~uint64_t{0});
        owner->initialize_job_identity(EXPECTED);
        bool interleaved = false;
        int const RESULT = terminal_source_signal(target, EXPECTED, 2, false, [&] {
            interleaved = true;
            ok &= !target.local_signal_admissions_drained() && target.signal_pending_bits() == 0;
            target.publish_accepted_signal_route(0x7B7A0001, 99);
            // The old no-route observation allowed capture here; admission
            // now forces exec's actual drain predicate to remain false.
            ok &= !target.local_signal_admissions_drained();
            auto const AFTER = target.begin_local_signal_admission();
            ok &= AFTER.remote && !AFTER.local_lease;
        });
        ok &= RESULT == 0 && interleaved && target.local_signal_admissions_drained();
        ok &= ker::net::wki::wki_exec_signal_snapshot(target).pending == 2;
        target.signal_clear_pending_mask(~uint64_t{0});
        // Accepted route without a submitted row: refusal never falls back.
        ok &= call() == -ESRCH && target.signal_pending_bits() == 0 && target.local_signal_admissions_drained();
        ok &= owner->ref_count.load() == 2 && worker->ref_count.load() == 1;
    }
    return ok;
}

auto process_selftest_terminal_signal_authority() -> bool {
    using namespace ker::mod;
    sched::task::Task target{};
    target.type = sched::task::TaskType::PROCESS;
    target.pid = 0x7B790001;
    target.hide_group_signals_until_publication();
    sched::session::Identity const EXPECTED{
        .session_id = 41, .pgid = 42, .origin = {.node = 17, .boot = 9}, .group_origin = {.node = 18, .boot = 10}};
    target.initialize_job_identity(EXPECTED);
    auto call = [&](int signal = 2) {
        if (!target.try_acquire()) return -ESRCH;
        return signal_terminal_as(target, EXPECTED, signal, ker::net::wki::WKI_TERMINAL_SIGNAL_HOPS);
    };
    bool ok = true;
    for (unsigned mutation = 0; mutation < 6; ++mutation) {
        auto changed = EXPECTED;
        switch (mutation) {
            case 0:
                ++changed.session_id;
                break;
            case 1:
                ++changed.pgid;
                break;
            case 2:
                ++changed.origin.node;
                break;
            case 3:
                ++changed.origin.boot;
                break;
            case 4:
                ++changed.group_origin.node;
                break;
            case 5:
                ++changed.group_origin.boot;
                break;
        }
        target.initialize_job_identity(changed);
        ok &= call() == -ESRCH && target.signal_pending_bits() == 0 && target.local_signal_admissions_drained();
    }
    target.initialize_job_identity(EXPECTED);
    ok &= call(0) == -EINVAL && call(65) == -EINVAL && target.signal_pending_bits() == 0;
    for (uint32_t uid : {0U, 100U, 104U, 200U}) {
        security::Snapshot credentials;
        ok &= security::Snapshot::bootstrap_root().prepare({.change = security::Change::SET_UID, .real = uid}, credentials) == 0;
        ok &= sched::resource::initialize_credentials(target, credentials) == 0;
        ok &= call() == 0 && target.signal_pending_bits() == 2 && target.credentials() == credentials;
        target.signal_clear_pending_mask(~uint64_t{0});
    }
    target.publish_accepted_signal_route(0x7B790001, 99);
    // No submitted row: a present remote refusal must never touch the old frame.
    ok &= call() == -ESRCH && target.signal_pending_bits() == 0;
    return ok && target.ref_count.load() == 1 && target.local_signal_admissions_drained();
}

auto process_selftest_fd_clone_failure_releases_refs() -> bool {
    ker::mod::sched::task::Task parent{};
    ker::mod::sched::task::Task child{};
    ker::vfs::File file{};
    file.refcount.store(1, std::memory_order_relaxed);

    constexpr uint64_t FD = 17;
    if (!parent.fd_table.insert(FD, &file)) {
        return false;
    }

    g_process_selftest_force_fd_clone_insert_failure.store(true, std::memory_order_relaxed);
    bool const CLONED = clone_fd_table_shared_checked(&parent, &child);
    g_process_selftest_force_fd_clone_insert_failure.store(false, std::memory_order_relaxed);

    bool ok = !CLONED && file.refcount.load(std::memory_order_relaxed) == 1 && child.fd_table.lookup(FD) == nullptr;
    parent.fd_table.remove(FD);
    return ok;
}

auto process_selftest_fork_child_preserves_entry_registers() -> bool {
    ker::mod::cpu::GPRegs entry_regs{};
    entry_regs.r15 = 0x00007fff'fffff150ULL;
    entry_regs.r14 = 0x00007fff'fffff140ULL;
    entry_regs.r13 = 0x00007fff'fffff130ULL;
    entry_regs.r12 = 0x00007fff'fffff120ULL;
    entry_regs.r11 = 0x246;
    entry_regs.rbp = 0x00007fff'fffff100ULL;
    entry_regs.rcx = 0x00000000'00401000ULL;
    entry_regs.rbx = 0x00007fff'fffff0b0ULL;
    entry_regs.rax = 0x1234;

    ker::mod::cpu::GPRegs const CHILD_REGS = fork_child_return_regs(entry_regs);
    return CHILD_REGS.r15 == entry_regs.r15 && CHILD_REGS.r14 == entry_regs.r14 && CHILD_REGS.r13 == entry_regs.r13 &&
           CHILD_REGS.r12 == entry_regs.r12 && CHILD_REGS.r11 == entry_regs.r11 && CHILD_REGS.rbp == entry_regs.rbp &&
           CHILD_REGS.rcx == entry_regs.rcx && CHILD_REGS.rbx == entry_regs.rbx && CHILD_REGS.rax == 0;
}
#endif

auto personality(uint64_t persona) -> uint64_t { return wos_proc_personality(persona); }

auto set_creation_mask(ker::mod::sched::task::Task& subject, uint32_t mask) -> int {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (subject.credential_owner() == nullptr) return -ESRCH;
    if (request_replaced_exec_thread_retirement(subject)) return -ECANCELED;
    return static_cast<int>(subject.exchange_creation_mask(mask));
}

auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, uint64_t a6, ker::mod::cpu::GPRegs& gpr)
    -> uint64_t {
    switch (op) {
        case abi::process::procmgmt_ops::GETRLIMIT:
            return ker::mod::sched::resource::syscall_limit(a2, a3, a4, false);
        case abi::process::procmgmt_ops::SETRLIMIT:
            return ker::mod::sched::resource::syscall_limit(a2, a3, a4, true);
        case abi::process::procmgmt_ops::RESOURCE_USAGE:
            return ker::mod::sched::resource::syscall_limit(a2, a3, a4, false, true);
        case abi::process::procmgmt_ops::SERVICE_RESOURCE_CONFIGURE:
            return ker::mod::sched::resource::syscall_service_configure(a2);
        case abi::process::procmgmt_ops::SERVICE_RESOURCE_SELECT:
            return ker::mod::sched::resource::syscall_service_select(a2);
        case abi::process::procmgmt_ops::SERVICE_RESOURCE_READ:
            return ker::mod::sched::resource::syscall_service_read(a2, a3, a4);
        case abi::process::procmgmt_ops::EXIT:
            wos_proc_exit(static_cast<int>(a2));
            __builtin_unreachable();
        case abi::process::procmgmt_ops::EXEC: {
            return wos_proc_exec(a2, a3, a4);
        }
        case abi::process::procmgmt_ops::SPAWN: {
            return wos_proc_spawn(a2, a3, a4, a5);
        }
        case abi::process::procmgmt_ops::INIT_CONTROL_SUBMIT: {
            return wos_proc_init_control_submit(a2);
        }
        case abi::process::procmgmt_ops::INIT_CONTROL_RECEIVE: {
            return wos_proc_init_control_receive(a2);
        }
        case abi::process::procmgmt_ops::INIT_STATUS_PUBLISH: {
            return wos_proc_init_status_publish(a2);
        }
        case abi::process::procmgmt_ops::INIT_STATUS_READ: {
            return wos_proc_init_status_read(a2);
        }
        case abi::process::procmgmt_ops::WAITPID: {
            return wos_proc_waitpid(static_cast<int64_t>(a2), reinterpret_cast<int32_t*>(a3), static_cast<int32_t>(a4), a5, gpr);
        }
        case abi::process::procmgmt_ops::GETPID: {
            return wos_proc_getpid();
        }
        case abi::process::procmgmt_ops::GETPPID: {
            return wos_proc_getppid();
        }
        case abi::process::procmgmt_ops::FORK: {
            return wos_proc_fork(gpr);
        }
        case abi::process::procmgmt_ops::SIGACTION: {
            return wos_proc_sigaction(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::SIGPROCMASK: {
            return wos_proc_sigprocmask(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::SIGPENDING: {
            return wos_proc_sigpending(a2);
        }
        case abi::process::procmgmt_ops::SIGSUSPEND: {
            return wos_proc_sigsuspend(a2, gpr);
        }
        case abi::process::procmgmt_ops::UNAME: {
            return wos_proc_uname(reinterpret_cast<KernelUtsname*>(a2));
        }
        case abi::process::procmgmt_ops::CLONE_VM_PROC: {
            return wos_proc_clone_vm(a2);
        }
        case abi::process::procmgmt_ops::PRCTL: {
            return wos_proc_prctl(static_cast<int>(a2), a3, a4, a5, a6);
        }
        case abi::process::procmgmt_ops::ARCH_PRCTL: {
            return wos_proc_arch_prctl(static_cast<int>(a2), a3);
        }
        case abi::process::procmgmt_ops::SIGALTSTACK: {
            return wos_proc_sigaltstack(reinterpret_cast<const KernelStackT*>(a2), reinterpret_cast<KernelStackT*>(a3), gpr);
        }
        case abi::process::procmgmt_ops::KILL: {
            return wos_proc_kill(static_cast<int64_t>(a2), static_cast<int>(a3));
        }
        case abi::process::procmgmt_ops::SIGRETURN: {
            // Signal the asm-level check_pending_signals to restore the saved context
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->do_sigreturn = true;
            }
            return 0;
        }

        // --- POSIX credential syscalls ---
        case abi::process::procmgmt_ops::GETUID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->credentials().user().real : 0;
        }
        case abi::process::procmgmt_ops::GETEUID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->credentials().user().effective : 0;
        }
        case abi::process::procmgmt_ops::GETGID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->credentials().group().real : 0;
        }
        case abi::process::procmgmt_ops::GETEGID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->credentials().group().effective : 0;
        }
        case abi::process::procmgmt_ops::GETRESUID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto const IDS = task->credentials().user();
            if (a2 == 0 || a3 == 0 || a4 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(IDS.real)) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a3, sizeof(IDS.effective)) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a4, sizeof(IDS.saved)) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a2, IDS.real) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, IDS.effective) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a4, IDS.saved)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }
        case abi::process::procmgmt_ops::GETRESGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto const IDS = task->credentials().group();
            if (a2 == 0 || a3 == 0 || a4 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(IDS.real)) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a3, sizeof(IDS.effective)) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a4, sizeof(IDS.saved)) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a2, IDS.real) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, IDS.effective) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a4, IDS.saved)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }
        case abi::process::procmgmt_ops::GETGROUPS: {
            return wos_proc_getgroups(static_cast<size_t>(a2), a3);
        }
        case abi::process::procmgmt_ops::SETUID:
        case abi::process::procmgmt_ops::SETGID:
        case abi::process::procmgmt_ops::SETEUID:
        case abi::process::procmgmt_ops::SETEGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return static_cast<uint64_t>(-ESRCH);
            using ker::mod::security::Change;
            auto change = Change::SET_UID;
            if (op == abi::process::procmgmt_ops::SETGID) change = Change::SET_GID;
            if (op == abi::process::procmgmt_ops::SETEUID) change = Change::SET_EUID;
            if (op == abi::process::procmgmt_ops::SETEGID) change = Change::SET_EGID;
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            return static_cast<uint64_t>(ker::mod::sched::resource::change_credentials(
                *task, {.change = change, .real = static_cast<uint32_t>(a2), .effective = static_cast<uint32_t>(a2)}));
        }
        case abi::process::procmgmt_ops::SETGROUPS: {
            return wos_proc_setgroups(static_cast<size_t>(a2), a3);
        }
        case abi::process::procmgmt_ops::GETUMASK: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->creation_mask() : 022;
        }
        case abi::process::procmgmt_ops::SETUMASK: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            return static_cast<uint64_t>(set_creation_mask(*task, static_cast<uint32_t>(a2)));
        }

        case abi::process::procmgmt_ops::SETSID: {
            auto* task = ker::mod::sched::get_current_task();
            return task != nullptr ? static_cast<uint64_t>(ker::mod::sched::session::create(*task)) : static_cast<uint64_t>(-ESRCH);
        }
        case abi::process::procmgmt_ops::GETSID: {
            auto pid = static_cast<int64_t>(a2);
            if (pid == 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                return task->job_identity().session_id;
            }
            auto* target = find_process_leader_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto sid = target->job_identity().session_id;
            target->release();
            return sid;
        }
        case abi::process::procmgmt_ops::SETPGID: {
            auto* task = ker::mod::sched::get_current_task();
            return task != nullptr ? static_cast<uint64_t>(
                                         ker::mod::sched::session::set_group(*task, static_cast<int64_t>(a2), static_cast<int64_t>(a3)))
                                   : static_cast<uint64_t>(-ESRCH);
        }
        case abi::process::procmgmt_ops::GETPGID: {
            auto pid = static_cast<int64_t>(a2);
            if (pid == 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                return task->job_identity().pgid;
            }
            auto* target = find_process_leader_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto pgid = target->job_identity().pgid;
            target->release();
            return pgid;
        }
        case abi::process::procmgmt_ops::EXECVE: {
            // POSIX replace-process execve
            return wos_proc_execve(a2, a3, a4, gpr);
        }
        case abi::process::procmgmt_ops::GETHOSTNAME: {
            auto bufsize = static_cast<size_t>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto const NAME = ker::util::hostname::snapshot();
            size_t const LEN = std::strlen(NAME.data());
            if (LEN + 1 > bufsize) {
                return static_cast<uint64_t>(-ENAMETOOLONG);
            }
            return ker::mod::sys::usercopy::copy_to_task(*task, a2, NAME.data(), LEN + 1) ? 0 : static_cast<uint64_t>(-EFAULT);
        }
        case abi::process::procmgmt_ops::SETHOSTNAME: {
            auto len = static_cast<size_t>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto const CREDENTIALS = task->credentials();
            int const AUTHORIZATION = ker::mod::security::authorize_hostname_change(CREDENTIALS);
            if (AUTHORIZATION != 0) {
                return static_cast<uint64_t>(AUTHORIZATION);
            }
            if (a2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (len == 0 || len >= ker::util::hostname::HOSTNAME_MAX) {
                return static_cast<uint64_t>(-EINVAL);
            }
            std::array<char, ker::util::hostname::HOSTNAME_MAX> name{};
            if (!ker::mod::sys::usercopy::copy_from_task(*task, a2, name.data(), len)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            int const R = ker::util::hostname::set(name.data(), len);
            return (R < 0) ? static_cast<uint64_t>(R) : 0;
        }
        case abi::process::procmgmt_ops::SETPRIORITY: {
            return wos_proc_setpriority(static_cast<int>(a2), static_cast<int64_t>(a3), static_cast<int>(a4));
        }
        case abi::process::procmgmt_ops::GETPRIORITY: {
            return wos_proc_getpriority(static_cast<int>(a2), static_cast<int64_t>(a3));
        }
        case abi::process::procmgmt_ops::SETWKITARGET: {
            return wos_proc_setwkitarget(a2, static_cast<size_t>(a3), static_cast<uint32_t>(a4));
        }
        case abi::process::procmgmt_ops::GETWKITARGET: {
            return wos_proc_getwkitarget(a2, static_cast<size_t>(a3), a4);
        }
        case abi::process::procmgmt_ops::PTRACE: {
            return ker::mod::debug::ptrace::sys_ptrace(static_cast<abi::ptrace::request>(a2), a3, a4, a5, gpr);
        }

        default:
            process_log::warn("unknown op %llu", static_cast<unsigned long long>(op));
            return static_cast<uint64_t>(ENOSYS);
    }
}
}  // namespace ker::syscall::process
