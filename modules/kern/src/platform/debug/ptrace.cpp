#include "ptrace.hpp"

#include <abi/ptrace.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <net/wki/remote_compute.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/loader/runtime_images.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/debug_state.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <type_traits>
#include <utility>
#ifdef WOS_SELFTEST
#include <platform/mm/phys.hpp>
#endif

#include "platform/asm/cpu.hpp"

namespace ker::mod::debug::ptrace {

struct PendingBuffer;

// All ownership belongs to the executing Task, including when a fatal exit
// abandons this stack. Namespace admission is retained without holding a lock.
class PendingOperation {
   public:
    using Task = ker::mod::sched::task::Task;
    explicit PendingOperation(Task& executor) : executor_(&executor), next_(executor.pending_ptrace_operations) {
        executor.pending_ptrace_operations = this;
    }
    PendingOperation(const PendingOperation&) = delete;
    auto operator=(const PendingOperation&) -> PendingOperation& = delete;
    ~PendingOperation() { cancel(); }
    void own_target(Task& target) { target_ = &target; }
    auto admit() -> int {
        if (executor_ == nullptr) return -ESRCH;
        auto const* scope = executor_->pid_namespace();
        if (scope == nullptr || (!scope->is_host() && executor_->namespace_tid() == 0)) return -ESRCH;
        return scope_.begin(*scope);
    }
    auto private_scope() const -> const ker::mod::sched::pid_ns::NamespaceRef* {
        auto const* scope = scope_.scope();
        return scope != nullptr && !scope->is_host() ? scope : nullptr;
    }
    auto visible_tid(const Task& target) const -> uint64_t { return private_scope() != nullptr ? target.namespace_tid() : target.pid; }
    // Caller holds VM publication. A lease covers only this nonblocking effect,
    // never an entire syscall wait which could resume the target's own exec.
    auto begin_effect(Task& target, bool allow_exited) -> int {
        if (executor_ == nullptr || !scope_.active()) return -ESRCH;
        auto const* scope = private_scope();
        if (scope == nullptr) return 0;
        if (local_effect_ != nullptr) return -EBUSY;
        if (!ker::mod::sched::task::same_pid_namespace(*executor_, target) || target.namespace_tid() == 0) return -ESRCH;
        ker::mod::sched::pid_ns::PublicationGuard const ADMISSION(*scope);
        if (!ADMISSION.open()) return -EIDRM;
        if (target.type != ker::mod::sched::task::TaskType::PROCESS) return -EPERM;
        if ((target.has_exited || target.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) &&
            !(allow_exited && target.exit_notify_ready.load(std::memory_order_acquire) && target.has_exited))
            return -ESRCH;
        auto const* owner = target.credential_owner();
        if (target.is_thread && (owner == nullptr || target.pagemap != owner->pagemap)) return -ESRCH;
        auto const LOCAL = target.begin_local_signal_admission();
        if (LOCAL.remote) return -ENOTSUP;
        if (!LOCAL.local_lease) return -EAGAIN;
        local_effect_ = &target;
        return 0;
    }
    void finish_effect() {
        if (auto* target = std::exchange(local_effect_, nullptr); target != nullptr) target->end_local_signal_admission();
    }
    void cancel();

   private:
    friend struct PendingBuffer;
    Task* executor_{};
    PendingOperation* next_{};
    Task* target_{};
    Task* local_effect_{};
    ker::mod::sched::pid_ns::OperationLease scope_{};
    PendingBuffer* buffers_{};
};

struct PendingBuffer {
    PendingOperation* owner{};
    PendingBuffer* next{};
    void (*cleanup)(PendingBuffer&) noexcept {};
    explicit PendingBuffer(PendingOperation* operation) : owner(operation) {
        if (owner != nullptr) {
            next = owner->buffers_;
            owner->buffers_ = this;
        }
    }
    void cancel() {
        if (owner != nullptr) {
            auto** link = &owner->buffers_;
            while (*link != this) link = &(*link)->next;
            *link = next;
            owner = nullptr;
        }
        if (cleanup != nullptr) cleanup(*this);
    }
};

template <typename T, bool Array = false>
struct OwnedBuffer : PendingBuffer {
    using Storage = std::conditional_t<Array, std::unique_ptr<T[]>, std::unique_ptr<T>>;
    Storage allocation;
    OwnedBuffer(PendingOperation* operation, T* value) : PendingBuffer(operation), allocation(value) {
        cleanup = [](PendingBuffer& buffer) noexcept { static_cast<OwnedBuffer&>(buffer).allocation.reset(); };
    }
    ~OwnedBuffer() { cancel(); }
    auto get() const -> T* { return allocation.get(); }
    void reset() { cancel(); }
};

void PendingOperation::cancel() {
    if (executor_ != nullptr) {
        auto** link = &executor_->pending_ptrace_operations;
        while (*link != this) link = &(*link)->next_;
        *link = next_;
        executor_ = nullptr;
    }
    while (buffers_ != nullptr) buffers_->cancel();
    finish_effect();
    if (auto* target = std::exchange(target_, nullptr); target != nullptr) target->release();
    scope_.reset();
}

void cleanup_operations(ker::mod::sched::task::Task& executor) {
    while (executor.pending_ptrace_operations != nullptr) executor.pending_ptrace_operations->cancel();
}

namespace {
using log = ker::mod::dbg::logger<"ptrace">;
using Task = ker::mod::sched::task::Task;
namespace child_events = ker::syscall::process::child_events;

struct EffectAdmission {
    PendingOperation* operation;
    uint64_t result{static_cast<uint64_t>(-EAGAIN)};
    explicit EffectAdmission(PendingOperation* context) : operation(context) {}
    EffectAdmission(PendingOperation* context, Task& target, bool allow_exited = false) : operation(context) {
        begin(target, allow_exited);
    }
    void begin(Task& target, bool allow_exited = false) {
        result = 0;
        if (operation != nullptr) result = static_cast<uint64_t>(operation->begin_effect(target, allow_exited));
    }
    ~EffectAdmission() {
        if (operation != nullptr && result == 0) operation->finish_effect();
    }
};

constexpr uint64_t X86_RFLAGS_TF = 1ULL << 8;
constexpr uint64_t PTRACE_REMOTE_NODE_INVALID = 0xffff'ffff'ffff'ffffULL;
constexpr uint32_t STOP_STATUS_LOW = 0x7f;
constexpr uint64_t X86_DR6_BREAKPOINT_MASK = 0xf;
constexpr uint64_t X86_DR7_LOCAL_ENABLE_MASK = 0x1;
constexpr uint64_t X86_DR7_SLOT_ENABLE_STRIDE = 2;
constexpr uint64_t X86_DR7_SLOT_CONTROL_BASE = 16;
constexpr uint64_t X86_DR7_SLOT_CONTROL_STRIDE = 4;
constexpr uint64_t X86_DR7_SLOT_CONTROL_MASK = 0xf;
constexpr uint64_t X86_DR7_RW_MASK = 0x3;
constexpr uint64_t X86_DR7_RESERVED_ONES = 1ULL << 10;
constexpr uint64_t PTRACE_O_TRACESYSGOOD = 0x00000001;
constexpr uint32_t WOS_SIGSYS = 31;

auto as_error(int err) -> uint64_t { return static_cast<uint64_t>(-err); }

auto is_syscall_stop(abi::ptrace::stop_reason reason) -> bool {
    return reason == abi::ptrace::stop_reason::SYSCALL_ENTER || reason == abi::ptrace::stop_reason::SYSCALL_EXIT;
}

auto stop_signal(const Task& task) -> uint32_t {
    uint32_t signal = task.ptrace_stop_signal != 0 ? task.ptrace_stop_signal : SIGTRAP;
    if (is_syscall_stop(task.ptrace_stop_reason) && (task.ptrace_options & PTRACE_O_TRACESYSGOOD) != 0) {
        signal |= 0x80U;
    }
    return signal;
}

auto stop_wait_status(const Task& task) -> int32_t { return static_cast<int32_t>((stop_signal(task) << 8U) | STOP_STATUS_LOW); }

[[nodiscard]] auto publish_stop_event(Task& task) -> bool {
    bool const PUBLISHED = child_events::publish_ptrace_stop(task, stop_wait_status(task));
    // Exception publication runs after parking. Recheck here so a fatal wake
    // which won before the park cannot be lost. Later arrivals wake normally.
    // Neither a queued nor a claimed stop event may hold up termination.
    if (task.ptrace_termination_pending()) ker::mod::sched::wake_task_for_signal(&task);
    return PUBLISHED;
}

// Only syscall entry/exit stop boundaries call this: no operation-owned scoped
// resources remain on the current kernel stack. Preserve internal retirement
// requests (including a request arriving after the first check) through exit's
// existing disposition selection.
void exit_pending_syscall_stop(Task& task) {
    ker::syscall::process::exit_current_if_process_exit_requested();
    if ((task.signal_pending_bits() & (uint64_t{1} << 8)) != 0) ker::syscall::process::wos_proc_exit_signal(SIGKILL);
}

auto target_exited(const Task& target) -> bool { return target.exit_notify_ready.load(std::memory_order_acquire) && target.has_exited; }

auto is_wki_execve_proxy_wait(const Task& task) -> bool {
    return task.wki_proxy_task_id != 0 && task.wait_channel_is(ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
}

auto is_wki_execve_proxy_handoff(const Task& task) -> bool { return task.deferred_task_switch && is_wki_execve_proxy_wait(task); }

auto deferred_switch_is_sched_yield(const Task& task) -> bool {
    if (!task.yield_switch) return false;
    // Same-CPU syscall wakes also request a channel-free yield. Match the
    // scheduler's classification so these retain an observable syscall exit.
    if (task.wait_channel == nullptr || task.wait_channel_kind == ker::mod::sched::task::WaitChannelKind::NONE) return true;
    return task.wait_channel_kind == ker::mod::sched::task::WaitChannelKind::GENERIC && std::strcmp(task.wait_channel, "sched_yield") == 0;
}

auto should_suppress_deferred_syscall_exit_stop(const Task& task) -> bool {
    return is_wki_execve_proxy_handoff(task) || (task.deferred_task_switch && !deferred_switch_is_sched_yield(task));
}

auto should_wake_after_trace_clear(const Task& target, bool was_stopped, bool was_ptrace_wait) -> bool {
    return (was_stopped || was_ptrace_wait) && !is_wki_execve_proxy_wait(target);
}

auto process_id(const Task& task) -> uint64_t { return ker::mod::sched::task::process_pid(task); }

auto can_trace(const Task& tracer, const Task& target, ker::mod::security::Snapshot* admitted_target = nullptr) -> bool {
    if (target.type != ker::mod::sched::task::TaskType::PROCESS) {
        return false;
    }
    if (tracer.pid == target.pid) {
        return false;
    }
    auto const TRACER = tracer.credentials();
    auto const TARGET = target.credentials();
    if (!ker::mod::security::may_trace(TRACER, TARGET)) return false;
    if (admitted_target != nullptr) *admitted_target = TARGET;
    return true;
}

auto get_target(uint64_t pid, Task& tracer) -> Task* {
    if (pid == 0) {
        return &tracer;
    }
    auto* target = ker::mod::sched::find_task_by_visible_pid_safe(tracer, pid);
    if (target == nullptr) {
        return nullptr;
    }
    return target;
}

auto prepare_target_memory(Task& target, uint64_t address, size_t size, bool writable, const ker::mod::security::Snapshot& admitted_target,
                           PendingOperation* operation = nullptr) -> uint64_t {
    if (!ker::mod::sys::usercopy::range_valid(address, size)) return as_error(EFAULT);
    if (target.credentials() != admitted_target) return as_error(EPERM);
    // Faults may allocate or wait. Drop every temporary pin before taking the
    // image mutex: exec holds that mutex while waiting for page-access pins.
    for (size_t done = 0; done < size;) {
        uint64_t const CURRENT = address + done;
        EffectAdmission effect(operation);
        if (operation != nullptr && operation->private_scope() != nullptr) {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (target.credentials() != admitted_target) return as_error(EPERM);
            effect.begin(target);
            if (effect.result != 0) return effect.result;
        }
        ker::mod::sys::usercopy::StableUserPage pin;
        if (!ker::mod::sys::usercopy::pin_task_user_page(target, CURRENT, writable, true, pin)) return as_error(EFAULT);
        done += std::min(size - done, ker::mod::mm::paging::PAGE_SIZE - (CURRENT & (ker::mod::mm::paging::PAGE_SIZE - 1)));
    }
    return 0;
}

auto copy_prepared_task_memory(Task& target, uint64_t target_addr, void* kernel_buffer, size_t len, bool write_target, size_t* transferred,
                               const ker::mod::security::Snapshot& admitted_target, PendingOperation* operation = nullptr) -> uint64_t {
    if (transferred != nullptr) {
        *transferred = 0;
    }
    if (kernel_buffer == nullptr || !ker::mod::sys::usercopy::range_valid(target_addr, len)) {
        return as_error(EFAULT);
    }

    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;

    auto* bytes = static_cast<uint8_t*>(kernel_buffer);
    size_t done = 0;
    while (done < len) {
        uint64_t const CUR = target_addr + done;
        size_t const PAGE_LEFT = ker::mod::mm::paging::PAGE_SIZE - (CUR & (ker::mod::mm::paging::PAGE_SIZE - 1));
        size_t const CHUNK = std::min(PAGE_LEFT, len - done);
        ker::mod::sys::usercopy::StableUserPage pin;
        if (!ker::mod::sys::usercopy::pin_task_user_page(target, CUR, write_target, false, pin)) {
            break;
        }
        {
            ker::mod::sys::usercopy::UserAccessGuard const ACCESS{};
            auto* hhdm = static_cast<uint8_t*>(pin.kernel_address());
            if (write_target) {
                std::memcpy(hhdm, bytes + done, CHUNK);
            } else {
                std::memcpy(bytes + done, hhdm, CHUNK);
            }
        }
        bool const COMMITTED = write_target ? pin.commit_write() : pin.still_mapped();
        if (!COMMITTED) {
            break;
        }
        done += CHUNK;
    }

    if (transferred != nullptr) {
        *transferred = done;
    }
    return done == len ? 0 : as_error(EFAULT);
}

auto copy_task_memory(Task& target, uint64_t target_addr, void* kernel_buffer, size_t len, bool write_target, size_t* transferred,
                      const ker::mod::security::Snapshot& admitted_target, PendingOperation* operation = nullptr) -> uint64_t {
    if (transferred != nullptr) *transferred = 0;
    if (kernel_buffer == nullptr) return as_error(EFAULT);
    uint64_t const PREPARED = prepare_target_memory(target, target_addr, len, write_target, admitted_target, operation);
    if (PREPARED != 0 && PREPARED != as_error(EFAULT)) return PREPARED;
    // A later unmapped page must not discard the valid prefix promised by the
    // bulk fault ABI. The no-fault copy stops at that page and reports progress.
    return copy_prepared_task_memory(target, target_addr, kernel_buffer, len, write_target, transferred, admitted_target, operation);
}

auto write_prepared_task_word(Task& target, uint64_t target_addr, uint64_t word, const ker::mod::security::Snapshot& admitted_target,
                              PendingOperation* operation = nullptr) -> uint64_t {
    if (!ker::mod::sys::usercopy::range_valid(target_addr, sizeof(word))) {
        return as_error(EFAULT);
    }

    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;

    size_t const FIRST_SIZE =
        std::min<size_t>(sizeof(word), ker::mod::mm::paging::PAGE_SIZE - (target_addr & (ker::mod::mm::paging::PAGE_SIZE - 1)));
    ker::mod::sys::usercopy::StableUserPage first_page;
    if (!ker::mod::sys::usercopy::pin_task_user_page(target, target_addr, true, false, first_page)) {
        return as_error(EFAULT);
    }

    ker::mod::sys::usercopy::StableUserPage second_page;
    if (FIRST_SIZE != sizeof(word) &&
        !ker::mod::sys::usercopy::pin_task_user_page(target, target_addr + FIRST_SIZE, true, false, second_page)) {
        return as_error(EFAULT);
    }
    if (!first_page.still_mapped() || (second_page.valid() && !second_page.still_mapped())) {
        return as_error(EFAULT);
    }

    auto const* bytes = reinterpret_cast<const uint8_t*>(&word);
    {
        ker::mod::sys::usercopy::UserAccessGuard const ACCESS{};
        std::memcpy(first_page.kernel_address(), bytes, FIRST_SIZE);
        if (second_page.valid()) {
            std::memcpy(second_page.kernel_address(), bytes + FIRST_SIZE, sizeof(word) - FIRST_SIZE);
        }
    }
    // Both halves were written through HHDM aliases. Commit each leaf even if
    // the other half raced an invalidation so every still-live mapping gets
    // the architectural dirty state expected by msync and VM accounting.
    bool const FIRST_COMMITTED = first_page.commit_write();
    bool const SECOND_COMMITTED = !second_page.valid() || second_page.commit_write();
    return FIRST_COMMITTED && SECOND_COMMITTED ? 0 : as_error(EFAULT);
}

auto write_task_word(Task& target, uint64_t target_addr, uint64_t word, const ker::mod::security::Snapshot& admitted_target,
                     PendingOperation* operation = nullptr) -> uint64_t {
    uint64_t const PREPARED = prepare_target_memory(target, target_addr, sizeof(word), true, admitted_target, operation);
    return PREPARED != 0 ? PREPARED : write_prepared_task_word(target, target_addr, word, admitted_target, operation);
}

auto is_user_canonical(uint64_t value) -> bool { return value != 0 && value < 0x0000'8000'0000'0000ULL; }

auto is_kernel_stack(uint64_t value) -> bool { return ker::mod::mm::is_kernel_stack_address(value); }

auto stack_region_contains(const Task& target, uint64_t addr, size_t size) -> bool {
    if (!is_kernel_stack(target.context.syscall_kernel_stack) || size == 0) {
        return false;
    }
    uint64_t const STACK_TOP = target.context.syscall_kernel_stack;
    uint64_t const STACK_BASE = STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE;
    uint64_t const END = addr + static_cast<uint64_t>(size);
    return END >= addr && addr >= STACK_BASE && END <= STACK_TOP;
}

auto uses_stop_snapshot(const Task& target) -> bool {
    return target.ptrace_stopped && (is_syscall_stop(target.ptrace_stop_reason) || target.ptrace_stop_uses_syscall_snapshot);
}

auto syscall_scratch(Task& target) -> ker::mod::cpu::PerCpu* {
    if (target.context.syscall_scratch_area == 0) {
        return nullptr;
    }
    return reinterpret_cast<ker::mod::cpu::PerCpu*>(target.context.syscall_scratch_area);
}

void normalize_syscall_snapshot(Task& target) {
    target.ptrace_syscall_regs.rcx = target.ptrace_syscall_frame.rip;
    target.ptrace_syscall_regs.r11 = target.ptrace_syscall_frame.flags;
}

auto live_syscall_saved_regs(Task& target, const ker::mod::cpu::PerCpu& scratch) -> ker::mod::cpu::GPRegs* {
    if (scratch.syscall_stack != target.context.syscall_kernel_stack || !is_kernel_stack(scratch.syscall_stack)) {
        return nullptr;
    }

    uint64_t const REGS_ADDR = scratch.syscall_stack - sizeof(uint64_t) - sizeof(ker::mod::cpu::GPRegs);
    uint64_t const RETVAL_ADDR = scratch.syscall_stack - sizeof(uint64_t);
    if (!stack_region_contains(target, REGS_ADDR, sizeof(ker::mod::cpu::GPRegs)) ||
        !stack_region_contains(target, RETVAL_ADDR, sizeof(uint64_t))) {
        return nullptr;
    }
    return reinterpret_cast<ker::mod::cpu::GPRegs*>(REGS_ADDR);
}

auto publish_stop_snapshot_to_syscall_scratch(Task& target) -> bool {
    auto* scratch = syscall_scratch(target);
    if (scratch == nullptr) {
        return false;
    }
    normalize_syscall_snapshot(target);
    scratch->user_rsp = target.ptrace_syscall_frame.rsp;
    scratch->syscall_ret_rip = target.ptrace_syscall_frame.rip;
    scratch->syscall_ret_flags = target.ptrace_syscall_frame.flags;
    if (auto* regs = live_syscall_saved_regs(target, *scratch); regs != nullptr) {
        regs->rcx = target.ptrace_syscall_frame.rip;
        regs->r11 = target.ptrace_syscall_frame.flags;
    }
    return true;
}

auto capture_async_syscall_return_snapshot(Task& target) -> bool {
    target.ptrace_stop_uses_syscall_snapshot = false;
    if (target.type != ker::mod::sched::task::TaskType::PROCESS || (target.context.frame.cs & 0x3U) == 3) {
        return false;
    }

    if (ker::mod::sched::current_cpu_for_task(&target) != UINT64_MAX) {
        return false;
    }
    auto* scratch = syscall_scratch(target);
    if (scratch == nullptr || !is_user_canonical(scratch->syscall_ret_rip) || !is_user_canonical(scratch->user_rsp) ||
        (scratch->syscall_ret_flags & 0x2ULL) == 0) {
        return false;
    }

    auto* saved_regs = live_syscall_saved_regs(target, *scratch);
    if (saved_regs == nullptr) {
        return false;
    }
    target.ptrace_syscall_regs = *saved_regs;
    target.ptrace_syscall_regs.rcx = scratch->syscall_ret_rip;
    target.ptrace_syscall_regs.r11 = scratch->syscall_ret_flags;
    target.ptrace_syscall_frame.int_num = 0;
    target.ptrace_syscall_frame.err_code = 0;
    target.ptrace_syscall_frame.rip = scratch->syscall_ret_rip;
    target.ptrace_syscall_frame.cs = ker::mod::desc::gdt::GDT_USER_CS;
    target.ptrace_syscall_frame.flags = scratch->syscall_ret_flags;
    target.ptrace_syscall_frame.rsp = scratch->user_rsp;
    target.ptrace_syscall_frame.ss = ker::mod::desc::gdt::GDT_USER_DS;
    target.ptrace_stop_uses_syscall_snapshot = true;
    return true;
}

auto stopped_user_frame(Task& target) -> ker::mod::gates::InterruptFrame* {
    if (!target.ptrace_stop_blocks_execution()) {
        return nullptr;
    }
    if (!uses_stop_snapshot(target)) {
        if (ker::mod::sched::current_cpu_for_task(&target) != UINT64_MAX) {
            return nullptr;
        }
        if ((target.context.frame.cs & 3U) != 3) {
            (void)capture_async_syscall_return_snapshot(target);
        }
    }
    auto* frame = uses_stop_snapshot(target) ? &target.ptrace_syscall_frame : &target.context.frame;
    if (frame->cs != ker::mod::desc::gdt::GDT_USER_CS || frame->ss != ker::mod::desc::gdt::GDT_USER_DS || !is_user_canonical(frame->rip) ||
        !is_user_canonical(frame->rsp)) {
        return nullptr;
    }
    return frame;
}

auto fill_gprs(Task& target, abi::ptrace::X86_64GprState& out) -> bool {
    if (stopped_user_frame(target) == nullptr) {
        return false;
    }
    bool const USE_SYSCALL_SNAPSHOT = uses_stop_snapshot(target);
    const auto& regs = USE_SYSCALL_SNAPSHOT ? target.ptrace_syscall_regs : target.context.regs;
    const auto& frame = USE_SYSCALL_SNAPSHOT ? target.ptrace_syscall_frame : target.context.frame;
    out.rax = regs.rax;
    out.rbx = regs.rbx;
    out.rcx = regs.rcx;
    out.rdx = regs.rdx;
    out.rsi = regs.rsi;
    out.rdi = regs.rdi;
    out.rbp = regs.rbp;
    out.rsp = frame.rsp;
    out.r8 = regs.r8;
    out.r9 = regs.r9;
    out.r10 = regs.r10;
    out.r11 = regs.r11;
    out.r12 = regs.r12;
    out.r13 = regs.r13;
    out.r14 = regs.r14;
    out.r15 = regs.r15;
    out.rip = frame.rip;
    out.rflags = frame.flags;
    out.cs = frame.cs;
    out.ss = frame.ss;
    out.fs_base = target.thread != nullptr ? target.thread->fsbase : 0;
    out.gs_base = target.thread != nullptr ? target.thread->gsbase : 0;
    return true;
}

auto set_gprs(Task& target, const abi::ptrace::X86_64GprState& in) -> uint64_t {
    if (!ker::mod::security::valid_debug_gprs(in, ker::mod::desc::gdt::GDT_USER_CS, ker::mod::desc::gdt::GDT_USER_DS)) {
        return as_error(EINVAL);
    }
    if (stopped_user_frame(target) == nullptr || (uses_stop_snapshot(target) && syscall_scratch(target) == nullptr)) {
        return as_error(EIO);
    }
    bool const USE_SYSCALL_SNAPSHOT = uses_stop_snapshot(target);
    auto& regs = USE_SYSCALL_SNAPSHOT ? target.ptrace_syscall_regs : target.context.regs;
    auto& frame = USE_SYSCALL_SNAPSHOT ? target.ptrace_syscall_frame : target.context.frame;
    regs.rax = in.rax;
    regs.rbx = in.rbx;
    regs.rcx = in.rcx;
    regs.rdx = in.rdx;
    regs.rsi = in.rsi;
    regs.rdi = in.rdi;
    regs.rbp = in.rbp;
    frame.rsp = in.rsp;
    regs.r8 = in.r8;
    regs.r9 = in.r9;
    regs.r10 = in.r10;
    regs.r11 = in.r11;
    regs.r12 = in.r12;
    regs.r13 = in.r13;
    regs.r14 = in.r14;
    regs.r15 = in.r15;
    frame.rip = in.rip;
    frame.flags = in.rflags;
    if (target.thread != nullptr) {
        target.thread->fsbase = in.fs_base;
        target.thread->gsbase = in.gs_base;
    }
    if (USE_SYSCALL_SNAPSHOT) {
        (void)publish_stop_snapshot_to_syscall_scratch(target);
    }
    return 0;
}

auto user_resume_frame_for_step(Task& target) -> ker::mod::gates::InterruptFrame* { return stopped_user_frame(target); }

void clear_task_trap_flags(Task& target) {
    target.context.frame.flags &= ~X86_RFLAGS_TF;
    target.ptrace_syscall_frame.flags &= ~X86_RFLAGS_TF;
}

auto record_signal_stop(Task& stopped, uint32_t signal) -> bool {
    if (!stopped.ptrace_traced || stopped.ptrace_tracer_pid == 0 ||
        stopped.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) {
        return false;
    }

    stopped.ptrace_stop_reason = abi::ptrace::stop_reason::SIGNAL;
    stopped.ptrace_stop_signal = signal;
    stopped.ptrace_stop_address = 0;
    stopped.ptrace_event_msg = 0;
    stopped.ptrace_stopped = true;
    stopped.ptrace_stop_pending = true;
    stopped.ptrace_single_step = false;
    stopped.set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    stopped.ptrace_stop_uses_syscall_snapshot = false;

    auto* current = ker::mod::sched::get_current_task();
    if (current == &stopped) {
        stopped.deferred_task_switch = true;
    } else {
        ker::mod::sched::debug_stop_task(&stopped);
        (void)capture_async_syscall_return_snapshot(stopped);
    }

    static_cast<void>(publish_stop_event(stopped));
    return true;
}

__attribute__((noinline, no_sanitize("address", "undefined", "coverage"))) void clear_local_trap_flag() {
    asm volatile(
        "pushfq\n\t"
        "andq %[mask], (%%rsp)\n\t"
        "popfq"
        :
        : [mask] "i"(~static_cast<int64_t>(X86_RFLAGS_TF))
        : "memory", "cc");
}

auto require_traced(Task& tracer, Task& target, ker::mod::security::Snapshot* admitted_target = nullptr) -> uint64_t {
    if (!target.ptrace_traced || target.ptrace_tracer_pid != tracer.pid || !can_trace(tracer, target, admitted_target)) {
        return as_error(EPERM);
    }
    return 0;
}

template <typename Admitted>
auto set_trace_options(Task& tracer, Task& target, uint64_t options, PendingOperation* operation, Admitted&& admitted) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    uint64_t const AUTH = require_traced(tracer, target);
    if (AUTH != 0) return AUTH;
    admitted();
    target.ptrace_options = options;
    return 0;
}

auto acknowledge_stop_for_resume(Task& tracer, Task& target) -> bool {
    if (!target.ptrace_stop_pending) {
        return true;
    }
    if (!child_events::acknowledge_ptrace_stop(target, tracer)) {
        return false;
    }
    target.ptrace_stop_pending = false;
    return true;
}

void clear_trace_state_and_wake(Task& target) {
    if (uses_stop_snapshot(target)) {
        (void)publish_stop_snapshot_to_syscall_scratch(target);
    }
    bool const WAS_STOPPED = target.ptrace_stopped;
    bool const WAS_PTRACE_WAIT = target.wait_channel_is(ker::mod::sched::task::WaitChannelKind::PTRACE);
    child_events::detach_tracer(target);
    target.ptrace_traced = false;
    target.ptrace_tracer_pid = 0;
    target.ptrace_options = 0;
    target.ptrace_event_msg = 0;
    target.ptrace_stop_reason = abi::ptrace::stop_reason::NONE;
    target.ptrace_stop_signal = 0;
    target.ptrace_stopped = false;
    target.ptrace_stop_pending = false;
    target.ptrace_single_step = false;
    target.ptrace_syscall_trace = false;
    target.ptrace_syscall_in_stop = false;
    target.ptrace_stop_uses_syscall_snapshot = false;
    target.ptrace_dr_addr = {};
    target.ptrace_dr6 = 0;
    target.ptrace_dr7 = 0;
    clear_task_trap_flags(target);
    if (WAS_PTRACE_WAIT) {
        target.clear_wait_channel();
    }
    if (should_wake_after_trace_clear(target, WAS_STOPPED, WAS_PTRACE_WAIT)) {
        ker::mod::sched::reschedule_task_for_cpu(target.cpu, &target);
    }
}

auto attach(Task& tracer, Task& target, bool seize, PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    if (!can_trace(tracer, target)) {
        return as_error(EPERM);
    }
    if (target.ptrace_traced && target.ptrace_tracer_pid != tracer.pid) {
        return as_error(EBUSY);
    }
    if (!child_events::attach_tracer(target, tracer)) {
        return as_error(EBUSY);
    }

    target.ptrace_traced = true;
    target.ptrace_tracer_pid = tracer.pid;
    target.ptrace_stop_reason = seize ? abi::ptrace::stop_reason::NONE : abi::ptrace::stop_reason::INTERRUPT;
    target.ptrace_stop_signal = seize ? 0 : SIGSTOP;
    target.ptrace_stopped = !seize;
    target.ptrace_stop_pending = !seize;
    target.ptrace_stop_uses_syscall_snapshot = false;
    if (target.ptrace_stopped) {
        target.set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
        ker::mod::sched::debug_stop_task(&target);
        (void)capture_async_syscall_return_snapshot(target);
        static_cast<void>(publish_stop_event(target));
    }
    log::debug("trace attach tracer=%lu target=%lu seize=%u", tracer.pid, target.pid, seize ? 1U : 0U);
    return 0;
}

auto detach(Task& tracer, Task& target, PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    EffectAdmission effect(operation, target, true);
    if (effect.result != 0) return effect.result;
    // Losing inspection authority must not prevent releasing an attachment.
    if (!target.ptrace_traced || target.ptrace_tracer_pid != tracer.pid) {
        return as_error(EPERM);
    }
    if (!acknowledge_stop_for_resume(tracer, target)) {
        return as_error(EBUSY);
    }
    clear_trace_state_and_wake(target);
    return 0;
}

auto resume_syscall_after_require(Task& tracer, Task& target) -> bool {
    if (!acknowledge_stop_for_resume(tracer, target)) {
        return false;
    }
    bool const USE_STOP_SNAPSHOT = uses_stop_snapshot(target);
    bool const WAS_STOPPED = target.ptrace_stopped;
    target.ptrace_stopped = false;
    target.ptrace_stop_pending = false;
    target.ptrace_single_step = false;
    target.ptrace_syscall_trace = true;
    if (target.ptrace_stop_reason != abi::ptrace::stop_reason::SYSCALL_ENTER) {
        target.ptrace_syscall_in_stop = false;
    }
    clear_task_trap_flags(target);
    if (USE_STOP_SNAPSHOT) {
        (void)publish_stop_snapshot_to_syscall_scratch(target);
    }
    target.ptrace_stop_uses_syscall_snapshot = false;
    if (WAS_STOPPED) {
        ker::mod::sched::reschedule_task_for_cpu(target.cpu, &target);
    }
    return true;
}

void fill_event(Task& target, abi::ptrace::Event& out, PendingOperation* operation = nullptr) {
    out.reason = target.ptrace_stop_reason;
    out.signal = stop_signal(target);
    out.reserved = 0;
    out.tid = operation != nullptr ? operation->visible_tid(target) : target.pid;
    out.address = target.ptrace_stop_address;
    out.message = target.ptrace_event_msg;
}

void fill_exit_stop_info(Task& target, abi::ptrace::StopInfo& out, PendingOperation* operation = nullptr) {
    std::memset(&out, 0, sizeof(out));
    out.event.reason = abi::ptrace::stop_reason::EXIT;
    out.event.tid = operation != nullptr ? operation->visible_tid(target) : target.pid;
    out.event.message = static_cast<uint64_t>(target.exit_status);
    out.flags = abi::ptrace::STOP_INFO_EXITED;
    out.wait_status = target.exit_status;
}

void fill_stop_info(Task& target, const child_events::ClaimedEvent& claimed, abi::ptrace::StopInfo& out,
                    PendingOperation* operation = nullptr) {
    out = {};
    if (claimed.kind == ker::mod::sched::task::ChildEventKind::EXIT) {
        fill_exit_stop_info(target, out, operation);
        out.event.message = static_cast<uint64_t>(claimed.status);
    } else {
        fill_event(target, out.event, operation);
        if (target.ptrace_stopped && fill_gprs(target, out.regs)) out.flags |= abi::ptrace::STOP_INFO_REGS_VALID;
    }
    out.wait_status = claimed.status;
}

void clear_tracer_wait_state(Task& tracer) {
    child_events::cancel_wait(tracer);
    tracer.clear_wait_channel();
}

void publish_tracer_wait(Task& tracer) { tracer.set_wait_channel("ptrace_wait", ker::mod::sched::task::WaitChannelKind::PTRACE); }

auto begin_syscall_wait(Task& tracer, Task& target, const ker::mod::security::Snapshot& admitted_target, bool& exited,
                        PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    EffectAdmission effect(operation, target, true);
    if (effect.result != 0) return effect.result;
    if (target.credentials() != admitted_target || !target.ptrace_traced || target.ptrace_tracer_pid != tracer.pid) return as_error(EPERM);
    exited = target_exited(target);
    if (exited) return 0;
    publish_tracer_wait(tracer);
    return resume_syscall_after_require(tracer, target) ? 0 : as_error(EBUSY);
}

// A stop reached after an authorized resume may follow exec or a credential
// change. Make its own current decision while capturing the owned result. The
// optional admitted value binds the initial, already-exited case to preflight.
auto capture_wait_event(Task& tracer, Task& target, const ker::mod::security::Snapshot* admitted_target,
                        child_events::ClaimedEvent& claimed, abi::ptrace::StopInfo& out, bool& ready, PendingOperation* operation = nullptr)
    -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    EffectAdmission effect(operation, target, true);
    if (effect.result != 0) return effect.result;
    if (admitted_target != nullptr) {
        if (target.credentials() != *admitted_target || !target.ptrace_traced || target.ptrace_tracer_pid != tracer.pid)
            return as_error(EPERM);
    } else {
        uint64_t const AUTH = require_traced(tracer, target);
        if (AUTH != 0) return AUTH;
    }
    bool const EXITED = target_exited(target);
    if (!EXITED && !target.ptrace_stopped) {
        ready = false;
        return 0;
    }
    // An unclaimed older stop may precede an exit produced by SIGKILL.
    if (EXITED && !acknowledge_stop_for_resume(tracer, target)) return as_error(EBUSY);
    auto const PROBE = child_events::claim_or_register(tracer, tracer, target.pid, child_events::OPTION_STOPPED, false, claimed);
    auto const EXPECTED = EXITED ? ker::mod::sched::task::ChildEventKind::EXIT : ker::mod::sched::task::ChildEventKind::PTRACE_STOP;
    if (PROBE != child_events::ProbeResult::CLAIMED || claimed.kind != EXPECTED) {
        if (PROBE == child_events::ProbeResult::CLAIMED) child_events::release_claim(tracer, claimed);
        return as_error(EBUSY);
    }
    fill_stop_info(target, claimed, out, operation);
    ready = true;
    return 0;
}

auto publish_wait_event(Task& tracer, uint64_t data, const child_events::ClaimedEvent& claimed, const abi::ptrace::StopInfo& out)
    -> uint64_t {
    if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, data, out)) {
        child_events::release_claim(tracer, claimed);
        clear_tracer_wait_state(tracer);
        return as_error(EFAULT);
    }
    if (!child_events::commit_claim(tracer, tracer, claimed, false)) {
        child_events::release_claim(tracer, claimed);
        clear_tracer_wait_state(tracer);
        return as_error(EBUSY);
    }
    clear_tracer_wait_state(tracer);
    return 0;
}

auto consume_exit_for_tracer(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                             PendingOperation* operation = nullptr) -> uint64_t {
    child_events::ClaimedEvent claimed{};
    abi::ptrace::StopInfo out{};
    bool ready = false;
    uint64_t const CAPTURE = capture_wait_event(tracer, target, &admitted_target, claimed, out, ready, operation);
    if (CAPTURE != 0 || !ready) {
        clear_tracer_wait_state(tracer);
        return CAPTURE != 0 ? CAPTURE : as_error(EBUSY);
    }
    return publish_wait_event(tracer, data, claimed, out);
}

auto syscall_wait(Task& tracer, Task& target, uint64_t data, PendingOperation* operation = nullptr) -> uint64_t {
    if (data == 0) return as_error(EFAULT);
    ker::mod::security::Snapshot admitted_target;
    uint64_t ret = require_traced(tracer, target, &admitted_target);
    if (ret != 0) return ret;
    if (!ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(abi::ptrace::StopInfo))) return as_error(EFAULT);
    bool exited = false;
    ret = begin_syscall_wait(tracer, target, admitted_target, exited, operation);
    if (ret != 0) {
        clear_tracer_wait_state(tracer);
        return ret;
    }
    if (exited) return consume_exit_for_tracer(tracer, target, data, admitted_target, operation);
    for (;;) {
        child_events::ClaimedEvent claimed{};
        abi::ptrace::StopInfo out{};
        bool ready = false;
        ret = capture_wait_event(tracer, target, nullptr, claimed, out, ready, operation);
        if (ret != 0) {
            clear_tracer_wait_state(tracer);
            return ret;
        }
        if (ready) return publish_wait_event(tracer, data, claimed, out);
        uint64_t deadline = 0;
        if (operation != nullptr && operation->private_scope() != nullptr) {
            auto const NOW = ker::mod::time::get_us();
            deadline = NOW > UINT64_MAX - 10'000 ? UINT64_MAX : NOW + 10'000;
        }
        ker::mod::sched::preemptible_syscall_park("ptrace_wait", ker::mod::sched::task::WaitChannelKind::PTRACE, deadline);
    }
}

using XStateBuffer = std::array<uint8_t, ker::mod::sched::task::FxState::XSAVE_AREA_SIZE>;

auto capture_gprs(Task& target, abi::ptrace::X86_64GprState& out, const ker::mod::security::Snapshot& admitted_target,
                  PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    return fill_gprs(target, out) ? 0 : as_error(EIO);
}

auto apply_gprs(Task& target, const abi::ptrace::X86_64GprState& input, const ker::mod::security::Snapshot& admitted_target,
                PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    return set_gprs(target, input);
}

auto capture_xstate(Task& target, XStateBuffer& out, const ker::mod::security::Snapshot& admitted_target,
                    PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    if (ker::mod::sched::current_cpu_for_task(&target) != UINT64_MAX) return as_error(EBUSY);
    if (stopped_user_frame(target) == nullptr) return as_error(EIO);
    std::memcpy(out.data(), target.fx_state.aligned(), out.size());
    return 0;
}

auto read_regset(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                 PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::RegsetIo io{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, io) || io.buffer == nullptr) {
        return as_error(EFAULT);
    }
    uint64_t const BUFFER_ADDR = reinterpret_cast<uint64_t>(io.buffer);
    if (io.kind == abi::ptrace::regset::X86_64_GPR) {
        if (io.size < sizeof(abi::ptrace::X86_64GprState)) {
            return as_error(EINVAL);
        }
        if (!ker::mod::sys::usercopy::ensure_writable(tracer, BUFFER_ADDR, sizeof(abi::ptrace::X86_64GprState)) ||
            !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(io))) {
            return as_error(EFAULT);
        }
        abi::ptrace::X86_64GprState out{};
        uint64_t const RESULT = capture_gprs(target, out, admitted_target, operation);
        if (RESULT != 0) return RESULT;
        if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, BUFFER_ADDR, out)) {
            return as_error(EFAULT);
        }
        io.size = sizeof(out);
    } else if (io.kind == abi::ptrace::regset::X86_64_XSAVE) {
        if (io.size < ker::mod::sched::task::FxState::XSAVE_AREA_SIZE) {
            return as_error(EINVAL);
        }
        XStateBuffer out{};
        uint64_t const RESULT = capture_xstate(target, out, admitted_target, operation);
        if (RESULT != 0) return RESULT;
        if (!ker::mod::sys::usercopy::ensure_writable(tracer, BUFFER_ADDR, ker::mod::sched::task::FxState::XSAVE_AREA_SIZE) ||
            !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(io)) ||
            !ker::mod::sys::usercopy::copy_to_task(tracer, BUFFER_ADDR, out.data(), out.size())) {
            return as_error(EFAULT);
        }
        io.size = ker::mod::sched::task::FxState::XSAVE_AREA_SIZE;
    } else {
        return as_error(EINVAL);
    }
    return ker::mod::sys::usercopy::copy_value_to_task(tracer, data, io) ? 0 : as_error(EFAULT);
}

auto supported_mxcsr_mask() -> uint32_t {
    alignas(16) std::array<uint8_t, 512> trusted{};
    asm volatile("fxsave64 %0" : "=m"(trusted) : : "memory");
    uint32_t mask = 0;
    std::memcpy(&mask, trusted.data() + 28, sizeof(mask));
    return mask != 0 ? mask : 0x0000ffbfU;
}

auto apply_xstate(Task& target, const XStateBuffer& state, const ker::mod::security::Snapshot& admitted_target,
                  PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    if (!ker::mod::security::valid_debug_xstate(state, ker::mod::cpu::xsave_feature_mask, supported_mxcsr_mask())) {
        return as_error(EINVAL);
    }
    if (ker::mod::sched::current_cpu_for_task(&target) != UINT64_MAX) return as_error(EBUSY);
    if (stopped_user_frame(target) == nullptr) return as_error(EIO);
    std::memcpy(target.fx_state.aligned(), state.data(), state.size());
    target.fx_state.saved = true;
    target.fx_state.live_saved = true;
    target.fx_state.initialized = true;
    return 0;
}

auto write_regset(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                  PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::RegsetIo io{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, io) || io.buffer == nullptr) {
        return as_error(EFAULT);
    }
    uint64_t const BUFFER_ADDR = reinterpret_cast<uint64_t>(io.buffer);
    if (io.kind == abi::ptrace::regset::X86_64_GPR) {
        if (io.size < sizeof(abi::ptrace::X86_64GprState)) {
            return as_error(EINVAL);
        }
        abi::ptrace::X86_64GprState in{};
        if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, BUFFER_ADDR, in)) {
            return as_error(EFAULT);
        }
        return apply_gprs(target, in, admitted_target, operation);
    }
    if (io.kind == abi::ptrace::regset::X86_64_XSAVE) {
        if (io.size < ker::mod::sched::task::FxState::XSAVE_AREA_SIZE) {
            return as_error(EINVAL);
        }
        XStateBuffer state{};
        if (!ker::mod::sys::usercopy::copy_from_task(tracer, BUFFER_ADDR, state.data(), state.size())) {
            return as_error(EFAULT);
        }
        return apply_xstate(target, state, admitted_target, operation);
    }
    return as_error(EINVAL);
}

auto capture_threads(Task& target, const ker::mod::security::Snapshot& admitted_target, uint64_t* tids, size_t capacity, size_t& count,
                     PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    uint64_t const GROUP = process_id(target);
    count = 0;
    for (size_t idx = 0; idx < capacity; ++idx) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(static_cast<uint32_t>(idx));
        if (candidate == nullptr) continue;
        if (process_id(*candidate) == GROUP &&
            (operation == nullptr || operation->private_scope() == nullptr ||
             (ker::mod::sched::task::same_pid_namespace(target, *candidate) && candidate->namespace_tid() != 0)))
            tids[count++] = operation != nullptr ? operation->visible_tid(*candidate) : candidate->pid;
        candidate->release();
    }
    return 0;
}

auto list_threads(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                  PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::ThreadList list{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, list) || (list.capacity != 0 && list.tids == nullptr) ||
        !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(list))) {
        return as_error(EFAULT);
    }

    size_t count = 0;
    uint64_t const ACTIVE_COUNT = ker::mod::sched::get_active_task_count();
    OwnedBuffer<uint64_t, true> storage(operation, ACTIVE_COUNT != 0 ? new (std::nothrow) uint64_t[ACTIVE_COUNT] : nullptr);
    auto* tids = storage.get();
    if (ACTIVE_COUNT != 0 && tids == nullptr) {
        return as_error(ENOMEM);
    }
    uint64_t const CAPTURE = capture_threads(target, admitted_target, tids, ACTIVE_COUNT, count, operation);
    if (CAPTURE != 0) {
        return CAPTURE;
    }

    size_t const COPY_COUNT = std::min(count, list.capacity);
    size_t const COPY_BYTES = COPY_COUNT * sizeof(uint64_t);
    uint64_t const TIDS_ADDR = reinterpret_cast<uint64_t>(list.tids);
    if ((COPY_BYTES != 0 && (!ker::mod::sys::usercopy::ensure_writable(tracer, TIDS_ADDR, COPY_BYTES) ||
                             !ker::mod::sys::usercopy::copy_to_task(tracer, TIDS_ADDR, tids, COPY_BYTES)))) {
        return as_error(EFAULT);
    }
    storage.reset();

    list.count = count;
    if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list)) {
        return as_error(EFAULT);
    }
    return count <= list.capacity ? 0 : as_error(ENOSPC);
}

auto copy_image_path(char* dst, size_t dst_size, const char* src) -> void {
    if (dst == nullptr || dst_size == 0) {
        return;
    }
    std::memset(dst, 0, dst_size);
    if (src == nullptr) {
        return;
    }
    std::strncpy(dst, src, dst_size - 1);
}

auto read_runtime_image_memory(void* opaque, uint64_t address, void* destination, size_t size) -> bool {
    auto* target = static_cast<Task*>(opaque);
    return target != nullptr && ker::mod::sys::usercopy::copy_from_task_mapped(*target, address, destination, size);
}

auto snapshot_target_images(Task& target, const ker::mod::security::Snapshot& admitted_target,
                            std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES>& runtime_images,
                            size_t& needed, ker::loader::runtime::SnapshotStatus& status, bool include_kernel,
                            PendingOperation* operation = nullptr, uint64_t* error = nullptr) -> bool {
    // Match exec/fork/mmap publication so the kernel-owned extents and every
    // target-memory read describe one address space.
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    // Credential publication uses this same guard. Do not read a replacement
    // image with a decision made before its dumpability/identity changed.
    if (target.credentials() != admitted_target) return false;
    EffectAdmission effect(operation, target);
    if (effect.result != 0) {
        if (error != nullptr) *error = effect.result;
        return false;
    }
    ker::loader::runtime::SnapshotSource const SOURCE{
        .program_header_addr = target.program_header_addr,
        .program_header_count = target.program_header_count,
        .program_header_ent_size = target.program_header_ent_size,
        .main_elf_header_addr = target.elf_header_addr,
        .main_load_base = target.image_load_base,
        .main_image_start = target.image_vaddr_start,
        .main_image_end = target.image_vaddr_end,
        .main_entry = target.entry,
        .interpreter_base = target.interp_base,
        .interpreter_image_start = target.interp_vaddr_start,
        .interpreter_image_end = target.interp_vaddr_end,
        .main_path = target.exe_path.data(),
        .interpreter_path = "/lib/ld.so",
    };
    status = ker::loader::runtime::snapshot_images(read_runtime_image_memory, &target, SOURCE, runtime_images, needed);
    if (include_kernel && !ker::loader::runtime::append_kernel_image(runtime_images, needed)) {
        status = needed >= runtime_images.size() ? ker::loader::runtime::SnapshotStatus::TRUNCATED
                                                 : ker::loader::runtime::SnapshotStatus::INCONSISTENT;
    }
    return true;
}

auto list_images(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                 PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::ImageList list{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, list) || (list.capacity != 0 && list.images == nullptr) ||
        !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(list))) {
        return as_error(EFAULT);
    }

    using Images = std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES>;
    OwnedBuffer<Images> runtime_storage(operation, new (std::nothrow) Images);
    auto* runtime_images = runtime_storage.get();
    if (runtime_images == nullptr) {
        return as_error(ENOMEM);
    }
    size_t needed = 0;
    auto status = ker::loader::runtime::SnapshotStatus::UNAVAILABLE;
    bool const INCLUDE_KERNEL = tracer.credentials().allows(ker::mod::security::Authority::KERNEL_DEBUG);
    uint64_t failure = as_error(EPERM);
    if (!snapshot_target_images(target, admitted_target, *runtime_images, needed, status, INCLUDE_KERNEL, operation, &failure)) {
        return failure;
    }
    if (needed == 0) {
        return as_error(EIO);
    }
    list.count = needed;
    if (list.capacity < needed) {
        runtime_storage.reset();
        if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list)) {
            return as_error(EFAULT);
        }
        return as_error(ENOSPC);
    }

    OwnedBuffer<abi::ptrace::ImageRecord, true> image_storage(operation, new (std::nothrow) abi::ptrace::ImageRecord[needed]{});
    auto* images = image_storage.get();
    if (images == nullptr) {
        return as_error(ENOMEM);
    }
    for (size_t i = 0; i < needed; ++i) {
        auto const& runtime = runtime_images->at(i);
        auto& image = images[i];
        copy_image_path(static_cast<char*>(image.path), abi::ptrace::ImageRecord::PATH_LEN, runtime.path.data());
        image.load_base = runtime.load_base;
        image.text_addr = runtime.text_start;
        image.text_size = runtime.text_end >= runtime.text_start ? runtime.text_end - runtime.text_start : 0;
        image.entry = runtime.entry;
        image.flags = runtime.flags;
    }
    runtime_storage.reset();

    size_t const IMAGE_BYTES = needed * sizeof(images[0]);
    uint64_t const IMAGES_ADDR = reinterpret_cast<uint64_t>(list.images);
    if (!ker::mod::sys::usercopy::ensure_writable(tracer, IMAGES_ADDR, IMAGE_BYTES) ||
        !ker::mod::sys::usercopy::copy_to_task(tracer, IMAGES_ADDR, images, IMAGE_BYTES) ||
        !ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list)) {
        return as_error(EFAULT);
    }
    return 0;
}

auto list_image_catalog(Task& tracer, Task& target, uint64_t data, const ker::mod::security::Snapshot& admitted_target,
                        PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::ImageCatalogList list{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, list) ||
        !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(list))) {
        return as_error(EFAULT);
    }
    if (list.version != abi::ptrace::IMAGE_CATALOG_VERSION || list.record_size != sizeof(abi::ptrace::ImageCatalogRecord) ||
        (list.capacity != 0 && list.images == nullptr)) {
        return as_error(EINVAL);
    }

    using Images = std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES>;
    OwnedBuffer<Images> runtime_storage(operation, new (std::nothrow) Images);
    auto* runtime_images = runtime_storage.get();
    if (runtime_images == nullptr) {
        return as_error(ENOMEM);
    }
    size_t needed = 0;
    auto status = ker::loader::runtime::SnapshotStatus::UNAVAILABLE;
    bool const INCLUDE_KERNEL = tracer.credentials().allows(ker::mod::security::Authority::KERNEL_DEBUG);
    uint64_t failure = as_error(EPERM);
    if (!snapshot_target_images(target, admitted_target, *runtime_images, needed, status, INCLUDE_KERNEL, operation, &failure)) {
        return failure;
    }
    list.count = needed;
    list.snapshot_status = static_cast<abi::ptrace::image_snapshot_status>(status);
    list.reserved = 0;
    if (needed == 0) {
        runtime_storage.reset();
        (void)ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list);
        return as_error(EIO);
    }
    if (list.capacity < needed) {
        runtime_storage.reset();
        if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list)) {
            return as_error(EFAULT);
        }
        return as_error(ENOSPC);
    }

    OwnedBuffer<abi::ptrace::ImageCatalogRecord, true> image_storage(operation,
                                                                     new (std::nothrow) abi::ptrace::ImageCatalogRecord[needed]{});
    auto* images = image_storage.get();
    if (images == nullptr) {
        return as_error(ENOMEM);
    }
    for (size_t i = 0; i < needed; ++i) {
        auto const& runtime = runtime_images->at(i);
        auto& image = images[i];
        copy_image_path(static_cast<char*>(image.path), abi::ptrace::ImageCatalogRecord::PATH_LEN, runtime.path.data());
        image.load_base = runtime.load_base;
        image.image_start = runtime.image_start;
        image.image_end = runtime.image_end;
        image.text_addr = runtime.text_start;
        image.text_size = runtime.text_end >= runtime.text_start ? runtime.text_end - runtime.text_start : 0;
        image.entry = runtime.entry;
        image.dynamic_addr = runtime.dynamic_addr;
        image.flags = runtime.flags;
        image.build_id_size = runtime.build_id_size;
        std::memcpy(static_cast<uint8_t*>(image.build_id), runtime.build_id.data(), sizeof(image.build_id));
    }
    runtime_storage.reset();

    size_t const IMAGE_BYTES = needed * sizeof(images[0]);
    uint64_t const IMAGES_ADDR = reinterpret_cast<uint64_t>(list.images);
    if (!ker::mod::sys::usercopy::ensure_writable(tracer, IMAGES_ADDR, IMAGE_BYTES) ||
        !ker::mod::sys::usercopy::copy_to_task(tracer, IMAGES_ADDR, images, IMAGE_BYTES) ||
        !ker::mod::sys::usercopy::copy_value_to_task(tracer, data, list)) {
        return as_error(EFAULT);
    }
    return 0;
}

auto capture_remote_info(Task& target, const ker::mod::security::Snapshot& admitted_target, abi::ptrace::RemoteInfo& out,
                         PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target, true);
    if (effect.result != 0) return effect.result;
    auto const IDENTITY = target.wki_execution_identity();
    if (operation != nullptr && operation->private_scope() != nullptr &&
        (target.wki_proxy_task || IDENTITY.proxy_task_id != 0 || IDENTITY.signal_task_id != 0 || IDENTITY.remote_pid != 0))
        return as_error(ENOTSUP);
    out = {};
    out.is_proxy = target.wki_proxy_task || IDENTITY.proxy_task_id != 0 ? 1U : 0U;
    out.state = target.has_exited ? 2U : (IDENTITY.remote_pid != 0 ? 1U : 0U);
    out.proxy_pid = operation != nullptr ? operation->visible_tid(target) : target.pid;
    out.task_id = IDENTITY.proxy_task_id;
    out.target_node = PTRACE_REMOTE_NODE_INVALID;
    out.remote_pid = IDENTITY.remote_pid;
    std::strncpy(out.target_hostname.data(), target.wki_target_hostname.data(), out.target_hostname.size() - 1);
    if (out.target_hostname.front() == '\0') {
        std::strncpy(out.target_hostname.data(), IDENTITY.submitter_hostname.data(), out.target_hostname.size() - 1);
    }
    return 0;
}

auto remote_info(Task& tracer, Task& target, uint64_t data, PendingOperation* operation = nullptr) -> uint64_t {
    ker::mod::security::Snapshot admitted_target;
    if (&tracer == &target) {
        admitted_target = target.credentials();
    } else if (!can_trace(tracer, target, &admitted_target)) {
        return as_error(EPERM);
    }
    if (data == 0) return as_error(EFAULT);
    abi::ptrace::RemoteInfo out{};
    uint64_t const CAPTURE = capture_remote_info(target, admitted_target, out, operation);
    if (CAPTURE != 0) return CAPTURE;
    // Routing may change independently of credentials. Resolve only the owned
    // execution ID; neither WKI locks nor tracer usercopy run under SharedVM.
    uint16_t node = 0;
    decltype(out.target_hostname) hostname{};
    if ((operation == nullptr || operation->private_scope() == nullptr) &&
        ker::net::wki::wki_submitted_task_remote_info(static_cast<uint32_t>(out.task_id), &node, hostname.data(), hostname.size())) {
        out.target_node = node;
        if (hostname.front() != '\0') out.target_hostname = hostname;
    }
    return ker::mod::sys::usercopy::copy_value_to_task(tracer, data, out) ? 0 : as_error(EFAULT);
}

auto apply_hw_break(Task& target, const abi::ptrace::HwBreak& desc, bool enable, const ker::mod::security::Snapshot& admitted_target,
                    PendingOperation* operation = nullptr) -> uint64_t {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    if (target.credentials() != admitted_target) return as_error(EPERM);
    EffectAdmission effect(operation, target);
    if (effect.result != 0) return effect.result;
    if (!ker::mod::security::valid_debug_breakpoint(desc, enable)) {
        return as_error(EINVAL);
    }
    uint64_t length_code = 0;
    if (enable) {
        if (desc.type == abi::ptrace::hw_break_type::EXECUTE && desc.length != 1) {
            return as_error(EINVAL);
        }
        switch (desc.length) {
            case 1:
                length_code = 0b00;
                break;
            case 2:
                length_code = 0b01;
                break;
            case 4:
                length_code = 0b11;
                break;
            case 8:
                length_code = 0b10;
                break;
            default:
                return as_error(EINVAL);
        }
        if (desc.type != abi::ptrace::hw_break_type::EXECUTE && (desc.address % desc.length) != 0) {
            return as_error(EINVAL);
        }
    }

    if (stopped_user_frame(target) == nullptr) {
        return as_error(EIO);
    }
    target.ptrace_dr_addr.at(desc.slot) = enable ? desc.address : 0;
    uint64_t const LOCAL_ENABLE = X86_DR7_LOCAL_ENABLE_MASK << (desc.slot * X86_DR7_SLOT_ENABLE_STRIDE);
    uint64_t const CONTROL_SHIFT = X86_DR7_SLOT_CONTROL_BASE + (desc.slot * X86_DR7_SLOT_CONTROL_STRIDE);
    target.ptrace_dr7 &= ~LOCAL_ENABLE;
    target.ptrace_dr7 &= ~(X86_DR7_SLOT_CONTROL_MASK << CONTROL_SHIFT);
    if (enable) {
        target.ptrace_dr7 |= X86_DR7_RESERVED_ONES;
        target.ptrace_dr7 |= LOCAL_ENABLE;
        target.ptrace_dr7 |= ((static_cast<uint64_t>(desc.type) & 0b11ULL) | (length_code << 2U)) << CONTROL_SHIFT;
    }
    return 0;
}

auto set_hw_break(Task& tracer, Task& target, uint64_t data, bool enable, const ker::mod::security::Snapshot& admitted_target,
                  PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::HwBreak desc{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, desc)) return as_error(EFAULT);
    return apply_hw_break(target, desc, enable, admitted_target, operation);
}

auto transfer_memory(Task& tracer, Task& target, uint64_t data, bool write_target, const ker::mod::security::Snapshot& admitted_target,
                     PendingOperation* operation = nullptr) -> uint64_t {
    abi::ptrace::MemIo io{};
    if (!ker::mod::sys::usercopy::copy_value_from_task(tracer, data, io) || io.buffer == nullptr ||
        !ker::mod::sys::usercopy::range_valid(io.address, io.size) ||
        !ker::mod::sys::usercopy::range_valid(reinterpret_cast<uint64_t>(io.buffer), io.size) ||
        !ker::mod::sys::usercopy::ensure_writable(tracer, data, sizeof(io))) {
        return as_error(EFAULT);
    }

    constexpr size_t BOUNCE_SIZE = ker::mod::mm::paging::PAGE_SIZE;
    std::array<uint8_t, BOUNCE_SIZE> bounce{};
    uint64_t const BUFFER_ADDR = reinterpret_cast<uint64_t>(io.buffer);
    size_t done = 0;
    uint64_t result = 0;

    while (done < io.size) {
        size_t const CHUNK = std::min(BOUNCE_SIZE, io.size - done);
        if (write_target) {
            auto const INPUT = ker::mod::sys::usercopy::copy_from_task_partial(tracer, BUFFER_ADDR + done, bounce.data(), CHUNK);
            if (INPUT.bytes_copied != 0) {
                size_t target_done = 0;
                uint64_t const TARGET_RESULT = copy_task_memory(target, io.address + done, bounce.data(), INPUT.bytes_copied, true,
                                                                &target_done, admitted_target, operation);
                done += target_done;
                if (TARGET_RESULT != 0 || target_done != INPUT.bytes_copied) {
                    result = TARGET_RESULT != 0 ? TARGET_RESULT : as_error(EFAULT);
                    break;
                }
            }
            if (INPUT.fault || INPUT.bytes_copied != CHUNK) {
                result = as_error(EFAULT);
                break;
            }
        } else {
            size_t target_done = 0;
            uint64_t const TARGET_RESULT =
                copy_task_memory(target, io.address + done, bounce.data(), CHUNK, false, &target_done, admitted_target, operation);
            if (target_done != 0) {
                auto const OUTPUT = ker::mod::sys::usercopy::copy_to_task_partial(tracer, BUFFER_ADDR + done, bounce.data(), target_done);
                done += OUTPUT.bytes_copied;
                if (OUTPUT.fault || OUTPUT.bytes_copied != target_done) {
                    result = as_error(EFAULT);
                    break;
                }
            }
            if (TARGET_RESULT != 0 || target_done != CHUNK) {
                result = TARGET_RESULT != 0 ? TARGET_RESULT : as_error(EFAULT);
                break;
            }
        }
    }

    // Initial credential denial publishes nothing. A later invalidated view
    // ends an already-authorized partial transfer with its existing fault ABI.
    if (result == as_error(EPERM) || result == as_error(EIDRM) || result == as_error(ENOTSUP) || result == as_error(ESRCH) ||
        result == as_error(EAGAIN)) {
        if (done == 0) return result;
        result = as_error(EFAULT);
    }
    io.transferred = done;
    if (!ker::mod::sys::usercopy::copy_value_to_task(tracer, data, io)) {
        return as_error(EFAULT);
    }
    return result;
}

auto decode_debug_register_stop(Task& task, uint64_t dr6, abi::ptrace::stop_reason& reason, uint64_t& address) -> void {
    if (reason != abi::ptrace::stop_reason::TRACE) {
        return;
    }
    address = 0;
    if ((dr6 & X86_DR6_BREAKPOINT_MASK) == 0 || task.ptrace_dr7 == 0) {
        return;
    }

    for (size_t slot = 0; slot < task.ptrace_dr_addr.size(); ++slot) {
        uint64_t const LOCAL_ENABLE = X86_DR7_LOCAL_ENABLE_MASK << (slot * X86_DR7_SLOT_ENABLE_STRIDE);
        if ((task.ptrace_dr7 & LOCAL_ENABLE) == 0 || (dr6 & (1ULL << slot)) == 0) {
            continue;
        }

        uint64_t const CONTROL_SHIFT = X86_DR7_SLOT_CONTROL_BASE + (slot * X86_DR7_SLOT_CONTROL_STRIDE);
        uint64_t const RW = (task.ptrace_dr7 >> CONTROL_SHIFT) & X86_DR7_RW_MASK;
        reason = RW == static_cast<uint64_t>(abi::ptrace::hw_break_type::EXECUTE) ? abi::ptrace::stop_reason::BREAKPOINT
                                                                                  : abi::ptrace::stop_reason::WATCHPOINT;
        address = task.ptrace_dr_addr.at(slot);
        return;
    }
}

void capture_syscall_stop_context(Task& task, ker::mod::cpu::GPRegs& gpr) {
    task.ptrace_syscall_regs = gpr;

    uint64_t return_rip = 0;
    uint64_t return_flags = 0;
    uint64_t user_rsp = 0;
    asm volatile("movq %%gs:0x28, %0" : "=r"(return_rip));
    asm volatile("movq %%gs:0x30, %0" : "=r"(return_flags));
    asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));

    task.ptrace_syscall_regs.rcx = return_rip;
    task.ptrace_syscall_regs.r11 = return_flags;
    task.ptrace_syscall_frame.int_num = 0;
    task.ptrace_syscall_frame.err_code = 0;
    task.ptrace_syscall_frame.rip = return_rip;
    task.ptrace_syscall_frame.cs = ker::mod::desc::gdt::GDT_USER_CS;
    task.ptrace_syscall_frame.flags = return_flags;
    task.ptrace_syscall_frame.rsp = user_rsp;
    task.ptrace_syscall_frame.ss = ker::mod::desc::gdt::GDT_USER_DS;
}

void restore_syscall_stop_context(Task& task, ker::mod::cpu::GPRegs& gpr) {
    normalize_syscall_snapshot(task);
    gpr = task.ptrace_syscall_regs;

    uint64_t const RETURN_RIP = task.ptrace_syscall_frame.rip;
    uint64_t const RETURN_FLAGS = task.ptrace_syscall_frame.flags;
    uint64_t const USER_RSP = task.ptrace_syscall_frame.rsp;
    asm volatile("movq %0, %%gs:0x28" ::"r"(RETURN_RIP) : "memory");
    asm volatile("movq %0, %%gs:0x30" ::"r"(RETURN_FLAGS) : "memory");
    asm volatile("movq %0, %%gs:0x08" ::"r"(USER_RSP) : "memory");
}

}  // namespace

auto sys_ptrace(abi::ptrace::request req, uint64_t pid, uint64_t addr, uint64_t data, ker::mod::cpu::GPRegs& /*unused*/) -> uint64_t {
    auto* tracer = ker::mod::sched::get_current_task();
    if (tracer == nullptr) {
        return as_error(ESRCH);
    }
    return ptrace_as(*tracer, req, pid, addr, data);
}

auto ptrace_as(ker::mod::sched::task::Task& requester, abi::ptrace::request req, uint64_t pid, uint64_t addr, uint64_t data) -> uint64_t {
    auto* tracer = &requester;
    auto const* scope = tracer->pid_namespace();
    if (scope == nullptr || (!scope->is_host() && tracer->namespace_tid() == 0)) return as_error(ESRCH);
    if (scope->closed()) return as_error(EIDRM);
    PendingOperation operation(*tracer);
    int const ADMITTED = operation.admit();
    if (ADMITTED != 0) return static_cast<uint64_t>(ADMITTED);

    if (req == abi::ptrace::request::TRACEME) {
        auto* parent =
            scope->is_host() ? ker::mod::sched::find_task_by_pid_safe(tracer->parent_pid) : child_events::acquire_namespace_parent(*tracer);
        if (parent == nullptr) {
            return scope->closed() ? as_error(EIDRM) : as_error(ESRCH);
        }
        operation.own_target(*parent);
        uint64_t ret = 0;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *tracer);
            if (effect.result != 0) return effect.result;
            if (operation.private_scope() != nullptr && parent->signal_admission_identity().remote) return as_error(ENOTSUP);
            ret = can_trace(*parent, *tracer) ? 0 : as_error(EPERM);
            if (ret == 0 && !child_events::attach_tracer(*tracer, *parent)) {
                ret = as_error(EBUSY);
            }
            if (ret == 0) {
                tracer->ptrace_traced = true;
                tracer->ptrace_tracer_pid = parent->pid;
            }
        }
        return ret;
    }

    auto* target = get_target(pid, *tracer);
    if (target == nullptr) {
        return scope->closed() ? as_error(EIDRM) : as_error(ESRCH);
    }
    // Positive lookup owns a reference even if it selects the executor itself;
    // pid0 borrows the executor's already-retained lifetime.
    if (pid != 0) operation.own_target(*target);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        EffectAdmission effect(&operation, *target);
        if (effect.result != 0) return effect.result;
    }

    uint64_t ret = 0;
    switch (req) {
        case abi::ptrace::request::ATTACH:
            ret = attach(*tracer, *target, false, &operation);
            break;
        case abi::ptrace::request::SEIZE:
            ret = attach(*tracer, *target, true, &operation);
            break;
        case abi::ptrace::request::DETACH:
            ret = detach(*tracer, *target, &operation);
            break;
        case abi::ptrace::request::KILL: {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *target);
            if (effect.result != 0) {
                ret = effect.result;
                break;
            }
            ret = require_traced(*tracer, *target);
            if (ret == 0 && !acknowledge_stop_for_resume(*tracer, *target)) {
                ret = as_error(EBUSY);
            } else if (ret == 0) {
                target->signal_add_pending_mask(1ULL << (SIGKILL - 1));
                target->ptrace_stopped = false;
                target->ptrace_stop_pending = false;
                target->ptrace_single_step = false;
                target->ptrace_syscall_trace = false;
                target->ptrace_syscall_in_stop = false;
                target->ptrace_stop_uses_syscall_snapshot = false;
                clear_task_trap_flags(*target);
                ker::mod::sched::reschedule_task_for_cpu(target->cpu, target);
            }
            break;
        }
        case abi::ptrace::request::CONT: {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *target);
            if (effect.result != 0) {
                ret = effect.result;
                break;
            }
            ret = require_traced(*tracer, *target);
            if (ret == 0 && !acknowledge_stop_for_resume(*tracer, *target)) {
                ret = as_error(EBUSY);
            } else if (ret == 0) {
                bool const USE_STOP_SNAPSHOT = uses_stop_snapshot(*target);
                bool const WAS_STOPPED = target->ptrace_stopped;
                target->ptrace_stopped = false;
                target->ptrace_stop_pending = false;
                target->ptrace_single_step = false;
                target->ptrace_syscall_trace = false;
                target->ptrace_syscall_in_stop = false;
                clear_task_trap_flags(*target);
                if (USE_STOP_SNAPSHOT) {
                    (void)publish_stop_snapshot_to_syscall_scratch(*target);
                }
                target->ptrace_stop_uses_syscall_snapshot = false;
                if (WAS_STOPPED) {
                    ker::mod::sched::reschedule_task_for_cpu(target->cpu, target);
                }
            }
            break;
        }
        case abi::ptrace::request::SINGLESTEP: {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *target);
            if (effect.result != 0) {
                ret = effect.result;
                break;
            }
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                auto* step_frame = user_resume_frame_for_step(*target);
                if (step_frame == nullptr) {
                    ret = as_error(EIO);
                    break;
                }
                if (!acknowledge_stop_for_resume(*tracer, *target)) {
                    ret = as_error(EBUSY);
                    break;
                }
                bool const USE_STOP_SNAPSHOT = uses_stop_snapshot(*target);
                bool const WAS_STOPPED = target->ptrace_stopped;
                target->ptrace_stopped = false;
                target->ptrace_stop_pending = false;
                target->ptrace_single_step = true;
                target->ptrace_syscall_trace = false;
                target->ptrace_syscall_in_stop = false;
                clear_task_trap_flags(*target);
                step_frame->flags |= X86_RFLAGS_TF;
                if (USE_STOP_SNAPSHOT) {
                    (void)publish_stop_snapshot_to_syscall_scratch(*target);
                }
                target->ptrace_stop_uses_syscall_snapshot = false;
                if (WAS_STOPPED) {
                    ker::mod::sched::reschedule_task_for_cpu(target->cpu, target);
                }
            }
            break;
        }
        case abi::ptrace::request::SYSCALL: {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *target);
            if (effect.result != 0) {
                ret = effect.result;
                break;
            }
            ret = require_traced(*tracer, *target);
            if (ret == 0 && !resume_syscall_after_require(*tracer, *target)) {
                ret = as_error(EBUSY);
            }
            break;
        }
        case abi::ptrace::request::INTERRUPT: {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            EffectAdmission effect(&operation, *target);
            if (effect.result != 0) {
                ret = effect.result;
                break;
            }
            ret = require_traced(*tracer, *target);
            if (ret == 0 && (target->ptrace_stopped || target->ptrace_stop_pending)) {
                ret = as_error(EBUSY);
            } else if (ret == 0) {
                target->ptrace_stop_reason = abi::ptrace::stop_reason::INTERRUPT;
                target->ptrace_stop_signal = SIGSTOP;
                target->ptrace_stopped = true;
                target->ptrace_stop_pending = true;
                target->ptrace_stop_uses_syscall_snapshot = false;
                target->set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
                ker::mod::sched::debug_stop_task(target);
                (void)capture_async_syscall_return_snapshot(*target);
                if (!publish_stop_event(*target)) {
                    ret = as_error(EBUSY);
                }
            }
            break;
        }
        case abi::ptrace::request::GETREGSET: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = read_regset(*tracer, *target, data, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::SETREGSET: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = write_regset(*tracer, *target, data, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::PEEKDATA: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                uint64_t word = 0;
                ret = copy_task_memory(*target, addr, &word, sizeof(word), false, nullptr, admitted_target, &operation);
                if (ret == 0) {
                    if (data == 0 || !ker::mod::sys::usercopy::copy_value_to_task(*tracer, data, word)) {
                        ret = as_error(EFAULT);
                    }
                }
            }
            break;
        }
        case abi::ptrace::request::POKEDATA: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = write_task_word(*target, addr, data, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::READ_MEM: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = transfer_memory(*tracer, *target, data, false, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::WRITE_MEM: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = transfer_memory(*tracer, *target, data, true, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::LIST_THREADS: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) ret = list_threads(*tracer, *target, data, admitted_target, &operation);
            break;
        }
        case abi::ptrace::request::GETEVENTMSG: {
            abi::ptrace::Event event{};
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
                EffectAdmission effect(&operation, *target, true);
                if (effect.result != 0) {
                    ret = effect.result;
                    break;
                }
                ret = require_traced(*tracer, *target);
                if (ret == 0) fill_event(*target, event, &operation);
            }
            if (ret == 0 && (data == 0 || !ker::mod::sys::usercopy::copy_value_to_task(*tracer, data, event))) ret = as_error(EFAULT);
            break;
        }
        case abi::ptrace::request::SETOPTIONS: {
            ret = set_trace_options(*tracer, *target, data, &operation, [] {});
            break;
        }
        case abi::ptrace::request::GET_REMOTE_INFO:
            ret = remote_info(*tracer, *target, data, &operation);
            break;
        case abi::ptrace::request::SET_HW_BREAK: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = set_hw_break(*tracer, *target, data, true, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::DEL_HW_BREAK: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = set_hw_break(*tracer, *target, data, false, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::SYSCALL_WAIT:
            ret = syscall_wait(*tracer, *target, data, &operation);
            break;
        case abi::ptrace::request::GET_IMAGES: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = list_images(*tracer, *target, data, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::GET_IMAGE_CATALOG: {
            ker::mod::security::Snapshot admitted_target;
            ret = require_traced(*tracer, *target, &admitted_target);
            if (ret == 0) {
                ret = list_image_catalog(*tracer, *target, data, admitted_target, &operation);
            }
            break;
        }
        case abi::ptrace::request::GET_MAPS:
            ret = as_error(ENOSYS);
            break;
        default:
            ret = as_error(EINVAL);
            break;
    }

    return ret;
}

namespace {

auto report_user_stop_common(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, abi::ptrace::stop_reason reason,
                             uint32_t signal, uint64_t address, uint64_t message) -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || !task->ptrace_traced) {
        return false;
    }

    bool const SOFTWARE_BREAKPOINT = reason == abi::ptrace::stop_reason::BREAKPOINT;
    uint64_t const RAW_DEBUG_STATUS = message;
    if (reason == abi::ptrace::stop_reason::TRACE) {
        decode_debug_register_stop(*task, RAW_DEBUG_STATUS, reason, address);
    }
    task->ptrace_dr6 = reason == abi::ptrace::stop_reason::TRACE ? RAW_DEBUG_STATUS : 0;

    if (SOFTWARE_BREAKPOINT && frame.rip > 0) {
        --frame.rip;
    }

    clear_local_trap_flag();
    frame.flags &= ~X86_RFLAGS_TF;
    task->context.regs = gpr;
    task->context.frame = frame;
    task->ptrace_stop_reason = reason;
    task->ptrace_stop_signal = signal;
    task->ptrace_stop_address = address;
    task->ptrace_event_msg = message;
    task->ptrace_stopped = true;
    task->ptrace_stop_pending = true;
    task->ptrace_single_step = false;
    task->ptrace_stop_uses_syscall_snapshot = false;
    clear_task_trap_flags(*task);
    task->set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    ker::mod::sched::place_task_in_wait_queue(gpr, frame, [](Task& stopped) { static_cast<void>(publish_stop_event(stopped)); });
    return true;
}

}  // namespace

auto report_user_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, abi::ptrace::stop_reason reason, uint32_t signal,
                      uint64_t address) -> bool {
    return report_user_stop_common(gpr, frame, reason, signal, address, address);
}

auto report_user_exception_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, uint32_t signal, uint64_t address,
                                uint64_t message) -> bool {
    return report_user_stop_common(gpr, frame, abi::ptrace::stop_reason::EXCEPTION, signal, address, message);
}

auto report_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum, bool exiting) -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || !task->ptrace_traced || !task->ptrace_syscall_trace) {
        return false;
    }
    exit_pending_syscall_stop(*task);

    if (exiting) {
        if (!task->ptrace_syscall_in_stop) {
            return false;
        }
        if (should_suppress_deferred_syscall_exit_stop(*task)) {
            task->ptrace_syscall_in_stop = false;
            return false;
        }
        task->ptrace_syscall_in_stop = false;
        task->ptrace_stop_reason = abi::ptrace::stop_reason::SYSCALL_EXIT;
    } else {
        if (task->ptrace_syscall_in_stop) {
            return false;
        }
        task->ptrace_syscall_in_stop = true;
        task->ptrace_stop_reason = abi::ptrace::stop_reason::SYSCALL_ENTER;
    }

    capture_syscall_stop_context(*task, gpr);
    task->ptrace_stop_signal = SIGTRAP;
    task->ptrace_stop_address = callnum;
    task->ptrace_event_msg = callnum;
    task->ptrace_stopped = true;
    task->ptrace_stop_pending = true;
    task->ptrace_stop_uses_syscall_snapshot = false;
    task->set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    static_cast<void>(publish_stop_event(*task));

    while (task->ptrace_stop_blocks_execution()) {
        ker::mod::sched::preemptible_syscall_park("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    }
    restore_syscall_stop_context(*task, gpr);
    exit_pending_syscall_stop(*task);
    return true;
}

auto report_fatal_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum) -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || !task->ptrace_traced) {
        return false;
    }
    exit_pending_syscall_stop(*task);

    capture_syscall_stop_context(*task, gpr);
    task->ptrace_stop_reason = abi::ptrace::stop_reason::EXCEPTION;
    task->ptrace_stop_signal = WOS_SIGSYS;
    task->ptrace_stop_address = callnum;
    task->ptrace_event_msg = callnum;
    task->ptrace_stopped = true;
    task->ptrace_stop_pending = true;
    task->ptrace_single_step = false;
    task->ptrace_syscall_in_stop = false;
    task->ptrace_stop_uses_syscall_snapshot = false;
    task->set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    static_cast<void>(publish_stop_event(*task));

    while (task->ptrace_stop_blocks_execution()) {
        ker::mod::sched::preemptible_syscall_park("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    }
    restore_syscall_stop_context(*task, gpr);
    exit_pending_syscall_stop(*task);
    return true;
}

auto report_signal_stop(ker::mod::sched::task::Task& task, uint32_t signal) -> bool { return record_signal_stop(task, signal); }

#ifdef WOS_SELFTEST
auto ptrace_selftest_namespace_effect_lifetime() -> bool {
    namespace pid_ns = ker::mod::sched::pid_ns;
    auto const baseline = pid_ns::live_namespace_count();
    bool ok = true;
    for (int interleaving = 0; interleaving < 3; ++interleaving) {
        pid_ns::NamespaceRef scope;
        if (pid_ns::create_namespace(scope) != 0) return false;
        Task tracer{}, target{};
        tracer.pid = ker::mod::sched::task::get_next_pid();
        target.pid = ker::mod::sched::task::get_next_pid();
        tracer.type = target.type = ker::mod::sched::task::TaskType::PROCESS;
        if (!tracer.initialize_pid_namespace(scope) || !target.initialize_pid_namespace(scope)) return false;
        target.ptrace_traced = true;
        target.ptrace_tracer_pid = tracer.pid;
        {
            PendingOperation operation(tracer);
            if (operation.admit() != 0) return false;
            if (!target.try_acquire()) return false;
            operation.own_target(target);
            ok &= !scope.operations_drained();
            if (interleaving == 0) scope.close();
            if (interleaving == 1) target.publish_accepted_signal_route(91, 92);
            bool admitted = false;
            auto const result = set_trace_options(tracer, target, 0x45, &operation, [&] {
                admitted = !target.local_signal_admissions_drained() && !scope.operations_drained();
                scope.close();
                target.publish_accepted_signal_route(93, 94);
            });
            if (interleaving == 2) {
                ok &= admitted && result == 0 && target.ptrace_options == 0x45;
                ok &= set_trace_options(tracer, target, 0x99, &operation, [] {}) == as_error(EIDRM);
            } else {
                ok &= !admitted && result == as_error(interleaving == 0 ? EIDRM : ENOTSUP) && target.ptrace_options == 0;
            }
            ok &= target.local_signal_admissions_drained() && target.ref_count.load() == 2;
        }
        ok &= scope.operations_drained() && tracer.pending_ptrace_operations == nullptr && target.ref_count.load() == 1;
    }
    return ok && pid_ns::live_namespace_count() == baseline;
}

auto ptrace_selftest_namespace_observations() -> bool {
    namespace pid_ns = ker::mod::sched::pid_ns;
    auto const baseline = pid_ns::live_namespace_count();
    bool ok = true;
    {
        pid_ns::NamespaceRef scope, foreign_scope;
        if (pid_ns::create_namespace(scope) != 0 || pid_ns::create_namespace(foreign_scope) != 0) return false;
        Task tracer{}, target{}, thread{}, foreign{};
        for (auto* task : {&tracer, &target, &thread, &foreign}) {
            task->pid = ker::mod::sched::task::get_next_pid();
            task->type = ker::mod::sched::task::TaskType::PROCESS;
        }
        if (!tracer.initialize_pid_namespace(scope) || !target.initialize_pid_namespace(scope) ||
            !foreign.initialize_pid_namespace(foreign_scope))
            return false;
        thread.is_thread = true;
        thread.owner_pid = target.pid;
        if (!ker::mod::sched::resource::bind_thread_owner(thread, target)) return false;
        struct Registration {
            Task& task;
            bool registered{};
            ~Registration() {
                if (registered) ker::mod::sched::scheduler_selftest_unregister_unpublished_task(task);
            }
            auto publish() -> bool { return registered = ker::mod::sched::scheduler_selftest_register_unpublished_task(task); }
        } target_registration{target}, thread_registration{thread}, foreign_registration{foreign};
        if (!target_registration.publish() || !thread_registration.publish() || !foreign_registration.publish()) return false;
        PendingOperation operation(tracer);
        if (operation.admit() != 0) return false;
        if (!target.try_acquire()) return false;
        operation.own_target(target);
        auto const credentials = target.credentials();
        size_t const capacity = ker::mod::sched::get_active_task_count();
        OwnedBuffer<uint64_t, true> tids(&operation, new (std::nothrow) uint64_t[capacity]);
        if (tids.get() == nullptr) return false;
        size_t count = 17;
        ok &= capture_threads(target, credentials, tids.get(), capacity, count, &operation) == 0 && count == 2;
        bool found_target = false, found_thread = false;
        for (size_t i = 0; i < count; ++i) {
            found_target |= tids.get()[i] == target.namespace_tid();
            found_thread |= tids.get()[i] == thread.namespace_tid();
            ok &= tids.get()[i] != target.pid && tids.get()[i] != thread.pid;
        }
        ok &= found_target && found_thread;
        abi::ptrace::RemoteInfo remote{};
        ok &= capture_remote_info(target, credentials, remote, &operation) == 0 && remote.proxy_pid == target.namespace_tid() &&
              remote.task_id == 0 && remote.remote_pid == 0;
        if (!child_events::attach_tracer(target, tracer)) return false;
        struct TraceCleanup {
            Task& tracer;
            Task& target;
            ~TraceCleanup() {
                child_events::cancel_wait(tracer);
                child_events::detach_tracer(target);
            }
        } trace_cleanup{tracer, target};
        // These Tasks have no execution context. Only the target remains live
        // for admission; lifecycle publication cannot enqueue the tracer.
        tracer.state.store(ker::mod::sched::task::TaskState::EXITING, std::memory_order_release);
        target.ptrace_traced = true;
        target.ptrace_tracer_pid = tracer.pid;
        target.ptrace_stopped = true;
        target.ptrace_stop_pending = true;
        target.ptrace_stop_reason = abi::ptrace::stop_reason::INTERRUPT;
        target.ptrace_stop_signal = SIGSTOP;
        ok &= publish_stop_event(target);
        abi::ptrace::StopInfo stop{};
        child_events::ClaimedEvent claimed{};
        bool ready = false;
        ok &= capture_wait_event(tracer, target, nullptr, claimed, stop, ready, &operation) == 0 && ready &&
              stop.event.tid == target.namespace_tid() && claimed.subject_pid == target.pid && target.local_signal_admissions_drained();
        auto const cookie = claimed.claim_cookie;
        child_events::ClaimedEvent competing{};
        bool competing_ready = false;
        ok &= capture_wait_event(tracer, target, nullptr, competing, stop, competing_ready, &operation) == as_error(EBUSY);
        child_events::release_claim(tracer, claimed);
        ready = false;
        ok &= capture_wait_event(tracer, target, nullptr, claimed, stop, ready, &operation) == 0 && ready && claimed.claim_cookie != cookie;
        scope.close();
        auto const owned_stop = stop;
        ok &= capture_wait_event(tracer, target, nullptr, competing, stop, competing_ready, &operation) == as_error(EIDRM) &&
              std::memcmp(&owned_stop, &stop, sizeof(stop)) == 0;
        ok &= child_events::commit_claim(tracer, tracer, claimed, false) && !target.ptrace_stop_pending;
        child_events::release_claim(tracer, claimed);
        count = 17;
        tids.get()[0] = 0x1234;
        ok &= capture_threads(target, credentials, tids.get(), capacity, count, &operation) == as_error(EIDRM) && count == 17 &&
              tids.get()[0] == 0x1234;
        operation.cancel();
        ok &= scope.operations_drained() && target.local_signal_admissions_drained() && tracer.pending_ptrace_operations == nullptr;
    }
    return ok && pid_ns::live_namespace_count() == baseline;
}

auto ptrace_selftest_namespace_register_images() -> bool {
    namespace pid_ns = ker::mod::sched::pid_ns;
    namespace runtime = ker::loader::runtime;
    auto const baseline = pid_ns::live_namespace_count();
    bool ok = true;
    {
        pid_ns::NamespaceRef scope;
        if (pid_ns::create_namespace(scope) != 0) return false;
        Task tracer{}, target{};
        tracer.pid = ker::mod::sched::task::get_next_pid();
        target.pid = ker::mod::sched::task::get_next_pid();
        tracer.type = target.type = ker::mod::sched::task::TaskType::PROCESS;
        if (!tracer.initialize_pid_namespace(scope) || !target.initialize_pid_namespace(scope)) return false;
        PendingOperation operation(tracer);
        if (operation.admit() != 0) return false;
        if (!target.try_acquire()) return false;
        operation.own_target(target);
        target.ptrace_stopped = true;
        target.context.frame = {.int_num = 0,
                                .err_code = 0,
                                .rip = 0x401000,
                                .cs = ker::mod::desc::gdt::GDT_USER_CS,
                                .flags = 0x202,
                                .rsp = 0x7fff0000,
                                .ss = ker::mod::desc::gdt::GDT_USER_DS};
        target.context.regs.rax = 123;
        auto const admitted = target.credentials();
        abi::ptrace::X86_64GprState regs{};
        ok &= capture_gprs(target, regs, admitted, &operation) == 0 && regs.rax == 123;
        regs.rax = 456;
        ok &= apply_gprs(target, regs, admitted, &operation) == 0 && target.context.regs.rax == 456;
        auto invalid = regs;
        invalid.cs = 0;
        ok &= apply_gprs(target, invalid, admitted, &operation) == as_error(EINVAL) && target.context.regs.rax == 456;
        abi::ptrace::HwBreak const point{
            .address = 0x401000, .length = 1, .type = abi::ptrace::hw_break_type::EXECUTE, .slot = 0, .reserved = 0};
        ok &= apply_hw_break(target, point, true, admitted, &operation) == 0 && target.ptrace_dr_addr[0] == point.address;
        ok &= apply_hw_break(target, point, false, admitted, &operation) == 0 && target.ptrace_dr_addr[0] == 0;
        using Images = std::array<runtime::RuntimeImage, runtime::MAX_IMAGES>;
        OwnedBuffer<Images> images(&operation, new (std::nothrow) Images{});
        if (images.get() == nullptr) return false;
        size_t needed = 17;
        auto status = runtime::SnapshotStatus::UNAVAILABLE;
        uint64_t error = as_error(EPERM);
        ok &= snapshot_target_images(target, admitted, *images.get(), needed, status, false, &operation, &error) && needed == 0;
        ker::mod::security::Snapshot changed;
        if (admitted.prepare({.change = ker::mod::security::Change::SET_DUMPABLE, .dumpable = !admitted.dumpable()}, changed) != 0 ||
            ker::mod::sched::resource::initialize_credentials(target, changed) != 0)
            return false;
        regs.rax = 789;
        ok &= apply_gprs(target, regs, admitted, &operation) == as_error(EPERM) && target.context.regs.rax == 456;
        scope.close();
        auto const before = regs;
        needed = 17;
        ok &= capture_gprs(target, regs, changed, &operation) == as_error(EIDRM) && std::memcmp(&before, &regs, sizeof(regs)) == 0;
        ok &= !snapshot_target_images(target, changed, *images.get(), needed, status, false, &operation, &error) &&
              error == as_error(EIDRM) && needed == 17;
        ok &= target.local_signal_admissions_drained();
        operation.cancel();
        ok &= scope.operations_drained() && target.ref_count.load() == 1;
    }
    return ok && pid_ns::live_namespace_count() == baseline;
}

auto ptrace_selftest_operation_cleanup() -> bool {
    namespace pid_ns = ker::mod::sched::pid_ns;
    auto const baseline = pid_ns::live_namespace_count();
    size_t destroyed = 0;
    struct Probe {
        size_t* destroyed{};
        ~Probe() {
            if (destroyed != nullptr) ++*destroyed;
        }
    };
    bool ok = true;
    {
        pid_ns::NamespaceRef scope;
        if (pid_ns::create_namespace(scope) != 0) return false;
        Task tracer{}, target{};
        tracer.pid = ker::mod::sched::task::get_next_pid();
        target.pid = ker::mod::sched::task::get_next_pid();
        tracer.type = target.type = ker::mod::sched::task::TaskType::PROCESS;
        if (!tracer.initialize_pid_namespace(scope) || !target.initialize_pid_namespace(scope)) return false;
        PendingOperation outer(tracer), inner(tracer);
        if (outer.admit() != 0 || inner.admit() != 0) return false;
        if (!target.try_acquire()) return false;
        outer.own_target(target);
        if (!target.try_acquire()) return false;
        inner.own_target(target);
        OwnedBuffer<Probe> single(&outer, new (std::nothrow) Probe{&destroyed});
        OwnedBuffer<Probe, true> array(&inner, new (std::nothrow) Probe[4]);
        OwnedBuffer<Probe> allocation_failure(&inner, nullptr);
        if (single.get() == nullptr || array.get() == nullptr) return false;
        for (size_t i = 0; i < 4; ++i) array.get()[i].destroyed = &destroyed;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (inner.begin_effect(target, false) != 0) return false;
        }
        ok &= target.ref_count.load() == 3 && !scope.operations_drained() && !target.local_signal_admissions_drained();
        cleanup_operations(tracer);
        ok &= tracer.pending_ptrace_operations == nullptr && scope.operations_drained() && target.local_signal_admissions_drained() &&
              target.ref_count.load() == 1 && single.get() == nullptr && array.get() == nullptr && allocation_failure.get() == nullptr &&
              destroyed == 5 && inner.begin_effect(target, false) == -ESRCH;
        cleanup_operations(tracer);
        outer.cancel();
        inner.cancel();
    }
    return ok && destroyed == 5 && pid_ns::live_namespace_count() == baseline;
}

auto ptrace_selftest_fatal_release_preserves_claims() -> bool {
    Task tracer{};
    Task target{};
    tracer.pid = 0x9F001;
    target.pid = 0x9F002;
    tracer.type = target.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const CREDENTIALS = ker::mod::security::Snapshot::bootstrap_root();
    if (ker::mod::sched::resource::initialize_credentials(tracer, CREDENTIALS) != 0 ||
        ker::mod::sched::resource::initialize_credentials(target, CREDENTIALS) != 0)
        return false;
    // Hidden objects must not be admitted to a runqueue by event wakeups.
    tracer.state.store(ker::mod::sched::task::TaskState::EXITING);
    target.state.store(ker::mod::sched::task::TaskState::EXITING);
    target.ptrace_traced = true;
    target.ptrace_tracer_pid = tracer.pid;
    target.ptrace_stopped = true;
    target.ptrace_stop_pending = true;
    target.ptrace_stop_reason = abi::ptrace::stop_reason::INTERRUPT;
    target.ptrace_stop_signal = SIGSTOP;
    if (!child_events::attach_tracer(target, tracer)) return false;
    bool ok = publish_stop_event(target) && target.ptrace_stop_blocks_execution();
    target.signal_add_pending_mask((uint64_t{1} << (SIGUSR1 - 1)) | (uint64_t{1} << (SIGCONT - 1)));
    ok &= target.ptrace_stop_blocks_execution();
    child_events::ClaimedEvent claimed{};
    abi::ptrace::StopInfo stop{};
    bool ready = false;
    ok &= capture_wait_event(tracer, target, nullptr, claimed, stop, ready) == 0 && ready;
    auto const COOKIE = claimed.claim_cookie;
    // A tracer-owned claim prevents CONT's acknowledgment but not fatal
    // execution admission, and exit uses an independent retained event slot.
    ok &= !acknowledge_stop_for_resume(tracer, target);
    target.signal_add_pending_mask(uint64_t{1} << (SIGKILL - 1));
    ok &= !target.ptrace_stop_blocks_execution() && target.ptrace_stop_pending && claimed.claim_cookie == COOKIE;
    target.exit_status = SIGKILL;
    target.has_exited = true;
    target.exit_notify_ready.store(true);
    ok &= child_events::publish_exit(target) && !target.ptrace_stop_blocks_execution();
    ok &= child_events::commit_claim(tracer, tracer, claimed, false);
    child_events::release_claim(tracer, claimed);
    ready = false;
    ok &= capture_wait_event(tracer, target, nullptr, claimed, stop, ready) == 0 && ready && stop.wait_status == SIGKILL;
    ok &= child_events::commit_claim(tracer, tracer, claimed, false);
    child_events::release_claim(tracer, claimed);
    child_events::detach_tracer(target);
    return ok && tracer.ref_count.load() == 1 && target.ref_count.load() == 1;
}

auto ptrace_selftest_observation_snapshot_and_claims() -> bool {
    using ker::mod::security::Change;
    using ker::mod::security::Snapshot;
    Task tracer{};
    Task target{};
    tracer.pid = 0x9E001;
    target.pid = 0x9E002;
    tracer.type = target.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const ADMITTED = Snapshot::bootstrap_root();
    Snapshot changed;
    if (ADMITTED.prepare({.change = Change::SET_DUMPABLE, .dumpable = !ADMITTED.dumpable()}, changed) != 0 ||
        ker::mod::sched::resource::initialize_credentials(tracer, ADMITTED) != 0 ||
        ker::mod::sched::resource::initialize_credentials(target, changed) != 0)
        return false;
    // Private fixtures must never be published by a lifecycle wake.
    tracer.state.store(ker::mod::sched::task::TaskState::EXITING, std::memory_order_release);
    target.state.store(ker::mod::sched::task::TaskState::EXITING, std::memory_order_release);
    target.ptrace_traced = true;
    target.ptrace_tracer_pid = tracer.pid;
    target.ptrace_stopped = true;
    target.ptrace_stop_pending = true;
    target.ptrace_stop_reason = abi::ptrace::stop_reason::INTERRUPT;
    target.ptrace_stop_signal = SIGSTOP;
    std::array<uint64_t, 2> tids{11, 22};
    size_t count = 17;
    abi::ptrace::RemoteInfo remote{};
    std::memset(&remote, 0xa5, sizeof(remote));
    auto const REMOTE_BEFORE = remote;
    abi::ptrace::StopInfo stop{};
    std::memset(&stop, 0xa5, sizeof(stop));
    auto const STOP_BEFORE = stop;
    child_events::ClaimedEvent claimed{};
    bool exited = true;
    bool ready = true;
    if (capture_threads(target, ADMITTED, tids.data(), tids.size(), count) != as_error(EPERM) || count != 17 ||
        tids != std::array<uint64_t, 2>{11, 22} || capture_remote_info(target, ADMITTED, remote) != as_error(EPERM) ||
        std::memcmp(&remote, &REMOTE_BEFORE, sizeof(remote)) != 0 ||
        begin_syscall_wait(tracer, target, ADMITTED, exited) != as_error(EPERM) || !exited || !target.ptrace_stopped ||
        target.ptrace_syscall_trace || capture_wait_event(tracer, target, &ADMITTED, claimed, stop, ready) != as_error(EPERM) || !ready ||
        claimed.node != nullptr || std::memcmp(&stop, &STOP_BEFORE, sizeof(stop)) != 0)
        return false;
    if (capture_threads(target, changed, nullptr, 0, count) != 0 || count != 0 || capture_remote_info(target, changed, remote) != 0 ||
        remote.proxy_pid != target.pid || remote.task_id != 0 || remote.state != 0 || remote.target_node != PTRACE_REMOTE_NODE_INVALID)
        return false;
    target.has_exited = true;
    target.exit_notify_ready.store(true, std::memory_order_release);
    exited = false;
    bool ok = begin_syscall_wait(tracer, target, changed, exited) == 0 && exited && target.ptrace_stopped && !target.ptrace_syscall_trace;
    target.has_exited = false;
    target.exit_notify_ready.store(false, std::memory_order_release);
    if (!child_events::attach_tracer(target, tracer)) return false;
    ok = ok && publish_stop_event(target);
    ready = false;
    ok = ok && capture_wait_event(tracer, target, nullptr, claimed, stop, ready) == 0 && ready && stop.event.tid == target.pid &&
         stop.event.reason == abi::ptrace::stop_reason::INTERRUPT && stop.wait_status == stop_wait_status(target);
    auto const COOKIE = claimed.claim_cookie;
    auto const CLAIMED_STOP = stop;
    child_events::ClaimedEvent competing{};
    bool competing_ready = false;
    ok = ok && capture_wait_event(tracer, target, nullptr, competing, stop, competing_ready) == as_error(EBUSY) && !competing_ready &&
         std::memcmp(&stop, &CLAIMED_STOP, sizeof(stop)) == 0;
    child_events::release_claim(tracer, claimed);
    ready = false;
    ok = ok && capture_wait_event(tracer, target, nullptr, claimed, stop, ready) == 0 && ready && claimed.claim_cookie != COOKIE;
    ok = ok && child_events::commit_claim(tracer, tracer, claimed, false) && !target.ptrace_stop_pending;
    child_events::release_claim(tracer, claimed);
    child_events::detach_tracer(target);
    return ok && tracer.ref_count.load(std::memory_order_acquire) == 1 && target.ref_count.load(std::memory_order_acquire) == 1;
}

auto ptrace_selftest_register_snapshot_rejects_changed_credentials() -> bool {
    using namespace ker::mod::security;
    Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.ptrace_stopped = true;
    target.context.frame.rip = 0x401000;
    target.context.frame.rsp = 0x7fff0000;
    target.context.frame.cs = ker::mod::desc::gdt::GDT_USER_CS;
    target.context.frame.ss = ker::mod::desc::gdt::GDT_USER_DS;
    target.context.frame.flags = 0x202;
    target.context.regs.rax = 123;
    target.fx_state.raw.fill(0x37);
    target.ptrace_dr_addr.at(0) = 0x1111;
    target.ptrace_dr7 = X86_DR7_RESERVED_ONES;
    auto const ADMITTED = Snapshot::bootstrap_root();
    Snapshot changed;
    if (ADMITTED.prepare({.change = Change::SET_DUMPABLE, .dumpable = !ADMITTED.dumpable()}, changed) != 0 ||
        ker::mod::sched::resource::initialize_credentials(target, changed) != 0)
        return false;
    abi::ptrace::X86_64GprState before{};
    if (!fill_gprs(target, before)) return false;
    auto input = before;
    input.rax = 456;
    abi::ptrace::X86_64GprState output{};
    std::memset(&output, 0xa5, sizeof(output));
    auto const SENTINEL = output;
    auto const FPU_BEFORE = target.fx_state.raw;
    XStateBuffer xstate{};
    xstate.fill(0xa5);
    auto const XSTATE_SENTINEL = xstate;
    XStateBuffer valid_xstate{};
    valid_xstate.at(0) = 0x7f;
    valid_xstate.at(1) = 0x03;
    valid_xstate.at(24) = 0x80;
    valid_xstate.at(25) = 0x1f;
    abi::ptrace::HwBreak const POINT{.address = 0x401000, .length = 1, .type = abi::ptrace::hw_break_type::EXECUTE, .slot = 0};
    if (capture_gprs(target, output, ADMITTED) != as_error(EPERM) || std::memcmp(&output, &SENTINEL, sizeof(output)) != 0 ||
        apply_gprs(target, input, ADMITTED) != as_error(EPERM) || target.context.regs.rax != before.rax ||
        capture_xstate(target, xstate, ADMITTED) != as_error(EPERM) || xstate != XSTATE_SENTINEL ||
        apply_xstate(target, valid_xstate, ADMITTED) != as_error(EPERM) || target.fx_state.raw != FPU_BEFORE || target.fx_state.saved ||
        target.fx_state.live_saved || target.fx_state.initialized || apply_hw_break(target, POINT, true, ADMITTED) != as_error(EPERM) ||
        apply_hw_break(target, POINT, false, ADMITTED) != as_error(EPERM) || target.ptrace_dr_addr.at(0) != 0x1111 ||
        target.ptrace_dr7 != X86_DR7_RESERVED_ONES)
        return false;
    if (capture_gprs(target, output, changed) != 0 || std::memcmp(&output, &before, sizeof(output)) != 0 ||
        apply_gprs(target, input, changed) != 0 || target.context.regs.rax != input.rax || apply_gprs(target, before, changed) != 0 ||
        capture_xstate(target, xstate, changed) != 0 || std::memcmp(xstate.data(), target.fx_state.aligned(), xstate.size()) != 0 ||
        apply_xstate(target, valid_xstate, changed) != 0 ||
        std::memcmp(valid_xstate.data(), target.fx_state.aligned(), valid_xstate.size()) != 0 || !target.fx_state.saved ||
        !target.fx_state.live_saved || !target.fx_state.initialized)
        return false;
    return apply_hw_break(target, POINT, true, changed) == 0 && target.ptrace_dr_addr.at(0) == POINT.address &&
           apply_hw_break(target, POINT, false, changed) == 0 && target.ptrace_dr_addr.at(0) == 0;
}

auto ptrace_selftest_memory_snapshot_rejects_changed_credentials() -> bool {
    using namespace ker::mod::security;
    Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const ADMITTED = Snapshot::bootstrap_root();
    Snapshot changed;
    if (ADMITTED.prepare({.change = Change::SET_DUMPABLE, .dumpable = !ADMITTED.dumpable()}, changed) != 0 ||
        ker::mod::sched::resource::initialize_credentials(target, changed) != 0)
        return false;
    std::array<uint8_t, 8> bytes{1, 2, 3, 4, 5, 6, 7, 8};
    auto const BEFORE = bytes;
    for (bool write : {false, true}) {
        size_t transferred = 19;
        if (copy_prepared_task_memory(target, 1, bytes.data(), bytes.size(), write, &transferred, ADMITTED) != as_error(EPERM) ||
            transferred != 0 || bytes != BEFORE)
            return false;
        transferred = 19;
        if (copy_task_memory(target, 1, bytes.data(), bytes.size(), write, &transferred, ADMITTED) != as_error(EPERM) || transferred != 0 ||
            bytes != BEFORE)
            return false;
        if (copy_prepared_task_memory(target, 1, bytes.data(), bytes.size(), write, &transferred, changed) != as_error(EFAULT) ||
            transferred != 0 || bytes != BEFORE)
            return false;
    }
    return write_prepared_task_word(target, 1, 0x1234, ADMITTED) == as_error(EPERM) &&
           write_task_word(target, 1, 0x1234, ADMITTED) == as_error(EPERM) &&
           write_prepared_task_word(target, 1, 0x1234, changed) == as_error(EFAULT) && target.credentials() == changed &&
           target.pagemap == nullptr && target.usercopy_pagemap_accesses.load() == 0;
}

auto ptrace_selftest_image_snapshot_rejects_changed_credentials() -> bool {
    using namespace ker::mod::security;
    namespace runtime = ker::loader::runtime;
    Task tracer{}, target{};
    tracer.pid = 71;
    target.pid = 72;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const ROOT = Snapshot::bootstrap_root();
    if (ker::mod::sched::resource::initialize_credentials(tracer, ROOT) != 0 ||
        ker::mod::sched::resource::initialize_credentials(target, ROOT) != 0)
        return false;
    target.ptrace_traced = true;
    target.ptrace_tracer_pid = tracer.pid;
    Snapshot admitted;
    if (require_traced(tracer, target, &admitted) != 0 || admitted != ROOT) return false;
    auto* images = new (std::nothrow) std::array<runtime::RuntimeImage, runtime::MAX_IMAGES>{};
    if (images == nullptr) return false;
    images->front().entry = 0x1234;
    images->back().entry = 0x5678;
    size_t needed = 17;
    auto status = runtime::SnapshotStatus::TRUNCATED;
    Snapshot changed;
    bool ok = ROOT.prepare({.change = Change::SET_DUMPABLE, .dumpable = !ROOT.dumpable()}, changed) == 0 &&
              ker::mod::sched::resource::initialize_credentials(target, changed) == 0;
    ok = ok && !snapshot_target_images(target, admitted, *images, needed, status, true) && needed == 17 &&
         status == runtime::SnapshotStatus::TRUNCATED && images->front().entry == 0x1234 && images->back().entry == 0x5678;
    // An empty private Task has no userspace image extents. Unprivileged
    // catalogs must not disclose the KASLR slide; a KERNEL_DEBUG tracer gets
    // the authenticated kernel record used by debugserver and WOSDBG.
    needed = 0;
    status = runtime::SnapshotStatus::UNAVAILABLE;
    ok = ok && require_traced(tracer, target, &admitted) == 0 && snapshot_target_images(target, admitted, *images, needed, status, false) &&
         needed == 0 && status == runtime::SnapshotStatus::STATIC_IMAGE;
    needed = 0;
    status = runtime::SnapshotStatus::UNAVAILABLE;
    ok = ok && snapshot_target_images(target, admitted, *images, needed, status, true) && needed == 1 &&
         status == runtime::SnapshotStatus::STATIC_IMAGE && (images->at(0).flags & runtime::IMAGE_KERNEL) != 0U;
    delete images;
    return ok;
}

auto ptrace_selftest_register_rejection_preserves_state() -> bool {
    Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.ptrace_stopped = true;
    target.context.frame.rip = 0x401000;
    target.context.frame.rsp = 0x7fff0000;
    target.context.frame.cs = ker::mod::desc::gdt::GDT_USER_CS;
    target.context.frame.ss = ker::mod::desc::gdt::GDT_USER_DS;
    target.context.frame.flags = 0x202;
    target.context.regs.rax = 123;
    abi::ptrace::X86_64GprState before{};
    if (!fill_gprs(target, before)) {
        return false;
    }
    for (unsigned bit = 12; bit < 16; ++bit) {
        auto input = before;
        input.rax = 456;
        input.rflags |= 1ULL << bit;
        abi::ptrace::X86_64GprState after{};
        if (set_gprs(target, input) != as_error(EINVAL) || !fill_gprs(target, after) || std::memcmp(&before, &after, sizeof(before)) != 0) {
            return false;
        }
    }
    auto input = before;
    input.rax = 789;
    if (set_gprs(target, input) != 0 || target.context.regs.rax != input.rax) {
        return false;
    }
    target.context.frame.cs = ker::mod::desc::gdt::GDT_KERN_CS;
    auto const REGS = target.context.regs;
    auto const FRAME = target.context.frame;
    abi::ptrace::X86_64GprState rejected{};
    std::memset(&rejected, 0xa5, sizeof(rejected));
    auto const OUTPUT = rejected;
    return !fill_gprs(target, rejected) && std::memcmp(&rejected, &OUTPUT, sizeof(OUTPUT)) == 0 &&
           set_gprs(target, input) == as_error(EIO) && std::memcmp(&REGS, &target.context.regs, sizeof(REGS)) == 0 &&
           std::memcmp(&FRAME, &target.context.frame, sizeof(FRAME)) == 0;
}

auto ptrace_selftest_syscall_snapshot_patches_live_sysret_state() -> bool {
    auto* stack = static_cast<uint8_t*>(ker::mod::mm::phys::kernel_stack_alloc("ptrace_sysret_state_ktest"));
    if (stack == nullptr) {
        return false;
    }

    uint64_t const STACK_TOP = reinterpret_cast<uint64_t>(stack) + ker::mod::mm::KERNEL_STACK_SIZE;
    uint64_t const REGS_ADDR = STACK_TOP - sizeof(uint64_t) - sizeof(ker::mod::cpu::GPRegs);
    auto* saved_regs = reinterpret_cast<ker::mod::cpu::GPRegs*>(REGS_ADDR);
    *saved_regs = {};
    saved_regs->rcx = 0x0000000000401000ULL;
    saved_regs->r11 = 0x202;

    ker::mod::cpu::PerCpu scratch{};
    scratch.syscall_stack = STACK_TOP;
    scratch.user_rsp = 0x00007fffffffe000ULL;
    scratch.syscall_ret_rip = saved_regs->rcx;
    scratch.syscall_ret_flags = saved_regs->r11;

    Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.context.syscall_kernel_stack = STACK_TOP;
    target.context.syscall_scratch_area = reinterpret_cast<uint64_t>(&scratch);
    target.ptrace_stopped = true;
    target.ptrace_stop_reason = abi::ptrace::stop_reason::SYSCALL_ENTER;
    target.ptrace_syscall_regs = {};
    target.ptrace_syscall_regs.rcx = saved_regs->rcx;
    target.ptrace_syscall_regs.r11 = saved_regs->r11;
    target.ptrace_syscall_frame.rip = 0x0000000000402000ULL;
    target.ptrace_syscall_frame.rsp = 0x00007fffffffd000ULL;
    target.ptrace_syscall_frame.flags = 0x246;

    bool const PUBLISHED = publish_stop_snapshot_to_syscall_scratch(target);
    bool const OK = PUBLISHED && scratch.syscall_ret_rip == target.ptrace_syscall_frame.rip &&
                    scratch.user_rsp == target.ptrace_syscall_frame.rsp && scratch.syscall_ret_flags == target.ptrace_syscall_frame.flags &&
                    saved_regs->rcx == target.ptrace_syscall_frame.rip && saved_regs->r11 == target.ptrace_syscall_frame.flags &&
                    target.ptrace_syscall_regs.rcx == target.ptrace_syscall_frame.rip &&
                    target.ptrace_syscall_regs.r11 == target.ptrace_syscall_frame.flags;

    ker::mod::mm::phys::page_free(stack);
    return OK;
}

auto ptrace_selftest_deferred_syscall_exit_stop_suppression() -> bool {
    Task internal_resched{};
    internal_resched.deferred_task_switch = true;
    internal_resched.yield_switch = true;
    bool ok = !should_suppress_deferred_syscall_exit_stop(internal_resched);

    Task waitpid{};
    waitpid.deferred_task_switch = true;
    waitpid.set_wait_channel("waitpid", ker::mod::sched::task::WaitChannelKind::WAITPID);

    ok = ok && should_suppress_deferred_syscall_exit_stop(waitpid);

    Task yielded{};
    yielded.deferred_task_switch = true;
    yielded.yield_switch = true;
    yielded.set_wait_channel("sched_yield");
    ok = ok && !should_suppress_deferred_syscall_exit_stop(yielded);

    Task stale_yield_waitpid{};
    stale_yield_waitpid.deferred_task_switch = true;
    stale_yield_waitpid.yield_switch = true;
    stale_yield_waitpid.set_wait_channel("waitpid", ker::mod::sched::task::WaitChannelKind::WAITPID);
    ok = ok && should_suppress_deferred_syscall_exit_stop(stale_yield_waitpid);

    Task target{};

    target.deferred_task_switch = true;
    target.wki_proxy_task_id = 42;
    target.set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
    ok = ok && is_wki_execve_proxy_handoff(target) && should_suppress_deferred_syscall_exit_stop(target);

    target.set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    ok = ok && !is_wki_execve_proxy_handoff(target);

    target.set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
    target.deferred_task_switch = false;
    ok = ok && !is_wki_execve_proxy_handoff(target);

    target.deferred_task_switch = true;
    target.wki_proxy_task_id = 0;
    ok = ok && !is_wki_execve_proxy_handoff(target);

    return ok;
}

auto ptrace_selftest_detach_preserves_wki_execve_proxy_wait() -> bool {
    Task proxy{};
    proxy.wki_proxy_task_id = 42;
    proxy.sched_queue = Task::sched_queue::WAITING;
    proxy.set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);

    bool ok = is_wki_execve_proxy_wait(proxy);
    ok = ok && !should_wake_after_trace_clear(proxy, false, false);
    ok = ok && !should_wake_after_trace_clear(proxy, true, false);
    ok = ok && !should_wake_after_trace_clear(proxy, false, true);

    Task ptrace_stop{};
    ptrace_stop.sched_queue = Task::sched_queue::WAITING;
    ptrace_stop.set_wait_channel("ptrace", ker::mod::sched::task::WaitChannelKind::PTRACE);
    ok = ok && should_wake_after_trace_clear(ptrace_stop, false, true);
    ok = ok && should_wake_after_trace_clear(ptrace_stop, true, true);

    Task blocked_tracee{};
    blocked_tracee.sched_queue = Task::sched_queue::WAITING;
    blocked_tracee.set_wait_channel("futex", ker::mod::sched::task::WaitChannelKind::GENERIC);
    ok = ok && !should_wake_after_trace_clear(blocked_tracee, false, false);

    return ok;
}

#endif

}  // namespace ker::mod::debug::ptrace
