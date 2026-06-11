#include "ptrace.hpp"

#include <extern/elf.h>

#include <abi/ptrace.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/remote_compute.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "platform/asm/cpu.hpp"

namespace ker::mod::debug::ptrace {
namespace {
using log = ker::mod::dbg::logger<"ptrace">;
using Task = ker::mod::sched::task::Task;

constexpr uint64_t X86_RFLAGS_TF = 1ULL << 8;
constexpr uint64_t PTRACE_REMOTE_NODE_INVALID = 0xffff'ffff'ffff'ffffULL;
constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
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

auto process_id(const Task& task) -> uint64_t { return ker::mod::sched::task::process_pid(task); }

auto can_trace(const Task& tracer, const Task& target) -> bool {
    if (target.type != ker::mod::sched::task::TaskType::PROCESS) {
        return false;
    }
    if (tracer.pid == target.pid) {
        return false;
    }
    if (tracer.euid == 0) {
        return true;
    }
    return tracer.euid == target.euid && tracer.uid == target.uid;
}

auto get_target(uint64_t pid, Task& tracer) -> Task* {
    if (pid == 0) {
        return &tracer;
    }
    auto* target = ker::mod::sched::find_task_by_pid_safe(pid);
    if (target == nullptr) {
        return nullptr;
    }
    return target;
}

void release_target(Task* target, Task& tracer) {
    if (target != nullptr && target != &tracer) {
        target->release();
    }
}

auto copy_task_memory(Task& target, uint64_t target_addr, void* user_buffer, size_t len, bool write_target, size_t* transferred)
    -> uint64_t {
    if (target.pagemap == nullptr || user_buffer == nullptr) {
        return as_error(EFAULT);
    }

    auto* bytes = static_cast<uint8_t*>(user_buffer);
    size_t done = 0;
    while (done < len) {
        uint64_t const CUR = target_addr + done;
        uint64_t const PHYS = ker::mod::mm::virt::translate(target.pagemap, CUR);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            break;
        }
        auto* hhdm = reinterpret_cast<uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
        size_t const PAGE_LEFT = 0x1000 - (CUR & 0xfff);
        size_t const CHUNK = std::min(PAGE_LEFT, len - done);
        if (write_target) {
            std::memcpy(hhdm, bytes + done, CHUNK);
        } else {
            std::memcpy(bytes + done, hhdm, CHUNK);
        }
        done += CHUNK;
    }

    if (transferred != nullptr) {
        *transferred = done;
    }
    return done == len ? 0 : as_error(EFAULT);
}

auto is_user_canonical(uint64_t value) -> bool { return value != 0 && value < 0x0000'8000'0000'0000ULL; }

auto uses_stop_snapshot(const Task& target) -> bool {
    return target.ptrace_stopped && (is_syscall_stop(target.ptrace_stop_reason) || target.ptrace_stop_uses_syscall_snapshot);
}

auto syscall_scratch(Task& target) -> ker::mod::cpu::PerCpu* {
    if (target.context.syscall_scratch_area == 0) {
        return nullptr;
    }
    return reinterpret_cast<ker::mod::cpu::PerCpu*>(target.context.syscall_scratch_area);
}

auto publish_stop_snapshot_to_syscall_scratch(Task& target) -> bool {
    auto* scratch = syscall_scratch(target);
    if (scratch == nullptr) {
        return false;
    }
    scratch->user_rsp = target.ptrace_syscall_frame.rsp;
    scratch->syscall_ret_rip = target.ptrace_syscall_frame.rip;
    scratch->syscall_ret_flags = target.ptrace_syscall_frame.flags;
    return true;
}

auto capture_async_syscall_return_snapshot(Task& target) -> bool {
    target.ptrace_stop_uses_syscall_snapshot = false;
    if (target.type != ker::mod::sched::task::TaskType::PROCESS || (target.context.frame.cs & 0x3U) == 3) {
        return false;
    }

    auto* scratch = syscall_scratch(target);
    if (scratch == nullptr || !is_user_canonical(scratch->syscall_ret_rip) || !is_user_canonical(scratch->user_rsp) ||
        (scratch->syscall_ret_flags & 0x2ULL) == 0) {
        return false;
    }

    target.ptrace_syscall_regs = target.context.regs;
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

auto fill_gprs(Task& target, abi::ptrace::X86_64GprState& out) -> void {
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
}

auto set_gprs(Task& target, const abi::ptrace::X86_64GprState& in) -> void {
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
}

auto user_resume_frame_for_step(Task& target) -> ker::mod::gates::InterruptFrame* {
    if (!target.ptrace_stopped) {
        return nullptr;
    }
    auto* frame = uses_stop_snapshot(target) ? &target.ptrace_syscall_frame : &target.context.frame;
    return (frame->cs & 0x3U) == 3 ? frame : nullptr;
}

void clear_task_trap_flags(Task& target) {
    target.context.frame.flags &= ~X86_RFLAGS_TF;
    target.ptrace_syscall_frame.flags &= ~X86_RFLAGS_TF;
}

void complete_trace_wait(Task& tracer, Task& stopped) {
    if (tracer.waiting_for_pid != stopped.pid && tracer.waiting_for_pid != WAIT_ANY_CHILD) {
        return;
    }

    if (tracer.wait_status_user_addr != 0 && tracer.pagemap != nullptr) {
        uint64_t const STATUS_PHYS = ker::mod::mm::virt::translate(tracer.pagemap, tracer.wait_status_user_addr);
        if (STATUS_PHYS != ker::mod::mm::virt::PADDR_INVALID && STATUS_PHYS != 0) {
            auto* status = reinterpret_cast<int32_t*>(ker::mod::mm::addr::get_virt_pointer(STATUS_PHYS));
            *status = static_cast<int32_t>((stop_signal(stopped) << 8U) | STOP_STATUS_LOW);
        }
    }

    tracer.context.regs.rax = stopped.pid;
    tracer.waiting_for_pid = 0;
    tracer.wait_status_user_addr = 0;
    tracer.wait_status_phys_addr = 0;
    tracer.wait_rusage_user_addr = 0;
    tracer.wait_rusage_phys_addr = 0;
    stopped.ptrace_stop_pending = false;
    ker::mod::sched::reschedule_task_for_cpu(tracer.cpu, &tracer);
}

void wake_tracer_for_stop(Task& stopped) {
    if (!stopped.ptrace_traced || stopped.ptrace_tracer_pid == 0) {
        return;
    }
    auto* tracer = ker::mod::sched::find_task_by_pid_safe(stopped.ptrace_tracer_pid);
    if (tracer == nullptr) {
        return;
    }
    complete_trace_wait(*tracer, stopped);
    tracer->release();
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
    stopped.wait_channel = "ptrace";
    stopped.ptrace_stop_uses_syscall_snapshot = false;

    auto* current = ker::mod::sched::get_current_task();
    if (current == &stopped) {
        stopped.deferred_task_switch = true;
    } else {
        ker::mod::sched::debug_stop_task(&stopped);
        (void)capture_async_syscall_return_snapshot(stopped);
    }

    wake_tracer_for_stop(stopped);
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

auto require_traced(Task& tracer, Task& target) -> uint64_t {
    if (!target.ptrace_traced || target.ptrace_tracer_pid != tracer.pid) {
        return as_error(EPERM);
    }
    return 0;
}

auto attach(Task& tracer, Task& target, bool seize) -> uint64_t {
    if (!can_trace(tracer, target)) {
        return as_error(EPERM);
    }
    if (target.ptrace_traced && target.ptrace_tracer_pid != tracer.pid) {
        return as_error(EBUSY);
    }

    target.ptrace_traced = true;
    target.ptrace_tracer_pid = tracer.pid;
    target.ptrace_stop_reason = seize ? abi::ptrace::stop_reason::NONE : abi::ptrace::stop_reason::INTERRUPT;
    target.ptrace_stop_signal = seize ? 0 : SIGSTOP;
    target.ptrace_stopped = !seize;
    target.ptrace_stop_pending = !seize;
    target.ptrace_stop_uses_syscall_snapshot = false;
    target.wait_channel = target.ptrace_stopped ? "ptrace" : target.wait_channel;
    if (target.ptrace_stopped) {
        ker::mod::sched::debug_stop_task(&target);
        (void)capture_async_syscall_return_snapshot(target);
        wake_tracer_for_stop(target);
    }
    log::debug("trace attach tracer=%lu target=%lu seize=%u", tracer.pid, target.pid, seize ? 1U : 0U);
    return 0;
}

auto detach(Task& tracer, Task& target) -> uint64_t {
    uint64_t const ERR = require_traced(tracer, target);
    if (ERR != 0) {
        return ERR;
    }
    if (uses_stop_snapshot(target)) {
        (void)publish_stop_snapshot_to_syscall_scratch(target);
    }
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
    if (target.wait_channel != nullptr && std::strcmp(target.wait_channel, "ptrace") == 0) {
        target.wait_channel = nullptr;
    }
    ker::mod::sched::reschedule_task_for_cpu(target.cpu, &target);
    return 0;
}

auto read_regset(Task& target, uint64_t data) -> uint64_t {
    auto* io = reinterpret_cast<abi::ptrace::RegsetIo*>(data);
    if (io == nullptr || io->buffer == nullptr) {
        return as_error(EFAULT);
    }
    if (io->kind == abi::ptrace::regset::X86_64_GPR) {
        if (io->size < sizeof(abi::ptrace::X86_64GprState)) {
            return as_error(EINVAL);
        }
        auto* out = static_cast<abi::ptrace::X86_64GprState*>(io->buffer);
        fill_gprs(target, *out);
        io->size = sizeof(abi::ptrace::X86_64GprState);
        return 0;
    }
    if (io->kind == abi::ptrace::regset::X86_64_XSAVE) {
        if (io->size < ker::mod::sched::task::FxState::XSAVE_AREA_SIZE) {
            return as_error(EINVAL);
        }
        std::memcpy(io->buffer, target.fx_state.aligned(), ker::mod::sched::task::FxState::XSAVE_AREA_SIZE);
        io->size = ker::mod::sched::task::FxState::XSAVE_AREA_SIZE;
        return 0;
    }
    return as_error(EINVAL);
}

auto write_regset(Task& target, uint64_t data) -> uint64_t {
    auto* io = reinterpret_cast<abi::ptrace::RegsetIo*>(data);
    if (io == nullptr || io->buffer == nullptr) {
        return as_error(EFAULT);
    }
    if (io->kind == abi::ptrace::regset::X86_64_GPR) {
        if (io->size < sizeof(abi::ptrace::X86_64GprState)) {
            return as_error(EINVAL);
        }
        const auto* in = static_cast<const abi::ptrace::X86_64GprState*>(io->buffer);
        set_gprs(target, *in);
        return 0;
    }
    if (io->kind == abi::ptrace::regset::X86_64_XSAVE) {
        if (io->size < ker::mod::sched::task::FxState::XSAVE_AREA_SIZE) {
            return as_error(EINVAL);
        }
        std::memcpy(target.fx_state.aligned(), io->buffer, ker::mod::sched::task::FxState::XSAVE_AREA_SIZE);
        target.fx_state.saved = true;
        return 0;
    }
    return as_error(EINVAL);
}

auto list_threads(Task& target, uint64_t data) -> uint64_t {
    auto* list = reinterpret_cast<abi::ptrace::ThreadList*>(data);
    if (list == nullptr || (list->capacity != 0 && list->tids == nullptr)) {
        return as_error(EFAULT);
    }

    size_t count = 0;
    uint64_t const GROUP = process_id(target);
    uint64_t const ACTIVE_COUNT = ker::mod::sched::get_active_task_count();
    for (uint64_t idx = 0; idx < ACTIVE_COUNT; ++idx) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(idx);
        if (candidate == nullptr) {
            continue;
        }
        bool const SAME_GROUP = process_id(*candidate) == GROUP;
        if (SAME_GROUP) {
            if (count < list->capacity) {
                list->tids[count] = candidate->pid;
            }
            ++count;
        }
        candidate->release();
    }
    list->count = count;
    return count <= list->capacity ? 0 : as_error(ENOSPC);
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

auto main_image_load_base(const Task& target) -> uint64_t {
    if (target.elf_buffer == nullptr || target.elf_buffer_size < sizeof(Elf64_Ehdr)) {
        return target.elf_header_addr;
    }
    auto const* ehdr = reinterpret_cast<const Elf64_Ehdr*>(target.elf_buffer);
    return ehdr->e_type == ET_DYN ? target.elf_header_addr : 0;
}

auto main_image_text_range(const Task& target, uint64_t load_base, uint64_t& text_addr, uint64_t& text_size) -> void {
    text_addr = load_base;
    text_size = 0;
    if (target.elf_buffer == nullptr || target.elf_buffer_size < sizeof(Elf64_Ehdr)) {
        return;
    }

    auto const* ehdr = reinterpret_cast<const Elf64_Ehdr*>(target.elf_buffer);
    if (ehdr->e_phoff == 0 || ehdr->e_phentsize < sizeof(Elf64_Phdr) ||
        ehdr->e_phoff + (static_cast<uint64_t>(ehdr->e_phnum) * ehdr->e_phentsize) > target.elf_buffer_size) {
        return;
    }

    bool found = false;
    uint64_t start = UINT64_MAX;
    uint64_t end = 0;
    for (Elf64_Half i = 0; i < ehdr->e_phnum; ++i) {
        auto const* ph =
            reinterpret_cast<const Elf64_Phdr*>(target.elf_buffer + ehdr->e_phoff + (static_cast<uint64_t>(i) * ehdr->e_phentsize));
        if (ph->p_type != PT_LOAD || (ph->p_flags & PF_X) == 0) {
            continue;
        }
        found = true;
        start = std::min(start, ph->p_vaddr + load_base);
        end = std::max(end, ph->p_vaddr + ph->p_memsz + load_base);
    }

    if (found && end >= start) {
        text_addr = start;
        text_size = end - start;
    }
}

auto list_images(Task& target, uint64_t data) -> uint64_t {
    auto* list = reinterpret_cast<abi::ptrace::ImageList*>(data);
    if (list == nullptr || (list->capacity != 0 && list->images == nullptr)) {
        return as_error(EFAULT);
    }

    size_t needed = target.interp_base != 0 ? 2 : 1;
    list->count = needed;
    if (list->capacity < needed) {
        return as_error(ENOSPC);
    }

    uint64_t const LOAD_BASE = main_image_load_base(target);
    uint64_t text_addr = 0;
    uint64_t text_size = 0;
    main_image_text_range(target, LOAD_BASE, text_addr, text_size);

    auto& main = list->images[0];
    std::memset(&main, 0, sizeof(main));
    copy_image_path(static_cast<char*>(main.path), abi::ptrace::ImageRecord::PATH_LEN, target.exe_path.data());
    main.load_base = LOAD_BASE;
    main.text_addr = text_addr;
    main.text_size = text_size;
    main.entry = target.entry;
    main.flags = 1U;

    if (target.interp_base != 0) {
        auto& interp = list->images[1];
        std::memset(&interp, 0, sizeof(interp));
        copy_image_path(static_cast<char*>(interp.path), abi::ptrace::ImageRecord::PATH_LEN, "/lib/ld.so");
        interp.load_base = target.interp_base;
        interp.text_addr = target.interp_base;
        interp.text_size = 0;
        interp.entry = 0;
        interp.flags = 2U;
    }
    return 0;
}

auto remote_info(Task& target, uint64_t data) -> uint64_t {
    auto* out = reinterpret_cast<abi::ptrace::RemoteInfo*>(data);
    if (out == nullptr) {
        return as_error(EFAULT);
    }
    std::memset(out, 0, sizeof(*out));
    out->is_proxy = target.wki_proxy_task || target.wki_proxy_task_id != 0 ? 1U : 0U;
    if (target.has_exited) {
        out->state = 2U;
    } else {
        out->state = target.wki_remote_pid != 0 ? 1U : 0U;
    }
    out->proxy_pid = target.pid;
    out->task_id = target.wki_proxy_task_id;
    out->target_node = PTRACE_REMOTE_NODE_INVALID;
    out->remote_pid = target.wki_remote_pid;
    uint16_t proxy_target_node = 0;
    if (ker::net::wki::wki_proxy_task_remote_info(&target, &proxy_target_node, out->target_hostname.data(), out->target_hostname.size())) {
        out->target_node = proxy_target_node;
    }
    if (out->target_hostname.at(0) == '\0') {
        std::strncpy(out->target_hostname.data(), target.wki_target_hostname.data(), out->target_hostname.size() - 1);
    }
    if (out->target_hostname.at(0) == '\0') {
        std::strncpy(out->target_hostname.data(), target.wki_submitter_hostname.data(), out->target_hostname.size() - 1);
    }
    return 0;
}

auto set_hw_break(Task& target, uint64_t data, bool enable) -> uint64_t {
    auto* desc = reinterpret_cast<abi::ptrace::HwBreak*>(data);
    if (desc == nullptr) {
        return as_error(EFAULT);
    }
    if (desc->slot >= target.ptrace_dr_addr.size()) {
        return as_error(EINVAL);
    }
    uint64_t length_code = 0;
    if (enable) {
        if (desc->type == abi::ptrace::hw_break_type::EXECUTE && desc->length != 1) {
            return as_error(EINVAL);
        }
        switch (desc->length) {
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
        if (desc->type != abi::ptrace::hw_break_type::EXECUTE && (desc->address % desc->length) != 0) {
            return as_error(EINVAL);
        }
    }

    target.ptrace_dr_addr.at(desc->slot) = enable ? desc->address : 0;
    uint64_t const LOCAL_ENABLE = X86_DR7_LOCAL_ENABLE_MASK << (desc->slot * X86_DR7_SLOT_ENABLE_STRIDE);
    uint64_t const CONTROL_SHIFT = X86_DR7_SLOT_CONTROL_BASE + (desc->slot * X86_DR7_SLOT_CONTROL_STRIDE);
    target.ptrace_dr7 &= ~LOCAL_ENABLE;
    target.ptrace_dr7 &= ~(X86_DR7_SLOT_CONTROL_MASK << CONTROL_SHIFT);
    if (enable) {
        target.ptrace_dr7 |= X86_DR7_RESERVED_ONES;
        target.ptrace_dr7 |= LOCAL_ENABLE;
        target.ptrace_dr7 |= ((static_cast<uint64_t>(desc->type) & 0b11ULL) | (length_code << 2U)) << CONTROL_SHIFT;
    }
    return 0;
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

    if (req == abi::ptrace::request::TRACEME) {
        auto* parent = ker::mod::sched::find_task_by_pid_safe(tracer->parent_pid);
        if (parent == nullptr) {
            return as_error(ESRCH);
        }
        const uint64_t RET = can_trace(*parent, *tracer) ? 0 : as_error(EPERM);
        if (RET == 0) {
            tracer->ptrace_traced = true;
            tracer->ptrace_tracer_pid = parent->pid;
        }
        parent->release();
        return RET;
    }

    auto* target = get_target(pid, *tracer);
    if (target == nullptr) {
        return as_error(ESRCH);
    }

    uint64_t ret = 0;
    switch (req) {
        case abi::ptrace::request::ATTACH:
            ret = attach(*tracer, *target, false);
            break;
        case abi::ptrace::request::SEIZE:
            ret = attach(*tracer, *target, true);
            break;
        case abi::ptrace::request::DETACH:
            ret = detach(*tracer, *target);
            break;
        case abi::ptrace::request::KILL:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                target->sig_pending |= 1ULL << (SIGKILL - 1);
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
        case abi::ptrace::request::CONT:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
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
        case abi::ptrace::request::SINGLESTEP:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                auto* step_frame = user_resume_frame_for_step(*target);
                if (step_frame == nullptr) {
                    ret = as_error(EIO);
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
        case abi::ptrace::request::SYSCALL:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                bool const USE_STOP_SNAPSHOT = uses_stop_snapshot(*target);
                bool const WAS_STOPPED = target->ptrace_stopped;
                target->ptrace_stopped = false;
                target->ptrace_stop_pending = false;
                target->ptrace_single_step = false;
                target->ptrace_syscall_trace = true;
                if (target->ptrace_stop_reason != abi::ptrace::stop_reason::SYSCALL_ENTER) {
                    target->ptrace_syscall_in_stop = false;
                }
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
        case abi::ptrace::request::INTERRUPT:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                target->ptrace_stop_reason = abi::ptrace::stop_reason::INTERRUPT;
                target->ptrace_stop_signal = SIGSTOP;
                target->ptrace_stopped = true;
                target->ptrace_stop_pending = true;
                target->ptrace_stop_uses_syscall_snapshot = false;
                target->wait_channel = "ptrace";
                ker::mod::sched::debug_stop_task(target);
                (void)capture_async_syscall_return_snapshot(*target);
                wake_tracer_for_stop(*target);
            }
            break;
        case abi::ptrace::request::GETREGSET:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = read_regset(*target, data);
            }
            break;
        case abi::ptrace::request::SETREGSET:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = write_regset(*target, data);
            }
            break;
        case abi::ptrace::request::PEEKDATA: {
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                uint64_t word = 0;
                ret = copy_task_memory(*target, addr, &word, sizeof(word), false, nullptr);
                if (ret == 0) {
                    auto* out = reinterpret_cast<uint64_t*>(data);
                    if (out == nullptr) {
                        ret = as_error(EFAULT);
                    } else {
                        *out = word;
                    }
                }
            }
            break;
        }
        case abi::ptrace::request::POKEDATA: {
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                uint64_t word = data;
                ret = copy_task_memory(*target, addr, &word, sizeof(word), true, nullptr);
            }
            break;
        }
        case abi::ptrace::request::READ_MEM: {
            ret = require_traced(*tracer, *target);
            auto* io = reinterpret_cast<abi::ptrace::MemIo*>(data);
            if (ret == 0 && io == nullptr) {
                ret = as_error(EFAULT);
            }
            if (ret == 0) {
                ret = copy_task_memory(*target, io->address, io->buffer, io->size, false, &io->transferred);
            }
            break;
        }
        case abi::ptrace::request::WRITE_MEM: {
            ret = require_traced(*tracer, *target);
            auto* io = reinterpret_cast<abi::ptrace::MemIo*>(data);
            if (ret == 0 && io == nullptr) {
                ret = as_error(EFAULT);
            }
            if (ret == 0) {
                ret = copy_task_memory(*target, io->address, io->buffer, io->size, true, &io->transferred);
            }
            break;
        }
        case abi::ptrace::request::LIST_THREADS:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = list_threads(*target, data);
            }
            break;
        case abi::ptrace::request::GETEVENTMSG:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                auto* out = reinterpret_cast<abi::ptrace::Event*>(data);
                if (out == nullptr) {
                    ret = as_error(EFAULT);
                } else {
                    out->reason = target->ptrace_stop_reason;
                    out->signal = stop_signal(*target);
                    out->tid = target->pid;
                    out->address = target->ptrace_stop_address;
                    out->message = target->ptrace_event_msg;
                }
            }
            break;
        case abi::ptrace::request::SETOPTIONS:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                target->ptrace_options = data;
            }
            break;
        case abi::ptrace::request::GET_REMOTE_INFO:
            ret = remote_info(*target, data);
            break;
        case abi::ptrace::request::SET_HW_BREAK:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = set_hw_break(*target, data, true);
            }
            break;
        case abi::ptrace::request::DEL_HW_BREAK:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = set_hw_break(*target, data, false);
            }
            break;
        case abi::ptrace::request::GET_IMAGES:
            ret = require_traced(*tracer, *target);
            if (ret == 0) {
                ret = list_images(*target, data);
            }
            break;
        case abi::ptrace::request::GET_MAPS:
            ret = as_error(ENOSYS);
            break;
        default:
            ret = as_error(EINVAL);
            break;
    }

    release_target(target, *tracer);
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
    task->wait_channel = "ptrace";
    ker::mod::sched::place_task_in_wait_queue(gpr, frame);
    wake_tracer_for_stop(*task);
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

    if (exiting) {
        if (!task->ptrace_syscall_in_stop) {
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
    task->wait_channel = "ptrace";
    wake_tracer_for_stop(*task);

    while (task->ptrace_stopped) {
        ker::mod::sched::kern_yield();
    }
    restore_syscall_stop_context(*task, gpr);
    return true;
}

auto report_fatal_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum) -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || !task->ptrace_traced) {
        return false;
    }

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
    task->wait_channel = "ptrace";
    wake_tracer_for_stop(*task);

    while (task->ptrace_stopped) {
        ker::mod::sched::kern_yield();
    }
    restore_syscall_stop_context(*task, gpr);
    return true;
}

auto report_signal_stop(ker::mod::sched::task::Task& task, uint32_t signal) -> bool { return record_signal_stop(task, signal); }

}  // namespace ker::mod::debug::ptrace
