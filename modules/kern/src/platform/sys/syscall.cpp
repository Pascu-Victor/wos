#include "syscall.hpp"

#include <cstdint>
#include <syscalls_impl/futex/futex.hpp>
#include <syscalls_impl/multiproc/threadControl.hpp>
#include <syscalls_impl/net/sys_net.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <syscalls_impl/time/time.hpp>
#include <syscalls_impl/vfs/sys_vfs.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

#include "abi/callnums.hpp"
#include "abi/callnums/multiproc.h"
#include "abi/callnums/process.h"
#include "abi/callnums/sys_log.h"
#include "mod/io/serial/serial.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/debug/ptrace.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/sched/scheduler.hpp"
#include "syscalls_impl/log/sys_log.hpp"
#include "syscalls_impl/multiproc/threadInfo.hpp"
#include "syscalls_impl/process/process.hpp"
#include "util/hcf.hpp"

namespace ker::mod::sys {

extern "C" auto syscall_handler(cpu::GPRegs regs) -> uint64_t {
    sched::begin_syscall_accounting();

    uint64_t callnum_raw = regs.rax;
    if (ker::mod::debug::ptrace::report_syscall_stop(regs, callnum_raw, false)) {
        callnum_raw = regs.rax;
    }

    auto callnum = static_cast<abi::callnums>(callnum_raw);
    uint64_t const A1 = regs.rdi;
    uint64_t const A2 = regs.rsi;
    uint64_t const A3 = regs.rdx;
    uint64_t const A4 = regs.r8;
    uint64_t const A5 = regs.r9;
    uint64_t const A6 = regs.r10;

    uint64_t result = 0;
    switch (callnum) {
        case abi::callnums::SYS_LOG:
            result = ker::syscall::log::sys_log(static_cast<abi::sys_log::sys_log_ops>(A1), reinterpret_cast<const char*>(A2), A3, A4,
                                                reinterpret_cast<const char*>(A5));
            break;
        case abi::callnums::FUTEX:
            result = ker::syscall::futex::sys_futex(A1, A2, A3, A4);
            break;
        case abi::callnums::THREADING:
            if (A1 >= 0x100) {
                result =
                    ker::syscall::multiproc::thread_control(static_cast<abi::multiproc::threadControlOps>(A1), reinterpret_cast<void*>(A2),
                                                            reinterpret_cast<void*>(A3), reinterpret_cast<void*>(A4));
            } else {
                result = ker::syscall::multiproc::thread_info(static_cast<abi::multiproc::threadInfoOps>(A1));
            }
            break;
        case abi::callnums::TIME:
            result = ker::syscall::time::sys_time_get(A1, reinterpret_cast<void*>(A2), reinterpret_cast<void*>(A3));
            break;
        case abi::callnums::VFS:
            result = ker::syscall::vfs::sys_vfs(A1, A2, A3, A4, A5);
            break;
        case abi::callnums::NET:
            result = ker::syscall::net::sys_net(A1, A2, A3, A4, A5, A6);
            break;
        case abi::callnums::VMEM:
            result = ker::syscall::vmem::sys_vmem(A1, A2, A3, A4, A5);
            break;
        case abi::callnums::VMEM_MAP:
            result = ker::syscall::vmem::sys_vmem_map(A1, A2, A3, A4, A5, A6);
            break;
        case abi::callnums::PROCESS:
            result = ker::syscall::process::process(static_cast<abi::process::procmgmt_ops>(A1), A2, A3, A4, A5, regs);
            break;
        case abi::callnums::SHM:
            result = ker::syscall::shm::sys_shm(A1, A2, A3, A4, A5);
            break;
        case abi::callnums::DEBUG:
            // a1=0: disable interrupts on this CPU; a1=1: re-enable
            // clang-tidy treats distinct inline asm branches as identical.
            // NOLINTBEGIN(bugprone-branch-clone)
            switch (A1) {
                case 0:
                    asm volatile("cli" ::: "memory");
                    break;
                case 1:
                default:
                    asm volatile("sti" ::: "memory");
                    break;
            }
            // NOLINTEND(bugprone-branch-clone)
            result = 0;
            break;

        default:
            io::serial::write("Syscall undefined\n");
            io::serial::write("Callnum: ");
            io::serial::write(static_cast<uint64_t>(callnum));
            io::serial::write("\n");
            io::serial::write("a1: ");
            io::serial::write(A1);
            io::serial::write("\n");
            io::serial::write("a2: ");
            io::serial::write(A2);
            io::serial::write("\n");
            io::serial::write("a3: ");
            io::serial::write(A3);
            io::serial::write("\n");
            io::serial::write("a4: ");
            io::serial::write(A4);
            io::serial::write("\n");
            io::serial::write("a5: ");
            io::serial::write(A5);
            io::serial::write("\n");
            io::serial::write("a6: ");
            io::serial::write(A6);
            io::serial::write("\n");
            io::serial::write("Halting\n");
            hcf();
            __builtin_unreachable();
    }

    auto exit_regs = regs;
    exit_regs.rax = result;
    sched::finish_syscall_accounting();
    if (ker::mod::debug::ptrace::report_syscall_stop(exit_regs, callnum_raw, true)) {
        result = exit_regs.rax;
    }
    return result;
}

// Called from syscall.asm when RCX (return RIP) was corrupted during syscall handling.
// This means the kernel stack has been overwritten between pushq (entry) and popq (exit).
extern "C" [[noreturn]] void wos_sysret_corrupt_panic(uint64_t actual_rcx, uint64_t expected_rcx, uint64_t user_rsp) {
    io::serial::write("\n!!! SYSRET CORRUPTION DETECTED !!!\n");
    io::serial::write("  RCX (actual):   0x");
    io::serial::write(actual_rcx);
    io::serial::write("\n  RCX (expected): 0x");
    io::serial::write(expected_rcx);
    io::serial::write("\n  User RSP:       0x");
    io::serial::write(user_rsp);

    // Dump CPU and task info
    io::serial::write("\n  CPU: ");
    io::serial::write(cpu::current_cpu());

    // Read the scratch area fields that might explain what happened
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t gs_kern_stack = 0;
    asm volatile("movq %%gs:0x0, %0" : "=r"(gs_kern_stack));
    io::serial::write("\n  gs:0x00 (kernel stack): 0x");
    io::serial::write(gs_kern_stack);

    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t gs_saved_rip = 0;
    asm volatile("movq %%gs:0x28, %0" : "=r"(gs_saved_rip));
    io::serial::write("\n  gs:0x28 (saved RIP):    0x");
    io::serial::write(gs_saved_rip);

    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t gs_cpu_id = 0;
    asm volatile("movq %%gs:0x10, %0" : "=r"(gs_cpu_id));
    io::serial::write("\n  gs:0x10 (cpuId):        0x");
    io::serial::write(gs_cpu_id);

    io::serial::write("\n  Halting.\n");
    for (;;) {
        asm volatile("hlt");
    }
}

extern "C" [[noreturn]] void wos_syscall_bad_stack_panic(uint64_t gs_stack, uint64_t user_rsp, uint64_t cpu_id) {
    io::serial::write("\n!!! SYSCALL STACK CORRUPTION DETECTED !!!\n");
    io::serial::write("  gs:0x00 stack: 0x");
    io::serial::write(gs_stack);
    io::serial::write("\n  user RSP:      0x");
    io::serial::write(user_rsp);
    io::serial::write("\n  gs:0x10 CPU:   0x");
    io::serial::write(cpu_id);
    io::serial::write("\n  Halting before touching the user red zone.\n");
    for (;;) {
        asm volatile("hlt");
    }
}

void init() {
    uint64_t efer = 0;
    cpu_get_msr(IA32_EFER, &efer);
    cpu_set_msr(IA32_EFER, efer | (1 << 0));                                                      // Enable syscall/sysret
    cpu_set_msr(IA32_FMASK, (1 << 9) | (1 << 8));                                                 // clear IF and TF
    uint64_t const KERN_CS = desc::gdt::GDT_ENTRY_KERNEL_CODE * 8;                                // Kernel CS
    uint64_t const USER_CS = ((desc::gdt::GDT_ENTRY_USER_CODE * 8) | desc::gdt::GDT_RING3) - 16;  // -16 since 64 bit sysret offsets
    uint64_t const STAR_VALUE = (KERN_CS | (USER_CS << 16)) << 32;  // kern/user CS stored in high half of STAR
    cpu_set_msr(IA32_STAR, STAR_VALUE);
    cpu_set_msr(IA32_LSTAR, reinterpret_cast<uint64_t>(&wos_asm_syscall_handler));
    uint32_t eax = 0;
    uint32_t edx = 0;
    cpu_get_msr(IA32_STAR, &eax, &edx);
}
}  // namespace ker::mod::sys
