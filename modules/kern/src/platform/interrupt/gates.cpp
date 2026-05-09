#include "gates.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/coredump.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <syscalls_impl/process/exit.hpp>

#include "mod/io/serial/serial.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sched/scheduler.hpp"

namespace ker::mod::gates {
namespace {
using journal = dbg::logger<"kernel">;
interruptHandler_t interrupt_handlers[256] = {nullptr};
std::atomic<bool> panic_lock{false};
std::atomic<int64_t> panic_lock_owner{-1};

// Context-based IRQ handlers (parallel array)
struct IrqContext {
    irq_handler_fn handler = nullptr;
    void* data = nullptr;
    const char* name = nullptr;
};
IrqContext irq_contexts[256] = {};

// Next vector to try for allocation (48+ to avoid legacy ISA range)
uint8_t next_alloc_vector = 48;

static auto lookup_user_pte(ker::mod::mm::paging::PageTable* pagemap, uint64_t vaddr) -> const ker::mod::mm::paging::PageTableEntry* {
    if (pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return nullptr;
    }

    const uint64_t IDX4 = (vaddr >> 39) & 0x1FF;
    const uint64_t IDX3 = (vaddr >> 30) & 0x1FF;
    const uint64_t IDX2 = (vaddr >> 21) & 0x1FF;
    const uint64_t IDX1 = (vaddr >> 12) & 0x1FF;

    if (!pagemap->entries[IDX4].present) {
        return nullptr;
    }
    auto* pml3 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pagemap->entries[IDX4].frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml3->entries[IDX3].present) {
        return nullptr;
    }
    auto* pml2 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pml3->entries[IDX3].frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml2->entries[IDX2].present) {
        return nullptr;
    }
    auto* pml1 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pml2->entries[IDX2].frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml1->entries[IDX1].present) {
        return nullptr;
    }

    return &pml1->entries[IDX1];
}
}  // namespace

void exception_handler(cpu::GPRegs& gpr, interruptFrame& frame) {
    // Page fault handler - handles COW faults and user-space segfaults.
    // This MUST run before the panic ownership CAS below, because page faults
    // (especially COW resolution) are normal concurrent events. If two CPUs
    // hit a COW fault at the same time the old code would make the second CPU
    // think it was a nested panic and halt.
    if (frame.intNum == 14) {
        uint64_t cr2 = cr2;
        asm volatile("mov %%cr2, %0" : "=r"(cr2));

        // If pagefault_handler resolved a COW fault, we're done.
        // Otherwise fall through to the normal exception path which will
        // crash the userspace process or kernel-panic as appropriate.
        if (mm::virt::pagefault_handler(cr2, frame, gpr)) {
            return;
        }
    }

    uint64_t cr0{};
    uint64_t cr2{};
    uint64_t cr3{};
    uint64_t cr4{};
    uint64_t cr8{};
    asm volatile("mov %%cr0, %0" : "=r"(cr0));
    asm volatile("mov %%cr2, %0" : "=r"(cr2));
    asm volatile("mov %%cr3, %0" : "=r"(cr3));
    asm volatile("mov %%cr4, %0" : "=r"(cr4));
    asm volatile("mov %%cr8, %0" : "=r"(cr8));

    // If an exception occurs while returning to or running in userspace,
    // it must not take down the whole kernel. Treat it as a process crash.
    // NOTE: userspace faults are handled BEFORE the panic-ownership CAS below
    // because they are normal concurrent events (multiple threads can crash
    // simultaneously) and must never be mistaken for nested kernel panics.
    uint64_t cpl = frame.cs & 0x3;
    if (cpl == 3) {
        auto* current_task_for_dump = ker::mod::sched::get_current_task();

        // DEBUG: Log detailed info about the mismatch between currentTask and CR3
        uint32_t apic_id = apic::getApicId();
        uint64_t cpu_id_from_apic = smt::get_cpu_index_from_apic_id(apic_id);
        journal::warn("USERFAULT DEBUG: apicId=%d cpuFromApic=%d task=%p taskPid=%d cr3=0x%lx taskPagemap=%p", apic_id, cpu_id_from_apic,
                      current_task_for_dump, (current_task_for_dump != nullptr) ? current_task_for_dump->pid : 0xDEAD, cr3,
                      (current_task_for_dump != nullptr) ? (void*)current_task_for_dump->pagemap : nullptr);

        ker::mod::dbg::coredump::tryWriteForTask(current_task_for_dump, gpr, frame, cr2, cr3, apic::getApicId());

        if (frame.intNum == 14) {
            journal::warn("Userspace page fault: cr2=0x%lx err=%d rip=0x%lx rsp=0x%lx pid=%d", cr2, frame.errCode, frame.rip, frame.rsp,
                          (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
            if (current_task_for_dump != nullptr) {
                bool ret_slot_matches_rip = false;
                journal::warn(" task_ctx: saved_rip=0x%lx saved_rsp=0x%lx entry=0x%lx thread=%p scratch=%p",
                              current_task_for_dump->context.frame.rip, current_task_for_dump->context.frame.rsp,
                              current_task_for_dump->entry, current_task_for_dump->thread,
                              reinterpret_cast<void*>(current_task_for_dump->context.syscallScratchArea));
                if (current_task_for_dump->pagemap != nullptr) {
                    const uint64_t RSP_PHYS = ker::mod::mm::virt::translate(current_task_for_dump->pagemap, frame.rsp);
                    const uint64_t RET_SLOT_VA = (frame.rsp >= sizeof(uint64_t)) ? (frame.rsp - sizeof(uint64_t)) : frame.rsp;
                    const uint64_t RET_SLOT_PHYS = ker::mod::mm::virt::translate(current_task_for_dump->pagemap, RET_SLOT_VA);

                    if (RET_SLOT_PHYS != ker::mod::mm::virt::PADDR_INVALID && RET_SLOT_PHYS != 0) {
                        const auto* ret_slot = reinterpret_cast<const uint64_t*>(ker::mod::mm::addr::get_virt_pointer(RET_SLOT_PHYS));
                        ret_slot_matches_rip = ret_slot[0] == frame.rip;
                        journal::warn(" fault_ret_slot_va=0x%lx phys=0x%lx q_m1=0x%lx rip_match=%d", RET_SLOT_VA, RET_SLOT_PHYS,
                                      ret_slot[0], ret_slot_matches_rip ? 1 : 0);
                    } else {
                        journal::warn(" fault_ret_slot_va=0x%lx phys=0x0 (unmapped)", RET_SLOT_VA);
                    }

                    if (RSP_PHYS != ker::mod::mm::virt::PADDR_INVALID && RSP_PHYS != 0) {
                        const auto* stack_words = reinterpret_cast<const uint64_t*>(ker::mod::mm::addr::get_virt_pointer(RSP_PHYS));
                        const uint64_t PAGE_OFF = frame.rsp & (ker::mod::mm::paging::PAGE_SIZE - 1);
                        size_t words_available = (ker::mod::mm::paging::PAGE_SIZE - PAGE_OFF) / sizeof(uint64_t);
                        words_available = std::min<size_t>(words_available, 4);
                        const uint64_t Q0 = (words_available > 0) ? stack_words[0] : 0;
                        const uint64_t Q1 = (words_available > 1) ? stack_words[1] : 0;
                        const uint64_t Q2 = (words_available > 2) ? stack_words[2] : 0;
                        const uint64_t Q3 = (words_available > 3) ? stack_words[3] : 0;
                        journal::warn(" fault_rsp_phys=0x%lx stack_qwords=%zu q0=0x%lx q1=0x%lx q2=0x%lx q3=0x%lx", RSP_PHYS,
                                      words_available, Q0, Q1, Q2, Q3);
                    } else {
                        journal::warn(" fault_rsp_phys=0x0 (unmapped)");
                    }

                    if (const auto* stack_pte = lookup_user_pte(current_task_for_dump->pagemap, frame.rsp); stack_pte != nullptr) {
                        const uint64_t STACK_PAGE_PHYS = static_cast<uint64_t>(stack_pte->frame) << ker::mod::mm::paging::PAGE_SHIFT;
                        uint64_t stack_pte_raw = 0;
                        __builtin_memcpy(&stack_pte_raw, stack_pte, sizeof(stack_pte_raw));
                        const bool STACK_COW = (stack_pte_raw & ker::mod::mm::paging::PAGE_COW) != 0U;
                        const auto STACK_REF =
                            ker::mod::mm::phys::pageRefGet(reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(STACK_PAGE_PHYS)));
                        journal::warn(" stack_pte: vaddr=0x%lx phys=0x%lx user=%u rw=%u nx=%u cow=%u ref=%llu",
                                      frame.rsp & ~(ker::mod::mm::paging::PAGE_SIZE - 1), STACK_PAGE_PHYS, stack_pte->user,
                                      stack_pte->writable, stack_pte->noExecute, STACK_COW ? 1U : 0U,
                                      static_cast<unsigned long long>(STACK_REF));

                        if (ret_slot_matches_rip) {
                            ker::mod::mm::virt::debugLogUserPhysMappings(STACK_PAGE_PHYS, "userfault-stack-ret", current_task_for_dump->pid,
                                                                         current_task_for_dump->name, true);
                        }
                    } else {
                        journal::warn(" stack_pte: vaddr=0x%lx unmapped", frame.rsp & ~(ker::mod::mm::paging::PAGE_SIZE - 1));
                    }
                }
            }

            auto* pml4 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::get_virt_pointer(cr3 & ~0xFFF);
            const uint64_t IDX4 = (cr2 >> 39) & 0x1FF;
            const uint64_t IDX3 = (cr2 >> 30) & 0x1FF;
            const uint64_t IDX2 = (cr2 >> 21) & 0x1FF;
            const uint64_t IDX1 = (cr2 >> 12) & 0x1FF;

            journal::warn("PML4[%d]: present=%d frame=0x%lx", IDX4, pml4->entries[IDX4].present, pml4->entries[IDX4].frame);
            if (pml4->entries[IDX4].present) {
                auto* pml3 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::get_virt_pointer(pml4->entries[IDX4].frame << 12);
                journal::warn(" PML3[%d]: present=%d frame=0x%lx", IDX3, pml3->entries[IDX3].present, pml3->entries[IDX3].frame);
                if (pml3->entries[IDX3].present) {
                    auto* pml2 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::get_virt_pointer(pml3->entries[IDX3].frame << 12);
                    journal::warn("  PML2[%d]: present=%d frame=0x%lx", IDX2, pml2->entries[IDX2].present, pml2->entries[IDX2].frame);
                    if (pml2->entries[IDX2].present) {
                        auto* pml1 =
                            (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::get_virt_pointer(pml2->entries[IDX2].frame << 12);
                        journal::warn("   PML1[%d]: present=%d frame=0x%lx user=%d rw=%d nx=%d", IDX1, pml1->entries[IDX1].present,
                                      pml1->entries[IDX1].frame, pml1->entries[IDX1].user, pml1->entries[IDX1].writable,
                                      pml1->entries[IDX1].noExecute);
                    }
                }
            }

            // Conventional exit code for a segfault-like crash.
            ker::syscall::process::wos_proc_exit(139);
            __builtin_unreachable();
        }

        if (frame.intNum == 1) {
            // Debug exception (hardware breakpoint or TF single-step).
            const uint64_t DR7_CLEAR = 0x400;       // bit 10 reserved=1, all enable bits=0
            const uint64_t DR6_CLEAR = 0xFFFF0FF0;  // reserved bits set, all status bits cleared
            asm volatile("mov %0, %%dr7" ::"r"(DR7_CLEAR));
            asm volatile("mov %0, %%dr6" ::"r"(DR6_CLEAR));
            journal::debug("Userspace debug trap (INT 1): rip=0x%lx pid=%x", frame.rip,
                           (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
            ker::syscall::process::wos_proc_exit(128 + 5);  // 133 = SIGTRAP
            __builtin_unreachable();
        }

        journal::warn("Userspace exception: int=%d err=%d rip=0x%lx pid=%x", frame.intNum, frame.errCode, frame.rip,
                      (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
        ker::syscall::process::wos_proc_exit(128 + (int)(frame.intNum & 0x7f));
        __builtin_unreachable();
    }

    // Kernel-mode exception - this is a kernel bug, don't try to write coredumps
    // as that would require memory allocation which could cause deadlocks if we
    // faulted while holding a spinlock (e.g., in pageAlloc).
    // Coredumps are only for userspace crashes, handled above.
    asm volatile("cli" ::: "memory");
    apic::oneShotTimer(0);

    // Serialize all panicking CPUs: the first CPU owns the dump and halts the
    // rest.  If this CPU already owns the lock, this is a nested fault during
    // panic reporting; halt immediately.
    {
        auto my_apic_id = static_cast<int64_t>(apic::getApicId());
        if (panic_lock_owner.load(std::memory_order_acquire) == my_apic_id) {
            hcf();
        }
        while (panic_lock.exchange(true, std::memory_order_acquire)) {
            asm volatile("pause");
        }
        panic_lock_owner.store(my_apic_id, std::memory_order_release);
    }

    ker::mod::smt::halt_other_cores();

    // CRITICAL: Enter epoch critical section to prevent GC from freeing currentTask
    // or its fields while we're printing panic info.
    ker::mod::sched::EpochManager::enterCriticalAPIC();

    // Acquire the serial lock BEFORE entering panic mode so other CPUs still
    // respect it. Once enterPanicMode() is called, all serial locking becomes
    // a no-op for other CPUs - they cannot deadlock waiting for our lock.
    // Other CPUs have been halted, but keep the serial panic path in its
    // unlocked mode so a nested panic on this CPU cannot deadlock on logging.
    io::serial::acquire_lock();
    io::serial::enter_panic_mode();

    journal::panic("PANIC!");

    // stack trace - validate RSP before dereferencing
    journal::panic("Stack trace:");
    auto* rsp = (uint64_t*)frame.rsp;
    auto rsp_addr = reinterpret_cast<uintptr_t>(rsp);
    const bool RSP_VALID = (rsp_addr >= 0xffff800000000000ULL && rsp_addr < 0xffff900000000000ULL) ||
                           (rsp_addr >= 0xffffffff80000000ULL && rsp_addr < 0xffffffffc0000000ULL);

    if (RSP_VALID) {
        constexpr uint64_t MAX_STACK_TRACE = 64;
        for (uint64_t i = 0; i < MAX_STACK_TRACE; i++) {
            journal::panic("%d: 0x%lx", i, rsp[i]);
        }
    } else {
        journal::panic("Invalid RSP: 0x%lx - skipping stack trace", rsp_addr);
    }

    // Dump current task information - use getCurrentTask() instead of hardcoded address
    // The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
    uint64_t cpu_id = apic::getApicId();
    sched::task::Task* current_task = nullptr;

    if (!sched::has_run_queues()) {
        journal::panic("WARNING: RunQueues not initialized OR runQueue not set - cannot get current task!");
        goto skip_task_dump;  // NOLINT
    }

    current_task = sched::get_current_task();

    journal::panic("=== Current Task Info ===");
    journal::panic("debug_task_ptrs[%d] = 0x%lx", cpu_id, (uint64_t)current_task);

    if (current_task != nullptr) {
        // CRITICAL: Validate task pointer is in valid memory range before dereferencing
        // This prevents nested faults if GC is freeing the task concurrently
        auto task_addr = reinterpret_cast<uintptr_t>(current_task);
        const bool IN_HHDM = (task_addr >= 0xffff800000000000ULL && task_addr < 0xffff900000000000ULL);
        const bool IN_KERNEL_STATIC = (task_addr >= 0xffffffff80000000ULL && task_addr < 0xffffffffc0000000ULL);

        if (!IN_HHDM && !IN_KERNEL_STATIC) {
            journal::panic("currentTask pointer 0x%lx is out of valid kernel range!", (uint64_t)current_task);
            journal::panic("=========================");
            goto skip_task_dump;  // NOLINT
        }

        // Use volatile access to detect if memory is being freed (may contain garbage)
        journal::panic("Task address: 0x%lx", (uint64_t)current_task);

        // Validate and print name with extreme caution
        const char* name_ptr = current_task->name;
        if (name_ptr != nullptr) {
            auto name_addr = reinterpret_cast<uintptr_t>(name_ptr);
            const bool NAME_IN_HHDM = (name_addr >= 0xffff800000000000ULL && name_addr < 0xffff900000000000ULL);
            const bool NAME_IN_KERNEL_STATIC = (name_addr >= 0xffffffff80000000ULL && name_addr < 0xffffffffc0000000ULL);

            if (NAME_IN_HHDM || NAME_IN_KERNEL_STATIC) {
                // Check first byte without assuming string is valid
                volatile const char* volatile_name_ptr = name_ptr;
                volatile char first_char = volatile_name_ptr[0];
                if (first_char >= 0x20 && first_char <= 0x7e) {
                    journal::panic("Task name: %s", name_ptr);
                } else {
                    journal::panic("Task name: <invalid: first byte 0x%x>", (unsigned char)first_char);
                }
            } else {
                journal::panic("Task name ptr: 0x%lx <out of range>", name_addr);
            }
        } else {
            journal::panic("Task name: <null>");
        }

        // Print primitive fields (safe - no pointer dereference)
        journal::panic("PID: 0x%x", current_task->pid);
        journal::panic("Type: 0x%x", (uint64_t)current_task->type);
        journal::panic("Entry: 0x%lx", current_task->entry);
        journal::panic("KthreadEntry: 0x%lx", (uint64_t)current_task->kthreadEntry);
        journal::panic("Pagemap: 0x%lx", (uint64_t)current_task->pagemap);
        journal::panic("Thread: 0x%lx", (uint64_t)current_task->thread);

        journal::panic("Task Context Frame:");
        journal::panic("  frame.rip: 0x%lx", current_task->context.frame.rip);
        journal::panic("  frame.cs: 0x%x", current_task->context.frame.cs);
        journal::panic("  frame.rsp: 0x%lx", current_task->context.frame.rsp);
        journal::panic("  frame.ss: 0x%x", current_task->context.frame.ss);
        journal::panic("  frame.flags: 0x%lx", current_task->context.frame.flags);

        journal::panic("Task Context:");
        journal::panic("  syscallKernelStack: 0x%lx", current_task->context.syscallKernelStack);
        journal::panic("  syscallScratchArea: 0x%lx", current_task->context.syscallScratchArea);
        journal::panic("Task Scheduler State:");
        journal::panic("  cpu=%lu queue=%u voluntary=%u wantsBlock=%u deferred=%u yield=%u", current_task->cpu,
                       static_cast<unsigned>(current_task->schedQueue), current_task->voluntaryBlock ? 1U : 0U,
                       current_task->wantsBlock ? 1U : 0U, current_task->deferredTaskSwitch ? 1U : 0U, current_task->yieldSwitch ? 1U : 0U);
        journal::panic("  preemptDepth=%u preemptPending=%u preemptMaxUs=%lu", current_task->preemptDisableDepth,
                       current_task->preemptPending ? 1U : 0U, current_task->preemptDisableMaxUs);
    } else {
        journal::panic("WARNING: currentTask is NULL!");
    }
    journal::panic("=========================");

skip_task_dump:

    // Check if page 0 is mapped (it should NOT be - null derefs should crash)
    auto* pml4 = (uint64_t*)(cr3 + 0xffff800000000000ULL);  // HHDM offset
    uint64_t pml4e = pml4[0];                               // Entry for VA 0x0
    journal::panic("Page 0 check: PML4[0] = 0x%lx (Present=%d)", pml4e, (pml4e & 1) ? 1 : 0);
    if ((pml4e & 1) != 0U) {
        journal::panic("WARNING: Page 0 is mapped! NULL derefs won't crash!");
    }

    // print frame info
    journal::panic("CPU: %d", cpu_id);
    journal::panic("Interrupt number: %d", frame.intNum);
    journal::panic("Error code: %d", frame.errCode);
    journal::panic("RIP: 0x%lx", frame.rip);
    journal::panic("CS: 0x%x", frame.cs);
    journal::panic("CALCULATED PRIVILEGE LEVEL: %d", frame.cs & 0x3);
    journal::panic("RFLAGS: 0x%lx", frame.flags);
    journal::panic("RSP: 0x%lx", frame.rsp);
    journal::panic("SS: 0x%x", frame.ss);
    journal::panic("CR0: 0x%lx", cr0);
    journal::panic("CR2: 0x%lx", cr2);
    journal::panic("CR3: 0x%lx", cr3);
    journal::panic("CR4: 0x%lx", cr4);
    journal::panic("CR8: 0x%lx", cr8);

    // print general purpose registers
    journal::panic("General purpose registers:");
    journal::panic("RAX: 0x%lx", gpr.rax);
    journal::panic("RBX: 0x%lx", gpr.rbx);
    journal::panic("RCX: 0x%lx", gpr.rcx);
    journal::panic("RDX: 0x%lx", gpr.rdx);
    journal::panic("RDI: 0x%lx", gpr.rdi);
    journal::panic("RSI: 0x%lx", gpr.rsi);
    journal::panic("RBP: 0x%lx", gpr.rbp);
    journal::panic("R8: 0x%lx", gpr.r8);
    journal::panic("R9: 0x%lx", gpr.r9);
    journal::panic("R10: 0x%lx", gpr.r10);
    journal::panic("R11: 0x%lx", gpr.r11);
    journal::panic("R12: 0x%lx", gpr.r12);
    journal::panic("R13: 0x%lx", gpr.r13);
    journal::panic("R14: 0x%lx", gpr.r14);
    journal::panic("R15: 0x%lx", gpr.r15);

    // print segment selectors and descriptors
    journal::panic("Segment selectors and descriptors:");
    struct GDTR {
        uint16_t limit;
        uint64_t base;
    } __attribute__((packed)) gdtr;
    asm volatile("sgdt %0" : "=m"(gdtr));

    // Validate GDTR before using it
    bool gdtr_valid = (gdtr.base >= 0xffff800000000000ULL || gdtr.base >= 0xffffffff80000000ULL);
    journal::panic("GDTR: base=0x%lx, limit=0x%x, valid=%d", gdtr.base, gdtr.limit, gdtr_valid ? 1 : 0);

    auto rdmsr = [](uint32_t msr) -> uint64_t {
        uint32_t lo = 0;
        uint32_t hi = 0;
        asm volatile("rdmsr" : "=a"(lo), "=d"(hi) : "c"(msr));
        return ((uint64_t)hi << 32) | lo;
    };

    uint16_t sel_cs = sel_cs;
    uint16_t sel_ds = sel_ds;
    uint16_t sel_es = sel_es;
    uint16_t sel_fs = sel_fs;
    uint16_t sel_gs = sel_gs;
    uint16_t sel_ss = sel_ss;
    uint16_t sel_tr = sel_tr;
    asm volatile("mov %%cs, %0" : "=r"(sel_cs));
    asm volatile("mov %%ds, %0" : "=r"(sel_ds));
    asm volatile("mov %%es, %0" : "=r"(sel_es));
    asm volatile("mov %%fs, %0" : "=r"(sel_fs));
    asm volatile("mov %%gs, %0" : "=r"(sel_gs));
    asm volatile("mov %%ss, %0" : "=r"(sel_ss));
    asm volatile("str %0" : "=r"(sel_tr));

    auto print_selector = [&](const char* name, uint16_t sel) -> void {
        journal::panic("%s: 0x%x (index=%d, rpl=%d)", name, sel, (uint64_t)(sel >> 3), (uint64_t)(sel & 0x3));

        if (sel == 0) {
            journal::panic("  NULL selector");
            return;
        }

        if (!gdtr_valid) {
            journal::panic("  Skipping descriptor dump (invalid GDTR)");
            return;
        }

        uint64_t gdt_base = gdtr.base;
        uint64_t index = (sel >> 3);
        uint64_t desc_addr = gdt_base + (index * 8);

        // Validate descriptor address is in valid kernel memory
        const bool DESC_ADDR_VALID = (desc_addr >= 0xffff800000000000ULL && desc_addr < 0xffff900000000000ULL) ||
                                     (desc_addr >= 0xffffffff80000000ULL && desc_addr < 0xffffffffc0000000ULL);

        if (!DESC_ADDR_VALID) {
            journal::panic("  Invalid descriptor address: 0x%lx", desc_addr);
            return;
        }

        uint64_t desc = *(uint64_t*)(desc_addr);

        journal::panic("  Raw descriptor: 0x%lx", desc);

        const uint64_t LIMIT_LOW = desc & 0xFFFF;
        const uint64_t BASE_0_15 = (desc >> 16) & 0xFFFF;
        const uint64_t BASE_16_23 = (desc >> 32) & 0xFF;
        const uint64_t ACCESS = (desc >> 40) & 0xFF;
        const uint64_t LIMIT_16_19 = (desc >> 48) & 0xF;
        const uint64_t FLAGS = (desc >> 52) & 0xF;
        const uint64_t BASE_24_31 = (desc >> 56) & 0xFF;

        uint64_t limit = LIMIT_LOW | (LIMIT_16_19 << 16);
        bool gran = (FLAGS >> 3) & 1;
        if (gran) {
            limit = (limit << 12) | 0xFFF;
        }

        const uint64_t BASE = BASE_0_15 | (BASE_16_23 << 16) | (BASE_24_31 << 24);

        bool s_bit = (ACCESS >> 4) & 1;  // 0 = system, 1 = code/data
        if (!s_bit) {
            // system descriptor (e.g., TSS) -- read second qword for full base
            const uint64_t DESC_ADDR_HIGH = desc_addr + 8;
            const bool DESC_ADDR_HIGH_VALID = (DESC_ADDR_HIGH >= 0xffff800000000000ULL && DESC_ADDR_HIGH < 0xffff900000000000ULL) ||
                                              (DESC_ADDR_HIGH >= 0xffffffff80000000ULL && DESC_ADDR_HIGH < 0xffffffffc0000000ULL);

            if (DESC_ADDR_HIGH_VALID) {
                const uint64_t DESC_HIGH = *(uint64_t*)(DESC_ADDR_HIGH);
                const uint64_t BASE_HIGH = DESC_HIGH & 0xFFFFFFFF;
                const uint64_t FULL_BASE = BASE | (BASE_HIGH << 32);
                journal::panic("  System descriptor (likely TSS). Base: 0x%lx, Limit: 0x%lx", FULL_BASE, limit);
                journal::panic("  Access: 0x%x, Flags: 0x%x", ACCESS, FLAGS);
                journal::panic("  Raw high: 0x%lx", DESC_HIGH);
            } else {
                journal::panic("  System descriptor (TSS). Base (partial): 0x%lx, Limit: 0x%lx", BASE, limit);
                journal::panic("  Access: 0x%x, Flags: 0x%x (high descriptor invalid)", ACCESS, FLAGS);
            }
        } else {
            // code/data descriptor
            journal::panic("  Code/Data descriptor. Base: 0x%lx, Limit: 0x%lx", BASE, limit);
            journal::panic("  Access: 0x%x, Flags: 0x%x", ACCESS, FLAGS);
        }
    };

    print_selector("CS", sel_cs);
    print_selector("DS", sel_ds);
    print_selector("ES", sel_es);
    print_selector("FS", sel_fs);
    print_selector("GS", sel_gs);
    print_selector("SS", sel_ss);
    print_selector("TR", sel_tr);

    // print common MSRs
    journal::panic("Common MSRs:");
    struct MsrInfo {
        const char* name;
        uint32_t id;
    };
    const MsrInfo MSRS[] = {{.name = "IA32_EFER", .id = 0xC0000080},      {.name = "IA32_STAR", .id = 0xC0000081},
                            {.name = "IA32_LSTAR", .id = 0xC0000082},     {.name = "IA32_FMASK", .id = 0xC0000084},
                            {.name = "IA32_APIC_BASE", .id = 0x1B},       {.name = "IA32_PAT", .id = 0x277},
                            {.name = "IA32_MISC_ENABLE", .id = 0x1A0},    {.name = "IA32_FS_BASE", .id = IA32_FS_BASE},
                            {.name = "IA32_GS_BASE", .id = IA32_GS_BASE}, {.name = "IA32_KERNEL_GS_BASE", .id = 0xC0000102}};
    for (const auto& m : MSRS) {
        uint64_t val = rdmsr(m.id);
        journal::panic("%s: 0x%lx", m.name, val);
    }

    journal::panic("Halting");
    apic::oneShotTimer(0);
    hcf();
}

extern "C" void iterrupt_handler(cpu::GPRegs gpr, interruptFrame frame) {
    if (frame.errCode != UINT64_MAX) {
        exception_handler(gpr, frame);
        return;
    }

    // Check context-based handler first (for device drivers)
    if (irq_contexts[frame.intNum].handler != nullptr) {
        irq_contexts[frame.intNum].handler(static_cast<uint8_t>(frame.intNum), irq_contexts[frame.intNum].data);
        ker::mod::apic::eoi();
        return;
    }

    if (interrupt_handlers[frame.intNum] != nullptr) {
        interrupt_handlers[frame.intNum](gpr, frame);
    } else if (isIrq(frame.intNum)) {
        // Unexpected hardware IRQ with no handler - log and ignore.
        ker::mod::io::serial::write("UNHANDLED IRQ: vector=");
        ker::mod::io::serial::write_hex(static_cast<uint8_t>(frame.intNum));
        ker::mod::io::serial::write("\n");
    } else {
        // CPU exception with no registered handler - route to exception_handler
        // which kills the userspace process or panics for kernel-mode faults.
        exception_handler(gpr, frame);
        __builtin_unreachable();
    }
    ker::mod::apic::eoi();
}

// Vector 32 (0x20) is the timer interrupt, hardcoded in gates.asm (isr32 -> task_switch_handler).
// It must NEVER be assigned to any other handler.
static constexpr uint8_t TIMER_VECTOR = 32;

void setInterruptHandler(uint8_t int_num, interruptHandler_t handler) {
    if (int_num == TIMER_VECTOR) {
        ker::mod::io::serial::write("setInterruptHandler: vector 32 is reserved for timer\n");
        return;
    }
    if (interrupt_handlers[int_num] != nullptr) {
        ker::mod::io::serial::write("Handler already set\n");
        return;
    }
    interrupt_handlers[int_num] = handler;
}

void removeInterruptHandler(uint8_t int_num) { interrupt_handlers[int_num] = nullptr; }

auto isInterruptHandlerSet(uint8_t int_num) -> bool { return interrupt_handlers[int_num] != nullptr; }

auto requestIrq(uint8_t vector, irq_handler_fn handler, void* data, const char* name) -> int {
    if (vector == TIMER_VECTOR) {
        journal::error("requestIrq: vector 32 is reserved for timer");
        return -1;
    }
    if (interrupt_handlers[vector] != nullptr || irq_contexts[vector].handler != nullptr) {
        journal::error("requestIrq: vector %d already in use (handler=%p context_handler=%p)", vector, interrupt_handlers[vector],
                       irq_contexts[vector].handler);
        return -1;
    }
    journal::info("requestIrq: vector=%d name=%s", vector, name);
    irq_contexts[vector].handler = handler;
    irq_contexts[vector].data = data;
    irq_contexts[vector].name = name;
    return 0;
}

void freeIrq(uint8_t vector) {
    irq_contexts[vector].handler = nullptr;
    irq_contexts[vector].data = nullptr;
    irq_contexts[vector].name = nullptr;
}

auto allocateVector() -> uint8_t {
    // Find a free vector starting from 48 (above legacy IRQ range).
    // Vectors 0-31 are CPU exceptions.
    // Vector 32 (0x20) is the timer interrupt - hardcoded in gates.asm
    // (isr32 -> task_switch_handler). NEVER allocate it.
    // Vectors 33-47 are reserved for legacy ISA IRQs.
    // Vectors 48-255 are available for MSI/dynamic allocation.
    for (uint16_t v = next_alloc_vector; v < 256; v++) {
        if (interrupt_handlers[v] == nullptr && irq_contexts[v].handler == nullptr) {
            next_alloc_vector = static_cast<uint8_t>(v + 1);
            return static_cast<uint8_t>(v);
        }
    }
    // Wrap around and search from 48
    for (uint16_t v = 48; v < next_alloc_vector; v++) {
        if (interrupt_handlers[v] == nullptr && irq_contexts[v].handler == nullptr) {
            return static_cast<uint8_t>(v);
        }
    }
    return 0;  // No free vector found
}

}  // namespace ker::mod::gates
