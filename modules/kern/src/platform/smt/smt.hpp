#pragma once
#include <extern/limine.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/boot/handover.hpp>
#include <platform/mm/mm.hpp>
#include <type_traits>
#include <utility>

namespace ker::mod::sched::task {
struct Task;
}  // namespace ker::mod::sched::task

namespace ker::mod::smt {

using CpuGotoAddr = void (*)(struct limine_mp_info*);

struct CpuInfo {
    uint32_t processor_id{};
    uint32_t lapic_id{};
    CpuGotoAddr* goto_address = nullptr;
    uint64_t* stack_pointer_ref = nullptr;
    sched::task::Task* current_task = nullptr;
    // Used by panic/OOM to indicate this CPU has entered the halted state for diagnostics.
    std::atomic<bool> is_halted_for_oom{false};
};

auto get_core_count() -> uint64_t;
auto get_early_cpu_count() -> uint64_t;  // Safe to call before smt::init()
auto get_cpu(uint64_t number) -> CpuInfo&;
auto has_cpu_data() -> bool;

// Get logical CPU index from APIC ID (reliable, doesn't depend on GS)
auto get_cpu_index_from_apic_id(uint32_t apic_id) -> uint64_t;

// Get the kernel PerCpu structure for a given CPU index
// Used to restore GS_BASE when entering idle loop (no task context)
auto get_kernel_per_cpu(uint64_t cpu_index) -> cpu::PerCpu*;

void start_smt(boot::HandoverModules& modules, uint64_t kernel_rsp);
void park_secondary_cpus_for_selftest();

void init();

auto this_cpu_info() -> const CpuInfo&;

template <typename T>
class PerCpuVar {
    static_assert(std::is_default_constructible_v<T> || std::is_arithmetic_v<T>,
                  "T must have a default constructor or be a primitive type");

   private:
    T* data;

   public:
    PerCpuVar(T default_value = T()) : data(new T[get_core_count()]) {
        for (uint64_t i = 0; i < get_core_count(); ++i) {
            data[i] = default_value;
        }
    }

    T& get() { return data[cpu::current_cpu()]; }

    auto operator->() -> T* { return &data[cpu::current_cpu()]; }

    void set(const T& value) { data[cpu::current_cpu()] = value; }

    auto operator=(const T& value) -> PerCpuVar& {
        data[cpu::current_cpu()] = value;
        return *this;
    }
};

template <typename T>
class PerCpuCrossAccess {
   private:
    struct alignas(64) PerCpuLock {
        std::atomic<bool> locked{false};
    };

    static_assert(sizeof(PerCpuLock) == 64, "PerCpuCrossAccess locks must not share cache lines");
    static_assert(alignof(PerCpuLock) == 64, "PerCpuCrossAccess locks must stay cache-line aligned");

    T* data;

    // Per-element spinlocks for fine-grained locking
    PerCpuLock* locks;

    // IRQ-safe lock: saves RFLAGS and disables interrupts before acquiring.
    // This prevents the timer interrupt (which calls process_tasks ->
    // thisCpuLocked) from firing while the lock is held, avoiding self-deadlock
    // when non-interrupt code (e.g. reschedule_task_for_cpu, get_run_queue_stats)
    // holds the same per-CPU lock.
    auto lock_cpu(uint64_t cpu) -> uint64_t {
        // Written by inline asm output constraints.
        // NOLINTNEXTLINE(misc-const-correctness)
        uint64_t flags = 0;
        asm volatile("pushfq; pop %0; cli" : "=r"(flags)::"memory");
        while (locks[cpu].locked.exchange(true, std::memory_order_acquire)) {
            // Spin with pause hint
            asm volatile("pause");
        }
        return flags;
    }

    void unlock_cpu(uint64_t cpu, uint64_t flags) {
        locks[cpu].locked.store(false, std::memory_order_release);
        // Restore interrupt state (re-enables interrupts if they were enabled before lock)
        asm volatile("push %0; popfq" ::"r"(flags) : "memory", "cc");
    }

   public:
    PerCpuCrossAccess() : data(new T[get_core_count()]), locks(nullptr) {
        uint64_t const CORE_COUNT = get_core_count();
        auto* raw_locks = new uint8_t[(CORE_COUNT * sizeof(PerCpuLock)) + 63];
        auto const RAW_ADDR = reinterpret_cast<uintptr_t>(raw_locks);
        auto const ALIGNED_ADDR = (RAW_ADDR + 63) & ~static_cast<uintptr_t>(63);
        locks = reinterpret_cast<PerCpuLock*>(ALIGNED_ADDR);

        for (uint64_t i = 0; i < CORE_COUNT; ++i) {
            new (&data[i]) T();  // Default construct each element (supports non-copyable types like std::atomic)
            new (&locks[i]) PerCpuLock();
            locks[i].locked.store(false, std::memory_order_relaxed);
        }
    }

    // Access current CPU's data (no locking needed - only this CPU accesses it)
    // WARNING: Use thisCpuLocked() if other CPUs might be modifying via withLock!
    auto this_cpu() -> T* { return &data[cpu::current_cpu()]; }

    // Locked access to current CPU's data - use when other CPUs might modify via withLock
    template <typename Func>
    auto this_cpu_locked(Func&& func) -> decltype(std::forward<Func>(func)(std::declval<T*>())) {
        uint64_t const CPU = cpu::current_cpu();
        uint64_t const FLAGS = lock_cpu(CPU);
        auto result = std::forward<Func>(func)(&data[CPU]);
        unlock_cpu(CPU, FLAGS);
        return result;
    }

    // Locked access to current CPU's data without return value
    template <typename Func>
    void this_cpu_locked_void(Func&& func) {
        uint64_t const CPU = cpu::current_cpu();
        uint64_t const FLAGS = lock_cpu(CPU);
        std::forward<Func>(func)(&data[CPU]);
        unlock_cpu(CPU, FLAGS);
    }

    // Access another CPU's data with locking
    auto that_cpu(uint64_t cpu) -> T* { return &data[cpu]; }

    // Locked access to another CPU's data - use when modifying cross-CPU
    template <typename Func>
    auto with_lock(uint64_t cpu, Func&& func) -> decltype(std::forward<Func>(func)(std::declval<T*>())) {
        uint64_t const FLAGS = lock_cpu(cpu);
        auto result = std::forward<Func>(func)(&data[cpu]);
        unlock_cpu(cpu, FLAGS);
        return result;
    }

    // Locked access without return value
    template <typename Func>
    void with_lock_void(uint64_t cpu, Func&& func) {
        uint64_t const FLAGS = lock_cpu(cpu);
        std::forward<Func>(func)(&data[cpu]);
        unlock_cpu(cpu, FLAGS);
    }

    // Non-blocking try-lock: returns false immediately if lock is held.
    // Safe to call from interrupt context and idle paths (no spinning).
    template <typename Func>
    auto try_with_lock(uint64_t cpu, Func&& func) -> bool {
        // NOLINTNEXTLINE(misc-const-correctness)
        uint64_t flags = 0;
        asm volatile("pushfq; pop %0; cli" : "=r"(flags)::"memory");
        bool const ACQUIRED = !locks[cpu].locked.exchange(true, std::memory_order_acquire);
        if (!ACQUIRED) {
            asm volatile("push %0; popfq" ::"r"(flags) : "memory", "cc");
            return false;
        }
        std::forward<Func>(func)(&data[cpu]);
        unlock_cpu(cpu, flags);
        return true;
    }

    void set_this_cpu(const T& value) { data[cpu::current_cpu()] = value; }

    void set_that_cpu(const T& value, uint64_t cpu) {
        uint64_t flags = lock_cpu(cpu);
        data[cpu] = value;
        unlock_cpu(cpu, flags);
    }
};

// ============================================================================
// CPU Domain Hierarchy
// Three levels: ROOT(0) -> GROUP(1) -> LEAF(2)
// Flat topology: ROOT -> 1 GROUP containing all CPUs -> optional LEAF domains
// ============================================================================

static constexpr uint32_t MAX_CPU_DOMAINS = 64;
static constexpr uint32_t DOMAIN_ID_INVALID = 0xFFFFFFFF;
using CpuDomainName = std::array<char, 32>;

enum class CpuDomainLevel : uint8_t {
    ROOT = 0,   // All online CPUs
    GROUP = 1,  // Socket / NUMA node grouping
    LEAF = 2,   // Named daemon CPU set (e.g. "net_domain")
};

struct CpuDomain {
    uint32_t id;  // Unique ID (0 = root)
    CpuDomainLevel level;
    uint64_t cpu_mask;    // Bitmask of CPUs in this domain
    uint32_t parent_id;   // Parent domain ID (DOMAIN_ID_INVALID for root)
    bool soft_exclusive;  // When true, compute tasks penalised for entering
    bool hard;            // When true, tasks never leave this domain
    CpuDomainName name;   // Human-readable NUL-terminated name
};
static_assert(sizeof(CpuDomainName) == 32);
static_assert(offsetof(CpuDomain, name) == 22);
static_assert(sizeof(CpuDomain) == 56);

// Domain registry API
void init_cpu_domains();  // Called from smt::init()
auto get_cpu_domain_count() -> uint32_t;
auto get_cpu_domain(uint32_t id) -> CpuDomain*;                                                            // nullptr if not found
auto create_leaf_domain(const char* name, uint64_t cpu_mask, bool soft_exclusive, bool hard) -> uint32_t;  // Returns domain id
auto find_group_for_cpu(uint64_t cpu_no) -> uint32_t;                                                      // Returns GROUP domain id

// Reverse lookup: logical CPU index -> APIC ID
auto get_apic_id_for_cpu(uint64_t cpu_no) -> uint32_t;

auto get_cpu_node(uint64_t cpu_no) -> uint64_t;

inline void start_cpu_task(uint64_t cpu_no, CpuGotoAddr task, mm::Stack<4096> stack) {
    auto cpu = get_cpu_node(cpu_no);
    auto& cpu_data = get_cpu(cpu);

    // push function arguments to the stack of the target CPU
    *stack.sp = reinterpret_cast<uint64_t>(task);
    stack.sp++;

    __atomic_store_n(&cpu_data.stack_pointer_ref, stack.sp, __ATOMIC_SEQ_CST);
    __atomic_store_n(cpu_data.goto_address, task, __ATOMIC_SEQ_CST);
}

// Overload accepting a CpuGotoAddr-compatible function (takes limine_mp_info*).
constexpr void exec_on_all_cpus(CpuGotoAddr func) {
    auto* init_stacks = new mm::Stack<4096>[get_core_count()];
    for (uint64_t i = 0; i < get_core_count(); i++) {
        if (i == cpu::current_cpu()) {
            continue;
        }
        start_cpu_task(i, func, init_stacks[i]);
    }
}

template <typename... FuncArgs>
constexpr void exec_on_all_cpus(void (*func)(FuncArgs...), FuncArgs... data) {
    auto* init_stacks = new mm::Stack<4096>[get_core_count()];
    for (uint64_t i = 0; i < get_core_count(); i++) {
        if (i == cpu::current_cpu()) {
            continue;
        }
        start_cpu_task(i, reinterpret_cast<CpuGotoAddr>(func), init_stacks[i], data...);
    }
}

auto cpu_count() -> uint64_t;

auto set_tcb(void* tcb) -> uint64_t;

// Permanently stop all other CPUs. This is a crash/OOM primitive: it sends NMI,
// fixed IPI, then INIT as a one-way fallback and does not expect CPUs to resume.
void halt_other_cores();

// C-linkage wrapper used by external C code to request halting other CPUs.
extern "C" void ker_smt_halt_other_cpus(void);

}  // namespace ker::mod::smt
